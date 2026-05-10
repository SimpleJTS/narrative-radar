#!/usr/bin/env python3
"""
叙事雷达 → 链上雷达 v1
纯Python，零AI成本（关键词匹配 + 叙事去重）

三条推送通道：
1. 全新叙事 — 从未见过的概念/故事，全链推
2. 马斯克/川普相关 — 重点ETH+SOL，BSC也推
3. 币安/CZ相关 — 只推BSC

数据源：GMGN新币 + DEXScreener搜索
叙事历史：SQLite去重
"""

import requests
import json
import time
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from difflib import SequenceMatcher

# === 配置 ===
DATA_DIR = os.path.expanduser("~/crypto-trading")
DB_FILE = os.path.join(DATA_DIR, "narrative_history.db")
LOG_FILE = os.path.join(DATA_DIR, "narrative_radar.log")
SEEN_FILE = os.path.join(DATA_DIR, "narrative_seen.json")
FLAP_SEEN_FILE = os.path.join(DATA_DIR, "flap_seen.json")

# 扫描间隔
SCAN_INTERVAL = 10  # 10秒（快速验证模式）

# 推送链过滤（测试用）
ENABLED_CHAINS = ['sol', 'eth']  # 推SOL+ETH，测试完成后把 'bsc','base' 加回来

# 动量追踪器 — 内存中记录每个币的价格/市值快照
# {address: [{'ts': timestamp, 'mc': market_cap, 'vol': volume, 'price': price}, ...]}
MOMENTUM_TRACKER = {}
MOMENTUM_PUSHED = {}  # {address: {'count': N, 'last_ts': ts, 'last_mc': mc}} 推送计数
MOMENTUM_CONSECUTIVE_UP = 2  # 连续涨2轮即可触发
MIN_KOL_COUNT = 2  # KOL买入门槛：≥2个KOL才推送

# ============================================================
# 信号跟踪器 — 记录每个推送币的初始状态，持续跟踪最大涨幅
# ============================================================
# {address: {
#   'name': str, 'symbol': str, 'chain': str,
#   'push_ts': float,          # 首次推送时间
#   'init_mc': float,         # 推送时市值
#   'init_price': float,      # 推送时价格
#   'init_holders': int,      # 推送时持有人
#   'peak_mc': float,         # 历史最高市值
#   'peak_price': float,      # 历史最高价
#   'peak_ts': float,         # 峰值时间
#   'last_check_ts': float,   # 上次检查时间
#   'report_count': int,      # 已推送30min汇报次数
#   'last_mid_push_ts': float,# 上次30min推送时间（用于防重复）
#   'last_mid_pct': float,    # 上次推送时的涨幅%
# }}
PUSH_TRACKER = {}
LAST_SUMMARY_TS = 0  # 上次汇总推送时间
SUMMARY_INTERVAL = 7200  # 2小时汇总一次（秒）
MID_CHECK_INTERVAL = 1800  # 30分钟中间检查（秒）
MID_PUSH_THRESHOLD = 0.50  # 30min汇报阈值：涨50%触发

# 从.env读取TG配置
def load_env():
    env = {}
    env_file = os.path.expanduser("~/.env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    env[k] = v
    return env

ENV = load_env()
TG_TOKEN = ENV.get('TG_BOT_TOKEN', '')
TG_CHAT_ID = int(ENV.get('TG_CHAT_ID', '0'))

GMGN_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://gmgn.ai/',
    'apikey': 'gmgn_9837755ef6632402b4947b5e21ad50eb',
}

XXYY_API_KEY = os.environ.get('XXYY_API_KEY', 'xxyy_ak_4d3b66c1a6ff43a18f1d3d')
XXYY_BASE = 'https://www.xxyy.io'

def xxyy_get(endpoint, params=None):
    """调用XXYY API，GET请求"""
    if not XXYY_API_KEY:
        return {}
    try:
        resp = requests.get(f'{XXYY_BASE}{endpoint}', headers={
            'Authorization': f'Bearer {XXYY_API_KEY}',
            'Accept': 'application/json',
        }, params=params, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            if d.get('code') == 200:
                return d.get('data', {}) or {}
    except:
        pass
    return {}

def xxyy_post(endpoint, params=None, json_body=None):
    """调用XXYY API，POST请求"""
    if not XXYY_API_KEY:
        return {}
    try:
        resp = requests.post(f'{XXYY_BASE}{endpoint}', headers={
            'Authorization': f'Bearer {XXYY_API_KEY}',
            'Content-Type': 'application/json',
        }, params=params, json=json_body, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            if d.get('code') == 200:
                return d.get('data', {}) or {}
    except:
        pass
    return {}

def xxyy_query_token(chain, ca):
    """查单个代币详情（MC/holders/KOL等）"""
    return xxyy_get('/api/trade/open/api/query', {'ca': ca, 'chain': chain})

def xxyy_get_kol_list(chain='sol'):
    """获取XXYY KOL买入列表（字典格式：address -> kol_count）"""
    data = xxyy_get('/api/trade/open/api/kol-buy-list', {'chain': chain})
    result = {}
    if isinstance(data, list):
        for item in data:
            mint = item.get('tokenMeta', {}).get('mint', '')
            cnt = item.get('walletBuyCnt', 0)
            if mint and cnt > 0:
                result[mint.lower()] = cnt
    return result

def xxyy_get_tag_holder_list(chain='sol'):
    """获取XXYY Tag Holder买入列表（字典格式：address -> insider_count）"""
    data = xxyy_get('/api/trade/open/api/tag-holder-buy-list', {'chain': chain})
    result = {}
    if isinstance(data, list):
        for item in data:
            mint = item.get('tokenMeta', {}).get('mint', '')
            cnt = item.get('walletBuyCnt', 0)
            if mint and cnt > 0:
                result[mint.lower()] = cnt
    return result

# ============================================================
# 马斯克/川普关键词库（大小写不敏感）
# ============================================================
MUSK_TRUMP_KEYWORDS = {
    # 马斯克核心
    'musk', 'elon', 'elonmusk',
    # SpaceX/Tesla/X
    'spacex', 'starship', 'tesla', 'cybertruck', 'roadster',
    'neuralink', 'boring', 'hyperloop', 'xai', 'grok',
    # 马斯克相关人物/宠物/梗
    'floki', 'shiba',  # 只在新币上下文中用
    'doge father', 'dogefather', 'technoking',
    'mars colony', 'mars',
    # 川普核心
    'trump', 'donald', 'maga', 'potus', 'trump47',
    'melania', 'barron', 'ivanka',
    # 川普相关
    'dark maga', 'darkmaga', 'ultra maga', 'save america',
    'truth social', 'covfefe',
    # 马斯克+川普联动
    'doge department', 'd.o.g.e', 'government efficiency',
}

# 马斯克/川普正则（捕捉变体）
MUSK_TRUMP_PATTERNS = [
    r'\belon\b', r'\bmusk\b', r'\btrump\b', r'\bmaga\b',
    r'\bspacex\b', r'\bstarship\b', r'\btesla\b', r'\bgrok\b',
    r'\bmelania\b', r'\bbarron\b', r'\bdoge\s*department\b',
    r'\bd\.?o\.?g\.?e\b',  # D.O.G.E变体
    r'\bx\s*ai\b', r'\bneuralink\b',
]

# 叙事关键词翻译（中英对照）
NARRATIVE_TRANSLATIONS = {
    # 马斯克相关
    'musk': '马斯克', 'elon': '马斯克', 'elonmusk': '马斯克',
    'spacex': 'SpaceX', 'starship': '星舰', 'tesla': '特斯拉',
    'cybertruck': '特斯拉卡车', 'roadster': '特斯拉跑车',
    'neuralink': 'Neuralink', 'boring': 'Boring公司', 'hyperloop': '超级高铁',
    'xai': 'xAI', 'grok': 'Grok',
    'floki': 'Floki', 'shiba': '柴犬', 'doge': 'Doge', 'doge father': 'Doge之父', 'dogefather': 'Doge之父',
    'technoking': '技术王', 'mars colony': '火星殖民地', 'mars': '火星',
    # 川普相关
    'trump': '川普', 'donald': '特朗普', 'maga': 'MAGA', 'potus': '美国总统',
    'trump47': '川普47', 'melania': '梅拉尼娅', 'barron': '巴伦', 'ivanka': '伊万卡',
    'dark maga': '暗黑MAGA', 'darkmaga': '暗黑MAGA', 'ultra maga': '终极MAGA',
    'save america': '拯救美国', 'truth social': 'Truth社媒', 'covfefe': 'Covfefe',
    'doge department': 'DOGE部门', 'd.o.g.e': 'D.O.G.E', 'government efficiency': '政府效率',
    # 币安/CZ相关
    'cz': 'CZ', 'changpeng': '赵长鹏', 'zhao': '赵长鹏', 'czb': 'CZ',
    'binance': '币安', 'bnb': 'BNB', 'pancake': 'Pancake',
    'pancakeswap': 'PancakeSwap', 'heyi': '何一', 'yi he': '何一',
    'fourmeme': 'Four.meme', 'four meme': 'Four.meme', '4meme': '4meme',
    # 名人/热点
    'vitalik': 'V神', 'buterin': 'V神', 'satoshi': '中本聪',
    'justin sun': '孙宇晨', 'sun yuchen': '孙宇晨', 'tron': '波场',
    'saylor': 'MicroStrategy创始人', 'blackrock': '贝莱德',
    'coinbase': 'Coinbase', 'etf': 'ETF', 'halving': '减半',
    'lobster': '龙虾', 'mrbeast': 'MrBeast',
    # 通用
    'novel': '新叙事', 'heating': '热点',
}

def translate_description(text):
    """对描述文本做关键词翻译（只替换英文关键词为中文，保留其他所有内容）"""
    if not text:
        return text
    # 按单词边界替换
    result = text
    # 按长度降序排列key，避免短词优先匹配破坏长词
    for kw in sorted(NARRATIVE_TRANSLATIONS.keys(), key=len, reverse=True):
        cn = NARRATIVE_TRANSLATIONS[kw]
        # 大小写敏感替换
        pattern = re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
        result = pattern.sub(f'{cn}({kw})', result)
    return result


def translate_narrative_tag(tag):
    """
    把英文叙事标签翻译成中文
    规则：关键词翻译成中文，英文原文保留在括号里
    例如：musk → 马斯克(musk)
          马斯克/川普概念 (musk, doge) → 马斯克/川普概念 (马斯克(musk), Doge(doge))
    """
    if not tag:
        return tag

    # 翻译单个关键词，保留原文在括号
    def _translate_word(word):
        w = word.lower().strip('.,!?(){}[]')
        if w in NARRATIVE_TRANSLATIONS:
            cn = NARRATIVE_TRANSLATIONS[w]
            if cn != word:
                return f"{cn}({word})"
        return word

    # 分割：保留分隔符
    parts = re.split(r'([,\s\(\)]+)', tag)
    translated = []
    for part in parts:
        if re.match(r'^[\s,\(\)]+$', part):
            translated.append(part)
        else:
            translated.append(_translate_word(part))
    return ''.join(translated)

# ============================================================
# 币安/CZ关键词库
# ============================================================
BINANCE_CZ_KEYWORDS = {
    # CZ核心
    'cz', 'changpeng', 'zhao', 'czb', 'czbinance',
    # 何一（BSC现在的核心推手！）
    'heyi', 'yi he', 'he yi', '何一', 'yihe',
    'sister yi', 'yi jie', '一姐', '何一姐',
    # 币安品牌
    'binance', 'bnb', 'pancake', 'pancakeswap',
    # CZ相关动态词（书、活动、推特高频词）
    'giggle academy', 'binance life', 'bnb chain',
    'principles', 'cz book',
    # YZi Labs (原Binance Labs)
    'yzi', 'yzi labs',
    # 中文关键词（BSC上常见）
    '赵长鹏', '币安', '长鹏', 'cz的', '何一的',
    # Four.meme平台相关
    'fourmeme', 'four meme', '4meme',
    # CZ/何一推特互动高频词
    'czs dog', 'cz dog', 'bnb dog',
    'build on bnb', 'bnb ecosystem',
}

BINANCE_CZ_PATTERNS = [
    r'\bcz\b', r'\bbinance\b', r'\bbnb\b',
    r'\bheyi\b', r'\byi\s*he\b', r'\bhe\s*yi\b',
    r'\b何一\b', r'\b一姐\b',
    r'\bpancake\b', r'\bgiggle\b', r'\byzi\b',
    r'\bfourmeme\b', r'\b4meme\b',
]

# ============================================================
# 推特热点/名人关键词库（★★级别）
# ============================================================
CELEBRITY_VIRAL_KEYWORDS = {
    # 科技名人
    'vitalik', 'buterin', 'sam altman', 'satoshi',
    'michael saylor', 'saylor', 'cathie wood',
    'jack dorsey', 'zuckerberg', 'bezos',
    'jensen huang', 'nvidia', 'tim cook',
    # 币圈名人
    'justin sun', 'sun yuchen', '孙宇晨', 'tron',
    'arthur hayes', 'su zhu', '3ac',
    'brian armstrong', 'coinbase',
    'larry fink', 'blackrock',
    'gary gensler', 'sec',
    'michael novogratz', 'galaxy',
    # 政治/社会名人
    'biden', 'obama', 'putin', 'xi jinping',
    'kanye', 'drake', 'snoop dogg', 'paris hilton',
    'mark cuban', 'mr beast', 'mrbeast',
    # 病毒式传播热词（龙虾级别的梗）
    'lobster', '龙虾', 'lobsta',
    'hawk tuah', 'griddy', 'skibidi',
    'rizz', 'sigma', 'gyatt',
    # 重大事件关键词
    'etf', 'halving', '减半',
    'world war', 'wwiii',
    'fed', 'rate cut', '降息',
    'tiktok ban', 'tiktok',
}

CELEBRITY_VIRAL_PATTERNS = [
    r'\bvitalik\b', r'\bsaylor\b', r'\bblackrock\b',
    r'\bcoinbase\b', r'\bjustin\s*sun\b', r'\blobster\b',
    r'\betf\b', r'\bhalving\b', r'\bmrbeast\b',
    r'\bsnoop\b', r'\bkanye\b', r'\bdrake\b',
]

# ============================================================
# 机器人刷量检测
# ============================================================
def detect_bot_pump(token):
    """
    检测机器人刷量盘子
    核心特征：
    1. 买卖比极端（buy/sell > 20）
    2. holders几乎不涨但交易笔数很多（人均交易笔数异常高）
    满足1+2即标记，3作为辅助参考
    返回: (is_bot: bool, reason: str)
    """
    buys = token.get('buys_1h', 0) or token.get('buys', 0) or 0
    sells = token.get('sells_1h', 0) or token.get('sells', 0) or 0
    holders = token.get('holders', 0) or 0
    volume = token.get('volume', 0) or 0
    total_txs = buys + sells

    # 条件1：买卖比极端
    buy_sell_ratio = buys / max(sells, 1)
    is_extreme_buy_ratio = buy_sell_ratio > 20

    # 条件2：人均交易笔数异常高（刷单机器人特征）
    estimated_increment = max(holders * 0.1, 2)
    tx_per_holder = total_txs / max(estimated_increment, 1)
    is_bot_tx_pattern = is_extreme_buy_ratio and tx_per_holder > 15

    # 条件3：单笔平均金额极低（辅助）
    avg_tx_usd = volume / max(total_txs, 1)
    is_micro_tx = avg_tx_usd < 3 and volume > 0

    if is_bot_tx_pattern:
        reason = f"🤖机器人: 买卖{buy_sell_ratio:.0f}:1 + 均{tx_per_holder:.0f}笔/人"
        return True, reason
    if is_extreme_buy_ratio and is_micro_tx and total_txs > 50:
        reason = f"🤖可疑: 买卖{buy_sell_ratio:.0f}:1 + 均额${avg_tx_usd:.1f}"
        return True, reason

    return False, ""


# ============================================================
# 通用垃圾词（过滤明显的骗局/低质量币）
# ============================================================
SPAM_PATTERNS = [
    r'airdrop', r'presale', r'pre\s*sale',
    r'1000x', r'100x guaranteed',
    r'safe\s*moon', r'baby\s*\w+',  # babydoge等仿盘
    r'pornhub', r'porn', r'xxx', r'nsfw',
    r'nigga', r'nigger', r'faggot',
    r'scam', r'rugpull', r'rug\s*pull',
    r'official\s*token', r'official\s*coin',
]

# 常见无叙事意义的单词（过滤单词名币）
COMMON_NOISE_WORDS = {
    'nice', 'good', 'bad', 'cool', 'hot', 'big', 'small',
    'life', 'love', 'hate', 'happy', 'sad', 'fun', 'lol',
    'cat', 'dog', 'moon', 'sun', 'star', 'king', 'queen',
    'gold', 'rich', 'cash', 'money', 'pay', 'buy', 'sell',
    'pump', 'dump', 'bull', 'bear', 'green', 'red',
    'hello', 'world', 'yes', 'no', 'wow', 'omg', 'lmao',
    'simp', 'chad', 'based', 'cope', 'seethe',
    'test', 'new', 'old', 'real', 'fake',
    # 垃圾币名常见词
    'shit', 'shitcoin', 'fuck', 'fart', 'poop', 'pee',
    'cum', 'dick', 'ass', 'boob', 'tit',
    'nigga', 'retard', 'slop',
    # 超通用币名
    'the', 'and', 'for', 'from', 'with', 'this', 'that',
    'coin', 'token', 'meme', 'pepe', 'wojak',
    'peg', 'usd', 'usdt', 'usdc', 'dai',
}

# ============================================================
# 工具函数
# ============================================================
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def load_flap_seen():
    if os.path.exists(FLAP_SEEN_FILE):
        try:
            with open(FLAP_SEEN_FILE) as f:
                return json.load(f)
        except:
            pass
    return {}

def save_flap_seen(data):
    # 只保留7天内的
    cutoff = int(time.time()) - 86400 * 7
    data = {k: v for k, v in data.items() if v > cutoff}
    with open(FLAP_SEEN_FILE, 'w') as f:
        json.dump(data, f)

def tg_send(text, parse_mode='Markdown', reply_to_message_id=None):
    if not TG_TOKEN:
        log(f"[TG] No token, skip: {text[:80]}")
        return False
    try:
        payload = {'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': parse_mode, 'disable_web_page_preview': True}
        if reply_to_message_id:
            payload['reply_to_message_id'] = reply_to_message_id
        resp = requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            json=payload,
            timeout=10
        )
        result = resp.json()
        if not result.get('ok'):
            # Markdown失败时降级到纯文本
            if 'can\'t parse' in str(result.get('description', '')).lower():
                payload_clean = {'chat_id': TG_CHAT_ID, 'text': text, 'disable_web_page_preview': True}
                if reply_to_message_id:
                    payload_clean['reply_to_message_id'] = reply_to_message_id
                resp = requests.post(
                    f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                    json=payload_clean,
                    timeout=10
                )
                result = resp.json()
            if not result.get('ok'):
                log(f"[TG] Error: {result.get('description', '')}")
                return False
        # 成功发送时返回 message_id（int），方便调用方捕获
        return result.get('result', {}).get('message_id', True)
    except Exception as e:
        log(f"[TG] Send error: {e}")
        return False

# ============================================================
# 叙事历史数据库
# ============================================================
def init_db():
    """初始化SQLite叙事历史库"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # 所有见过的叙事主题
    c.execute('''CREATE TABLE IF NOT EXISTS narratives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        theme TEXT NOT NULL,           -- 归一化的叙事主题（小写）
        first_token_name TEXT,         -- 第一次出现时的代币名
        first_token_address TEXT,      -- 第一次出现时的地址
        first_chain TEXT,              -- 第一次出现的链
        first_seen_at INTEGER,         -- 第一次看到的时间戳
        token_count INTEGER DEFAULT 1, -- 出现过多少次
        last_seen_at INTEGER           -- 最近一次看到
    )''')
    
    # 所有扫描过的代币
    c.execute('''CREATE TABLE IF NOT EXISTS tokens_seen (
        address TEXT PRIMARY KEY,
        chain TEXT,
        name TEXT,
        symbol TEXT,
        narrative_theme TEXT,
        category TEXT,                 -- 'musk_trump' / 'binance_cz' / 'novel' / 'common'
        first_seen_at INTEGER,
        market_cap REAL,
        pushed INTEGER DEFAULT 0,      -- 是否已推送
        seen_count INTEGER DEFAULT 1   -- 出现次数
    )''')
    
    # 索引
    c.execute('CREATE INDEX IF NOT EXISTS idx_theme ON narratives(theme)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_addr ON tokens_seen(address)')
    
    conn.commit()
    return conn

def normalize_theme(name, symbol):
    """
    从代币名称+符号提取归一化的叙事主题
    例如：'Elon Mars Colony' → 'elon mars colony'
          'TRUMP2028' → 'trump'
          'PancakeBunny' → 'pancake bunny'
    """
    # 合并name和symbol
    text = f"{name} {symbol}".lower().strip()
    
    # 去除常见后缀/前缀
    noise = ['token', 'coin', 'inu', 'swap', 'finance', 'protocol',
             'dao', 'defi', 'nft', 'meta', 'verse', 'fi', 'ai',
             'pepe', 'wojak', 'chad', 'based']
    
    # 分割camelCase
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    # 去除数字（如2028、1000x）
    text = re.sub(r'\d+x?', '', text)
    # 只保留字母和空格
    text = re.sub(r'[^a-z\s]', ' ', text)
    # 去噪
    words = [w for w in text.split() if w and len(w) > 1 and w not in noise]
    
    if not words:
        return name.lower().strip()
    
    return ' '.join(sorted(set(words)))

def is_similar_theme(theme1, theme2, threshold=0.7):
    """模糊匹配两个叙事主题"""
    if theme1 == theme2:
        return True
    # 子串匹配
    if theme1 in theme2 or theme2 in theme1:
        return True
    # 词重叠
    words1 = set(theme1.split())
    words2 = set(theme2.split())
    if words1 and words2:
        overlap = len(words1 & words2) / min(len(words1), len(words2))
        if overlap >= 0.6:
            return True
    # 序列匹配
    return SequenceMatcher(None, theme1, theme2).ratio() >= threshold

def check_narrative_novelty(conn, theme, name, symbol, address, chain):
    """
    检查叙事状态
    返回：
      ('novel', None)                    — 第一次见到
      ('heating', narrative_row)         — 短时间内持续出现新币！热点信号！
      ('existing', existing_theme_row)   — 已有叙事，不热
    
    核心逻辑：同一主题在30分钟内出现2+个不同的币 = 热点
    """
    c = conn.cursor()
    now = int(time.time())
    HEAT_WINDOW = 1800  # 30分钟窗口
    HEAT_THRESHOLD = 2  # 窗口内出现2个以上同主题币就是热点
    
    # 精确匹配
    c.execute('SELECT id, theme, first_token_name, first_token_address, first_chain, first_seen_at, token_count, last_seen_at FROM narratives WHERE theme = ?', (theme,))
    exact = c.fetchone()
    if exact:
        row_id, _, _, _, _, first_seen, count, last_seen = exact
        # 更新计数
        new_count = count + 1
        c.execute('UPDATE narratives SET token_count = ?, last_seen_at = ? WHERE theme = ?',
                  (new_count, now, theme))
        conn.commit()
        
        # 热点判断：在HEAT_WINDOW内出现了多个币
        if now - first_seen < HEAT_WINDOW and new_count >= HEAT_THRESHOLD:
            return ('heating', exact)
        # 或者：最近一次和这次间隔很短（说明持续在冒）
        if now - last_seen < HEAT_WINDOW and new_count >= HEAT_THRESHOLD:
            return ('heating', exact)
        
        return ('existing', exact)
    
    # 模糊匹配 — 取最近1000个主题比对
    c.execute('SELECT id, theme, first_token_name, first_token_address, first_chain, first_seen_at, token_count, last_seen_at FROM narratives ORDER BY last_seen_at DESC LIMIT 1000')
    for row in c.fetchall():
        if is_similar_theme(theme, row[1]):
            row_id, _, _, _, _, first_seen, count, last_seen = row
            new_count = count + 1
            c.execute('UPDATE narratives SET token_count = ?, last_seen_at = ? WHERE id = ?',
                      (new_count, now, row[0]))
            conn.commit()
            
            # 热点判断
            if now - last_seen < HEAT_WINDOW and new_count >= HEAT_THRESHOLD:
                return ('heating', row)
            
            return ('existing', row)
    
    # 第一次见到 — 记录
    c.execute('''INSERT INTO narratives (theme, first_token_name, first_token_address, first_chain, first_seen_at, last_seen_at)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (theme, name, address, chain, now, now))
    conn.commit()
    return ('novel', None)

def get_token_seen_count(conn, address):
    """获取代币出现次数"""
    c = conn.cursor()
    c.execute('SELECT seen_count FROM tokens_seen WHERE address = ?', (address,))
    row = c.fetchone()
    return row[0] if row else 0

def is_token_seen(conn, address):
    """检查代币是否已经扫描过"""
    c = conn.cursor()
    c.execute('SELECT address FROM tokens_seen WHERE address = ?', (address,))
    return c.fetchone() is not None

def record_token(conn, address, chain, name, symbol, theme, category, mc, pushed=False):
    """记录已扫描的代币 — 重复出现时计数+1"""
    c = conn.cursor()
    # 检查是否已存在
    c.execute('SELECT seen_count FROM tokens_seen WHERE address = ?', (address,))
    existing = c.fetchone()
    if existing:
        # 已存在：计数+1，更新市值
        new_count = existing[0] + 1
        c.execute('''UPDATE tokens_seen SET seen_count = ?, market_cap = ?, category = ?
                     WHERE address = ?''', (new_count, mc, category, address))
    else:
        # 新记录
        c.execute('''INSERT INTO tokens_seen 
                     (address, chain, name, symbol, narrative_theme, category, first_seen_at, market_cap, pushed, seen_count)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)''',
                  (address, chain, name, symbol, theme, category, int(time.time()), mc, 1 if pushed else 0))
    conn.commit()

# ============================================================
# 叙事分类引擎
# ============================================================
def classify_narrative(name, symbol, chain):
    """
    分类代币叙事
    返回：('musk_trump', matched_keywords) / ('binance_cz', matched_keywords) / ('novel', None) / ('common', None)
    """
    text = f"{name} {symbol}".lower()
    
    # 1. 检查是否是垃圾币
    for pat in SPAM_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return ('spam', None)
    
    # 2. 马斯克/川普检测
    matched_mt = []
    for kw in MUSK_TRUMP_KEYWORDS:
        if kw.lower() in text:
            matched_mt.append(kw)
    if not matched_mt:
        for pat in MUSK_TRUMP_PATTERNS:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                matched_mt.append(m.group())
    
    if matched_mt:
        # 马斯克/川普：重点ETH+SOL，BSC也可以
        chain_lower = chain.lower()
        if chain_lower in ('eth', 'ethereum', 'sol', 'solana', 'bsc', 'base'):
            return ('musk_trump', matched_mt)
    
    # 3. 币安/CZ检测 — 只在BSC上推
    matched_bc = []
    for kw in BINANCE_CZ_KEYWORDS:
        if kw.lower() in text:
            matched_bc.append(kw)
    if not matched_bc:
        for pat in BINANCE_CZ_PATTERNS:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                matched_bc.append(m.group())
    
    if matched_bc:
        chain_lower = chain.lower()
        if chain_lower in ('bsc',):
            return ('binance_cz', matched_bc)
        else:
            return ('binance_cz_wrong_chain', matched_bc)
    
    # 4. 名人/推特热点检测（★★级别）
    matched_cv = []
    for kw in CELEBRITY_VIRAL_KEYWORDS:
        if kw.lower() in text:
            matched_cv.append(kw)
    if not matched_cv:
        for pat in CELEBRITY_VIRAL_PATTERNS:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                matched_cv.append(m.group())
    
    if matched_cv:
        return ('celebrity_viral', matched_cv)
    
    # 5. 都不匹配 → 需要进一步检查是否全新叙事
    return ('check_novelty', None)

# ============================================================
# 安全检查（复用现有逻辑）
# ============================================================
def check_token_safety(chain, address):
    """快速安全检查 — 只拦硬伤（蜜罐/可增发），卖税不作为否决条件"""
    if chain in ('sol', 'solana'):
        try:
            r = requests.get(f'https://api.rugcheck.xyz/v1/tokens/{address}/report', timeout=10)
            if r.status_code == 200:
                data = r.json()
                score = data.get('score', 999)
                mint = data.get('mintAuthority')
                freeze = data.get('freezeAuthority')
                return {
                    'safe': not mint and not freeze,
                    'score': score, 'mint': mint is not None,
                    'freeze': freeze is not None
                }
        except:
            pass
    else:
        chain_map = {'ethereum': '1', 'eth': '1', 'bsc': '56', 'base': '8453'}
        cid = chain_map.get(chain, '1')
        try:
            r = requests.get(f'https://api.gopluslabs.io/api/v1/token_security/{cid}?contract_addresses={address}', timeout=10)
            if r.status_code == 200:
                result = r.json().get('result', {})
                data = result.get(address.lower(), {})
                if data:
                    honeypot = data.get('is_honeypot', '0') == '1'
                    mintable = data.get('is_mintable', '0') == '1'
                    sell_tax = float(data.get('sell_tax', '0') or '0')
                    buy_tax = float(data.get('buy_tax', '0') or '0')
                    return {
                        'safe': not honeypot and not mintable,  # 卖税不作为否决
                        'honeypot': honeypot, 'mintable': mintable,
                        'sell_tax': sell_tax, 'buy_tax': buy_tax
                    }
        except:
            pass
    return {'safe': False, 'reason': '无法检查'}  # 无法检查时不推，宁可错过不踩坑

# ============================================================
# GMGN数据获取
# ============================================================
def gmgn_get(url):
    try:
        resp = requests.get(url, headers=GMGN_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json().get('data', {})
    except:
        pass
    return {}

def fetch_token_description(chain, address):
    """获取代币描述/故事 — 叙事雷达核心信息"""
    desc = ''
    
    # SOL链：Pump.fun有最完整的description
    if chain in ('sol', 'solana'):
        try:
            r = requests.get(f'https://frontend-api-v3.pump.fun/coins/{address}', timeout=8)
            if r.status_code == 200:
                data = r.json()
                desc = data.get('description', '') or ''
                twitter = data.get('twitter', '') or ''
                telegram = data.get('telegram', '') or ''
                website = data.get('website', '') or ''
                return {
                    'description': desc.strip(),
                    'twitter': twitter,
                    'telegram': telegram,
                    'website': website,
                }
        except:
            pass
    
    # 所有链：DEXScreener info字段（网站+社交链接）
    try:
        chain_dex = {'sol': 'solana', 'eth': 'ethereum', 'bsc': 'bsc', 'base': 'base',
                     'solana': 'solana', 'ethereum': 'ethereum'}.get(chain, chain)
        r = requests.get(f'https://api.dexscreener.com/latest/dex/tokens/{address}', timeout=8)
        if r.status_code == 200:
            pairs = r.json().get('pairs', [])
            if pairs:
                info = pairs[0].get('info', {})
                websites = info.get('websites', [])
                socials = info.get('socials', [])
                twitter = ''
                telegram = ''
                website = ''
                for s in socials:
                    if s.get('type') == 'twitter':
                        twitter = s.get('url', '')
                    elif s.get('type') == 'telegram':
                        telegram = s.get('url', '')
                for w in websites:
                    if w.get('label', '').lower() == 'website':
                        website = w.get('url', '')
                if not desc:
                    # DEXScreener没有description但有社交信息
                    return {
                        'description': desc,
                        'twitter': twitter,
                        'telegram': telegram,
                        'website': website,
                    }
    except:
        pass
    
    return {'description': desc, 'twitter': '', 'telegram': '', 'website': ''}


def fetch_sol_tokens():
    """从Pump.fun(多offset) + DexScreener获取SOL新币，不漏币"""
    all_tokens = []
    seen = set()

    def _add_coin(c, source=''):
        addr = c.get('mint', '') or c.get('address', '') or c.get('id', '')
        if not addr or addr in seen:
            return
        seen.add(addr)

        mc = float(c.get('usd_market_cap', 0) or c.get('market_cap', 0) or 0)
        if mc <= 0:
            mc = float(c.get('mc', 0) or 0)

        virtual_sol = float(c.get('virtual_sol_reserves', 0) or 0)
        real_sol = float(c.get('real_sol_reserves', 0) or 0)
        complete = c.get('complete', False) or c.get('graduated', False)
        liq_sol = real_sol if complete else virtual_sol
        liq_usd = liq_sol / 1e9 * 150 if liq_sol else 0

        created_ts = c.get('created_timestamp', 0) or c.get('createdAt', 0) or c.get('timestamp', 0)
        age_h = (time.time() - created_ts / 1000) / 3600 if created_ts > 0 else 999

        liq = liq_usd if complete else max(liq_usd, 500)
        if mc < 800 and liq < 500:
            return

        all_tokens.append({
            'address': addr,
            'chain': 'sol',
            'name': c.get('name', '?'),
            'symbol': c.get('symbol', '?').replace('..fmN1', ''),
            'mc': max(mc, 1000),
            'liq': liq,
            'volume': 0,
            'holders': 0,
            'sm': 0,
            'chg_1h': 0,
            'chg_24h': 0,
            'age_h': age_h,
            'price': 0,
        })

    # Pump.fun: 多offset采样，覆盖排序不规则导致的漏币
    for offset in range(0, 300, 50):
        try:
            r = requests.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"limit": 50, "offset": offset, "sort": "created_at", "direction": "desc"},
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=15
            )
            if r.status_code == 200:
                for c in r.json():
                    _add_coin(c)
        except:
            pass

    # DexScreener: 补充已毕业(Pump已下架)的大市值SOL币
    try:
        r = requests.get(
            "https://api.dexscreener.com/v1/search?q=sol&limit=50",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            pairs = data.get('pairs', []) if isinstance(data, dict) else []
            for p in pairs[:30]:
                mc = float(p.get('market_cap', 0) or 0)
                if mc >= 5000:  # DexScreener补充市值>$5K的币
                    _add_coin({
                        'mint': p.get('base_token', {}).get('address', ''),
                        'name': p.get('base_token', {}).get('name', '?'),
                        'symbol': p.get('base_token', {}).get('symbol', '?'),
                        'usd_market_cap': mc,
                        'created_timestamp': p.get('created_at', 0),
                    })
    except:
        pass

    return all_tokens



def fetch_new_tokens():
    """从GMGN+XXYY获取各链新币，XXYY补充KOL数据"""
    all_tokens = []
    seen_addrs = set()

    # 预先拉取XXYY KOL数据（所有链），合并到tokens里
    xxyy_kol = {}
    xxyy_tag = {}
    if XXYY_API_KEY:
        xxyy_kol = xxyy_get_kol_list('sol')
        xxyy_tag = xxyy_get_tag_holder_list('sol')
        time.sleep(0.3)
        xxyy_kol_bsc = xxyy_get_kol_list('bsc')
        xxyy_tag_bsc = xxyy_get_tag_holder_list('bsc')
        time.sleep(0.3)
        xxyy_kol_eth = xxyy_get_kol_list('eth')
        xxyy_tag_eth = xxyy_get_tag_holder_list('eth')
        xxyy_kol.update({k: v for k, v in xxyy_kol_bsc.items()})
        xxyy_tag.update({k: v for k, v in xxyy_tag_bsc.items()})
        xxyy_kol.update({k: v for k, v in xxyy_kol_eth.items()})
        xxyy_tag.update({k: v for k, v in xxyy_tag_eth.items()})
        log(f"[XXYY] kol买入列表: sol={len(xxyy_kol)}, bsc={len(xxyy_kol_bsc)}, eth={len(xxyy_kol_eth)}, tag买入列表: sol={len(xxyy_tag)}, bsc={len(xxyy_tag_bsc)}, eth={len(xxyy_tag_eth)}")
    else:
        log(f"[XXYY] XXYY_API_KEY 未设置，KOL过滤将失效（需设置环境变量 XXYY_API_KEY）")

    def _merge_sm(token):
        """用XXYY数据补充sm（KOL数），只用XXYY数据，不用GMGN的smart_degen_count"""
        addr = token.get('address', '').lower()
        kol = xxyy_kol.get(addr, 0)
        tag = xxyy_tag.get(addr, 0)
        # 只用XXYY的kol和tag，忽略GMGN的smart_degen_count
        token['sm'] = max(kol, tag)
        return token

    # SOL: 先拉 GMGN（有真实 holders），再补充 DexScreener（无 holders）
    sol_gmgn_urls = [
        f'https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=open_timestamp&direction=desc&limit=100',
        f'https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=swaps&direction=desc&limit=50',
    ]
    for url in sol_gmgn_urls:
        data = gmgn_get(url)
        tokens = data.get('rank', [])
        for t in tokens:
            addr = t.get('address', '')
            if not addr or addr in seen_addrs:
                continue
            mc = t.get('market_cap', 0) or t.get('fdv', 0) or 0
            liq = t.get('liquidity', 0) or 0
            if mc < 1000 or liq < 500 or mc > 10000000:
                continue
            age_ts = t.get('open_timestamp', 0)
            age_h = (time.time() - age_ts) / 3600 if age_ts > 0 else 999
            seen_addrs.add(addr)
            t_data = {
                'address': addr,
                'chain': 'sol',
                'name': t.get('name', '?'),
                'symbol': t.get('symbol', '?'),
                'mc': mc,
                'liq': liq,
                'volume': t.get('volume', 0) or 0,
                'holders': t.get('holder_count', 0) or 0,
                'sm': 0,
                'chg_1h': t.get('price_change_percent1h', 0) or 0,
                'chg_24h': t.get('price_change_percent', 0) or 0,
                'age_h': age_h,
                'price': t.get('price', 0),
                'buys_1h': t.get('buys', 0) or 0,
                'sells_1h': t.get('sells', 0) or 0,
            }
            all_tokens.append(_merge_sm(t_data))
        time.sleep(0.3)

    # SOL DexScreener 兜底补充（GMGN 已有的地址会被 seen_addrs 过滤掉）
    sol_ds_tokens = fetch_sol_tokens()
    for t in sol_ds_tokens:
        if t['address'] not in seen_addrs:
            seen_addrs.add(t['address'])
            all_tokens.append(_merge_sm(t))
    
    # GMGN ETH/BSC/BASE — GMGN对ETH返回403，改用DexScreener补充
    for chain in ['bsc', 'base']:
        # 多维度拉数据，避免漏掉
        urls = [
            # 按创建时间 — 最新的币
            f'https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/1h?orderby=open_timestamp&direction=desc&limit=100',
            # 按交易量 — 最活跃的币
            f'https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/1h?orderby=swaps&direction=desc&limit=50',
        ]
        
        for url in urls:
            data = gmgn_get(url)
            tokens = data.get('rank', [])
            
            for t in tokens:
                addr = t.get('address', '')
                if not addr or addr in seen_addrs:
                    continue
                
                mc = t.get('market_cap', 0) or t.get('fdv', 0) or 0
                liq = t.get('liquidity', 0) or 0
                
                # 基本过滤：太小的不看
                if mc < 1000 or liq < 500 or mc > 10000000:
                    continue
                
                age_ts = t.get('open_timestamp', 0)
                age_h = (time.time() - age_ts) / 3600 if age_ts > 0 else 999
                
                seen_addrs.add(addr)
                t_data = {
                    'address': addr,
                    'chain': chain,
                    'name': t.get('name', '?'),
                    'symbol': t.get('symbol', '?'),
                    'mc': mc,
                    'liq': liq,
                    'volume': t.get('volume', 0) or 0,
                    'holders': t.get('holder_count', 0) or 0,
                    'sm': 0,
                    'chg_1h': t.get('price_change_percent1h', 0) or 0,
                    'chg_24h': t.get('price_change_percent', 0) or 0,
                    'age_h': age_h,
                    'price': t.get('price', 0),
                    'buys_1h': t.get('buys', 0) or 0,
                    'sells_1h': t.get('sells', 0) or 0,
                }
                all_tokens.append(_merge_sm(t_data))
            time.sleep(0.3)

    # ETH: GMGN返回403，改用DexScreener补充
    try:
        ds_resp = requests.get(
            'https://api.dexscreener.com/latest/dex/search?q=eth&limit=100&chainId=ethereum',
            headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'},
            timeout=15
        )
        if ds_resp.status_code == 200:
            ds_data = ds_resp.json()
            for p in ds_data.get('pairs', []):
                if p.get('chainId') != 'ethereum':
                    continue
                addr = p.get('baseToken', {}).get('address', '')
                if not addr or addr.lower() in seen_addrs or addr == '0x0000000000000000000000000000000000000000':
                    continue
                mc = float(p.get('marketCap') or p.get('fdv') or 0)
                liq = float(p.get('liquidity', {}).get('usd', 0) or 0)
                if mc < 1000 or liq < 500 or mc > 10000000:
                    continue
                age_ts = p.get('pairCreatedAt', 0)
                age_h = (time.time() - age_ts / 1000) / 3600 if age_ts > 0 else 999
                price_chg = p.get('priceChange', {})
                seen_addrs.add(addr.lower())
                t_data = {
                    'address': addr.lower(),
                    'chain': 'eth',
                    'name': p.get('baseToken', {}).get('name', '?'),
                    'symbol': p.get('baseToken', {}).get('symbol', '?'),
                    'mc': mc,
                    'liq': liq,
                    'volume': float(p.get('volume', {}).get('h1', 0) or 0),
                    'holders': 0,
                    'sm': 0,
                    'chg_1h': float(price_chg.get('h1', 0) or 0),
                    'chg_24h': float(price_chg.get('h24', 0) or 0),
                    'age_h': age_h,
                    'price': float(p.get('priceUsd', 0) or 0),
                    'buys_1h': 0,
                    'sells_1h': 0,
                }
                all_tokens.append(_merge_sm(t_data))
            log(f"[DexScreener] ETH补充: {len(ds_data.get('pairs', []))} 个交易对")
    except Exception as e:
        log(f"[DexScreener] ETH补充失败: {e}")
    
    return all_tokens

def fetch_flap_tokens():
    """
    FLAP平台扫描 — BSC社区驱动型发射台
    找形态：跌下来但有底部支撑（有庄在低位推）
    特征：24h跌了，但1h企稳/反弹，买入>卖出，holders在涨
    """
    data = gmgn_get(
        'https://gmgn.ai/defi/quotation/v1/rank/bsc/swaps/24h?launchpad=flap&orderby=volume&direction=desc&limit=30'
    )
    tokens = data.get('rank', [])
    
    candidates = []
    for t in tokens:
        addr = t.get('address', '')
        if not addr:
            continue
        
        mc = t.get('market_cap', 0) or 0
        liq = t.get('liquidity', 0) or 0
        vol = t.get('volume', 0) or 0
        holders = t.get('holder_count', 0) or 0
        buys = t.get('buys', 0) or 0
        sells = t.get('sells', 0) or 0
        chg_1h = t.get('price_change_percent1h', 0) or 0
        chg_24h = t.get('price_change_percent', 0) or 0
        age_ts = t.get('open_timestamp', 0)
        age_h = (time.time() - age_ts) / 3600 if age_ts > 0 else 0
        
        # 基本门槛
        if mc < 1000 or liq < 500:
            continue
        if holders < 5:
            continue
        
        # 底部支撑形态判断：
        # 条件1: 24h跌了（或者涨幅有限），说明不是刚拉的
        # 条件2: 1h跌幅小于24h跌幅，说明在企稳
        # 条件3: 买入 > 卖出，有人在接
        buy_ratio = buys / max(sells, 1)
        
        is_support = False
        reason = ''
        
        # 形态A: 24h跌了，1h在企稳/反弹
        if chg_24h < -10 and chg_1h > chg_24h * 0.3:
            is_support = True
            reason = f'24h跌{chg_24h:.0f}%但1h企稳{chg_1h:+.0f}%'
        
        # 形态B: 24h微跌或横盘，1h微涨，买卖比健康
        if -10 <= chg_24h <= 30 and chg_1h > -5 and buy_ratio > 1.1:
            is_support = True
            reason = f'底部横盘 买卖比{buy_ratio:.2f}'
        
        # 形态C: 大跌后强反弹
        if chg_24h < -30 and chg_1h > 10:
            is_support = True
            reason = f'大跌{chg_24h:.0f}%后反弹{chg_1h:+.0f}%'
        
        if is_support and buy_ratio >= 1.0:
            candidates.append({
                'address': addr,
                'chain': 'bsc',
                'name': t.get('name', '?'),
                'symbol': t.get('symbol', '?'),
                'mc': mc,
                'liq': liq,
                'volume': vol,
                'holders': holders,
                'sm': 0,
                'chg_1h': chg_1h,
                'chg_24h': chg_24h,
                'age_h': age_h,
                'price': t.get('price', 0),
                'buys': buys,
                'sells': sells,
                'buy_ratio': buy_ratio,
                'support_reason': reason,
                'launchpad': 'flap',
            })
    
    # 按市值排序
    candidates.sort(key=lambda x: x['mc'], reverse=True)
    return candidates

def _fmt_k(v):
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v/1_000:.0f}K"
    else:
        return f"${v:.0f}"

def _links_section(desc_info):
    """统一社交链接格式"""
    links = []
    twitter = (desc_info or {}).get('twitter', '')
    telegram = (desc_info or {}).get('telegram', '')
    website = (desc_info or {}).get('website', '')
    if twitter:
        links.append(f"𝕏 {twitter}")
    if telegram:
        links.append(f"💬 {telegram}")
    if website:
        web_short = website.replace('https://','').replace('http://','').split('/')[0]
        links.append(f"🌐 {web_short}")
    return '  ·  '.join(links) if links else ''


def format_flap_alert(token, desc_info=None):
    """FLAP低吸信号推送"""
    chain_emoji = '🟡'
    star_str = "⭐⭐"
    msg = f"{chain_emoji} BSC  {star_str}  FLAP低吸信号\n\n"
    msg += f"◈ {token['name']}  ({token['symbol']})\n"
    addr = token['address']
    gmgn_url = f"https://gmgn.ai/bsc/token/{addr}"
    msg += f"📈 GMGN: {gmgn_url}\n\n"

    mc_str = _fmt_k(token['mc'])
    liq_str = _fmt_k(token['liq'])
    vol_str = _fmt_k(token.get('volume', 0))
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    buy_r = token.get('buy_ratio', 0)
    chg_1h = token.get('chg_1h', 0)
    chg_24h = token.get('chg_24h', 0)

    msg += f"市值 {mc_str}  ·  流动性 {liq_str}  ·  24h量 {vol_str}\n"
    msg += f"持有人 {holders_str}  ·  买卖比 {buy_r:.2f}\n"
    msg += f"1h {chg_1h:+.1f}%  ·  24h {chg_24h:+.1f}%\n\n"
    msg += f"🏷️ FLAP社区币  ·  {token.get('support_reason', '')}\n\n"

    desc = (desc_info or {}).get('description', '')
    if desc:
        desc = desc.strip()
        if len(desc) > 120:
            desc = desc[:120] + '…'
        msg += f"💬 {desc}\n\n"

    links = _links_section(desc_info)
    if links:
        msg += links + "\n"
    return msg


def format_musk_trump_alert(token, matched_kw, desc_info=None):
    """马斯克/川普叙事推送"""
    chain_emoji = {'sol': '🔸', 'eth': '🔷', 'bsc': '🟡', 'base': '🔵'}.get(token['chain'], '●')
    ch = {'sol': 'SOL', 'eth': 'ETH', 'bsc': 'BSC', 'base': 'BASE'}.get(token['chain'], token['chain'].upper())
    msg = f"{chain_emoji} {ch}  ⭐⭐⭐  马斯克/川普概念\n\n"
    msg += f"◈ {token['name']}  ({token['symbol']})\n"
    addr = token['address']
    gmgn_url = f"https://gmgn.ai/{token['chain']}/token/{addr}"
    msg += f"📈 GMGN: {gmgn_url}\n\n"

    mc_str = _fmt_k(token['mc'])
    liq_str = _fmt_k(token['liq'])
    chg_str = f"{token['chg_1h']:+.1f}%"
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    age_str = f"{token['age_h']:.1f}h"

    msg += f"市值 {mc_str}  ·  流动性 {liq_str}  ·  1h {chg_str}\n"
    msg += f"持有人 {holders_str}  ·  币龄 {age_str}\n\n"
    msg += f"🏷️ {'/'.join(matched_kw[:3])}\n\n"

    desc = (desc_info or {}).get('description', '')
    if desc:
        desc = desc.strip()
        if len(desc) > 120:
            desc = desc[:120] + '…'
        msg += f"💬 {desc}\n\n"

    links = _links_section(desc_info)
    if links:
        msg += links + "\n"
    return msg


def format_binance_cz_alert(token, matched_kw, desc_info=None):
    """币安/CZ叙事推送"""
    msg = f"🟡 BSC  ⭐⭐⭐  币安/CZ概念\n\n"
    msg += f"◈ {token['name']}  ({token['symbol']})\n"
    addr = token['address']
    gmgn_url = f"https://gmgn.ai/bsc/token/{addr}"
    msg += f"📈 GMGN: {gmgn_url}\n\n"

    mc_str = _fmt_k(token['mc'])
    liq_str = _fmt_k(token['liq'])
    chg_str = f"{token['chg_1h']:+.1f}%"
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    age_str = f"{token['age_h']:.1f}h"

    msg += f"市值 {mc_str}  ·  流动性 {liq_str}  ·  1h {chg_str}\n"
    msg += f"持有人 {holders_str}  ·  币龄 {age_str}\n\n"
    msg += f"🏷️ {'/'.join(matched_kw[:3])}\n\n"

    desc = (desc_info or {}).get('description', '')
    if desc:
        desc = desc.strip()
        if len(desc) > 120:
            desc = desc[:120] + '…'
        msg += f"💬 {desc}\n\n"

    links = _links_section(desc_info)
    if links:
        msg += links + "\n"
    return msg


def format_novel_narrative_alert(token, theme, desc_info=None):
    """全新叙事推送 — 保留备用"""
    return format_heating_narrative_alert(token, theme, 1, desc_info)


def format_heating_narrative_alert(token, theme, count, desc_info=None):
    """叙事热点推送"""
    chain_emoji = {'sol': '🔸', 'eth': '🔷', 'bsc': '🟡', 'base': '🔵'}.get(token['chain'], '●')
    ch = {'sol': 'SOL', 'eth': 'ETH', 'bsc': 'BSC', 'base': 'BASE'}.get(token['chain'], token['chain'].upper())
    msg = f"{chain_emoji} {ch}  ⭐⭐  叙事热点 · 同主题{count}个币\n\n"
    msg += f"◈ {token['name']}  ({token['symbol']})\n"
    addr = token['address']
    gmgn_url = f"https://gmgn.ai/{token['chain']}/token/{addr}"
    msg += f"📈 GMGN: {gmgn_url}\n\n"

    mc_str = _fmt_k(token['mc'])
    liq_str = _fmt_k(token['liq'])
    chg_str = f"{token['chg_1h']:+.1f}%"
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    age_str = f"{token['age_h']:.1f}h"

    msg += f"市值 {mc_str}  ·  流动性 {liq_str}  ·  1h {chg_str}\n"
    msg += f"持有人 {holders_str}  ·  币龄 {age_str}\n\n"
    msg += f"🏷️ {theme}\n\n"

    desc = (desc_info or {}).get('description', '')
    if desc:
        desc = desc.strip()
        if len(desc) > 120:
            desc = desc[:120] + '…'
        msg += f"💬 {desc}\n\n"

    links = _links_section(desc_info)
    if links:
        msg += links + "\n"
    return msg

# ============================================================
# 动量追踪器 — 持续上涨+放量检测
# ============================================================
def track_momentum(tokens):
    """
    每轮扫描更新币的快照。
    连续多轮市值上涨+成交量增加 = 动量信号，直接推。
    """
    global MOMENTUM_TRACKER, MOMENTUM_PUSHED
    now = time.time()
    alerts = []
    
    # 当前轮所有地址
    current_addrs = set()
    
    for token in tokens:
        addr = token['address']
        mc = token['mc']
        vol = token.get('volume', 0) or 0
        price = token.get('price', 0) or 0
        buys = token.get('buys_1h', 0) or token.get('buys', 0) or 0
        
        current_addrs.add(addr)
        
        # 基本门槛
        if mc < 1000 or token.get('liq', 0) < 500 or mc > 10000000:
            continue
        
        # 记录快照 — 只有数据真正变化时才记录（GMGN有缓存）
        if addr not in MOMENTUM_TRACKER:
            MOMENTUM_TRACKER[addr] = []
        
        snapshots = MOMENTUM_TRACKER[addr]
        
        # 跳过重复数据（跟上一次完全一样就不记录）
        if snapshots and snapshots[-1]['mc'] == mc and snapshots[-1].get('vol', 0) == vol:
            continue  # 数据没变，跳过
        
        holders = token.get('holders', 0) or 0
        snapshots.append({
            'ts': now,
            'mc': mc,
            'vol': vol,
            'price': price,
            'buys': buys,
            'holders': holders,
        })
        
        # 只保留最近20个快照（约200秒）
        if len(snapshots) > 20:
            snapshots[:] = snapshots[-20:]
        
        # 至少需要3个快照才能判断
        if len(snapshots) < MOMENTUM_CONSECUTIVE_UP:
            continue
        
        # 检测最近N轮是否持续涨
        recent = snapshots[-MOMENTUM_CONSECUTIVE_UP:]
        consecutive_up = True
        total_gain = 0
        
        for i in range(1, len(recent)):
            prev_mc = recent[i-1]['mc']
            curr_mc = recent[i]['mc']
            if prev_mc <= 0:
                consecutive_up = False
                break
            gain = (curr_mc - prev_mc) / prev_mc
            if gain <= 0:  # 任何一轮没涨就不算
                consecutive_up = False
                break
            total_gain += gain
        
        if not consecutive_up:
            continue

        # === 持有人数连续2轮增长（meme需要真实社区）===
        holders_recent = [s['holders'] for s in recent]
        holders_growing = all(
            holders_recent[i] > holders_recent[i-1]
            for i in range(1, len(holders_recent))
        )
        if not holders_growing:
            continue

        # === Mayhem模因过滤 ===
        name_lower = token.get('name', '').lower()
        symbol_lower = token.get('symbol', '').lower()
        mayhem_kw = ['mayhem', 'mayed', 'mayham', 'mayem', 'mayhem']
        if any(kw in name_lower or kw in symbol_lower for kw in mayhem_kw):
            continue

        # 连续涨了！检查放量（成交量在增）
        vol_increasing = True
        for i in range(1, len(recent)):
            if recent[i]['buys'] < recent[i-1]['buys'] * 0.8:  # 允许小幅波动
                vol_increasing = False
                break
        
        # 计算总涨幅
        first_mc = recent[0]['mc']
        last_mc = recent[-1]['mc']
        pct_gain = ((last_mc - first_mc) / first_mc * 100) if first_mc > 0 else 0
        
        # 推送条件：连续涨 + 涨幅>5%
        if pct_gain < 5:
            continue
        
        # === 严格市值/币龄过滤 ===
        age_h = token.get('age_h', 999)
        if age_h < 1 and mc < 60000:
            # 币龄<1小时 且 市值<60k：必须两轮总涨幅>50%
            if pct_gain <= 50:
                continue
        elif age_h >= 1:
            # 币龄>=1小时：市值必须>100k
            if mc < 100000:
                continue
        
        # 信号计数：同一个币每次触发信号，计数+1
        push_info = MOMENTUM_PUSHED.get(addr, {'count': 0, 'last_ts': 0, 'last_mc': 0})
        
        # 必须比上次推送时市值还高才推（真的还在涨）
        if push_info['count'] > 0 and last_mc <= push_info['last_mc']:
            continue
        
        push_info['count'] += 1
        push_info['last_ts'] = now
        push_info['last_mc'] = last_mc
        signal_count = push_info['count']
        
        # KOL买入检测 — 有XXYY KOL数据时才过滤，否则跳过（避免sm=0全被误杀）
        sm_count = token.get('sm', 0) or 0
        if XXYY_API_KEY and sm_count < MIN_KOL_COUNT:
            log(f"[KOL过滤] {token.get('symbol')} sm={sm_count} < {MIN_KOL_COUNT}，跳过")
            continue

        # 安全检查
        safety = check_token_safety(token['chain'], addr)
        if not safety.get('safe'):
            continue

        # 机器人刷量检测
        is_bot, bot_reason = detect_bot_pump(token)
        if is_bot:
            continue  # 不推机器人盘

        # 叙事分类 → 星级评分
        category, matched_kw = classify_narrative(token['name'], token['symbol'], token['chain'])
        is_flap = token.get('launchpad') == 'flap'
        
        if category == 'musk_trump':
            stars = 3
            narrative_tag = f"马斯克/川普概念 ({', '.join(matched_kw[:3])})"
        elif category == 'binance_cz':
            stars = 3
            narrative_tag = f"币安/CZ概念 ({', '.join(matched_kw[:3])})"
        elif category == 'celebrity_viral':
            stars = 2
            narrative_tag = f"名人/热点 ({', '.join(matched_kw[:3])})"
        elif is_flap:
            stars = 2
            narrative_tag = "FLAP社区币"
        else:
            # 检查是否全新叙事
            theme = normalize_theme(token['name'], token['symbol'])
            theme_words = [w for w in theme.split() if w not in COMMON_NOISE_WORDS and len(w) > 2]
            if len(theme_words) >= 2:
                stars = 2
                narrative_tag = f"叙事: {theme}"
            else:
                stars = 1
                narrative_tag = "无明确叙事"
        
        # 生成推送
        desc_info = fetch_token_description(token['chain'], addr)
        
        # FLAP币额外标注社区/CTO信息
        if is_flap:
            has_twitter = bool(desc_info.get('twitter'))
            has_tg = bool(desc_info.get('telegram'))
            has_web = bool(desc_info.get('website'))
            community_tags = []
            if has_twitter:
                community_tags.append("有推特")
            if has_tg:
                community_tags.append("有TG群")
            if has_web:
                community_tags.append("有官网")
            if community_tags:
                narrative_tag += f" | {' '.join(community_tags)}"
                stars = min(3, stars + 1)  # 有社区加一星
            else:
                narrative_tag += " | 无社区链接"
        
        # 生成推送（gmgn info 追加到 format 之后）
        msg = format_momentum_alert(token, pct_gain, len(recent), vol_increasing, stars, narrative_tag, desc_info, signal_count)

        alerts.append({'msg': msg, 'token': token, 'chain': token['chain'], 'pct_gain': pct_gain})
        MOMENTUM_PUSHED[addr] = push_info

        # === 注册到信号跟踪器 ===
        now = time.time()
        if addr not in PUSH_TRACKER:
            PUSH_TRACKER[addr] = {
                'name': token['name'],
                'symbol': token['symbol'],
                'chain': token['chain'],
                'push_ts': now,
                'init_mc': last_mc,
                'init_price': recent[0]['price'],
                'init_holders': recent[0]['holders'],
                'peak_mc': last_mc,
                'peak_price': last_mc / recent[0]['mc'] * recent[0]['price'] if recent[0]['mc'] > 0 else last_mc,
                'peak_ts': now,
                'last_check_ts': now,
                'report_count': 0,
                'last_mid_push_ts': 0,
                'last_mid_pct': 0,
                'first_msg_id': None,  # 主循环发报后回填
            }

        log(f"[动量信号{signal_count}] {token['name']} ({token['symbol']}) on {token['chain']} — 连涨{len(recent)}轮 +{pct_gain:.1f}%")
    
    # 清理不再出现的币
    stale = [a for a in MOMENTUM_TRACKER if a not in current_addrs]
    for a in stale:
        if now - MOMENTUM_TRACKER[a][-1]['ts'] > 600:  # 10分钟没出现就清理
            del MOMENTUM_TRACKER[a]
    
    # 清理推送记录 — 1小时没出现的清掉
    MOMENTUM_PUSHED = {k: v for k, v in MOMENTUM_PUSHED.items() if now - v.get('last_ts', 0) < 3600}
    
    return alerts

def format_momentum_alert(token, pct_gain, rounds, vol_up, stars, narrative_tag, desc_info=None, seen_count=0):
    """
    动量推送 — HTML格式（Telegram parse_mode=HTML）
    无链接预览，GMGN和推文链接仅作纯文本显示
    """
    chain_map = {'sol': 'SOL', 'eth': 'ETH', 'bsc': 'BSC', 'base': 'BASE'}
    ch = chain_map.get(token['chain'], token['chain'].upper())

    # 顶部一行
    top_line = f"🔸 <b>[{ch}] 异动提醒 · 连涨 {rounds} 轮 (+{pct_gain:.1f}%)</b>"

    # 代币名 + 推送次数 + 币龄
    name = token['name']
    sym = token['symbol']
    age_str = f"{token['age_h']:.1f}h"
    push_line = f"📊 推送次数：{seen_count}次 | ⏳ 币龄：{age_str}h"
    token_line = f"◈ <b>${sym}</b> | {name}\n{push_line}"

    # GMGN链接（无href，纯文本避免预览）
    addr = token['address']
    gmgn_url = (f"https://gmgn.ai/sol/token/{addr}"
                 if token['chain'] == 'sol'
                 else f"https://gmgn.ai/{token['chain']}/token/{addr}")
    gmgn_line = f"🔗 GMGN：{gmgn_url}"

    def fmt_k(v):
        if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
        elif v >= 1_000: return f"${v/1_000:.0f}K"
        else: return f"${v:.0f}"

    mc_str = fmt_k(token['mc'])
    liq_str = fmt_k(token['liq'])
    chg_1h = token.get('chg_1h', 0)
    chg_str = f"{chg_1h:+.1f}%"
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    sm_count = token.get('sm', 0) or 0
    sm_str = str(sm_count) if sm_count > 0 else "—"

    # 数据行
    mc_line = f"💰 <b>市值：</b> <code>{mc_str}</code>"
    liq_line = f"💧 <b>池子：</b> <code>{liq_str}</code> (1h {chg_str})"
    sm_line = f"👑 <b>KOL人数：</b> <u><b>{sm_str}</b></u> 🔥 | 👥 <b>持有人：</b> {holders_str}"

    # 叙事标签（翻译成中文，英文原文保留括号，去掉原有"叙事:"前缀避免重复）
    narrative_line = ""
    if narrative_tag and narrative_tag not in ('无明确叙事',):
        clean_tag = narrative_tag.replace('★', '⭐').replace('☆', '').strip()
        # 去掉原有的"叙事:"前缀
        if clean_tag.startswith('叙事:') or clean_tag.startswith('叙事：'):
            clean_tag = clean_tag.split(':', 1)[-1].split('：', 1)[-1].strip()
        translated_tag = translate_narrative_tag(clean_tag)
        narrative_line = f"🏷️ <b>叙事：</b> {translated_tag}"

    # 推文链接（无href，纯文本）
    twitter = (desc_info or {}).get('twitter', '')
    tweet_line = f"🐦 推文：{twitter}" if twitter else ""

    # 组装
    divider = "━━━━━━━━━━━━━━"
    msg_parts = [
        top_line,
        divider,
        token_line,
        "",
        mc_line,
        liq_line,
        sm_line,
    ]
    if narrative_line:
        msg_parts.extend(["", narrative_line])
    msg_parts.extend(["", gmgn_line])
    if tweet_line:
        msg_parts.append(tweet_line)

    return "\n".join(msg_parts)

def format_celebrity_alert(token, matched_kw, desc_info=None):
    """名人/推特热点推送"""
    chain_emoji = {'sol': '🔸', 'eth': '🔷', 'bsc': '🟡', 'base': '🔵'}.get(token['chain'], '●')
    ch = {'sol': 'SOL', 'eth': 'ETH', 'bsc': 'BSC', 'base': 'BASE'}.get(token['chain'], token['chain'].upper())
    msg = f"{chain_emoji} {ch}  ⭐⭐  名人/热点\n\n"
    msg += f"◈ {token['name']}  ({token['symbol']})\n"
    addr = token['address']
    gmgn_url = f"https://gmgn.ai/{token['chain']}/token/{addr}"
    msg += f"📈 GMGN: {gmgn_url}\n\n"

    mc_str = _fmt_k(token['mc'])
    liq_str = _fmt_k(token['liq'])
    chg_str = f"{token['chg_1h']:+.1f}%"
    holders = token.get('holders', 0) or 0
    holders_str = f"{holders:,}" if holders else "—"
    age_str = f"{token['age_h']:.1f}h"

    msg += f"市值 {mc_str}  ·  流动性 {liq_str}  ·  1h {chg_str}\n"
    msg += f"持有人 {holders_str}  ·  币龄 {age_str}\n\n"
    msg += f"🏷️ {'/'.join(matched_kw[:3])}\n\n"

    desc = (desc_info or {}).get('description', '')
    if desc:
        desc = desc.strip()
        if len(desc) > 120:
            desc = desc[:120] + '…'
        msg += f"💬 {desc}\n\n"

    links = _links_section(desc_info)
    if links:
        msg += links + "\n"
    return msg

# ============================================================
# 核心扫描逻辑
# ============================================================
def scan_narratives():
    """主扫描函数"""
    conn = init_db()
    now = time.time()
    tokens = fetch_new_tokens()

    # 写入MC快照缓存（供sim_trade.py读取）
    try:
        mc_cache = [{"address": t["address"], "mc": t.get("mc", 0), "price": t.get("price", 0)} for t in tokens]
        with open(os.path.expanduser("~/crypto-trading/momentum_tracker_cache.json"), "w") as f:
            json.dump(mc_cache, f)
    except Exception as e:
        log(f"[MC缓存写入失败] {e}")

    log(f"扫描 {len(tokens)} 个新币...")
    
    # === 动量追踪 — 每轮更新所有币的快照，检测持续上涨 ===
    # 拉FLAP币一起喂进动量追踪器
    flap_tokens = []
    try:
        flap_tokens = fetch_flap_tokens()
    except:
        pass
    all_momentum_tokens = tokens + flap_tokens
    momentum_alerts = track_momentum(all_momentum_tokens)
    
    for token in tokens:
        addr = token['address']
        chain = token['chain']
        name = token['name']
        symbol = token['symbol']
        
        # 已扫描过的 — 更新seen_count和narratives的token_count，但不重复推
        if is_token_seen(conn, addr):
            # 更新seen_count
            c = conn.cursor()
            c.execute('UPDATE tokens_seen SET seen_count = seen_count + 1, market_cap = ? WHERE address = ?', (token['mc'], addr))
            # 更新narratives表的token_count（按主题）
            theme_tmp = normalize_theme(name, symbol)
            if theme_tmp:
                c.execute('UPDATE narratives SET token_count = token_count + 1, last_seen_at = ? WHERE theme = ?', (int(time.time()), theme_tmp))
            conn.commit()
            continue
        
        # 分类叙事
        category, matched_kw = classify_narrative(name, symbol, chain)
        
        if category == 'spam':
            record_token(conn, addr, chain, name, symbol, '', 'spam', token['mc'])
            continue
        
        # 基本质量门槛（防止推太多垃圾）
        min_mc = 1000
        min_liq = 500
        if token['mc'] < min_mc or token['liq'] < min_liq:
            record_token(conn, addr, chain, name, symbol, '', 'too_small', token['mc'])
            continue
        
        theme = normalize_theme(name, symbol)
        
        # 所有分类只记录，不直接推送 — 推送统一走动量引擎
        record_token(conn, addr, chain, name, symbol, theme, category, token['mc'])
        check_narrative_novelty(conn, theme, name, symbol, addr, chain)
    
    conn.close()
    
    # === 推送动量信号 ===
    pushed = 0
    for ma in momentum_alerts[:8]:  # 单轮最多推8个
        if ma.get('chain') not in ENABLED_CHAINS:
            continue
        addr = ma['token']['address']
        reply_id = PUSH_TRACKER.get(addr, {}).get('first_msg_id')
        msg_id = tg_send(ma['msg'], parse_mode='HTML', reply_to_message_id=reply_id)
        if msg_id:
            pushed += 1
            # 写入模拟交易信号
            try:
                sig_file = "/root/crypto-trading/sim_signals.json"
                sig = {
                    "address": addr,
                    "name": ma['token'].get('name', ''),
                    "symbol": ma['token'].get('symbol', ''),
                    "chain": ma['token'].get('chain', 'sol'),
                    "entry_mc": ma['token'].get('mc', 0),
                    "entry_price": ma['token'].get('price', 0),
                    "push_ts": now,
                }
                if os.path.exists(sig_file):
                    with open(sig_file) as f:
                        existing = json.load(f)
                else:
                    existing = []
                existing.append(sig)
                with open(sig_file, 'w') as f:
                    json.dump(existing, f)
            except Exception as e:
                log(f"[信号写入失败] {e}")

            # 首次推送：回填 first_msg_id
            if addr in PUSH_TRACKER and not PUSH_TRACKER[addr].get('first_msg_id'):
                PUSH_TRACKER[addr]['first_msg_id'] = msg_id
            time.sleep(1)  # 避免TG限流

    # === 信号跟踪：每轮更新峰值 + 中间汇报 ===
    tokens_by_addr = {t['address']: t for t in tokens + flap_tokens}
    check_signal_tracking(tokens_by_addr)

    return pushed, len(momentum_alerts)

# ============================================================
# 信号跟踪检查 & 2小时汇总
# ============================================================
def check_signal_tracking(tokens_by_addr, force_summary=False):
    """
    每轮扫描后调用：
    1. 更新 PUSH_TRACKER 中每个已推送币的当前市值/价格，更新峰值
    2. 30分钟检查：从init_mc算起涨50%触发推送（跌不推）
    3. 2小时汇总：展示 init_mc → current_mc → pnl（从进入时刻算起）

    tokens_by_addr: {address: token_dict} 当前轮次的币数据
    """
    global PUSH_TRACKER, LAST_SUMMARY_TS
    now = time.time()

    def _fk(v):
        if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
        elif v >= 1_000: return f"${v/1_000:.0f}K"
        else: return f"${v:.0f}"
    def _fa(v):
        if abs(v) >= 1_000_000: return f"+${v/1_000_000:.1f}M" if v > 0 else f"-${abs(v)/1_000_000:.1f}M"
        elif abs(v) >= 1_000: return f"+${v/1_000:.0f}K" if v > 0 else f"-${abs(v)/1_000:.0f}K"
        else: return f"+${v:.0f}" if v > 0 else f"-${abs(v):.0f}"

    # === 更新峰值 ===
    for addr, record in list(PUSH_TRACKER.items()):
        token = tokens_by_addr.get(addr)
        current_mc = token['mc'] if token else 0
        current_price = token.get('price', 0) if token else 0
        if current_mc > record['peak_mc']:
            record['peak_mc'] = current_mc
            record['peak_ts'] = now
            if record['init_price'] > 0 and current_price > 0:
                record['peak_price'] = current_price
        record['last_check_ts'] = now

    # === 30分钟中间检查：涨50%触发（跌不推）- 只推ENABLED_CHAINS ===
    for addr, record in list(PUSH_TRACKER.items()):
        if record['chain'] not in ENABLED_CHAINS:
            continue
        token = tokens_by_addr.get(addr)
        current_mc = token['mc'] if token else 0
        elapsed_min = (now - record['push_ts']) / 60

        if elapsed_min < 30:
            continue

        if record['init_mc'] > 0 and current_mc > 0:
            current_pct = (current_mc - record['init_mc']) / record['init_mc']

            # 涨幅>=50% 且 比上次推送再涨了>=10% 才推，最多推3次
            if (current_pct >= MID_PUSH_THRESHOLD and
                (current_pct - record.get('last_mid_pct', 0)) >= 0.10 and
                record['report_count'] < 3):
                mc_change = current_mc - record['init_mc']
                emoji = "🚀"
                chain_emoji = {'sol': '🔸', 'eth': '🔷', 'bsc': '🟡', 'base': '🔵'}.get(record['chain'], '●')
                gmgn_url = (f"https://gmgn.ai/{record['chain']}/token/{addr}"
                           if record['chain'] != 'sol'
                           else f"https://gmgn.ai/sol/token/{addr}")
                msg = (
                    f"{emoji} 信号加速  {chain_emoji} {record['name']} ({record['symbol']})\n\n"
                    f"持仓 {elapsed_min:.0f}min  ·  从初始市值 +{current_pct*100:.0f}%\n"
                    f"初始 {_fk(record['init_mc'])} → 当前 {_fk(current_mc)}  "
                    f"{_fa(mc_change)}\n"
                    f"📈 GMGN: {gmgn_url}"
                )
                reply_id = record.get('first_msg_id')
                tg_send(msg, reply_to_message_id=reply_id)
                record['report_count'] += 1
                record['last_mid_push_ts'] = now
                record['last_mid_pct'] = current_pct
                time.sleep(0.5)

    # === 2小时汇总推送 ===
    if force_summary or (now - LAST_SUMMARY_TS) >= SUMMARY_INTERVAL:
        LAST_SUMMARY_TS = now

        active = [(addr, r) for addr, r in PUSH_TRACKER.items()
                  if r['chain'] in ENABLED_CHAINS and now - r['push_ts'] < 86400]

        if not active:
            tg_send("📊 2h信号汇总\n过去2小时无有效信号，继续监控中...")
            return

        active.sort(key=lambda x: x[1]['peak_mc'] / x[1]['init_mc'] if x[1]['init_mc'] > 0 else 0, reverse=True)

        winners = []
        losers = []

        for addr, r in active:
            token = tokens_by_addr.get(addr)
            current_mc = token['mc'] if token else r['peak_mc']
            init_mc = r['init_mc']
            mc_change = current_mc - init_mc
            mc_pct = ((current_mc / init_mc) - 1) * 100 if init_mc > 0 else 0
            elapsed_h = (now - r['push_ts']) / 3600
            age_tag = f"{elapsed_h:.1f}h"
            chain_emoji = {'sol': '🔸', 'eth': '🔷', 'bsc': '🟡', 'base': '🔵'}.get(r['chain'], '●')

            if mc_pct > 0:
                winners.append((r['name'], r['symbol'], chain_emoji, init_mc, current_mc, mc_change, mc_pct, age_tag))
            else:
                losers.append((r['name'], r['symbol'], chain_emoji, init_mc, current_mc, mc_change, mc_pct, age_tag))

        lines = [f"📊 2h信号汇总 — 跟踪{len(active)}个\n"]

        if winners:
            lines.append(f"\n🚀 盈利  ({len(winners)})\n")
            for name, sym, ce, init, curr, change, pct, age in winners[:8]:
                lines.append(
                    f"{ce} {name}  初始{_fk(init)} → 当前{_fk(curr)}  "
                    f"{_fa(change)} ({pct:+.0f}%)  {age}"
                )
            lines.append("")

        if losers:
            lines.append(f"\n📉 保本/亏损  ({len(losers)})\n")
            for name, sym, ce, init, curr, change, pct, age in losers[:5]:
                lines.append(
                    f"{ce} {name}  初始{_fk(init)} → 当前{_fk(curr)}  "
                    f"{_fa(change)} ({pct:+.0f}%)  {age}"
                )

        tg_send("\n".join(lines))

        # 清理过时的跟踪记录（推送超过24小时的）
        for addr in list(PUSH_TRACKER.keys()):
            if now - PUSH_TRACKER[addr]['push_ts'] >= 86400:
                del PUSH_TRACKER[addr]




# ============================================================
# 主循环
# ============================================================
def main():
    log("=" * 50)
    log("链上雷达 v1 启动")
    log(f"扫描间隔: {SCAN_INTERVAL}s")
    log(f"推送逻辑: 动量优先 — 连涨才推，叙事只做分类标签")
    log("=" * 50)

    # 初始化DB
    init_db()

    # 启动通知
    tg_send(
        "链上雷达 v1 已启动\n\n"
        "核心逻辑: 动量优先\n"
        "连涨2轮+涨幅>5%+KOL≥1才推送\n"
        "叙事只做分类标签:\n"
        "★★★ 马斯克/川普 | 币安/CZ | FLAP有社区\n"
        "★★ 名人热点 | FLAP无社区 | 有叙事\n"
        "★ 无明确叙事\n\n"
        f"扫描频率: 每{SCAN_INTERVAL}秒\n"
        "机器人盘过滤: 买卖比+人均笔数\n"
        "信号跟踪: 涨50%触发加速推送 | 2h汇总(init→当前pnl)"
    )

    scan_count = 0
    total_pushed = 0

    while True:
        try:
            scan_count += 1
            pushed, found = scan_narratives()
            total_pushed += pushed

            if pushed > 0:
                log(f"第{scan_count}轮: 发现{found}个, 推送{pushed}个 (累计推送{total_pushed})")
            else:
                if scan_count % 20 == 0:  # 每20轮报一次无信号
                    log(f"第{scan_count}轮: 无新信号 (累计推送{total_pushed})")

        except Exception as e:
            import traceback
            log(f"扫描异常: {e}\n{traceback.format_exc()}")

        time.sleep(SCAN_INTERVAL)

if __name__ == '__main__':
    main()
