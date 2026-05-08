#!/usr/bin/env python3
"""
SOL 链模拟交易计算器
- 监听 narrative_radar 的推送信号
- 模拟买入：按 init_mc 对应等比例仓位的 SOL
- SL: 5% from entry
- TP: 30% from entry (移动止损保护)
- 持仓超过2h强制结算
- 每5分钟扫描更新 + 推送2h汇总报告
"""

import json, time, os, sqlite3
from datetime import datetime

DATA_DIR = os.path.expanduser("~/crypto-trading")
SIGNAL_FILE = os.path.join(DATA_DIR, "sim_signals.json")
DB_FILE = os.path.join(DATA_DIR, "sim_trades.db")
LOG_FILE = os.path.join(DATA_DIR, "sim_trade.log")
TELEGRAM_ID = "1354071067"  # 你的 Telegram ID

# 交易参数
INIT_CAPITAL = 119.36     # 起始资金 (U)
STOP_LINE = 80            # 连续亏损停止线 (U)
MAX_POSITIONS = 3          # 最大同时持仓数
SL_PCT = 0.05              # 止损 5%
TP_PCT = 0.30              # 止盈 30%
HOLD_TIMEOUT = 7200        # 强制结算 2h
SCAN_INTERVAL = 300         # 扫描间隔 5分钟

# 全局状态
positions = {}   # {addr: {entry_mc, entry_price, entry_ts, name, symbol, side, size_u, tp_mc, sl_mc, exited, exit_reason, exit_ts, pnl_u}}
capital = INIT_CAPITAL
consecutive_loss = 0
total_pnl = 0
trade_count = 0

# ============================================================
# 工具函数
# ============================================================

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def tg_send(text):
    try:
        import requests
        token = "7812695282:AAEQ4gJC4hOiJSCdL6_9Ezy6xSnN03phNu4"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
    except:
        pass

def fmt_u(v):
    return f"+{v:.2f}U" if v >= 0 else f"{v:.2f}U"

def save_state():
    state = {
        "capital": capital,
        "consecutive_loss": consecutive_loss,
        "total_pnl": total_pnl,
        "trade_count": trade_count,
        "positions": positions,
    }
    with open(os.path.join(DATA_DIR, "sim_state.json"), "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def load_state():
    global capital, consecutive_loss, total_pnl, trade_count, positions
    p = os.path.join(DATA_DIR, "sim_state.json")
    if os.path.exists(p):
        with open(p) as f:
            s = json.load(f)
        capital = s.get("capital", INIT_CAPITAL)
        consecutive_loss = s.get("consecutive_loss", 0)
        total_pnl = s.get("total_pnl", 0)
        trade_count = s.get("trade_count", 0)
        positions = s.get("positions", {})

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT, name TEXT, symbol TEXT, chain TEXT,
            entry_ts INTEGER, exit_ts INTEGER,
            entry_mc REAL, exit_mc REAL,
            size_u REAL, pnl_u REAL,
            exit_reason TEXT, hold_secs REAL,
            UNIQUE(address, entry_ts)
        )
    """)
    conn.commit()
    return conn

# ============================================================
# 核心逻辑
# ============================================================

def open_position(addr, signal):
    """开仓：按 init_mc 换算等比例仓位"""
    global capital, positions

    if addr in positions:
        return False
    if len(positions) >= MAX_POSITIONS:
        return False
    if capital < 5:  # 最少5U才开
        return False

    entry_mc = signal["entry_mc"]
    entry_price = signal.get("entry_price", 0)
    if entry_mc <= 0:
        return False

    # 每次用 1/3 仓位
    size_u = capital / 3
    size_u = min(size_u, 20)  # 单币最高20U

    # TP/SL 价格
    tp_mc = entry_mc * (1 + TP_PCT)
    sl_mc = entry_mc * (1 - SL_PCT)

    positions[addr] = {
        "name": signal["name"],
        "symbol": signal["symbol"],
        "chain": signal["chain"],
        "entry_mc": entry_mc,
        "entry_price": entry_price,
        "entry_ts": signal["push_ts"],
        "size_u": size_u,
        "tp_mc": tp_mc,
        "sl_mc": sl_mc,
        "peak_mc": entry_mc,
        "exited": False,
        "exit_reason": None,
        "exit_ts": None,
        "pnl_u": 0,
        "signal_ts": signal["push_ts"],
    }
    log(f"[开仓] {signal['symbol']} @MC {entry_mc:.0f} size={size_u:.2f}U TP={TP_PCT*100:.0f}% SL={SL_PCT*100:.0f}%")
    return True

def check_exit(addr, record, current_mc):
    """检查是否触发止盈/止损/超时"""
    global positions, capital, consecutive_loss, total_pnl, trade_count

    entry_mc = record["entry_mc"]
    size_u = record["size_u"]
    current_mc = max(current_mc, 0)

    # 更新峰值
    if current_mc > record["peak_mc"]:
        positions[addr]["peak_mc"] = current_mc
        # 移动止损：TP线上移（从峰值回撤8%触发）
        new_tp = record["peak_mc"] * (1 + TP_PCT * 0.5)  # 峰值涨15%后，回撤8%才出
        positions[addr]["trail_sl"] = new_tp

    # 计算当前盈亏
    pnl_pct = (current_mc - entry_mc) / entry_mc
    pnl_u = size_u * pnl_pct

    exit_reason = None

    # 止损
    if current_mc <= record["sl_mc"]:
        exit_reason = "SL"

    # 止盈（移动止损保护）
    if current_mc >= record["tp_mc"]:
        trail_sl = record.get("trail_sl", record["sl_mc"])
        if current_mc >= record["peak_mc"] * 0.92:  # 从峰值回撤8%
            exit_reason = "TP"

    # 超时强制结算
    elapsed = time.time() - record["entry_ts"]
    if elapsed >= HOLD_TIMEOUT:
        exit_reason = "TIMEOUT"

    if exit_reason:
        final_pnl = size_u * ((current_mc - entry_mc) / entry_mc)
        positions[addr]["exited"] = True
        positions[addr]["exit_reason"] = exit_reason
        positions[addr]["exit_ts"] = time.time()
        positions[addr]["pnl_u"] = final_pnl
        positions[addr]["exit_mc"] = current_mc

        # 更新资金
        capital += final_pnl
        total_pnl += final_pnl
        trade_count += 1

        if final_pnl < 0:
            consecutive_loss += 1
        else:
            consecutive_loss = 0

        log(f"[平仓] {record['symbol']} {exit_reason} pnl={fmt_u(final_pnl)} capital={capital:.2f}U")

        # 写数据库
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("""
                INSERT OR IGNORE INTO trades
                (address, name, symbol, chain, entry_ts, exit_ts, entry_mc, exit_mc, size_u, pnl_u, exit_reason, hold_secs)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                addr, record["name"], record["symbol"], record["chain"],
                record["entry_ts"], positions[addr]["exit_ts"],
                entry_mc, current_mc, size_u, final_pnl, exit_reason,
                elapsed
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            log(f"[DB写入失败] {e}")

        del positions[addr]
        save_state()
        return True
    return False

def scan_positions(current_prices):
    """扫描所有持仓，更新状态"""
    exited_any = False
    for addr in list(positions.keys()):
        record = positions[addr]
        mc = current_prices.get(addr, {}).get("mc", record.get("peak_mc", 0))
        if mc > 0:
            if check_exit(addr, record, mc):
                exited_any = True
    return exited_any

def get_current_mcs():
    """从 narrative_radar 的扫描结果里读当前 MC"""
    # 读取最新扫描缓存
    cache_file = os.path.join(DATA_DIR, "momentum_tracker_cache.json")
    mcs = {}
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
            for item in data:
                mcs[item["address"].lower()] = {"mc": item.get("mc", 0), "price": item.get("price", 0)}
        except:
            pass
    return mcs

def fmt_positions():
    """格式化当前持仓"""
    if not positions:
        return "无持仓"
    lines = []
    now = time.time()
    for addr, r in positions.items():
        elapsed = now - r["entry_ts"]
        age = f"{elapsed/3600:.1f}h"
        pnl_pct = (r["peak_mc"] - r["entry_mc"]) / r["entry_mc"] * 100 if r["entry_mc"] > 0 else 0
        lines.append(
            f"• {r['symbol']} {age} | {pnl_pct:+.1f}% "
            f"(MC {r['entry_mc']:.0f}→{r['peak_mc']:.0f})"
        )
    return "\n".join(lines)

# ============================================================
# 2h 汇总报告
# ============================================================

def generate_report():
    """生成2h P&L汇总"""
    if trade_count == 0:
        return None

    conn = sqlite3.connect(DB_FILE)
    now = time.time()
    two_hours_ago = now - 7200

    # 近2h的交易
    recent = conn.execute("""
        SELECT name, symbol, entry_ts, exit_ts, entry_mc, exit_mc, size_u, pnl_u, exit_reason, hold_secs
        FROM trades WHERE exit_ts > ?
        ORDER BY exit_ts DESC
    """, (two_hours_ago,)).fetchall()

    # 全部历史统计
    all_trades = conn.execute("""
        SELECT COUNT(*), SUM(pnl_u), AVG(pnl_u)
        FROM trades
    """).fetchone()

    win_rate = conn.execute("SELECT COUNT(*) FROM trades WHERE pnl_u > 0").fetchone()[0]
    total_trades = all_trades[0] or 0

    lines = ["📊 *模拟交易汇总*\n"]
    lines.append(f"起始: {INIT_CAPITAL:.2f}U | 当前: {capital:.2f}U | 总PnL: {fmt_u(total_pnl)}\n")

    if total_trades > 0:
        wr = win_rate / total_trades * 100
        avg = all_trades[2] or 0
        lines.append(f"历史胜率: {wr:.0f}% ({win_rate}/{total_trades}) | 均盈亏: {fmt_u(avg)}\n")

    if recent:
        total_recent_pnl = sum(r[7] for r in recent)
        lines.append(f"\n⏱ 近2h交易 ({len(recent)}笔) PnL: {fmt_u(total_recent_pnl)}\n")
        for name, sym, ets, ex_ts, emc, xmc, size, pnl, reason, hold in recent:
            exit_tag = {"SL": "🔴止损", "TP": "🟢止盈", "TIMEOUT": "⏱超时"}.get(reason, reason)
            lines.append(
                f"{exit_tag} {sym} | {fmt_u(pnl)} | "
                f"{hold/3600:.1f}h | MC{emc:.0f}→{xmc:.0f}"
            )
            lines.append("")
    else:
        lines.append("\n近2h无结算交易\n")

    lines.append(f"\n💰 当前资金: {capital:.2f}U | 连续亏损: {consecutive_loss}次")
    if consecutive_loss >= 3 or capital <= STOP_LINE:
        lines.append(f"\n⚠️ 已触及停止线 ({STOP_LINE}U)，建议暂停!")

    conn.close()
    return "\n".join(lines)

# ============================================================
# 信号接收（从文件）
# ============================================================

def load_signals():
    """读取 narrative_radar 写入的待处理信号"""
    if not os.path.exists(SIGNAL_FILE):
        return []
    try:
        with open(SIGNAL_FILE) as f:
            data = json.load(f)
        # 写完就清空
        with open(SIGNAL_FILE, "w") as f:
            json.dump([], f)
        return data
    except:
        return []

def write_mc_cache(tokens):
    """把当前扫描的MC快照写缓存，让sim_trade读"""
    cache = [{"address": t["address"], "mc": t.get("mc", 0), "price": t.get("price", 0)} for t in tokens]
    with open(os.path.join(DATA_DIR, "momentum_tracker_cache.json"), "w") as f:
        json.dump(cache, f)

# ============================================================
# 主循环
# ============================================================

def main():
    global capital, consecutive_loss

    os.makedirs(DATA_DIR, exist_ok=True)
    load_state()
    conn = init_db()

    log(f"={' '*40}")
    log(f"模拟交易计算器启动 | 资金: {capital:.2f}U | SL={SL_PCT*100:.0f}% TP={TP_PCT*100:.0f}%")
    log(f"={' '*40}")

    last_report_ts = 0
    REPORT_INTERVAL = 7200  # 2h一报

    while True:
        try:
            now = time.time()

            # 1. 接收新信号 -> 开仓
            new_signals = load_signals()
            for sig in new_signals:
                addr = sig["address"]
                if open_position(addr, sig):
                    save_state()

            # 2. 更新MC快照
            current_mcs = get_current_mcs()

            # 3. 扫描持仓
            if positions:
                scan_positions(current_mcs)

            # 4. 定时汇总
            if now - last_report_ts >= REPORT_INTERVAL:
                report = generate_report()
                if report:
                    log("=" * 40)
                    log(report)
                    tg_send(report)
                    log("=" * 40)
                last_report_ts = now

            save_state()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log("模拟交易器退出")
            save_state()
            break
        except Exception as e:
            log(f"[异常] {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
