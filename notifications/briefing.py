"""
briefing.py — Daily end-of-day briefing sent at 16:30 ET.

Queries live positions, paper trades, top screener opportunities,
and system health. Formats into a single Telegram message that fits
on one phone screen.
"""

import logging
import subprocess
from datetime import datetime, date, timezone
from typing import Optional

import psycopg2.pool

from engine.screener import OptionScreener

log = logging.getLogger(__name__)


# ── SQL helpers ───────────────────────────────────────────────────────────────

_LIVE_POSITIONS_SQL = """
SELECT
    t.trade_id,
    t.expiry_date,
    t.strike,
    t.contracts,
    t.entry_price,
    t.entry_underlying_price,
    t.premium_collected,
    u.symbol,
    ocs.underlying_price  AS current_underlying_price,
    ocs.mid               AS current_put_mid,
    ocs.delta             AS current_delta,
    ocs.dte               AS current_dte
FROM trade_log t
JOIN underlyings u ON t.underlying_id = u.underlying_id
LEFT JOIN options_chain_snapshots ocs ON
    ocs.underlying_id  = t.underlying_id
    AND ocs.strike     = t.strike
    AND ocs.expiry_date = t.expiry_date
    AND ocs.option_type = 'P'
    AND ocs.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM options_chain_snapshots
        WHERE underlying_id = t.underlying_id
    )
WHERE t.trade_mode = 'LIVE'
  AND t.exit_time IS NULL
ORDER BY t.entry_time;
"""

_PAPER_OPEN_SQL = """
SELECT u.symbol, t.strike, t.expiry_date
FROM trade_log t
JOIN underlyings u ON t.underlying_id = u.underlying_id
WHERE t.trade_mode = 'PAPER'
  AND t.exit_time IS NULL
ORDER BY t.entry_time;
"""

_PAPER_CLOSED_TODAY_SQL = """
SELECT
    COUNT(*)                      AS closed_count,
    AVG(t.pnl_pct_of_premium)    AS avg_pnl_pct
FROM trade_log t
WHERE t.trade_mode = 'PAPER'
  AND DATE(t.exit_time AT TIME ZONE 'America/New_York') = CURRENT_DATE;
"""

_SNAPSHOT_HEALTH_SQL = """
SELECT
    COUNT(DISTINCT DATE_TRUNC('minute', snapshot_time)) AS cycles_today,
    COUNT(DISTINCT underlying_id)                       AS underlyings_covered
FROM options_chain_snapshots
WHERE DATE(snapshot_time AT TIME ZONE 'America/New_York') = CURRENT_DATE;
"""


# ── Formatters ────────────────────────────────────────────────────────────────

def _expiry_display(expiry_date: date) -> str:
    return expiry_date.strftime("%b%d")


def _fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def _status_emoji(delta_abs: float, dte: int) -> str:
    if delta_abs > 0.40 or dte < 5:
        return "🔴 ACT"
    if delta_abs > 0.25:
        return "🟠 WATCH"
    return "🟢 SAFE"


# ── Section builders ──────────────────────────────────────────────────────────

def _build_live_section(positions: list[dict]) -> str:
    n = len(positions)
    lines = [f"━━━━━━━━━━━━━━━━━━━━━━", f"LIVE POSITIONS ({n} open)", "━━━━━━━━━━━━━━━━━━━━━━"]

    if not positions:
        lines.append("No open live positions.")
        return "\n".join(lines)

    for pos in positions:
        symbol       = pos["symbol"]
        strike       = float(pos["strike"])
        expiry       = _expiry_display(pos["expiry_date"])
        und_price    = pos["current_underlying_price"]
        put_mid      = pos["current_put_mid"]
        entry_price  = float(pos["entry_price"] or 0)
        contracts    = int(pos["contracts"])
        delta_abs    = abs(float(pos["current_delta"])) if pos["current_delta"] else 0.0
        dte          = int(pos["current_dte"]) if pos["current_dte"] else 0

        und_str = f"${float(und_price):.2f}" if und_price else "N/A"
        put_str = f"${float(put_mid):.2f}"   if put_mid  else "N/A"

        if put_mid and entry_price > 0:
            pnl_usd = (entry_price - float(put_mid)) * 100 * contracts
            pnl_pct = (entry_price - float(put_mid)) / entry_price
            pnl_str = f"${pnl_usd:,.0f} (+{_fmt_pct(pnl_pct)})"
        else:
            pnl_str = "N/A"

        status = _status_emoji(delta_abs, dte)
        lines.append(
            f"*{symbol} ${strike:.0f}P exp {expiry}*\n"
            f"  Close: {und_str} | Put: {put_str} | P&L: {pnl_str}\n"
            f"  DTE: {dte} | Status: {status}"
        )

    return "\n".join(lines)


def _build_paper_section(open_pos: list[dict], closed_count: int,
                         avg_pnl_pct: Optional[float]) -> str:
    n_open = len(open_pos)
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"PAPER TRADES ({n_open} open, {closed_count} closed today)",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    if closed_count > 0 and avg_pnl_pct is not None:
        lines.append(
            f"Closed today: {closed_count} | Avg P&L: {_fmt_pct(float(avg_pnl_pct))} of premium"
        )

    if open_pos:
        open_strs = [
            f"{p['symbol']} ${float(p['strike']):.0f}P"
            for p in open_pos
        ]
        lines.append(f"Open: {', '.join(open_strs)}")
    else:
        lines.append("No open paper positions.")

    return "\n".join(lines)


def _build_opportunities_section(screener: OptionScreener, config: dict) -> str:
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━",
        "TOP OPPORTUNITIES (screener)",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    try:
        all_results = screener.run()
        passing = [c for c in all_results if c.passed_all_filters]
        top3    = passing[:3]
    except Exception as exc:
        log.error("Screener failed in briefing: %s", exc)
        lines.append("Screener unavailable.")
        return "\n".join(lines)

    contracts_per_trade = config.get("position_management", {}).get("contracts_per_trade", 5)

    if not top3:
        sgov_nav   = config.get("portfolio", {}).get("sgov_nav_usd", 0)
        daily_sgov = sgov_nav * 0.044 / 365
        lines.append(
            f"No contracts passed all filters today.\n"
            f"SGOV continues to earn ~${daily_sgov:,.0f}/day."
        )
        return "\n".join(lines)

    for i, c in enumerate(top3, 1):
        expiry   = _expiry_display(c.expiry_date)
        premium5 = c.total_premium_5_contracts
        entry    = c.effective_entry_if_assigned
        iv_pct   = c.iv_percentile_52w or c.iv_percentile_30d or 0.0
        lines.append(
            f"*#{i} {c.symbol} ${c.strike:.0f}P exp {expiry}*\n"
            f"   Score: {c.composite_score:.2f} | ROI: {_fmt_pct(c.annualised_roi)} "
            f"| PoP: {_fmt_pct(c.prob_profit)} | IV%: {iv_pct:.0f}\n"
            f"   Premium ({contracts_per_trade} contracts): ${premium5:,.0f} "
            f"| Entry if assigned: ${entry:.2f}"
        )

    return "\n".join(lines)


def _build_health_section(pool: psycopg2.pool.ThreadedConnectionPool,
                          config: dict) -> str:
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━",
        "SYSTEM HEALTH",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # Snapshot stats from DB
    try:
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(_SNAPSHOT_HEALTH_SQL)
                row = cur.fetchone()
                cycles   = int(row[0]) if row else 0
                und_cov  = int(row[1]) if row else 0
        finally:
            pool.putconn(conn)
    except Exception as exc:
        log.error("Health snapshot query failed: %s", exc)
        cycles, und_cov = 0, 0

    # Total underlyings
    try:
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM underlyings WHERE active = TRUE")
                total_und = cur.fetchone()[0]
        finally:
            pool.putconn(conn)
    except Exception:
        total_und = 9

    # SGOV daily yield
    sgov_nav   = config.get("portfolio", {}).get("sgov_nav_usd", 0)
    daily_sgov = sgov_nav * 0.044 / 365

    # Paper trader cycles today (approximate from log or just show status)
    lines.append(f"Snapshots today: {cycles} cycles | Underlyings: {und_cov}/{total_und}")
    lines.append(f"SGOV yield (est.): +${daily_sgov:,.0f} today")

    return "\n".join(lines)


# ── Main entry point ──────────────────────────────────────────────────────────

async def send_daily_briefing(pool, config: dict, bot) -> None:
    """
    Build and send the full daily briefing. Called at 16:30 ET.
    Sends to the configured Telegram chat_id.
    """
    chat_id  = config["telegram"]["chat_id"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    import asyncio

    # ── Query DB ──────────────────────────────────────────────────────────────
    try:
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(_LIVE_POSITIONS_SQL)
                cols = [d[0] for d in cur.description]
                live_positions = [dict(zip(cols, row)) for row in cur.fetchall()]

                cur.execute(_PAPER_OPEN_SQL)
                paper_open = [{"symbol": r[0], "strike": r[1], "expiry_date": r[2]}
                              for r in cur.fetchall()]

                cur.execute(_PAPER_CLOSED_TODAY_SQL)
                row = cur.fetchone()
                paper_closed_count   = int(row[0]) if row else 0
                paper_avg_pnl        = float(row[1]) if row and row[1] else None
        finally:
            pool.putconn(conn)
    except Exception as exc:
        log.error("send_daily_briefing: DB error: %s", exc)
        return

    # ── Build screener ────────────────────────────────────────────────────────
    screener = OptionScreener(pool, config)

    # ── Assemble message ──────────────────────────────────────────────────────
    header = f"📈 *QUANTSTAND DAILY BRIEFING — {today_str}*\n"

    sections = [
        header,
        _build_live_section(live_positions),
        _build_paper_section(paper_open, paper_closed_count, paper_avg_pnl),
        _build_opportunities_section(screener, config),
        _build_health_section(pool, config),
    ]

    message = "\n\n".join(sections)

    # Telegram has a 4096-char limit — truncate gracefully if needed
    if len(message) > 4000:
        message = message[:3990] + "\n\n_(truncated)_"

    for attempt in range(1, 3):
        try:
            await bot.send_message(chat_id=chat_id, text=message,
                                   parse_mode="Markdown")
            log.info("Daily briefing sent successfully")
            return
        except Exception as exc:
            if attempt == 1:
                log.warning("Briefing send failed (attempt 1), retrying in 30s: %s", exc)
                await asyncio.sleep(30)
            else:
                log.error("Briefing send failed after 2 attempts: %s", exc)
