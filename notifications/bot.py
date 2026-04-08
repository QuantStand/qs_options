"""
bot.py — Telegram command handlers for on-demand queries.

Responds 24/7 — not restricted to market hours.
Commands: /status, /opportunities, /pnl, /rules, /health, /help

All handlers are async, as required by python-telegram-bot v20+.
"""

import json
import logging
import subprocess
from datetime import datetime, date, timezone
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from engine.screener import OptionScreener
from notifications.alerts import _load_positions

log = logging.getLogger(__name__)

# ── SQL helpers ───────────────────────────────────────────────────────────────

_ALL_POSITIONS_SQL = """
SELECT
    t.trade_id,
    t.trade_mode,
    t.expiry_date,
    t.strike,
    t.contracts,
    t.entry_price,
    t.entry_underlying_price,
    t.premium_collected,
    t.collateral_assigned,
    t.entry_delta,
    t.entry_dte,
    u.symbol,
    ocs.underlying_price  AS current_underlying_price,
    ocs.mid               AS current_put_mid,
    ocs.delta             AS current_delta,
    ocs.dte               AS current_dte
FROM trade_log t
JOIN underlyings u ON t.underlying_id = u.underlying_id
LEFT JOIN options_chain_snapshots ocs ON
    ocs.underlying_id   = t.underlying_id
    AND ocs.strike      = t.strike
    AND ocs.expiry_date = t.expiry_date
    AND ocs.option_type = 'P'
    AND ocs.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM options_chain_snapshots
        WHERE underlying_id = t.underlying_id
    )
WHERE t.exit_time IS NULL
ORDER BY t.trade_mode DESC, t.entry_time;
"""

_PNL_SQL = """
SELECT
    t.trade_mode,
    u.symbol,
    t.strike,
    t.contracts,
    t.entry_price,
    t.realised_pnl,
    t.pnl_pct_of_premium,
    t.exit_reason,
    ocs.mid AS current_put_mid
FROM trade_log t
JOIN underlyings u ON t.underlying_id = u.underlying_id
LEFT JOIN options_chain_snapshots ocs ON
    ocs.underlying_id   = t.underlying_id
    AND ocs.strike      = t.strike
    AND ocs.expiry_date = t.expiry_date
    AND ocs.option_type = 'P'
    AND ocs.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM options_chain_snapshots
        WHERE underlying_id = t.underlying_id
    )
ORDER BY t.trade_mode DESC, t.entry_time;
"""

_RULES_SQL = """
SELECT
    t.trade_id,
    t.expiry_date,
    t.strike,
    t.contracts,
    t.entry_price,
    t.entry_underlying_price,
    t.entry_delta,
    t.entry_time,
    u.symbol,
    ocs.underlying_price  AS current_underlying_price,
    ocs.mid               AS current_put_mid,
    ocs.delta             AS current_delta,
    ocs.dte               AS current_dte
FROM trade_log t
JOIN underlyings u ON t.underlying_id = u.underlying_id
LEFT JOIN options_chain_snapshots ocs ON
    ocs.underlying_id   = t.underlying_id
    AND ocs.strike      = t.strike
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

_INCEPTION_DATE_SQL = """
SELECT MIN(entry_time) FROM trade_log WHERE trade_mode = 'LIVE';
"""


def _expiry_display(expiry_date) -> str:
    if isinstance(expiry_date, date):
        return expiry_date.strftime("%b%d")
    return str(expiry_date)


def _fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def _query(pool, sql: str, params=None) -> list[dict]:
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        pool.putconn(conn)


def _scalar(pool, sql: str, params=None):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        pool.putconn(conn)


# ── /status ───────────────────────────────────────────────────────────────────

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool   = context.bot_data["pool"]
    config = context.bot_data["config"]

    now_et = datetime.now(timezone.utc).strftime("%H:%M UTC")
    sgov_nav   = config.get("portfolio", {}).get("sgov_nav_usd", 0)
    daily_sgov = sgov_nav * 0.044 / 365

    try:
        rows = _query(pool, _ALL_POSITIONS_SQL)
    except Exception as exc:
        await update.message.reply_text(f"❌ DB error: {exc}")
        return

    live  = [r for r in rows if r["trade_mode"] == "LIVE"]
    paper = [r for r in rows if r["trade_mode"] == "PAPER"]

    lines = [f"📊 *POSITION STATUS — {now_et}*\n"]

    lines.append(f"*LIVE ({len(live)} open):*")
    if live:
        for pos in live:
            strike   = float(pos["strike"])
            expiry   = _expiry_display(pos["expiry_date"])
            und_str  = f"${float(pos['current_underlying_price']):.2f}" if pos["current_underlying_price"] else "N/A"
            put_str  = f"${float(pos['current_put_mid']):.2f}" if pos["current_put_mid"] else "N/A"
            ep       = float(pos["entry_price"] or 0)
            cm       = float(pos["current_put_mid"] or 0)
            pnl_usd  = (ep - cm) * 100 * int(pos["contracts"]) if ep and cm else None
            pnl_str  = f"+${pnl_usd:,.0f} ({_fmt_pct((ep - cm)/ep)})" if pnl_usd and ep > 0 else "N/A"
            lines.append(f"• {pos['symbol']} ${strike:.0f}P exp {expiry} — {pos['symbol']}@{und_str} | Put@{put_str} | P&L: {pnl_str}")
    else:
        lines.append("No open live positions.")

    lines.append(f"\n*PAPER ({len(paper)} open):*")
    if paper:
        for pos in paper:
            strike = float(pos["strike"])
            expiry = _expiry_display(pos["expiry_date"])
            ep     = float(pos["entry_price"] or 0)
            cm     = float(pos["current_put_mid"] or 0)
            pnl_usd = (ep - cm) * 100 * int(pos["contracts"]) if ep and cm else None
            pnl_str = f"+${pnl_usd:,.0f}" if pnl_usd else "N/A"
            lines.append(f"• {pos['symbol']} ${strike:.0f}P exp {expiry} — P&L: {pnl_str}")
    else:
        lines.append("No open paper positions.")

    lines.append(f"\n*SGOV:* ${sgov_nav:,.0f} earning ~${daily_sgov:,.0f}/day")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /opportunities ────────────────────────────────────────────────────────────

async def cmd_opportunities(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool   = context.bot_data["pool"]
    config = context.bot_data["config"]
    now_et = datetime.now(timezone.utc).strftime("%H:%M UTC")

    await update.message.reply_text(
        f"🔍 *LIVE SCREENER — {now_et}*\n\nRunning across all underlyings...",
        parse_mode="Markdown",
    )

    try:
        screener = OptionScreener(pool, config)
        results  = screener.run()
        passing  = [c for c in results if c.passed_all_filters]
        top3     = passing[:3]
    except Exception as exc:
        await update.message.reply_text(f"❌ Screener error: {exc}")
        return

    if not top3:
        await update.message.reply_text("No contracts passing all filters right now.")
        return

    contracts_per_trade = config.get("position_management", {}).get("contracts_per_trade", 5)
    lines = []
    for i, c in enumerate(top3, 1):
        expiry   = _expiry_display(c.expiry_date)
        iv_pct   = c.iv_percentile_52w or c.iv_percentile_30d or 0.0
        lines.append(
            f"*#{i} {c.symbol} ${c.strike:.0f}P exp {expiry}*\n"
            f"   Score: {c.composite_score:.2f} | ROI: {_fmt_pct(c.annualised_roi)} "
            f"| PoP: {_fmt_pct(c.prob_profit)} | IV%: {iv_pct:.0f}\n"
            f"   Premium ({contracts_per_trade} contracts): ${c.total_premium_5_contracts:,.0f} "
            f"| Entry if assigned: ${c.effective_entry_if_assigned:.2f}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


# ── /pnl ──────────────────────────────────────────────────────────────────────

async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool   = context.bot_data["pool"]
    config = context.bot_data["config"]

    try:
        rows          = _query(pool, _PNL_SQL)
        inception_raw = _scalar(pool, _INCEPTION_DATE_SQL)
    except Exception as exc:
        await update.message.reply_text(f"❌ DB error: {exc}")
        return

    live_open  = [r for r in rows if r["trade_mode"] == "LIVE"  and r["realised_pnl"] is None]
    paper_rows = [r for r in rows if r["trade_mode"] == "PAPER"]
    paper_closed = [r for r in paper_rows if r["realised_pnl"] is not None]

    # Live unrealised P&L
    live_unrealised = 0.0
    live_detail_lines = []
    for pos in live_open:
        ep = float(pos["entry_price"] or 0)
        cm = float(pos["current_put_mid"] or 0)
        if ep > 0 and cm > 0:
            pnl = (ep - cm) * 100 * int(pos["contracts"])
            live_unrealised += pnl
            live_detail_lines.append(
                f"  Unrealised: +${pnl:,.0f} ({pos['symbol']} ${float(pos['strike']):.0f}P)"
            )

    # Paper stats
    paper_n     = len(paper_closed)
    paper_total = sum(float(r["realised_pnl"]) for r in paper_closed)
    paper_wins  = sum(1 for r in paper_closed if float(r["realised_pnl"] or 0) > 0)
    win_rate    = paper_wins / paper_n if paper_n > 0 else 0

    # SGOV income estimate
    sgov_nav     = config.get("portfolio", {}).get("sgov_nav_usd", 0)
    inception_dt = inception_raw
    if inception_dt:
        days_since = max(1, (datetime.now(timezone.utc) - inception_dt).days)
        sgov_income = sgov_nav * 0.044 / 365 * days_since
        sgov_str    = f"+${sgov_income:,.0f} since inception ({days_since}d)"
    else:
        sgov_str = "N/A"

    lines = [
        "💰 *P&L SUMMARY*\n",
        "*Live trades:*",
        f"  Realised:   $0",
    ]
    if live_detail_lines:
        lines.extend(live_detail_lines)
    else:
        lines.append("  Unrealised: $0")

    lines += [
        f"\n*Paper trades:*",
        f"  Closed: {paper_n} | Total P&L: ${paper_total:,.0f} | Win rate: {win_rate:.0%}",
        f"\n*SGOV income (est.):* {sgov_str}",
    ]

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /rules ────────────────────────────────────────────────────────────────────

async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool   = context.bot_data["pool"]
    config = context.bot_data["config"]

    try:
        positions = _query(pool, _RULES_SQL)
    except Exception as exc:
        await update.message.reply_text(f"❌ DB error: {exc}")
        return

    if not positions:
        await update.message.reply_text("No open LIVE positions to evaluate.")
        return

    rules_cfg    = config["rules"]
    earnings_cfg = config.get("earnings_calendar", {})

    all_lines = []
    for pos in positions:
        symbol      = pos["symbol"]
        strike      = float(pos["strike"])
        expiry_date = pos["expiry_date"]
        expiry      = _expiry_display(expiry_date)
        entry_price = float(pos["entry_price"] or 0)
        current_mid = float(pos["current_put_mid"]) if pos["current_put_mid"] else None
        current_und = float(pos["current_underlying_price"]) if pos["current_underlying_price"] else None
        delta_abs   = abs(float(pos["current_delta"])) if pos["current_delta"] else None
        dte         = int(pos["current_dte"]) if pos["current_dte"] else None

        # Rule 1 status
        if current_mid and entry_price > 0:
            dte_boundary   = rules_cfg["early_close_dte_boundary"]
            threshold      = (rules_cfg["early_close_threshold_tight"]
                              if dte and dte <= dte_boundary
                              else rules_cfg["early_close_threshold_standard"])
            threshold_usd  = entry_price * threshold
            r1_status      = (f"TRIGGERED ✅" if current_mid <= threshold_usd
                              else f"NOT triggered | Put@${current_mid:.2f}")
            r1_threshold   = f"${threshold_usd:.2f}"
        else:
            r1_status, r1_threshold = "N/A", "N/A"

        # Rule 2 status
        earnings_str = earnings_cfg.get(symbol)
        if earnings_str:
            try:
                earnings_date = date.fromisoformat(str(earnings_str))
                if earnings_date > expiry_date:
                    r2_status = f"DEACTIVATED — earnings {earnings_date.strftime('%b %d')} after expiry"
                else:
                    days_to = (earnings_date - date.today()).days
                    r2_status = f"⚠️ ACTIVE — earnings {earnings_date.strftime('%b %d')} ({days_to}d)"
            except ValueError:
                r2_status = "date parse error"
        else:
            r2_status = "DEACTIVATED — no earnings date configured"

        # Rule 3 status
        stop_threshold = rules_cfg["stop_loss_underlying_price"]
        if current_und:
            r3_status = (f"NOT triggered | {symbol}@${current_und:.2f}"
                         if current_und >= stop_threshold
                         else f"⚠️ TRIGGERED | {symbol}@${current_und:.2f} < ${stop_threshold:.0f}")
        else:
            r3_status = "N/A"

        # Rule 4 (assignment readiness)
        effective_entry = strike - entry_price
        r4_status = f"Ready to accept at ${effective_entry:.2f}"

        # Overall status
        triggered = ("⚠️" in r2_status or "TRIGGERED" in r3_status)
        overall   = "⚠️ Action required" if triggered else "All rules: CLEAR ✅"

        all_lines.append(
            f"📋 *POSITION RULES — {symbol} ${strike:.0f}P exp {expiry}*\n"
            f"\n"
            f"Rule 1 (early close ≤{r1_threshold}): {r1_status}\n"
            f"Rule 2 (pre-earnings):  {r2_status}\n"
            f"Rule 3 (reassess at ${stop_threshold:.0f}): {r3_status}\n"
            f"Rule 4 (assignment): {r4_status}\n"
            f"\n"
            f"{overall}"
        )

    await update.message.reply_text("\n\n---\n\n".join(all_lines), parse_mode="Markdown")


# ── /health ───────────────────────────────────────────────────────────────────

async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool = context.bot_data["pool"]
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── PM2 process status ────────────────────────────────────────────────────
    pm2_lines = []
    try:
        result = subprocess.run(
            ["pm2", "jlist"], capture_output=True, text=True, timeout=10
        )
        processes = json.loads(result.stdout)
        expected  = ["candle-collector", "options-collector", "paper-trader", "qs-notifier"]
        pm2_status = {p["name"]: p for p in processes}
        for name in expected:
            proc = pm2_status.get(name)
            if proc:
                status  = proc.get("pm2_env", {}).get("status", "unknown")
                uptime  = proc.get("pm2_env", {}).get("pm_uptime", 0)
                uptime_h = int((datetime.now().timestamp() * 1000 - uptime) / 3600000)
                icon     = "✅" if status == "online" else "❌"
                pm2_lines.append(f"  {name}: {icon} {status} [{uptime_h}h uptime]")
            else:
                pm2_lines.append(f"  {name}: ❓ not found")
    except (FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        pm2_lines.append(f"  PM2 query failed: {exc}")

    # ── Last snapshot time ────────────────────────────────────────────────────
    try:
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(snapshot_time), COUNT(DISTINCT underlying_id)
                    FROM options_chain_snapshots
                    WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
                """)
                row = cur.fetchone()
                last_snap  = row[0]
                und_count  = int(row[1]) if row[1] else 0

                cur.execute("SELECT COUNT(*) FROM underlyings WHERE active = TRUE")
                total_und = cur.fetchone()[0]
        finally:
            pool.putconn(conn)

        if last_snap:
            age_min = int((datetime.now(timezone.utc) - last_snap).total_seconds() / 60)
            snap_str = f"{last_snap.strftime('%H:%M UTC')} ({age_min} min ago)"
        else:
            snap_str = "none today"
    except Exception as exc:
        snap_str, und_count, total_und = f"DB error: {exc}", 0, 9

    # ── Next briefing ─────────────────────────────────────────────────────────
    briefing_str = "Today 16:30 ET"

    lines = [
        f"⚙️ *SYSTEM HEALTH — {now}*\n",
        "*PM2:*",
    ] + pm2_lines + [
        f"\nLast snapshot: {snap_str}",
        f"Underlyings with data: {und_count}/{total_und}",
        f"Next briefing: {briefing_str}",
    ]

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /help ─────────────────────────────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "🤖 *QuantStand Bot — Commands*\n"
        "\n"
        "/status — Open positions with live P&L\n"
        "/opportunities — Run live screener, show top 3\n"
        "/pnl — Full P&L summary (live + paper + SGOV)\n"
        "/rules — Rule check for each live position\n"
        "/health — PM2 process status and data freshness\n"
        "/help — This message"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ── Handler registration ──────────────────────────────────────────────────────

def register_handlers(app: Application) -> None:
    """Register all command handlers on an already-built Application."""
    app.add_handler(CommandHandler("status",        cmd_status))
    app.add_handler(CommandHandler("opportunities", cmd_opportunities))
    app.add_handler(CommandHandler("pnl",           cmd_pnl))
    app.add_handler(CommandHandler("rules",         cmd_rules))
    app.add_handler(CommandHandler("health",        cmd_health))
    app.add_handler(CommandHandler("help",          cmd_help))
    app.add_handler(CommandHandler("start",         cmd_help))
