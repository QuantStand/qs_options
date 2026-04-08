"""
alerts.py — Real-time position rule monitoring.

Runs every 5 minutes during market hours (09:30–16:00 ET).
Checks every open LIVE position against Rules 1–3 and the morning check.
Sends a Telegram message immediately when any rule triggers.

Alert deduplication: the same (trade_id, rule) combination is suppressed
for one hour after its first send. State is in-memory — resets on restart,
which is intentional (better to re-alert than to miss).
"""

import logging
from datetime import datetime, date, timedelta, timezone
from typing import Optional

import psycopg2.pool

log = logging.getLogger(__name__)

# ── In-memory deduplication store ────────────────────────────────────────────
# Key: (trade_id, rule_name)   Value: datetime the alert was last sent (UTC)
_sent_alerts: dict[tuple, datetime] = {}
_DEDUP_WINDOW_HOURS = 1

# ── SQL: open live positions with latest snapshot data ────────────────────────
_POSITIONS_SQL = """
SELECT
    t.trade_id,
    t.underlying_id,
    t.expiry_date,
    t.strike,
    t.option_type,
    t.contracts,
    t.entry_time,
    t.entry_price,
    t.entry_underlying_price,
    t.entry_delta,
    t.premium_collected,
    t.collateral_assigned,
    u.symbol,
    ocs.underlying_price  AS current_underlying_price,
    ocs.mid               AS current_put_mid,
    ocs.bid               AS current_bid,
    ocs.ask               AS current_ask,
    ocs.delta             AS current_delta,
    ocs.dte               AS current_dte,
    ocs.snapshot_time     AS latest_snapshot_time
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
  AND t.exit_time IS NULL;
"""

# ── Staleness guard: 20 minutes ───────────────────────────────────────────────
_MAX_SNAPSHOT_AGE_MINUTES = 20
_STALE_ALERT_KEY = "__stale_data__"


def _is_deduped(trade_id: int, rule: str) -> bool:
    """Return True if this alert was already sent within the dedup window."""
    key = (trade_id, rule)
    last_sent = _sent_alerts.get(key)
    if last_sent is None:
        return False
    age = datetime.now(timezone.utc) - last_sent
    return age.total_seconds() < _DEDUP_WINDOW_HOURS * 3600


def _mark_sent(trade_id: int, rule: str) -> None:
    _sent_alerts[(trade_id, rule)] = datetime.now(timezone.utc)


def _fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def _expiry_display(expiry_date: date) -> str:
    return expiry_date.strftime("%b%d")


def _load_positions(pool: psycopg2.pool.ThreadedConnectionPool) -> list[dict]:
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(_POSITIONS_SQL)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        pool.putconn(conn)


# ── Rule 1: Early close opportunity ──────────────────────────────────────────

def _check_rule_1(pos: dict, rules_cfg: dict) -> Optional[str]:
    """
    Returns a formatted alert string if Rule 1 triggers, else None.

    Rule 1: Current put mid has decayed to ≤ threshold% of entry price.
    Threshold is looser before the DTE boundary, tighter inside it.
    """
    entry_price = float(pos["entry_price"] or 0)
    current_mid = pos["current_put_mid"]
    current_dte = pos["current_dte"]

    if entry_price <= 0 or current_mid is None or current_dte is None:
        return None

    current_mid = float(current_mid)
    current_dte = int(current_dte)
    dte_boundary = rules_cfg["early_close_dte_boundary"]

    threshold = (
        rules_cfg["early_close_threshold_tight"]
        if current_dte <= dte_boundary
        else rules_cfg["early_close_threshold_standard"]
    )

    pct_remaining = current_mid / entry_price
    if pct_remaining > threshold:
        return None

    symbol   = pos["symbol"]
    strike   = float(pos["strike"])
    expiry   = _expiry_display(pos["expiry_date"])
    contracts = int(pos["contracts"])
    pnl_per_contract = (entry_price - current_mid) * 100
    total_pnl = pnl_per_contract * contracts
    premium_pct = (entry_price - current_mid) / entry_price

    suggested_limit = max(0.05, current_mid - 0.10)

    return (
        f"🟢 *EARLY CLOSE OPPORTUNITY — {symbol} ${strike:.0f}P {expiry}*\n"
        f"\n"
        f"Put has decayed to ${current_mid:.2f} ({_fmt_pct(pct_remaining)} of entry ${entry_price:.2f}).\n"
        f"Early close threshold: {_fmt_pct(threshold)} — TRIGGERED.\n"
        f"\n"
        f"Profit if closed: ${total_pnl:,.0f} ({_fmt_pct(premium_pct)} of premium)\n"
        f"Recommended limit order: ${suggested_limit:.2f}\n"
        f"\n"
        f"Rule 1: Place buy-to-close limit order at ${suggested_limit:.2f}."
    )


# ── Rule 2: Pre-earnings hard close ──────────────────────────────────────────

def _check_rule_2(pos: dict, rules_cfg: dict, earnings_cfg: dict) -> Optional[str]:
    """
    Returns alert string if earnings fall within the warning window BEFORE expiry.
    Returns None if earnings are after expiry (rule deactivated) or too far out.
    """
    symbol = pos["symbol"]
    expiry_date = pos["expiry_date"]
    earnings_str = earnings_cfg.get(symbol)

    if not earnings_str:
        return None

    try:
        earnings_date = date.fromisoformat(str(earnings_str))
    except (ValueError, TypeError):
        log.warning("Invalid earnings date for %s: %s", symbol, earnings_str)
        return None

    # Rule 2 deactivated: earnings fall AFTER expiry — position expires safely
    if earnings_date > expiry_date:
        return None

    today = date.today()
    days_to_earnings = (earnings_date - today).days
    warning_dte = rules_cfg.get("earnings_warning_dte", 5)

    if days_to_earnings > warning_dte:
        return None

    # Triggered: earnings within warning window and before expiry
    strike   = float(pos["strike"])
    expiry   = _expiry_display(expiry_date)
    contracts = int(pos["contracts"])
    current_ask = pos.get("current_ask")
    entry_price  = float(pos["entry_price"] or 0)
    cost_to_close = float(current_ask) if current_ask else None
    pnl = (entry_price - float(current_ask)) * 100 * contracts if current_ask else None
    pct_pnl = (entry_price - float(current_ask)) / entry_price if current_ask else None

    cost_str = f"${float(current_ask):.2f}" if current_ask else "N/A"
    pnl_str  = f"${pnl:,.0f} ({_fmt_pct(pct_pnl)} of premium)" if pnl else "N/A"

    return (
        f"🔴 *HARD CLOSE REQUIRED — {symbol} ${strike:.0f}P {expiry}*\n"
        f"\n"
        f"Earnings on {earnings_date.strftime('%b %d')} — {days_to_earnings} day(s) before expiry.\n"
        f"Position must be closed TODAY per Rule 2.\n"
        f"\n"
        f"Current put ask: {cost_str}\n"
        f"Profit if closed now: {pnl_str}\n"
        f"\n"
        f"Open IBKR and close this position."
    )


# ── Rule 3: Stop-loss / reassessment ─────────────────────────────────────────

def _check_rule_3(pos: dict, rules_cfg: dict) -> Optional[str]:
    """
    Returns alert string if the underlying has dropped below the reassessment
    trigger price. Recommendation is CLOSE NOW or HOLD AND MONITOR based on
    delta and DTE conditions.
    """
    current_price = pos["current_underlying_price"]
    current_delta = pos["current_delta"]
    current_dte   = pos["current_dte"]

    if current_price is None:
        return None

    current_price = float(current_price)
    threshold     = float(rules_cfg["stop_loss_underlying_price"])

    if current_price >= threshold:
        return None

    symbol   = pos["symbol"]
    strike   = float(pos["strike"])
    expiry   = _expiry_display(pos["expiry_date"])

    delta_abs = abs(float(current_delta)) if current_delta else None
    dte_val   = int(current_dte) if current_dte else None
    entry_price = float(pos["entry_price"] or 0)
    current_mid = pos["current_put_mid"]
    contracts   = int(pos["contracts"])

    delta_display = f"{delta_abs:.2f}" if delta_abs else "N/A"
    delta_status  = (
        "ELEVATED — above 0.40" if (delta_abs and delta_abs > rules_cfg["stop_loss_delta_threshold"])
        else "OK"
    ) if delta_abs else "N/A"

    close_cost = float(current_mid) if current_mid else None
    pnl        = (entry_price - close_cost) * 100 * contracts if close_cost else None
    pct_pnl    = (entry_price - close_cost) / entry_price if close_cost and entry_price > 0 else None
    cost_str   = f"${close_cost:.2f} ({_fmt_pct(pct_pnl)} of premium)" if close_cost else "N/A"
    pnl_str    = f"${pnl:,.0f}" if pnl else "N/A"

    # Determine recommendation
    should_close = (
        delta_abs is not None and delta_abs > rules_cfg["stop_loss_delta_threshold"]
        and dte_val is not None and dte_val < rules_cfg["stop_loss_dte_threshold"]
    )
    recommendation = "CLOSE NOW" if should_close else "HOLD AND MONITOR"

    return (
        f"🟠 *REASSESSMENT TRIGGERED — {symbol} ${strike:.0f}P*\n"
        f"\n"
        f"{symbol} at ${current_price:.2f} — below ${threshold:.0f} trigger.\n"
        f"\n"
        f"Checklist:\n"
        f"• Current delta: {delta_display} ({delta_status})\n"
        f"• DTE remaining: {dte_val if dte_val else 'N/A'}\n"
        f"• Buy-to-close cost: {cost_str}\n"
        f"• Profit if closed: {pnl_str}\n"
        f"\n"
        f"Status: *{recommendation}*"
    )


# ── Morning position check (once at 09:35 ET) ─────────────────────────────────

def build_morning_check(positions: list[dict]) -> str:
    """Format the 09:35 ET morning position summary."""
    today = date.today().strftime("%Y-%m-%d")

    if not positions:
        return (
            f"📊 *MORNING CHECK — {today}*\n"
            f"\n"
            f"No open LIVE positions."
        )

    lines = [f"📊 *MORNING CHECK — {today}*\n"]
    all_clear = True

    for pos in positions:
        symbol        = pos["symbol"]
        strike        = float(pos["strike"])
        expiry        = _expiry_display(pos["expiry_date"])
        und_price     = pos["current_underlying_price"]
        put_mid       = pos["current_put_mid"]
        current_delta = pos["current_delta"]
        dte           = pos["current_dte"]
        entry_price   = float(pos["entry_price"] or 0)
        contracts     = int(pos["contracts"])

        und_str   = f"${float(und_price):.2f}" if und_price else "N/A"
        put_str   = f"${float(put_mid):.2f}" if put_mid else "N/A"
        delta_str = f"{abs(float(current_delta)):.2f}" if current_delta else "N/A"
        dte_str   = str(int(dte)) if dte else "N/A"

        if put_mid and entry_price > 0:
            pnl_usd = (entry_price - float(put_mid)) * 100 * contracts
            pnl_pct = (entry_price - float(put_mid)) / entry_price
            pnl_str = f"${pnl_usd:,.0f} (+{_fmt_pct(pnl_pct)})"
        else:
            pnl_str = "N/A"

        # Status emoji
        delta_abs = abs(float(current_delta)) if current_delta else 0
        if delta_abs > 0.40 or (dte and int(dte) < 5):
            status = "🔴 ACT"
            all_clear = False
        elif delta_abs > 0.25:
            status = "🟠 WATCH"
            all_clear = False
        else:
            status = "🟢 SAFE"

        lines.append(
            f"*{symbol} ${strike:.0f}P exp {expiry}*\n"
            f"  Stock: {und_str} | Put: {put_str} | P&L: {pnl_str}\n"
            f"  DTE: {dte_str} | Delta: {delta_str} | Status: {status}"
        )

    summary = "All rules: ALL CLEAR ✅" if all_clear else "⚠️ Action required — see above"
    lines.append(f"\n{summary}")
    return "\n\n".join(lines)


# ── Stale data warning ────────────────────────────────────────────────────────

def _check_stale_data(positions: list[dict]) -> Optional[str]:
    """
    Return a warning message if market hours are active and the most recent
    snapshot is older than _MAX_SNAPSHOT_AGE_MINUTES.
    Suppressed to once per hour via deduplication.
    """
    now_utc = datetime.now(timezone.utc)
    for pos in positions:
        snap_time = pos.get("latest_snapshot_time")
        if snap_time is None:
            continue
        age_minutes = (now_utc - snap_time).total_seconds() / 60
        if age_minutes > _MAX_SNAPSHOT_AGE_MINUTES:
            return (
                "⚠️ *DATA WARNING*\n"
                f"\n"
                f"No chain snapshot in {int(age_minutes)}+ minutes.\n"
                f"options-collector may have an issue.\n"
                f"Check: `pm2 logs options-collector`"
            )
    return None


# ── Main entry point ──────────────────────────────────────────────────────────

async def check_all_alerts(pool, config: dict, bot) -> None:
    """
    Called every 5 minutes by the scheduler during market hours.
    Evaluates all open LIVE positions against Rules 1–3.
    Sends Telegram messages for any triggered rules, with deduplication.
    """
    rules_cfg    = config["rules"]
    earnings_cfg = config.get("earnings_calendar", {})
    chat_id      = config["telegram"]["chat_id"]

    try:
        positions = _load_positions(pool)
    except Exception as exc:
        log.error("check_all_alerts: DB error loading positions: %s", exc)
        return

    if not positions:
        log.debug("check_all_alerts: no open LIVE positions")
        return

    # ── Stale data check ──────────────────────────────────────────────────────
    stale_msg = _check_stale_data(positions)
    if stale_msg and not _is_deduped(0, _STALE_ALERT_KEY):
        try:
            await bot.send_message(chat_id=chat_id, text=stale_msg,
                                   parse_mode="Markdown")
            _mark_sent(0, _STALE_ALERT_KEY)
        except Exception as exc:
            log.error("Failed to send stale-data warning: %s", exc)

    # ── Per-position rule checks ──────────────────────────────────────────────
    for pos in positions:
        trade_id = pos["trade_id"]

        # Skip if snapshot is missing — never alert on absent data
        if pos["current_put_mid"] is None and pos["current_underlying_price"] is None:
            log.warning(
                "check_all_alerts: no snapshot data for trade_id=%d (%s) — skipping",
                trade_id, pos["symbol"],
            )
            continue

        # Rule 2 — highest priority
        msg = _check_rule_2(pos, rules_cfg, earnings_cfg)
        if msg and not _is_deduped(trade_id, "RULE_2"):
            await _send_alert(bot, chat_id, msg, trade_id, "RULE_2")

        # Rule 3 — high priority
        msg = _check_rule_3(pos, rules_cfg)
        if msg and not _is_deduped(trade_id, "RULE_3"):
            await _send_alert(bot, chat_id, msg, trade_id, "RULE_3")

        # Rule 1 — medium priority
        msg = _check_rule_1(pos, rules_cfg)
        if msg and not _is_deduped(trade_id, "RULE_1"):
            await _send_alert(bot, chat_id, msg, trade_id, "RULE_1")


async def send_morning_check(pool, config: dict, bot) -> None:
    """Called once at 09:35 ET every trading day."""
    chat_id = config["telegram"]["chat_id"]
    try:
        positions = _load_positions(pool)
        msg = build_morning_check(positions)
        await bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        log.info("Morning check sent — %d positions", len(positions))
    except Exception as exc:
        log.error("send_morning_check failed: %s", exc)


async def _send_alert(bot, chat_id: str, text: str, trade_id: int, rule: str) -> None:
    """Send a single alert with one retry on failure."""
    import asyncio
    for attempt in range(1, 3):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            _mark_sent(trade_id, rule)
            log.info("Alert sent: trade_id=%d rule=%s", trade_id, rule)
            return
        except Exception as exc:
            if attempt == 1:
                log.warning("Alert send failed (attempt 1), retrying in 30s: %s", exc)
                await asyncio.sleep(30)
            else:
                log.error("Alert send failed after 2 attempts: trade_id=%d rule=%s: %s",
                          trade_id, rule, exc)
