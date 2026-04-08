"""
test_alerts.py — Tests for notifications/alerts.py rule logic.

All tests are pure Python — no DB connections, no Telegram API calls.
Position dicts are constructed directly to mirror the SQL query output.
"""

import sys
import os
from datetime import date, datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifications.alerts import (
    _check_rule_1,
    _check_rule_2,
    _check_rule_3,
    _is_deduped,
    _mark_sent,
    _sent_alerts,
    build_morning_check,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

RULES_CFG = {
    "early_close_threshold_standard": 0.25,
    "early_close_threshold_tight":    0.40,
    "early_close_dte_boundary":        10,
    "stop_loss_underlying_price":     250.0,
    "stop_loss_delta_threshold":       0.40,
    "stop_loss_dte_threshold":         10,
    "earnings_warning_dte":             5,
}


def _make_pos(
    trade_id=1,
    symbol="VRT",
    strike=245.0,
    expiry_date=None,
    contracts=5,
    entry_price=12.00,
    entry_underlying_price=280.0,
    current_underlying_price=278.0,
    current_put_mid=6.00,
    current_ask=6.20,
    current_bid=5.80,
    current_delta=-0.25,
    current_dte=16,
    premium_collected=6000.0,
    collateral_assigned=122500.0,
    latest_snapshot_time=None,
):
    if expiry_date is None:
        expiry_date = date.today() + timedelta(days=16)
    if latest_snapshot_time is None:
        latest_snapshot_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    return {
        "trade_id":                trade_id,
        "symbol":                  symbol,
        "strike":                  strike,
        "expiry_date":             expiry_date,
        "contracts":               contracts,
        "entry_price":             entry_price,
        "entry_underlying_price":  entry_underlying_price,
        "current_underlying_price": current_underlying_price,
        "current_put_mid":         current_put_mid,
        "current_ask":             current_ask,
        "current_bid":             current_bid,
        "current_delta":           current_delta,
        "current_dte":             current_dte,
        "premium_collected":       premium_collected,
        "collateral_assigned":     collateral_assigned,
        "latest_snapshot_time":    latest_snapshot_time,
    }


# ── Rule 1 ────────────────────────────────────────────────────────────────────

class TestRule1:

    def test_rule_1_triggers(self):
        """Put at 20% of entry price (below 25% threshold) → alert."""
        pos = _make_pos(entry_price=12.00, current_put_mid=2.20, current_dte=15)
        # 2.20 / 12.00 = 0.183 — below standard threshold 0.25
        result = _check_rule_1(pos, RULES_CFG)
        assert result is not None
        assert "EARLY CLOSE OPPORTUNITY" in result
        assert "VRT" in result

    def test_rule_1_no_trigger(self):
        """Put at 50% of entry price → no alert."""
        pos = _make_pos(entry_price=12.00, current_put_mid=6.00, current_dte=15)
        # 6.00 / 12.00 = 0.50 — above threshold
        result = _check_rule_1(pos, RULES_CFG)
        assert result is None

    def test_rule_1_tight_threshold_inside_dte_boundary(self):
        """Inside DTE boundary (≤10 days), threshold tightens to 0.40."""
        pos = _make_pos(entry_price=12.00, current_put_mid=4.60, current_dte=8)
        # 4.60 / 12.00 = 0.383 — below tight threshold 0.40
        result = _check_rule_1(pos, RULES_CFG)
        assert result is not None

    def test_rule_1_at_boundary_no_trigger(self):
        """At exactly boundary DTE=10, standard threshold applies (0.25). 30% → no trigger."""
        pos = _make_pos(entry_price=12.00, current_put_mid=3.60, current_dte=10)
        # 3.60 / 12.00 = 0.30 — above standard 0.25
        result = _check_rule_1(pos, RULES_CFG)
        assert result is None

    def test_rule_1_missing_current_price_no_trigger(self):
        """If current_put_mid is None, no alert."""
        pos = _make_pos(current_put_mid=None)
        result = _check_rule_1(pos, RULES_CFG)
        assert result is None


# ── Rule 2 ────────────────────────────────────────────────────────────────────

class TestRule2:

    def test_rule_2_triggers(self):
        """Earnings 3 days from now, expiry 10 days from now → HARD CLOSE."""
        today = date.today()
        earnings_cfg = {"VRT": str(today + timedelta(days=3))}
        pos = _make_pos(expiry_date=today + timedelta(days=10))
        result = _check_rule_2(pos, RULES_CFG, earnings_cfg)
        assert result is not None
        assert "HARD CLOSE REQUIRED" in result
        assert "VRT" in result

    def test_rule_2_deactivated(self):
        """Earnings AFTER expiry → Rule 2 deactivated, no alert."""
        today = date.today()
        earnings_cfg = {"VRT": str(today + timedelta(days=30))}
        pos = _make_pos(expiry_date=today + timedelta(days=16))
        result = _check_rule_2(pos, RULES_CFG, earnings_cfg)
        assert result is None

    def test_rule_2_earnings_outside_warning_window(self):
        """Earnings in 20 days, warning window is 5 days → no alert yet."""
        today = date.today()
        earnings_cfg = {"VRT": str(today + timedelta(days=20))}
        pos = _make_pos(expiry_date=today + timedelta(days=25))
        result = _check_rule_2(pos, RULES_CFG, earnings_cfg)
        assert result is None

    def test_rule_2_no_earnings_configured(self):
        """No earnings date in config → no alert."""
        pos = _make_pos()
        result = _check_rule_2(pos, RULES_CFG, {})
        assert result is None

    def test_rule_2_symbol_not_in_calendar(self):
        """Symbol not present in earnings_calendar → no alert."""
        earnings_cfg = {"GEV": "2026-05-01"}  # VRT not in there
        pos = _make_pos(symbol="VRT")
        result = _check_rule_2(pos, RULES_CFG, earnings_cfg)
        assert result is None


# ── Rule 3 ────────────────────────────────────────────────────────────────────

class TestRule3:

    def test_rule_3_triggers_close_recommendation(self):
        """Underlying below threshold AND delta > 0.40 AND DTE < 10 → CLOSE NOW."""
        pos = _make_pos(
            current_underlying_price=240.0,  # below 250 threshold
            current_delta=-0.45,              # > 0.40
            current_dte=8,                    # < 10
        )
        result = _check_rule_3(pos, RULES_CFG)
        assert result is not None
        assert "REASSESSMENT TRIGGERED" in result
        assert "CLOSE NOW" in result

    def test_rule_3_partial_hold_recommendation(self):
        """Underlying below threshold but delta < 0.40 → HOLD AND MONITOR."""
        pos = _make_pos(
            current_underlying_price=240.0,  # below 250 threshold
            current_delta=-0.28,              # below 0.40 — delta OK
            current_dte=15,                   # above DTE threshold
        )
        result = _check_rule_3(pos, RULES_CFG)
        assert result is not None
        assert "REASSESSMENT TRIGGERED" in result
        assert "HOLD AND MONITOR" in result

    def test_rule_3_no_trigger_above_threshold(self):
        """Underlying above threshold → no alert."""
        pos = _make_pos(current_underlying_price=270.0)
        result = _check_rule_3(pos, RULES_CFG)
        assert result is None

    def test_rule_3_missing_price_no_trigger(self):
        """No current underlying price → no alert."""
        pos = _make_pos(current_underlying_price=None)
        result = _check_rule_3(pos, RULES_CFG)
        assert result is None


# ── Deduplication ─────────────────────────────────────────────────────────────

class TestDeduplication:

    def setup_method(self):
        """Clear sent_alerts before each test."""
        _sent_alerts.clear()

    def test_deduplication_suppresses_within_window(self):
        """Same alert within one hour is suppressed on second check."""
        _mark_sent(trade_id=1, rule="RULE_1")
        assert _is_deduped(trade_id=1, rule="RULE_1") is True

    def test_deduplication_passes_after_window(self):
        """Alert older than one hour is not deduped — passes through."""
        # Manually insert a stale timestamp (2 hours ago)
        _sent_alerts[(1, "RULE_1")] = datetime.now(timezone.utc) - timedelta(hours=2)
        assert _is_deduped(trade_id=1, rule="RULE_1") is False

    def test_deduplication_different_rule_not_suppressed(self):
        """Different rule on same trade is not suppressed."""
        _mark_sent(trade_id=1, rule="RULE_1")
        assert _is_deduped(trade_id=1, rule="RULE_2") is False

    def test_deduplication_different_trade_not_suppressed(self):
        """Same rule on different trade is not suppressed."""
        _mark_sent(trade_id=1, rule="RULE_1")
        assert _is_deduped(trade_id=2, rule="RULE_1") is False

    def test_no_alerts_when_clear(self):
        """Healthy position returns None for all rules."""
        pos = _make_pos(
            current_underlying_price=278.0,  # above 250 stop threshold
            current_put_mid=6.00,            # 50% of entry 12.00 — above threshold
            current_delta=-0.25,             # below 0.40
            current_dte=16,
        )
        assert _check_rule_1(pos, RULES_CFG) is None
        assert _check_rule_2(pos, RULES_CFG, {}) is None
        assert _check_rule_3(pos, RULES_CFG) is None


# ── Morning check ─────────────────────────────────────────────────────────────

class TestMorningCheck:

    def test_morning_check_formats_single_position(self):
        """Single position formats without error."""
        pos = _make_pos()
        result = build_morning_check([pos])
        assert "MORNING CHECK" in result
        assert "VRT" in result
        assert "$245" in result

    def test_morning_check_empty_positions(self):
        """No positions produces clean message."""
        result = build_morning_check([])
        assert "MORNING CHECK" in result
        assert "No open LIVE positions" in result

    def test_morning_check_elevated_delta_shows_act(self):
        """Delta above 0.40 shows 🔴 ACT status."""
        pos = _make_pos(current_delta=-0.45)
        result = build_morning_check([pos])
        assert "🔴 ACT" in result

    def test_morning_check_high_delta_shows_watch(self):
        """Delta between 0.25 and 0.40 shows 🟠 WATCH."""
        pos = _make_pos(current_delta=-0.30)
        result = build_morning_check([pos])
        assert "🟠 WATCH" in result

    def test_morning_check_healthy_shows_safe(self):
        """Delta below 0.25 shows 🟢 SAFE."""
        pos = _make_pos(current_delta=-0.20)
        result = build_morning_check([pos])
        assert "🟢 SAFE" in result

    def test_morning_check_all_clear_message(self):
        """All safe positions shows ALL CLEAR."""
        pos = _make_pos(current_delta=-0.18)
        result = build_morning_check([pos])
        assert "ALL CLEAR" in result
