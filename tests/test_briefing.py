"""
test_briefing.py — Tests for notifications/briefing.py formatting logic.

All tests are pure Python — no DB connections, no Telegram API calls.
Screener is mocked; formatting functions are tested with synthetic data.
"""

import sys
import os
from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifications.briefing import (
    _build_live_section,
    _build_paper_section,
    _build_opportunities_section,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

CONFIG = {
    "telegram": {
        "bot_token": "TEST_TOKEN",
        "chat_id":   "123456",
    },
    "portfolio": {
        "sgov_nav_usd": 2350000,
    },
    "position_management": {
        "contracts_per_trade": 5,
    },
    "scoring": {
        "min_composite_score":       0.70,
        "min_annualised_roi":        70.0,
        "min_probability_of_profit": 70.0,
        "min_open_interest":         500,
        "dte_min": 15,
        "dte_max": 25,
        "iv_percentile_min": 70.0,
        "strike_range_pct": 20.0,
    },
    "rules": {
        "early_close_dte_boundary": 10,
        "early_close_threshold_standard": 0.25,
        "early_close_threshold_tight": 0.40,
        "stop_loss_underlying_price": 250.0,
        "stop_loss_delta_threshold": 0.40,
        "stop_loss_dte_threshold": 10,
    },
}


def _make_live_pos(
    symbol="VRT",
    strike=245.0,
    expiry_date=None,
    contracts=5,
    entry_price=12.00,
    current_underlying_price=278.0,
    current_put_mid=6.20,
    current_delta=-0.22,
    current_dte=16,
):
    if expiry_date is None:
        expiry_date = date.today() + timedelta(days=16)
    return {
        "symbol":                   symbol,
        "strike":                   strike,
        "expiry_date":              expiry_date,
        "contracts":                contracts,
        "entry_price":              entry_price,
        "entry_underlying_price":   278.0,
        "current_underlying_price": current_underlying_price,
        "current_put_mid":          current_put_mid,
        "current_delta":            current_delta,
        "current_dte":              current_dte,
        "premium_collected":        6000.0,
    }


def _make_scored_contract(
    symbol="GLD",
    strike=400.0,
    composite_score=0.85,
    annualised_roi=0.89,
    prob_profit=0.75,
    iv_percentile_52w=78.0,
    total_premium_5_contracts=3500.0,
    effective_entry_if_assigned=395.0,
    passed_all_filters=True,
):
    c = MagicMock()
    c.symbol                      = symbol
    c.strike                      = strike
    c.expiry_date                 = date.today() + timedelta(days=21)
    c.composite_score             = composite_score
    c.annualised_roi              = annualised_roi
    c.prob_profit                 = prob_profit
    c.iv_percentile_52w           = iv_percentile_52w
    c.iv_percentile_30d           = iv_percentile_52w
    c.total_premium_5_contracts   = total_premium_5_contracts
    c.effective_entry_if_assigned = effective_entry_if_assigned
    c.passed_all_filters          = passed_all_filters
    return c


# ── Live section ──────────────────────────────────────────────────────────────

class TestLiveSection:

    def test_briefing_formats_live_position_correctly(self):
        """Live position section includes symbol, strike, P&L, status."""
        pos    = _make_live_pos()
        result = _build_live_section([pos])
        assert "LIVE POSITIONS" in result
        assert "VRT" in result
        assert "$245" in result
        assert "🟢 SAFE" in result

    def test_live_section_no_positions(self):
        """Zero live positions shows placeholder message."""
        result = _build_live_section([])
        assert "LIVE POSITIONS (0 open)" in result
        assert "No open live positions" in result

    def test_live_section_multiple_positions(self):
        """Multiple positions all appear in output."""
        positions = [
            _make_live_pos(symbol="VRT", strike=245.0),
            _make_live_pos(symbol="GLD", strike=410.0),
        ]
        result = _build_live_section(positions)
        assert "VRT" in result
        assert "GLD" in result
        assert "LIVE POSITIONS (2 open)" in result

    def test_live_section_elevated_delta_shows_watch(self):
        """Delta > 0.25 shows WATCH status."""
        pos    = _make_live_pos(current_delta=-0.30)
        result = _build_live_section([pos])
        assert "🟠 WATCH" in result

    def test_live_section_act_status_for_high_delta(self):
        """Delta > 0.40 shows ACT status."""
        pos    = _make_live_pos(current_delta=-0.45)
        result = _build_live_section([pos])
        assert "🔴 ACT" in result


# ── Paper section ─────────────────────────────────────────────────────────────

class TestPaperSection:

    def test_paper_section_with_closed_and_open(self):
        """Closed count, avg P&L, and open positions all appear."""
        open_pos = [
            {"symbol": "ETN", "strike": 290.0, "expiry_date": date.today() + timedelta(days=18)},
        ]
        result = _build_paper_section(open_pos, closed_count=3, avg_pnl_pct=0.62)
        assert "PAPER TRADES" in result
        assert "1 open" in result
        assert "3 closed today" in result
        assert "ETN" in result
        assert "62.0%" in result

    def test_no_paper_positions(self):
        """Zero open and zero closed formats cleanly."""
        result = _build_paper_section([], closed_count=0, avg_pnl_pct=None)
        assert "PAPER TRADES" in result
        assert "No open paper positions" in result

    def test_closed_only_today(self):
        """Closed count shows even with no open positions."""
        result = _build_paper_section([], closed_count=2, avg_pnl_pct=0.45)
        assert "2 closed today" in result


# ── Opportunities section ─────────────────────────────────────────────────────

class TestOpportunitiesSection:

    def test_briefing_top3_opportunities(self):
        """Three passing contracts appear in the opportunities section."""
        top3    = [_make_scored_contract(symbol=s) for s in ["GLD", "VRT", "QQQ"]]
        screener = MagicMock()
        screener.run.return_value = top3

        result = _build_opportunities_section(screener, CONFIG)
        assert "TOP OPPORTUNITIES" in result
        assert "#1" in result
        assert "#2" in result
        assert "#3" in result
        assert "GLD" in result

    def test_no_opportunities_shows_sgov_message(self):
        """Zero passing contracts shows SGOV fallback message."""
        failing = [_make_scored_contract(passed_all_filters=False)]
        screener = MagicMock()
        screener.run.return_value = failing

        result = _build_opportunities_section(screener, CONFIG)
        assert "No contracts passed all filters" in result
        assert "SGOV" in result

    def test_screener_error_handled_gracefully(self):
        """Screener exception produces 'unavailable' message, does not raise."""
        screener = MagicMock()
        screener.run.side_effect = RuntimeError("DB connection failed")

        result = _build_opportunities_section(screener, CONFIG)
        assert "unavailable" in result.lower()

    def test_opportunities_include_key_fields(self):
        """Score, ROI, PoP, IV% all present in the output."""
        top1     = [_make_scored_contract()]
        screener = MagicMock()
        screener.run.return_value = top1

        result = _build_opportunities_section(screener, CONFIG)
        assert "Score:" in result
        assert "ROI:"   in result
        assert "PoP:"   in result
        assert "IV%:"   in result
        assert "Premium" in result
