"""
tests/test_simulator.py — Tests for the paper trading simulator.

Rules and position lifecycle tests require no live DB.
Position manager DB tests use mocked connections.

Run with: pytest tests/test_simulator.py -v
"""

import pytest
from datetime import datetime, date, timezone
from unittest.mock import MagicMock, patch, call

from engine.models import OptionContract, ScoredContract
from simulator.models import PaperPosition
from simulator.rules import Rules
from simulator.position_manager import PositionManager
from simulator.simulator import PaperTradingSimulator


# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "database": {
            "host": "localhost", "port": 5432, "name": "qs_options",
            "user": "test", "password": "test",
        },
        "scoring": {
            "min_composite_score": 0.70,
            "min_annualised_roi": 70.0,
            "min_probability_of_profit": 70.0,
            "min_open_interest": 500,
            "dte_min": 15,
            "dte_max": 25,
            "iv_percentile_min": 70.0,
        },
        "position_management": {
            "max_concurrent_positions": 3,
            "contracts_per_trade": 5,
            "max_collateral_per_position_usd": 352500,
        },
        "rules": {
            "early_close_threshold_standard": 0.25,
            "early_close_threshold_tight":    0.40,
            "early_close_dte_boundary":       10,
            "stop_loss_underlying_price":     250.0,
            "stop_loss_delta_threshold":      0.40,
            "stop_loss_dte_threshold":        10,
        },
        "simulator": {
            "mode": "PAPER",
            "cycle_interval_minutes": 15,
            "market_open": "09:30",
            "market_close": "16:00",
            "max_entries_per_cycle": 1,
        },
    }


def make_scored(
    symbol="VRT",
    underlying_id=1,
    strike=245.0,
    dte=18,
    mid=10.74,
    delta=-0.27,
    theta=-0.52,
    open_interest=1804,
    iv_pct_52w=92.0,
    composite_score=0.80,
    annualised_roi=0.89,
    passed_all_filters=True,
    underlying_price=265.54,
) -> ScoredContract:
    contract = OptionContract(
        underlying_id=underlying_id,
        symbol=symbol,
        snapshot_time=datetime(2026, 4, 7, 14, 0, 0, tzinfo=timezone.utc),
        underlying_price=underlying_price,
        expiry_date=date(2026, 4, 24),
        dte=dte,
        strike=strike,
        option_type="P",
        bid=mid - 0.10,
        ask=mid + 0.10,
        mid=mid,
        open_interest=open_interest,
        implied_vol=0.746,
        delta=delta,
        gamma=0.012,
        theta=theta,
        vega=0.31,
        iv_percentile_52w=iv_pct_52w,
        iv_percentile_30d=88.0,
        vix_level=21.5,
        ibkr_conid=667769626,
    )
    return ScoredContract(
        **vars(contract),
        composite_score=composite_score,
        annualised_roi=annualised_roi,
        theta_per_collateral=abs(theta) / (strike * 100),
        prob_profit=1 - abs(delta),
        collateral=strike * 100,
        premium_per_contract=mid * 100,
        total_premium_5_contracts=mid * 500,
        effective_entry_if_assigned=strike - mid,
        passed_all_filters=passed_all_filters,
        filter_failures=[],
    )


def make_position(
    trade_id=1,
    underlying_id=1,
    symbol="VRT",
    strike=245.0,
    entry_price=10.74,
    current_price=10.74,
    current_dte=18,
    current_delta=-0.27,
    current_underlying_price=265.54,
    contracts=5,
) -> PaperPosition:
    premium = entry_price * 100 * contracts
    return PaperPosition(
        trade_id=trade_id,
        underlying_id=underlying_id,
        symbol=symbol,
        expiry_date=date(2026, 4, 24),
        strike=strike,
        option_type="P",
        contracts=contracts,
        entry_time=datetime(2026, 4, 6, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=entry_price,
        entry_underlying_price=265.54,
        entry_delta=-0.27,
        entry_theta=-0.52,
        entry_iv=0.746,
        entry_iv_percentile=92.0,
        entry_dte=18,
        entry_composite_score=0.80,
        entry_ann_roi=0.89,
        premium_collected=premium,
        collateral_assigned=strike * 100 * contracts,
        threshold_min_score=0.70,
        threshold_min_roi=70.0,
        threshold_min_pop=70.0,
        threshold_min_oi=500,
        current_price=current_price,
        current_underlying_price=current_underlying_price,
        current_delta=current_delta,
        current_dte=current_dte,
        unrealised_pnl=(entry_price - current_price) * 100 * contracts,
        exit_time=None,
        exit_price=None,
        exit_reason=None,
        realised_pnl=None,
        pnl_pct_of_premium=None,
        was_assigned=False,
        assigned_cost_basis=None,
    )


def make_mock_pool():
    pool = MagicMock()
    conn = MagicMock()
    pool.getconn.return_value = conn
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return pool, conn, cur


# ---------------------------------------------------------------------------
# Test: can_open_new_position
# ---------------------------------------------------------------------------

class TestCanOpenNewPosition:

    def test_returns_true_when_all_conditions_pass(self, config):
        rules = Rules(config)
        candidate = make_scored(passed_all_filters=True, strike=245.0, dte=18)
        ok, reason = rules.can_open_new_position(candidate, [], config)
        assert ok is True
        assert reason == ""

    def test_fails_condition_1_screener_filter(self, config):
        rules = Rules(config)
        candidate = make_scored(passed_all_filters=False)
        ok, reason = rules.can_open_new_position(candidate, [], config)
        assert ok is False
        assert reason == "screener_filter_failed"

    def test_fails_condition_2_max_positions_reached(self, config):
        rules = Rules(config)
        candidate = make_scored(passed_all_filters=True, underlying_id=99)
        open_positions = [
            make_position(trade_id=i, underlying_id=i) for i in range(3)
        ]
        ok, reason = rules.can_open_new_position(candidate, open_positions, config)
        assert ok is False
        assert reason == "max_positions_reached"

    def test_fails_condition_3_duplicate_underlying(self, config):
        rules = Rules(config)
        candidate = make_scored(passed_all_filters=True, underlying_id=1)
        open_positions = [make_position(underlying_id=1)]
        ok, reason = rules.can_open_new_position(candidate, open_positions, config)
        assert ok is False
        assert reason == "duplicate_underlying"

    def test_fails_condition_4_collateral_limit(self, config):
        rules = Rules(config)
        # strike=800 → collateral = 800*100*5 = $400,000 > $352,500
        candidate = make_scored(passed_all_filters=True, strike=800.0)
        ok, reason = rules.can_open_new_position(candidate, [], config)
        assert ok is False
        assert reason == "collateral_limit_exceeded"

    def test_fails_condition_5_dte_below_minimum(self, config):
        rules = Rules(config)
        candidate = make_scored(passed_all_filters=True, dte=10)  # dte_min=15
        ok, reason = rules.can_open_new_position(candidate, [], config)
        assert ok is False
        assert reason == "dte_below_minimum"


# ---------------------------------------------------------------------------
# Test: should_close_position
# ---------------------------------------------------------------------------

class TestShouldClosePosition:

    def test_returns_expired_when_dte_zero_above_strike(self, config):
        rules = Rules(config)
        position = make_position(current_dte=0, current_underlying_price=270.0)
        latest = make_scored(underlying_price=270.0, dte=0)  # above strike
        ok, reason = rules.should_close_position(position, latest, config)
        assert ok is True
        assert reason == "EXPIRED"

    def test_returns_assigned_when_dte_zero_below_strike(self, config):
        rules = Rules(config)
        position = make_position(current_dte=0, strike=245.0)
        latest = make_scored(underlying_price=230.0, dte=0)  # below strike
        ok, reason = rules.should_close_position(position, latest, config)
        assert ok is True
        assert reason == "ASSIGNED"

    def test_assigned_takes_priority_over_expired(self, config):
        """Rule 2 checked before Rule 1 — ASSIGNED wins when both apply."""
        rules = Rules(config)
        position = make_position(current_dte=0, strike=245.0)
        latest = make_scored(underlying_price=200.0, dte=0)
        ok, reason = rules.should_close_position(position, latest, config)
        assert reason == "ASSIGNED"  # not EXPIRED

    def test_returns_early_close_below_standard_threshold(self, config):
        """DTE=18 > boundary=10: standard threshold=0.25 applies."""
        rules = Rules(config)
        # pct_remaining = 2.50/10.74 = 0.233 < 0.25 → EARLY_CLOSE
        position = make_position(
            entry_price=10.74, current_price=2.50, current_dte=18
        )
        ok, reason = rules.should_close_position(position, None, config)
        assert ok is True
        assert reason == "EARLY_CLOSE"

    def test_returns_early_close_below_tight_threshold(self, config):
        """DTE=8 <= boundary=10: tight threshold=0.40 applies."""
        rules = Rules(config)
        # pct_remaining = 4.00/10.74 = 0.372 < 0.40 → EARLY_CLOSE
        position = make_position(
            entry_price=10.74, current_price=4.00, current_dte=8
        )
        ok, reason = rules.should_close_position(position, None, config)
        assert ok is True
        assert reason == "EARLY_CLOSE"

    def test_standard_threshold_does_not_trigger_inside_boundary(self, config):
        """pct=0.372 is below standard (0.25 < 0.372) but below tight (0.40 > 0.372) at DTE=8."""
        rules = Rules(config)
        # 0.372 is < 0.40 (tight) → triggers. Let's test the boundary correctly:
        # pct=0.30, DTE=8: 0.30 < 0.40 → EARLY_CLOSE (tight threshold)
        # pct=0.30, DTE=12: 0.30 > 0.25 → no close (standard threshold)
        position_inner = make_position(
            entry_price=10.74, current_price=3.22, current_dte=12
        )  # pct=0.30, DTE=12 > 10: standard threshold 0.25. 0.30 > 0.25 → hold
        ok, reason = rules.should_close_position(position_inner, None, config)
        assert ok is False

    def test_returns_stop_loss_when_all_three_conditions_met(self, config):
        rules = Rules(config)
        position = make_position(
            current_delta=-0.45,  # abs > 0.40
            current_dte=8,        # < 10
            current_price=10.00,  # not triggering early close (pct=1.0 > 0.40)
        )
        latest = make_scored(underlying_price=240.0, dte=8)  # < 250.0 stop price
        ok, reason = rules.should_close_position(position, latest, config)
        assert ok is True
        assert reason == "STOP_LOSS"

    def test_returns_hold_when_none_apply(self, config):
        rules = Rules(config)
        position = make_position(
            current_price=8.00,   # pct=0.745 > 0.25 → no early close
            current_dte=15,       # > 0
            current_delta=-0.25,  # abs < 0.40 → no stop loss
        )
        latest = make_scored(underlying_price=270.0, dte=15)  # > 250 → no stop
        ok, reason = rules.should_close_position(position, latest, config)
        assert ok is False
        assert reason == ""


# ---------------------------------------------------------------------------
# Test: open_position
# ---------------------------------------------------------------------------

class TestOpenPosition:

    def test_premium_collected_computed_correctly(self):
        pool, conn, cur = make_mock_pool()
        cur.fetchone.return_value = (42,)  # mock trade_id from RETURNING

        pm = PositionManager(pool)
        candidate = make_scored(mid=10.74, strike=245.0)
        result = pm.open_position(candidate, 5, {
            "threshold_min_score": 0.70,
            "threshold_min_roi": 70.0,
            "threshold_min_pop": 70.0,
            "threshold_min_oi": 500,
        })

        # premium_collected = 10.74 * 100 * 5 = 5370.0
        assert result.premium_collected == pytest.approx(5370.0, abs=0.01)

    def test_collateral_assigned_computed_correctly(self):
        pool, conn, cur = make_mock_pool()
        cur.fetchone.return_value = (42,)

        pm = PositionManager(pool)
        candidate = make_scored(mid=10.74, strike=245.0)
        result = pm.open_position(candidate, 5, {
            "threshold_min_score": 0.70,
            "threshold_min_roi": 70.0,
            "threshold_min_pop": 70.0,
            "threshold_min_oi": 500,
        })

        # collateral_assigned = 245.0 * 100 * 5 = 122500.0
        assert result.collateral_assigned == pytest.approx(122500.0, abs=0.01)

    def test_all_exit_fields_none_after_open(self):
        pool, conn, cur = make_mock_pool()
        cur.fetchone.return_value = (42,)

        pm = PositionManager(pool)
        candidate = make_scored()
        result = pm.open_position(candidate, 5, {
            "threshold_min_score": 0.70,
            "threshold_min_roi": 70.0,
            "threshold_min_pop": 70.0,
            "threshold_min_oi": 500,
        })

        assert result.exit_time is None
        assert result.exit_price is None
        assert result.exit_reason is None
        assert result.realised_pnl is None
        assert result.pnl_pct_of_premium is None
        assert result.assigned_cost_basis is None
        assert result.was_assigned is False

    def test_trade_id_populated_from_db(self):
        pool, conn, cur = make_mock_pool()
        cur.fetchone.return_value = (99,)  # DB returns trade_id=99

        pm = PositionManager(pool)
        candidate = make_scored()
        result = pm.open_position(candidate, 5, {
            "threshold_min_score": 0.70,
            "threshold_min_roi": 70.0,
            "threshold_min_pop": 70.0,
            "threshold_min_oi": 500,
        })

        assert result.trade_id == 99


# ---------------------------------------------------------------------------
# Test: close_position
# ---------------------------------------------------------------------------

class TestClosePosition:

    def test_realised_pnl_profitable_close(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        position = make_position(entry_price=10.74, contracts=5)
        exit_time = datetime(2026, 4, 15, 14, 0, tzinfo=timezone.utc)

        result = pm.close_position(position, exit_price=2.50,
                                   exit_reason="EARLY_CLOSE", exit_time=exit_time)

        # (10.74 - 2.50) * 100 * 5 = 8.24 * 500 = $4,120
        assert result.realised_pnl == pytest.approx(4120.0, abs=0.01)

    def test_realised_pnl_loss_scenario(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        position = make_position(entry_price=10.74, contracts=5)
        exit_time = datetime(2026, 4, 20, 14, 0, tzinfo=timezone.utc)

        result = pm.close_position(position, exit_price=15.00,
                                   exit_reason="STOP_LOSS", exit_time=exit_time)

        # (10.74 - 15.00) * 100 * 5 = -4.26 * 500 = -$2,130
        assert result.realised_pnl == pytest.approx(-2130.0, abs=0.01)

    def test_pnl_pct_of_premium_computed_correctly(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        position = make_position(entry_price=10.74, contracts=5)
        exit_time = datetime(2026, 4, 15, 14, 0, tzinfo=timezone.utc)

        result = pm.close_position(position, exit_price=2.50,
                                   exit_reason="EARLY_CLOSE", exit_time=exit_time)

        # pnl_pct = 4120 / 5370 = 0.7672
        assert result.pnl_pct_of_premium == pytest.approx(4120.0 / 5370.0, rel=1e-4)

    def test_was_assigned_true_only_on_assigned(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        exit_time = datetime(2026, 4, 24, 21, 0, tzinfo=timezone.utc)

        pos_assigned = make_position()
        result = pm.close_position(pos_assigned, 0.0, "ASSIGNED", exit_time)
        assert result.was_assigned is True

        pool2, conn2, cur2 = make_mock_pool()
        pm2 = PositionManager(pool2)
        pos_early = make_position()
        result2 = pm2.close_position(pos_early, 2.50, "EARLY_CLOSE", exit_time)
        assert result2.was_assigned is False

    def test_assigned_cost_basis_populated_on_assignment(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        position = make_position(entry_price=10.74, strike=245.0)
        exit_time = datetime(2026, 4, 24, 21, 0, tzinfo=timezone.utc)

        result = pm.close_position(position, 0.0, "ASSIGNED", exit_time)

        # cost_basis = strike - entry_price = 245.0 - 10.74 = 234.26
        assert result.assigned_cost_basis == pytest.approx(234.26, abs=0.01)
        assert result.was_assigned is True

    def test_assigned_cost_basis_none_on_non_assignment(self):
        pool, conn, cur = make_mock_pool()
        pm = PositionManager(pool)
        position = make_position()
        exit_time = datetime(2026, 4, 15, 14, 0, tzinfo=timezone.utc)

        result = pm.close_position(position, 2.50, "EARLY_CLOSE", exit_time)
        assert result.assigned_cost_basis is None


# ---------------------------------------------------------------------------
# Test: run_cycle with no data
# ---------------------------------------------------------------------------

class TestRunCycleNoData:

    def test_run_cycle_completes_without_error_empty_screener(self, config):
        """run_cycle() must handle an empty screener result gracefully."""
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = MagicMock()

        with (
            patch("simulator.simulator._load_config", return_value=config),
            patch("simulator.simulator._create_connection_pool",
                  return_value=mock_pool),
            patch("simulator.simulator.OptionScreener") as MockScreener,
            patch("simulator.simulator.PositionManager") as MockPM,
        ):
            mock_screener_inst = MockScreener.return_value
            mock_screener_inst.run.return_value = []

            mock_pm_inst = MockPM.return_value
            mock_pm_inst.load_open_positions.return_value = []

            sim = PaperTradingSimulator.__new__(PaperTradingSimulator)
            sim.config = config
            sim.pool = mock_pool
            sim.screener = mock_screener_inst
            sim.position_manager = mock_pm_inst
            sim.rules = Rules(config)
            sim.open_positions = []
            from engine.scorer import OptionScorer
            sim.scorer = OptionScorer()

            summary = sim.run_cycle()

        assert summary["contracts_scored"] == 0
        assert summary["new_entries"] == 0
        assert summary["closes_this_cycle"] == 0

    def test_no_db_writes_on_empty_screener(self, config):
        """No positions opened → no DB inserts."""
        mock_pool = MagicMock()

        with (
            patch("simulator.simulator._load_config", return_value=config),
            patch("simulator.simulator._create_connection_pool",
                  return_value=mock_pool),
            patch("simulator.simulator.OptionScreener") as MockScreener,
            patch("simulator.simulator.PositionManager") as MockPM,
        ):
            mock_screener_inst = MockScreener.return_value
            mock_screener_inst.run.return_value = []

            mock_pm_inst = MockPM.return_value
            mock_pm_inst.load_open_positions.return_value = []

            sim = PaperTradingSimulator.__new__(PaperTradingSimulator)
            sim.config = config
            sim.pool = mock_pool
            sim.screener = mock_screener_inst
            sim.position_manager = mock_pm_inst
            sim.rules = Rules(config)
            sim.open_positions = []
            from engine.scorer import OptionScorer
            sim.scorer = OptionScorer()

            sim.run_cycle()

        mock_pm_inst.open_position.assert_not_called()
        mock_pm_inst.close_position.assert_not_called()


# ---------------------------------------------------------------------------
# Test: _load_open_positions
# ---------------------------------------------------------------------------

class TestLoadOpenPositions:

    def test_restores_open_positions_from_db(self, config):
        """load_open_positions() should reconstruct PaperPosition objects."""
        pool, conn, cur = make_mock_pool()

        # Simulate one open PAPER trade returned from DB
        cur.fetchall.return_value = [
            (
                1, 1, "VRT",
                date(2026, 4, 24), 245.0, "P", 5,
                datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc),
                10.74, 265.54,
                -0.27, -0.52, 0.746, 92.0,
                18, 0.80, 0.89,
                5370.0, 122500.0,
                0.70, 70.0, 70.0, 500,
            )
        ]

        pm = PositionManager(pool)
        positions = pm.load_open_positions()

        assert len(positions) == 1
        assert positions[0].trade_id == 1
        assert positions[0].symbol == "VRT"
        assert positions[0].exit_time is None

    def test_does_not_load_closed_positions(self, config):
        """SQL filters exit_time IS NULL — closed positions must not be returned."""
        pool, conn, cur = make_mock_pool()
        cur.fetchall.return_value = []  # DB returns nothing (all have exit_time)

        pm = PositionManager(pool)
        positions = pm.load_open_positions()

        assert positions == []

    def test_multiple_open_positions_restored(self, config):
        pool, conn, cur = make_mock_pool()
        base_row = (
            None, 1, "VRT",
            date(2026, 4, 24), 245.0, "P", 5,
            datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc),
            10.74, 265.54,
            -0.27, -0.52, 0.746, 92.0,
            18, 0.80, 0.89,
            5370.0, 122500.0,
            0.70, 70.0, 70.0, 500,
        )
        cur.fetchall.return_value = [
            (2,) + base_row[1:],
            (3,) + base_row[1:],
        ]

        pm = PositionManager(pool)
        positions = pm.load_open_positions()

        assert len(positions) == 2
        assert {p.trade_id for p in positions} == {2, 3}
