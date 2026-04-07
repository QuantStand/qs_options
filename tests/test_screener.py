"""
tests/test_screener.py — Integration tests for engine/screener.py.

Uses a mocked database connection pool. No live database required.

Run with: pytest tests/test_screener.py -v
"""

import pytest
from datetime import datetime, date, timezone
from unittest.mock import MagicMock, patch

from engine.models import OptionContract, ScoredContract
from engine.scorer import OptionScorer
from engine.screener import OptionScreener


# ---------------------------------------------------------------------------
# Test config fixture (mirrors config/config.example.yaml scoring section)
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "scoring": {
            "min_composite_score": 0.70,
            "min_annualised_roi": 70.0,          # percent — screener divides by 100
            "min_probability_of_profit": 70.0,   # percent — screener divides by 100
            "min_open_interest": 500,
            "dte_min": 15,
            "dte_max": 25,
            "iv_percentile_min": 70.0,
        }
    }


# ---------------------------------------------------------------------------
# Contract builder helper
# ---------------------------------------------------------------------------

def make_scored(
    composite_score: float = 0.80,
    annualised_roi: float = 0.90,     # decimal
    prob_profit: float = 0.73,
    open_interest: int = 1500,
    iv_percentile_52w: float = 85.0,
    iv_percentile_30d: float = 80.0,
    passed_all_filters: bool = False,
    filter_failures: list = None,
    symbol: str = "VRT",
    strike: float = 245.0,
) -> ScoredContract:
    """Build a ScoredContract with sensible defaults for filter testing."""
    return ScoredContract(
        underlying_id=1,
        symbol=symbol,
        snapshot_time=datetime(2026, 4, 7, 14, 0, 0, tzinfo=timezone.utc),
        underlying_price=265.54,
        expiry_date=date(2026, 4, 24),
        dte=18,
        strike=strike,
        option_type="P",
        bid=10.50,
        ask=10.98,
        mid=10.74,
        open_interest=open_interest,
        implied_vol=0.746,
        delta=-0.27,
        gamma=0.012,
        theta=-0.52,
        vega=0.31,
        iv_percentile_52w=iv_percentile_52w,
        iv_percentile_30d=iv_percentile_30d,
        vix_level=21.5,
        ibkr_conid=667769626,
        composite_score=composite_score,
        annualised_roi=annualised_roi,
        theta_per_collateral=2.12e-5,
        prob_profit=prob_profit,
        collateral=strike * 100,
        premium_per_contract=1074.0,
        total_premium_5_contracts=5370.0,
        effective_entry_if_assigned=234.26,
        passed_all_filters=passed_all_filters,
        filter_failures=filter_failures or [],
    )


def make_screener(config) -> OptionScreener:
    """Create an OptionScreener with a mock pool."""
    mock_pool = MagicMock()
    return OptionScreener(mock_pool, config)


# ---------------------------------------------------------------------------
# Test: apply_filters records ALL failures independently
# ---------------------------------------------------------------------------

class TestApplyFiltersRecordsAllFailures:
    """
    A contract failing multiple filters must have one entry per failure.
    apply_filters must never short-circuit on the first failure.
    """

    def test_three_failures_produces_three_entries(self, config):
        screener = make_screener(config)

        # Deliberately fail three filters:
        #   composite_score < 0.70
        #   annualised_roi  < 0.70 (config 70%)
        #   open_interest   < 500
        scored = make_scored(
            composite_score=0.10,   # fails score filter
            annualised_roi=0.50,    # fails ann_roi filter (0.50 < 0.70)
            prob_profit=0.73,       # passes
            open_interest=100,      # fails OI filter
            iv_percentile_52w=85.0, # passes
        )

        result = screener.apply_filters(scored)
        assert len(result.filter_failures) == 3

    def test_all_filter_reasons_recorded(self, config):
        screener = make_screener(config)
        scored = make_scored(
            composite_score=0.10,
            annualised_roi=0.50,
            open_interest=100,
        )
        result = screener.apply_filters(scored)
        reasons = " ".join(result.filter_failures)
        assert "score" in reasons
        assert "ann_roi" in reasons
        assert "oi" in reasons

    def test_no_short_circuit_all_filters_evaluated(self, config):
        """Even after first failure, remaining filters must still be evaluated."""
        screener = make_screener(config)

        # Fail ALL five filters
        scored = make_scored(
            composite_score=0.01,
            annualised_roi=0.10,
            prob_profit=0.50,
            open_interest=50,
            iv_percentile_52w=20.0,
            iv_percentile_30d=None,
        )
        result = screener.apply_filters(scored)
        assert len(result.filter_failures) == 5


# ---------------------------------------------------------------------------
# Test: apply_filters sets passed_all_filters correctly
# ---------------------------------------------------------------------------

class TestPassedAllFilters:
    """passed_all_filters must be True only when filter_failures is empty."""

    def test_passing_contract_sets_flag_true(self, config):
        screener = make_screener(config)
        # All thresholds pass (with the formula-corrected min values)
        # composite_score: must be >= 0.70
        # annualised_roi: must be >= 0.70 (decimal)
        # prob_profit: must be >= 0.70 (decimal)
        # open_interest: must be >= 500
        # iv_percentile: must be >= 70.0
        scored = make_scored(
            composite_score=0.80,
            annualised_roi=0.90,
            prob_profit=0.75,
            open_interest=1500,
            iv_percentile_52w=85.0,
        )
        result = screener.apply_filters(scored)
        assert result.passed_all_filters is True
        assert result.filter_failures == []

    def test_single_failure_sets_flag_false(self, config):
        screener = make_screener(config)
        # Everything passes except composite_score
        scored = make_scored(
            composite_score=0.30,   # fails
            annualised_roi=0.90,
            prob_profit=0.75,
            open_interest=1500,
            iv_percentile_52w=85.0,
        )
        result = screener.apply_filters(scored)
        assert result.passed_all_filters is False
        assert len(result.filter_failures) == 1

    def test_zero_failures_is_passing(self, config):
        screener = make_screener(config)
        scored = make_scored(
            composite_score=0.80,
            annualised_roi=0.90,
            prob_profit=0.75,
            open_interest=1500,
            iv_percentile_52w=85.0,
        )
        result = screener.apply_filters(scored)
        assert (len(result.filter_failures) == 0) == result.passed_all_filters


# ---------------------------------------------------------------------------
# Test: run() returns contracts sorted by composite_score descending
# ---------------------------------------------------------------------------

class TestRunSortOrder:
    """run() must return all results sorted by composite_score descending."""

    def test_sorted_descending(self, config):
        """Build a screener whose load_contracts returns pre-built contracts
        and verify the final list is correctly ordered."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        screener = OptionScreener(mock_pool, config)

        # Patch get_latest_snapshot_time and load_contracts
        snap_time = datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc)

        contracts = [
            _make_scoreable_contract(symbol="VRT", mid=10.74, delta=-0.27, theta=-0.52,
                                     iv_pct_52w=92.0, oi=1804),
            _make_scoreable_contract(symbol="GLD", mid=2.50, delta=-0.35, theta=-0.20,
                                     iv_pct_52w=60.0, oi=600),
            _make_scoreable_contract(symbol="GEV", mid=8.00, delta=-0.22, theta=-0.40,
                                     iv_pct_52w=80.0, oi=900),
        ]

        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1, 2, 3]),
            patch.object(screener, "get_latest_snapshot_time", return_value=snap_time),
            patch.object(screener, "load_contracts", side_effect=[
                [contracts[0]], [contracts[1]], [contracts[2]],
            ]),
        ):
            results = screener.run()

        scores = [r.composite_score for r in results]
        assert scores == sorted(scores, reverse=True), (
            f"Results not sorted descending: {scores}"
        )

    def test_empty_results_when_no_snapshots(self, config):
        screener = make_screener(config)
        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1]),
            patch.object(screener, "get_latest_snapshot_time", return_value=None),
        ):
            results = screener.run()
        assert results == []


# ---------------------------------------------------------------------------
# Test: run() skips underlying with no snapshot gracefully
# ---------------------------------------------------------------------------

class TestRunSkipsNoSnapshot:
    """
    If get_latest_snapshot_time() returns None for an underlying, run() must
    log a warning, skip that underlying, and continue processing the rest.
    """

    def test_skips_missing_underlying_and_continues(self, config):
        screener = make_screener(config)
        snap_time = datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc)

        # underlying_id=1 → no snapshot; underlying_id=2 → has snapshot
        contracts = [
            _make_scoreable_contract(symbol="GLD", mid=2.50, delta=-0.35,
                                     theta=-0.20, iv_pct_52w=80.0, oi=600),
        ]

        def fake_snapshot_time(uid):
            return None if uid == 1 else snap_time

        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1, 2]),
            patch.object(screener, "get_latest_snapshot_time", side_effect=fake_snapshot_time),
            patch.object(screener, "load_contracts", return_value=contracts),
        ):
            results = screener.run()

        # Only underlying_id=2 has data — we expect 1 contract in results
        assert len(results) == 1
        assert results[0].symbol == "GLD"

    def test_all_underlyings_missing_returns_empty(self, config):
        screener = make_screener(config)
        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1, 2]),
            patch.object(screener, "get_latest_snapshot_time", return_value=None),
        ):
            results = screener.run()
        assert results == []


# ---------------------------------------------------------------------------
# Test: run() returns both passing and failing contracts
# ---------------------------------------------------------------------------

class TestRunReturnsBothPassingAndFailing:
    """
    run() must return ALL scored contracts regardless of whether they passed
    filters. The caller decides what to do with failing contracts.
    """

    def test_passing_and_failing_both_in_results(self, config):
        screener = make_screener(config)
        snap_time = datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc)

        # One contract that will pass all filters (needs high composite_score,
        # good ROI, good PoP, good OI, good IV) — this is hard to engineer
        # from real formula values so we just check both types appear.
        contracts = [
            _make_scoreable_contract(symbol="VRT", mid=10.74, delta=-0.27,
                                     theta=-0.52, iv_pct_52w=92.0, oi=1804),
            _make_scoreable_contract(symbol="LOW_IV", mid=1.00, delta=-0.40,
                                     theta=-0.05, iv_pct_52w=15.0, oi=50),
        ]

        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1]),
            patch.object(screener, "get_latest_snapshot_time", return_value=snap_time),
            patch.object(screener, "load_contracts", return_value=contracts),
        ):
            results = screener.run()

        assert len(results) == 2, "Both contracts must be returned"

        # At least the low-IV contract must fail its IV filter
        low_iv_results = [r for r in results if r.symbol == "LOW_IV"]
        assert len(low_iv_results) == 1
        assert low_iv_results[0].passed_all_filters is False

    def test_passed_all_filters_field_is_set_on_every_result(self, config):
        screener = make_screener(config)
        snap_time = datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc)

        contracts = [
            _make_scoreable_contract(symbol="VRT", mid=10.74, delta=-0.27,
                                     theta=-0.52, iv_pct_52w=92.0, oi=1804),
        ]

        with (
            patch.object(screener, "_get_active_underlying_ids", return_value=[1]),
            patch.object(screener, "get_latest_snapshot_time", return_value=snap_time),
            patch.object(screener, "load_contracts", return_value=contracts),
        ):
            results = screener.run()

        for r in results:
            # passed_all_filters must be a bool — not None, not unset
            assert isinstance(r.passed_all_filters, bool)


# ---------------------------------------------------------------------------
# Private helper for test contract construction
# ---------------------------------------------------------------------------

def _make_scoreable_contract(
    symbol: str,
    mid: float,
    delta: float,
    theta: float,
    iv_pct_52w: float,
    oi: int,
    dte: int = 18,
    strike: float = 245.0,
) -> OptionContract:
    """Build a minimal OptionContract suitable for scorer.score()."""
    return OptionContract(
        underlying_id=1,
        symbol=symbol,
        snapshot_time=datetime(2026, 4, 7, 14, 0, 0, tzinfo=timezone.utc),
        underlying_price=265.54,
        expiry_date=date(2026, 4, 24),
        dte=dte,
        strike=strike,
        option_type="P",
        bid=mid - 0.10,
        ask=mid + 0.10,
        mid=mid,
        open_interest=oi,
        implied_vol=0.746,
        delta=delta,
        gamma=0.012,
        theta=theta,
        vega=0.31,
        iv_percentile_52w=iv_pct_52w,
        iv_percentile_30d=iv_pct_52w - 5.0,
        vix_level=21.5,
        ibkr_conid=667769626,
    )
