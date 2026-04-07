"""
tests/test_scorer.py — Unit tests for engine/scorer.py.

No database required. All tests construct OptionContract objects directly
and assert computed fields on the returned ScoredContract.

Run with: pytest tests/test_scorer.py -v
"""

import pytest
from datetime import datetime, date, timezone

from engine.models import OptionContract
from engine.scorer import OptionScorer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_contract(**overrides) -> OptionContract:
    """
    Build a baseline OptionContract for VRT Apr24 245P (live trade, 2026-04-07).
    Override any field by passing it as a keyword argument.
    """
    defaults = dict(
        underlying_id=1,
        symbol="VRT",
        snapshot_time=datetime(2026, 4, 7, 14, 0, 0, tzinfo=timezone.utc),
        underlying_price=265.54,
        expiry_date=date(2026, 4, 24),
        dte=18,
        strike=245.00,
        option_type="P",
        bid=10.50,
        ask=10.98,
        mid=10.74,
        open_interest=1804,
        implied_vol=0.746,
        delta=-0.27,
        gamma=0.012,
        theta=-0.52,
        vega=0.31,
        iv_percentile_52w=92.0,
        iv_percentile_30d=88.0,
        vix_level=21.5,
        ibkr_conid=667769626,
    )
    defaults.update(overrides)
    return OptionContract(**defaults)


scorer = OptionScorer()


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestZeroDeltaGuard:
    """delta == 0 must produce composite_score == 0.0 without raising."""

    def test_composite_score_is_zero(self):
        contract = make_contract(delta=0.0)
        result = scorer.score(contract)
        assert result.composite_score == 0.0

    def test_does_not_raise(self):
        contract = make_contract(delta=0.0)
        scorer.score(contract)  # must not raise ZeroDivisionError


class TestLowOIPenalty:
    """open_interest = 250 (below 500 threshold) → liquidity_factor = 0.5."""

    def test_liquidity_factor_is_half(self):
        contract = make_contract(open_interest=250)
        result = scorer.score(contract)
        # liquidity_factor = min(250/500, 1.0) = 0.5
        # composite_score should be exactly half of the baseline score
        baseline = scorer.score(make_contract(open_interest=500))
        assert result.composite_score == pytest.approx(baseline.composite_score * 0.5, rel=1e-6)

    def test_composite_score_lower_than_500_oi(self):
        low_oi = scorer.score(make_contract(open_interest=250))
        high_oi = scorer.score(make_contract(open_interest=1000))
        assert low_oi.composite_score < high_oi.composite_score

    def test_oi_above_500_is_capped(self):
        """OI above 500 gives no additional reward — scores should be equal."""
        at_500 = scorer.score(make_contract(open_interest=500))
        at_2000 = scorer.score(make_contract(open_interest=2000))
        assert at_500.composite_score == pytest.approx(at_2000.composite_score, rel=1e-9)


class TestZeroIVPercentile:
    """iv_percentile_52w = 0.0 and iv_percentile_30d = None → composite_score == 0.0."""

    def test_composite_score_is_zero(self):
        contract = make_contract(iv_percentile_52w=0.0, iv_percentile_30d=None)
        result = scorer.score(contract)
        # iv_factor = 0.0/100.0 = 0.0 → composite_score = 0.0
        assert result.composite_score == 0.0

    def test_both_none_also_zero(self):
        contract = make_contract(iv_percentile_52w=None, iv_percentile_30d=None)
        result = scorer.score(contract)
        assert result.composite_score == 0.0


class TestIVFallbackTo30d:
    """iv_percentile_52w = None → engine falls back to iv_percentile_30d."""

    def test_iv_factor_uses_30d_value(self):
        contract = make_contract(iv_percentile_52w=None, iv_percentile_30d=45.0)
        result = scorer.score(contract)

        # Manually compute expected composite_score with iv_factor = 0.45
        delta_abs = 0.27
        theta_abs = 0.52
        collateral = 245.0 * 100.0
        premium = 10.74
        annualised_roi = (premium / 245.0) * (365.0 / 18)
        theta_per_collateral = theta_abs / collateral
        iv_factor = 45.0 / 100.0          # ← key assertion
        liquidity_factor = min(1804 / 500.0, 1.0)
        expected = theta_per_collateral * (1.0 / delta_abs) * iv_factor * liquidity_factor * annualised_roi

        assert result.composite_score == pytest.approx(expected, rel=1e-6)

    def test_iv_52w_takes_priority_over_30d(self):
        """When 52w is available it must be used, not the 30d fallback."""
        using_52w = scorer.score(make_contract(iv_percentile_52w=92.0, iv_percentile_30d=45.0))
        using_30d = scorer.score(make_contract(iv_percentile_52w=None, iv_percentile_30d=45.0))
        # 92.0 > 45.0 so composite_score using 52w must be higher
        assert using_52w.composite_score > using_30d.composite_score


class TestZeroDTEGuard:
    """DTE == 0 must not raise ZeroDivisionError."""

    def test_does_not_raise(self):
        contract = make_contract(dte=0)
        scorer.score(contract)  # must not raise

    def test_annualised_roi_is_zero(self):
        contract = make_contract(dte=0)
        result = scorer.score(contract)
        assert result.annualised_roi == 0.0

    def test_composite_score_is_zero(self):
        contract = make_contract(dte=0)
        result = scorer.score(contract)
        assert result.composite_score == 0.0


class TestVRTLiveTradeValidation:
    """
    Validate scorer output against the live VRT Apr24'26 245P trade.

    Input values (from spec Section 4.5 and position_management_rules.md):
        symbol         = VRT
        strike         = 245.00
        mid (premium)  = 10.74
        delta          = -0.27
        theta          = -0.52
        open_interest  = 1804
        implied_vol    = 0.746
        iv_percentile  = 92.0 (52w)
        dte            = 18
        underlying     = 265.54

    ---- IMPORTANT NOTE ON SPEC SECTION 4.5 ----

    The spec states the following expected outputs:
        annualised_roi  = 0.7940  (tolerance ±0.001)
        composite_score = 0.653   (tolerance ±0.005)

    However the formula as written in Sections 4.1–4.3 produces:
        annualised_roi  ≈ 0.8891  (not 0.7940)
        composite_score ≈ 6.43e-5 (not 0.653)

    These discrepancies are outside the stated tolerances and cannot be
    reconciled by rounding or floating-point error. The formula is implemented
    exactly as specified. The expected values in Section 4.5 appear to have
    been computed with a different (possibly earlier) version of the formula.

    ACTION REQUIRED: Farhad to confirm either:
      (a) The Section 4.5 expected values are wrong — formula is correct, or
      (b) The formula definition is wrong — provide the intended formula.

    Until resolved, these tests assert the ACTUAL formula outputs, not the
    spec's stated values. All other outputs (collateral, premium_per_contract,
    prob_profit, effective_entry) match the spec exactly.
    """

    @pytest.fixture
    def vrt_result(self):
        contract = make_contract()   # baseline = VRT 245P values from Section 4.5
        return scorer.score(contract)

    # --- Fields where spec and formula agree ---

    def test_collateral(self, vrt_result):
        # strike * 100 = 245.00 * 100 = 24,500.00 — spec says exact
        assert vrt_result.collateral == 24_500.00

    def test_premium_per_contract(self, vrt_result):
        # mid * 100 = 10.74 * 100 = 1,074.00 — spec says exact
        assert vrt_result.premium_per_contract == pytest.approx(1_074.00, abs=0.01)

    def test_total_premium_5_contracts(self, vrt_result):
        # mid * 500 = 10.74 * 500 = 5,370.00 — spec says exact
        assert vrt_result.total_premium_5_contracts == pytest.approx(5_370.00, abs=0.01)

    def test_prob_profit(self, vrt_result):
        # 1 - abs(delta) = 1 - 0.27 = 0.73 — spec says exact
        assert vrt_result.prob_profit == pytest.approx(0.73, abs=1e-9)

    def test_effective_entry_if_assigned(self, vrt_result):
        # strike - mid = 245.00 - 10.74 = 234.26 — spec tolerance ±0.01
        assert vrt_result.effective_entry_if_assigned == pytest.approx(234.26, abs=0.01)

    def test_theta_per_collateral(self, vrt_result):
        # abs(theta) / collateral = 0.52 / 24500 = 2.1224e-5 — spec tolerance ±1e-6
        assert vrt_result.theta_per_collateral == pytest.approx(2.1224e-5, abs=1e-6)

    # --- Fields where spec expected value does not match formula output ---
    # See class docstring for the discrepancy note.

    def test_annualised_roi_formula_output(self, vrt_result):
        # Formula: (10.74 / 245.00) * (365 / 18) ≈ 0.8891
        # Spec states 0.7940 — see discrepancy note above.
        expected_formula = (10.74 / 245.00) * (365.0 / 18)
        assert vrt_result.annualised_roi == pytest.approx(expected_formula, rel=1e-6)

    def test_composite_score_formula_output(self, vrt_result):
        # Formula produces ≈ 6.43e-5, not spec's stated 0.653.
        # See discrepancy note in class docstring.
        delta_abs = 0.27
        theta_abs = 0.52
        collateral = 245.0 * 100.0
        premium = 10.74
        annualised_roi = (premium / 245.0) * (365.0 / 18)
        theta_per_collateral = theta_abs / collateral
        iv_factor = 92.0 / 100.0
        liquidity_factor = 1.0   # OI=1804 → min(1804/500, 1.0)=1.0
        expected = theta_per_collateral * (1.0 / delta_abs) * iv_factor * liquidity_factor * annualised_roi
        assert vrt_result.composite_score == pytest.approx(expected, rel=1e-6)

    def test_composite_score_is_positive(self, vrt_result):
        assert vrt_result.composite_score > 0.0

    # --- Structural checks ---

    def test_passed_all_filters_not_set_by_scorer(self, vrt_result):
        """Scorer never sets passed_all_filters — that is screener.py's job."""
        assert vrt_result.passed_all_filters is False

    def test_filter_failures_empty_from_scorer(self, vrt_result):
        """Scorer leaves filter_failures empty."""
        assert vrt_result.filter_failures == []

    def test_inherits_all_option_contract_fields(self, vrt_result):
        assert vrt_result.symbol == "VRT"
        assert vrt_result.strike == 245.00
        assert vrt_result.dte == 18
        assert vrt_result.ibkr_conid == 667769626
