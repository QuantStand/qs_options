"""
scorer.py — Formula implementation for the QuantStand options scoring engine.

Contains one class: OptionScorer
Contains one public method: score(contract: OptionContract) -> ScoredContract

The formula is fixed by strategy. Do not modify it, add terms, or change weights.
The ML layer will tune thresholds — the formula structure is permanent.
"""

import logging

from .models import OptionContract, ScoredContract

log = logging.getLogger(__name__)


class OptionScorer:
    """
    Stateless scorer. Computes composite score and derived fields for a single
    OptionContract. Contains no configuration, no database access, no side effects.
    """

    def score(self, contract: OptionContract) -> ScoredContract:
        """
        Score one put option contract.

        Returns a ScoredContract with all computed fields populated.
        passed_all_filters is always False at this stage — screener.py sets it.
        filter_failures is always [] at this stage — screener.py populates it.
        """

        delta_abs = abs(contract.delta)
        theta_abs = abs(contract.theta)
        collateral = contract.strike * 100.0
        premium = contract.mid

        # Shared derived fields used in both the guard paths and the main path
        prob_profit = 1.0 - delta_abs
        premium_per_contract = premium * 100.0
        total_premium_5_contracts = premium * 500.0
        effective_entry_if_assigned = contract.strike - premium

        # --- Guard: DTE == 0 causes division by zero in the ROI formula ---
        if contract.dte == 0:
            log.debug(
                "Contract %s strike=%.2f skipped — DTE is 0",
                contract.symbol, contract.strike,
            )
            return ScoredContract(
                **vars(contract),
                composite_score=0.0,
                annualised_roi=0.0,
                theta_per_collateral=theta_abs / collateral if collateral else 0.0,
                prob_profit=prob_profit,
                collateral=collateral,
                premium_per_contract=premium_per_contract,
                total_premium_5_contracts=total_premium_5_contracts,
                effective_entry_if_assigned=effective_entry_if_assigned,
                passed_all_filters=False,
                filter_failures=[],
            )

        # --- Step 1: Derive base values ---

        annualised_roi = (premium / contract.strike) * (365.0 / contract.dte)
        theta_per_collateral = theta_abs / collateral

        # IV percentile: use 52w if available, fall back to 30d, then 0.0
        iv_percentile = contract.iv_percentile_52w
        if iv_percentile is None:
            iv_percentile = contract.iv_percentile_30d
        if iv_percentile is None:
            # Cannot score — will fail IV filter downstream
            iv_percentile = 0.0
            log.debug(
                "Contract %s strike=%.2f has no IV percentile — using 0.0",
                contract.symbol, contract.strike,
            )

        # --- Step 2: Compute factor components ---

        iv_factor = iv_percentile / 100.0

        liquidity_factor = min(contract.open_interest / 500.0, 1.0)
        # Capped at 1.0 — OI above 500 gives no additional reward

        roi_factor = annualised_roi
        # Not capped — higher annualised ROI is rewarded directly

        # --- Step 3: Composite score ---

        if delta_abs == 0.0:
            log.debug(
                "Contract %s strike=%.2f has delta=0 — setting composite_score=0.0",
                contract.symbol, contract.strike,
            )
            composite_score = 0.0
        else:
            composite_score = (
                (theta_per_collateral * 10_000)  # scaling constant — keeps scores in interpretable range
                * (1.0 / delta_abs)
                * iv_factor
                * liquidity_factor
                * roi_factor
            )

        # --- Step 4: Return ScoredContract ---
        # passed_all_filters and filter_failures are set to their initial values here.
        # screener.py calls apply_filters() to populate them.

        return ScoredContract(
            **vars(contract),
            composite_score=composite_score,
            annualised_roi=annualised_roi,
            theta_per_collateral=theta_per_collateral,
            prob_profit=prob_profit,
            collateral=collateral,
            premium_per_contract=premium_per_contract,
            total_premium_5_contracts=total_premium_5_contracts,
            effective_entry_if_assigned=effective_entry_if_assigned,
            passed_all_filters=False,
            filter_failures=[],
        )
