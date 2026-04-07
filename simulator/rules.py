"""
rules.py — Position management rules for the QuantStand paper trading simulator.

Rules are configurable via config.yaml. No magic numbers hardcoded here.
These rules encode docs/trading/position_management_rules.md exactly.
Do not change threshold defaults without updating that document first
and committing the change with a reason in the changelog.
"""

import logging
from typing import Optional

from engine.models import ScoredContract
from .models import PaperPosition

log = logging.getLogger(__name__)


class Rules:
    """
    Encapsulates all entry and exit decision logic.
    Config is passed at construction and again at each call (matching
    the calling convention used in simulator.py).
    """

    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # Entry rules
    # ------------------------------------------------------------------

    def can_open_new_position(
        self,
        candidate: ScoredContract,
        open_positions: list,
        config: dict,
    ) -> tuple:
        """
        Returns (True, '') if the candidate can be entered.
        Returns (False, reason) if it cannot.

        Checks all 5 conditions in order — first blocking reason returned.
        """
        pm = config["position_management"]
        scoring = config["scoring"]

        # Condition 1 — Passed all screener filters
        if not candidate.passed_all_filters:
            return (False, "screener_filter_failed")

        # Condition 2 — Maximum concurrent positions
        if len(open_positions) >= pm["max_concurrent_positions"]:
            return (False, "max_positions_reached")

        # Condition 3 — No existing position in same underlying
        for pos in open_positions:
            if pos.underlying_id == candidate.underlying_id:
                return (False, "duplicate_underlying")

        # Condition 4 — Collateral available
        contracts = pm["contracts_per_trade"]
        collateral_needed = candidate.strike * 100.0 * contracts
        if collateral_needed > pm["max_collateral_per_position_usd"]:
            return (False, "collateral_limit_exceeded")

        # Condition 5 — Minimum DTE
        if candidate.dte < scoring["dte_min"]:
            return (False, "dte_below_minimum")

        return (True, "")

    # ------------------------------------------------------------------
    # Exit rules
    # ------------------------------------------------------------------

    def should_close_position(
        self,
        position: PaperPosition,
        latest_snapshot: Optional[ScoredContract],
        config: dict,
    ) -> tuple:
        """
        Returns (True, exit_reason) if the position should be closed now.
        Returns (False, '') if it should continue to be held.

        Rules are checked in priority order — first match wins.
        Rule 2 (ASSIGNED) is checked before Rule 1 (EXPIRED) because
        assignment is a more specific outcome that must take precedence.
        """
        rules_cfg = config["rules"]

        # Rule 2 — Assignment at expiry (checked before Rule 1)
        if position.current_dte == 0:
            if (
                latest_snapshot is not None
                and latest_snapshot.underlying_price < position.strike
            ):
                return (True, "ASSIGNED")

        # Rule 1 — Expiry
        if position.current_dte == 0:
            return (True, "EXPIRED")

        # Rule 3 — Early close threshold (time-decaying)
        # Avoids division by zero if somehow entry_price is 0
        if position.entry_price > 0:
            pct_remaining = position.current_price / position.entry_price
        else:
            pct_remaining = 1.0

        if position.current_dte > rules_cfg["early_close_dte_boundary"]:
            threshold = rules_cfg["early_close_threshold_standard"]
        else:
            threshold = rules_cfg["early_close_threshold_tight"]

        if pct_remaining <= threshold:
            log.debug(
                "%s strike=%.2f: pct_remaining=%.3f <= threshold=%.2f "
                "(dte=%d boundary=%d) → EARLY_CLOSE",
                position.symbol, position.strike,
                pct_remaining, threshold,
                position.current_dte, rules_cfg["early_close_dte_boundary"],
            )
            return (True, "EARLY_CLOSE")

        # Rule 4 — Stop loss / reassessment trigger
        if (
            latest_snapshot is not None
            and latest_snapshot.underlying_price
                < rules_cfg["stop_loss_underlying_price"]
            and abs(position.current_delta) > rules_cfg["stop_loss_delta_threshold"]
            and position.current_dte < rules_cfg["stop_loss_dte_threshold"]
        ):
            log.debug(
                "%s strike=%.2f: stop-loss triggered — underlying=%.2f "
                "delta=%.3f dte=%d",
                position.symbol, position.strike,
                latest_snapshot.underlying_price,
                position.current_delta,
                position.current_dte,
            )
            return (True, "STOP_LOSS")

        # Rule 5 — Hold
        return (False, "")
