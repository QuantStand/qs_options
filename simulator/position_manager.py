"""
position_manager.py — Position lifecycle for the QuantStand paper trading simulator.

Handles open, monitor, close, and contract matching for paper positions.
Writes to trade_log on open and close events only.
Monitoring updates are in-memory — no DB writes during the hold period.
"""

import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

import psycopg2

from engine.models import ScoredContract, OptionContract
from .models import PaperPosition

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# SQL
# ------------------------------------------------------------------

_INSERT_SQL = """
    INSERT INTO trade_log (
        underlying_id, trade_mode,
        expiry_date, strike, option_type, contracts,
        entry_time, entry_price, entry_underlying_price,
        entry_delta, entry_theta, entry_iv, entry_iv_percentile,
        entry_dte, entry_composite_score, entry_ann_roi,
        premium_collected, collateral_assigned,
        threshold_min_score, threshold_min_roi,
        threshold_min_pop, threshold_min_oi
    ) VALUES (
        %s, 'PAPER',
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s
    )
    RETURNING trade_id
"""

_UPDATE_SQL = """
    UPDATE trade_log SET
        exit_time          = %s,
        exit_price         = %s,
        exit_reason        = %s,
        realised_pnl       = %s,
        pnl_pct_of_premium = %s,
        was_assigned       = %s,
        assigned_cost_basis = %s
    WHERE trade_id = %s
"""

_LOAD_OPEN_SQL = """
    SELECT
        tl.trade_id,
        tl.underlying_id,
        u.symbol,
        tl.expiry_date,
        tl.strike,
        tl.option_type,
        tl.contracts,
        tl.entry_time,
        tl.entry_price,
        tl.entry_underlying_price,
        tl.entry_delta,
        tl.entry_theta,
        tl.entry_iv,
        tl.entry_iv_percentile,
        tl.entry_dte,
        tl.entry_composite_score,
        tl.entry_ann_roi,
        tl.premium_collected,
        tl.collateral_assigned,
        tl.threshold_min_score,
        tl.threshold_min_roi,
        tl.threshold_min_pop,
        tl.threshold_min_oi
    FROM trade_log tl
    JOIN underlyings u ON tl.underlying_id = u.underlying_id
    WHERE tl.trade_mode = 'PAPER'
      AND tl.exit_time IS NULL
"""

# Query latest snapshot for a specific contract (for monitoring near-expiry positions
# that have fallen below the screener's DTE range).
_FETCH_SNAPSHOT_SQL = """
    SELECT
        ocs.mid,
        ocs.underlying_price,
        ocs.delta,
        ocs.dte,
        ocs.snapshot_time
    FROM options_chain_snapshots ocs
    WHERE ocs.underlying_id  = %s
      AND ocs.expiry_date    = %s
      AND ocs.strike         = %s
      AND ocs.option_type    = %s
      AND ocs.mid IS NOT NULL
    ORDER BY ocs.snapshot_time DESC
    LIMIT 1
"""


class PositionManager:
    """
    Manages the full lifecycle of paper positions.

    DB writes occur only at open and close events.
    Monitoring updates are in-memory only.
    """

    def __init__(self, db_connection_pool):
        self.pool = db_connection_pool

    # ------------------------------------------------------------------
    # open_position
    # ------------------------------------------------------------------

    def open_position(
        self,
        candidate: ScoredContract,
        contracts: int,
        threshold_params: dict,
    ) -> PaperPosition:
        """
        Create a PaperPosition from a ScoredContract and INSERT it into trade_log.
        Returns the PaperPosition with trade_id populated from RETURNING.
        """
        premium_collected = candidate.mid * 100.0 * contracts
        collateral_assigned = candidate.strike * 100.0 * contracts
        entry_time = datetime.now(timezone.utc)

        iv_percentile = (
            candidate.iv_percentile_52w
            if candidate.iv_percentile_52w is not None
            else (candidate.iv_percentile_30d or 0.0)
        )

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _INSERT_SQL,
                    (
                        candidate.underlying_id,
                        candidate.expiry_date,
                        candidate.strike,
                        candidate.option_type,
                        contracts,
                        entry_time,
                        candidate.mid,
                        candidate.underlying_price,
                        candidate.delta,
                        candidate.theta,
                        candidate.implied_vol,
                        iv_percentile,
                        candidate.dte,
                        candidate.composite_score,
                        candidate.annualised_roi,
                        premium_collected,
                        collateral_assigned,
                        threshold_params["threshold_min_score"],
                        threshold_params["threshold_min_roi"],
                        threshold_params["threshold_min_pop"],
                        threshold_params["threshold_min_oi"],
                    ),
                )
                trade_id = cur.fetchone()[0]
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

        return PaperPosition(
            trade_id=trade_id,
            underlying_id=candidate.underlying_id,
            symbol=candidate.symbol,
            expiry_date=candidate.expiry_date,
            strike=candidate.strike,
            option_type=candidate.option_type,
            contracts=contracts,
            entry_time=entry_time,
            entry_price=candidate.mid,
            entry_underlying_price=candidate.underlying_price,
            entry_delta=candidate.delta,
            entry_theta=candidate.theta,
            entry_iv=candidate.implied_vol,
            entry_iv_percentile=iv_percentile,
            entry_dte=candidate.dte,
            entry_composite_score=candidate.composite_score,
            entry_ann_roi=candidate.annualised_roi,
            premium_collected=premium_collected,
            collateral_assigned=collateral_assigned,
            threshold_min_score=threshold_params["threshold_min_score"],
            threshold_min_roi=threshold_params["threshold_min_roi"],
            threshold_min_pop=threshold_params["threshold_min_pop"],
            threshold_min_oi=threshold_params["threshold_min_oi"],
            # Current state initialised to entry values
            current_price=candidate.mid,
            current_underlying_price=candidate.underlying_price,
            current_delta=candidate.delta,
            current_dte=candidate.dte,
            unrealised_pnl=0.0,
            # Exit state — all None at open
            exit_time=None,
            exit_price=None,
            exit_reason=None,
            realised_pnl=None,
            pnl_pct_of_premium=None,
            was_assigned=False,
            assigned_cost_basis=None,
        )

    # ------------------------------------------------------------------
    # update_position
    # ------------------------------------------------------------------

    def update_position(
        self,
        position: PaperPosition,
        latest_contract: Optional[ScoredContract],
    ) -> PaperPosition:
        """
        Update in-memory current state from the latest snapshot.
        No DB write — monitoring state is in-memory only.

        If latest_contract is None (no snapshot data), logs a WARNING
        and returns the position unchanged.
        """
        if latest_contract is None:
            log.warning(
                "No snapshot data for %s %sP exp=%s — position unchanged",
                position.symbol, position.strike, position.expiry_date,
            )
            return position

        position.current_price = float(latest_contract.mid)
        position.current_underlying_price = float(latest_contract.underlying_price)
        position.current_delta = float(latest_contract.delta)
        position.current_dte = int(latest_contract.dte)
        position.unrealised_pnl = (
            (position.entry_price - latest_contract.mid)
            * 100.0
            * position.contracts
        )
        return position

    # ------------------------------------------------------------------
    # close_position
    # ------------------------------------------------------------------

    def close_position(
        self,
        position: PaperPosition,
        exit_price: float,
        exit_reason: str,
        exit_time: datetime,
    ) -> PaperPosition:
        """
        Set all exit fields and UPDATE the existing trade_log row.
        Never inserts a new row — always updates the open row by trade_id.
        """
        realised_pnl = (
            (position.entry_price - exit_price) * 100.0 * position.contracts
        )
        pnl_pct = (
            realised_pnl / position.premium_collected
            if position.premium_collected != 0
            else 0.0
        )
        was_assigned = exit_reason == "ASSIGNED"
        assigned_cost_basis = (
            position.strike - position.entry_price if was_assigned else None
        )

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _UPDATE_SQL,
                    (
                        exit_time,
                        exit_price,
                        exit_reason,
                        realised_pnl,
                        pnl_pct,
                        was_assigned,
                        assigned_cost_basis,
                        position.trade_id,
                    ),
                )
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

        position.exit_time = exit_time
        position.exit_price = exit_price
        position.exit_reason = exit_reason
        position.realised_pnl = realised_pnl
        position.pnl_pct_of_premium = pnl_pct
        position.was_assigned = was_assigned
        position.assigned_cost_basis = assigned_cost_basis
        return position

    # ------------------------------------------------------------------
    # find_matching_contract
    # ------------------------------------------------------------------

    def find_matching_contract(
        self,
        position: PaperPosition,
        scored_contracts: list,
    ) -> Optional[ScoredContract]:
        """
        Find the scored contract matching this position's
        underlying_id, expiry_date, strike, and option_type.

        First searches scored_contracts (fast path — covers contracts still
        within the screener's DTE range).

        If not found (e.g. near-expiry positions below dte_min), falls back
        to a direct DB query for the latest snapshot of this specific contract.
        This ensures expiry and assignment checks work correctly even when
        the contract has aged out of the screener's DTE window.

        Returns the matching ScoredContract or None if unavailable.
        """
        # Fast path: search the in-memory scored list
        for sc in scored_contracts:
            if (
                sc.underlying_id == position.underlying_id
                and sc.expiry_date == position.expiry_date
                and float(sc.strike) == float(position.strike)
                and sc.option_type == position.option_type
            ):
                return sc

        # Fallback: query DB for latest snapshot of this specific contract
        return self._fetch_contract_snapshot(position)

    def _fetch_contract_snapshot(
        self, position: PaperPosition
    ) -> Optional[ScoredContract]:
        """
        Direct DB query for the most recent snapshot of a specific contract.
        Returns a minimal ScoredContract with price/delta/dte fields populated.
        Scoring fields (composite_score etc.) are set to 0 — sufficient for
        monitoring purposes.
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _FETCH_SNAPSHOT_SQL,
                    (
                        position.underlying_id,
                        position.expiry_date,
                        position.strike,
                        position.option_type,
                    ),
                )
                row = cur.fetchone()
        finally:
            self.pool.putconn(conn)

        if row is None:
            return None

        mid, underlying_price, delta, dte, snapshot_time = row

        # Build a minimal ScoredContract — only fields needed for monitoring
        from datetime import date as date_type
        contract = OptionContract(
            underlying_id=position.underlying_id,
            symbol=position.symbol,
            snapshot_time=snapshot_time,
            underlying_price=float(underlying_price),
            expiry_date=position.expiry_date,
            dte=int(dte) if dte is not None else 0,
            strike=float(position.strike),
            option_type=position.option_type,
            bid=float(mid),
            ask=float(mid),
            mid=float(mid),
            open_interest=0,
            implied_vol=0.0,
            delta=float(delta) if delta is not None else 0.0,
            gamma=0.0,
            theta=0.0,
            vega=0.0,
            iv_percentile_52w=None,
            iv_percentile_30d=None,
            vix_level=None,
            ibkr_conid=0,
        )

        from engine.models import ScoredContract as SC
        return SC(
            **vars(contract),
            composite_score=0.0,
            annualised_roi=0.0,
            theta_per_collateral=0.0,
            prob_profit=0.0,
            collateral=position.strike * 100.0,
            premium_per_contract=float(mid) * 100.0,
            total_premium_5_contracts=float(mid) * 500.0,
            effective_entry_if_assigned=position.strike - float(mid),
            passed_all_filters=False,
            filter_failures=["monitoring_only"],
        )

    # ------------------------------------------------------------------
    # load_open_positions (called by simulator on startup)
    # ------------------------------------------------------------------

    def load_open_positions(self) -> list:
        """
        Query trade_log for all open PAPER positions and reconstruct
        PaperPosition objects. Called on simulator startup to restore
        state after a restart.

        Current state fields are initialised to entry values — they will
        be updated on the next monitoring cycle.
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(_LOAD_OPEN_SQL)
                rows = cur.fetchall()
        finally:
            self.pool.putconn(conn)

        positions = []
        for row in rows:
            (
                trade_id, underlying_id, symbol,
                expiry_date, strike, option_type, contracts,
                entry_time, entry_price, entry_underlying_price,
                entry_delta, entry_theta, entry_iv, entry_iv_percentile,
                entry_dte, entry_composite_score, entry_ann_roi,
                premium_collected, collateral_assigned,
                threshold_min_score, threshold_min_roi,
                threshold_min_pop, threshold_min_oi,
            ) = row

            positions.append(PaperPosition(
                trade_id=int(trade_id),
                underlying_id=int(underlying_id),
                symbol=symbol,
                expiry_date=expiry_date,
                strike=float(strike),
                option_type=option_type.strip(),
                contracts=int(contracts),
                entry_time=entry_time,
                entry_price=float(entry_price),
                entry_underlying_price=float(entry_underlying_price),
                entry_delta=float(entry_delta) if entry_delta is not None else 0.0,
                entry_theta=float(entry_theta) if entry_theta is not None else 0.0,
                entry_iv=float(entry_iv) if entry_iv is not None else 0.0,
                entry_iv_percentile=float(entry_iv_percentile) if entry_iv_percentile is not None else 0.0,
                entry_dte=int(entry_dte) if entry_dte is not None else 0,
                entry_composite_score=float(entry_composite_score) if entry_composite_score is not None else 0.0,
                entry_ann_roi=float(entry_ann_roi) if entry_ann_roi is not None else 0.0,
                premium_collected=float(premium_collected),
                collateral_assigned=float(collateral_assigned),
                threshold_min_score=float(threshold_min_score) if threshold_min_score is not None else 0.0,
                threshold_min_roi=float(threshold_min_roi) if threshold_min_roi is not None else 0.0,
                threshold_min_pop=float(threshold_min_pop) if threshold_min_pop is not None else 0.0,
                threshold_min_oi=int(threshold_min_oi) if threshold_min_oi is not None else 0,
                # Initialise current state to entry values — updated on next cycle
                current_price=float(entry_price),
                current_underlying_price=float(entry_underlying_price),
                current_delta=float(entry_delta) if entry_delta is not None else 0.0,
                current_dte=int(entry_dte) if entry_dte is not None else 0,
                unrealised_pnl=0.0,
                exit_time=None,
                exit_price=None,
                exit_reason=None,
                realised_pnl=None,
                pnl_pct_of_premium=None,
                was_assigned=False,
                assigned_cost_basis=None,
            ))

        log.info("Loaded %d open paper position(s) from DB", len(positions))
        return positions
