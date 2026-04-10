"""
screener.py — Orchestration layer for the QuantStand options scoring engine.

Reads from DB, calls scorer, applies filters, returns ranked results.
Strictly read-only. Never writes to any database table.
"""

import logging
import traceback
from datetime import datetime, date
from typing import Optional

import psycopg2

from .models import OptionContract, ScoredContract
from .scorer import OptionScorer

log = logging.getLogger(__name__)

# SQL: load all scoreable puts for a given underlying and snapshot time.
# LEFT JOIN on volatility_surface is mandatory — missing vol surface rows must
# not silently drop contracts. Contracts with NULL mid/delta/theta are excluded
# at the SQL level and never reach the Python layer.
_LOAD_CONTRACTS_SQL = """
    SELECT
        ocs.underlying_id,
        u.symbol,
        ocs.snapshot_time,
        ocs.underlying_price,
        ocs.expiry_date,
        ocs.dte,
        ocs.strike,
        ocs.option_type,
        ocs.bid,
        ocs.ask,
        ocs.mid,
        ocs.open_interest,
        ocs.implied_vol,
        ocs.delta,
        ocs.gamma,
        ocs.theta,
        ocs.vega,
        ocs.ibkr_conid,
        vs.iv_percentile_52w,
        vs.iv_percentile_30d,
        vs.vix_level
    FROM options_chain_snapshots ocs
    JOIN underlyings u ON ocs.underlying_id = u.underlying_id
    LEFT JOIN volatility_surface vs
        ON  vs.underlying_id = ocs.underlying_id
        AND vs.snapshot_time = ocs.snapshot_time
    WHERE ocs.underlying_id = %s
      AND ocs.snapshot_time = %s
      AND ocs.option_type   = 'P'
      AND ocs.dte BETWEEN %s AND %s
      AND ocs.mid   IS NOT NULL
      AND ocs.delta IS NOT NULL
      AND ocs.theta IS NOT NULL
"""


class OptionScreener:
    """
    Orchestrates a full screening run across one or more underlyings.

    Constructor args:
        db_connection_pool — psycopg2 ThreadedConnectionPool
        config             — parsed config.yaml dict
    """

    def __init__(self, db_connection_pool, config: dict):
        self.pool = db_connection_pool
        self.scorer = OptionScorer()

        scoring_cfg = config["scoring"]

        self.min_composite_score = scoring_cfg["min_composite_score"]

        # annualised_roi and probability_of_profit are stored as decimals (0–1+)
        # in ScoredContract, but the config expresses them as percentages (0–100).
        # Divide by 100 here so that filter comparisons work correctly:
        #   e.g. annualised_roi=0.89 vs min=0.70 (converted from config 70.0)
        self.min_annualised_roi = scoring_cfg["min_annualised_roi"] / 100.0
        self.min_prob_profit = scoring_cfg["min_probability_of_profit"] / 100.0

        self.min_open_interest = scoring_cfg["min_open_interest"]
        self.dte_min = scoring_cfg["dte_min"]
        self.dte_max = scoring_cfg["dte_max"]

        # iv_percentile is stored on a 0–100 scale; config is also 0–100 — no conversion.
        self.iv_percentile_min = scoring_cfg["iv_percentile_min"]

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def get_latest_snapshot_time(self, underlying_id: int) -> Optional[datetime]:
        """
        Return MAX(snapshot_time) for the given underlying, or None if no rows exist.
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(snapshot_time) FROM options_chain_snapshots "
                    "WHERE underlying_id = %s",
                    (underlying_id,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self.pool.putconn(conn)

    def _get_active_underlying_ids(self) -> list:
        """Return underlying_id for all rows in underlyings where active = true."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT underlying_id FROM underlyings WHERE active = true"
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            self.pool.putconn(conn)

    def _get_all_snapshot_times(
        self,
        underlying_ids: list,
        start_date: date,
        end_date: date,
    ) -> list:
        """
        Return all distinct snapshot_time values for the given underlyings
        in [start_date, end_date), ordered ascending.
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT snapshot_time
                    FROM options_chain_snapshots
                    WHERE underlying_id = ANY(%s)
                      AND snapshot_time >= %s
                      AND snapshot_time  < %s
                    ORDER BY snapshot_time
                    """,
                    (underlying_ids, start_date, end_date),
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            self.pool.putconn(conn)

    def load_contracts(
        self,
        underlying_id: int,
        snapshot_time: datetime,
    ) -> list:
        """
        Load all scoreable put contracts for one underlying at one snapshot time.

        Contracts with NULL mid/delta/theta are excluded at the SQL level.
        Returns list[OptionContract].
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _LOAD_CONTRACTS_SQL,
                    (underlying_id, snapshot_time, self.dte_min, self.dte_max),
                )
                rows = cur.fetchall()

            contracts = []
            for row in rows:
                (
                    uid, symbol, snap_time, underlying_price,
                    expiry_date, dte, strike, option_type,
                    bid, ask, mid, open_interest, implied_vol,
                    delta, gamma, theta, vega, ibkr_conid,
                    iv_pct_52w, iv_pct_30d, vix_level,
                ) = row

                if dte is None or dte == 0:
                    log.debug(
                        "Skipping %s strike=%.2f at %s — DTE is %s",
                        symbol, strike, snap_time, dte,
                    )
                    continue

                contracts.append(OptionContract(
                    underlying_id=uid,
                    symbol=symbol,
                    snapshot_time=snap_time,
                    underlying_price=float(underlying_price),
                    expiry_date=expiry_date,
                    dte=int(dte),
                    strike=float(strike),
                    option_type=option_type,
                    bid=float(bid) if bid is not None else 0.0,
                    ask=float(ask) if ask is not None else 0.0,
                    mid=float(mid),
                    open_interest=int(open_interest) if open_interest is not None else 0,
                    implied_vol=float(implied_vol),
                    delta=float(delta),
                    gamma=float(gamma) if gamma is not None else 0.0,
                    theta=float(theta),
                    vega=float(vega) if vega is not None else 0.0,
                    iv_percentile_52w=float(iv_pct_52w) if iv_pct_52w is not None else None,
                    iv_percentile_30d=float(iv_pct_30d) if iv_pct_30d is not None else None,
                    vix_level=float(vix_level) if vix_level is not None else None,
                    ibkr_conid=int(ibkr_conid),
                ))

            return contracts

        finally:
            self.pool.putconn(conn)

    # ------------------------------------------------------------------
    # Filter logic
    # ------------------------------------------------------------------

    def apply_filters(self, scored: ScoredContract) -> ScoredContract:
        """
        Evaluate every threshold filter independently against a scored contract.

        CRITICAL: Never short-circuit. Evaluate ALL filters regardless of earlier
        failures. The ML layer needs to know which specific filters failed for
        every contract, not just whether it passed or failed overall.

        Sets scored.filter_failures (list of failure reason strings) and
        scored.passed_all_filters (True only when filter_failures is empty).
        Returns the mutated ScoredContract.
        """
        failures = []

        if scored.composite_score < self.min_composite_score:
            failures.append(
                f"score {scored.composite_score:.6f} < min {self.min_composite_score}"
            )

        if scored.annualised_roi < self.min_annualised_roi:
            failures.append(
                f"ann_roi {scored.annualised_roi:.1%} < min {self.min_annualised_roi:.1%}"
            )

        if scored.prob_profit < self.min_prob_profit:
            failures.append(
                f"pop {scored.prob_profit:.1%} < min {self.min_prob_profit:.1%}"
            )

        if scored.open_interest < self.min_open_interest:
            failures.append(
                f"oi {scored.open_interest} < min {self.min_open_interest}"
            )

        iv_pct = scored.iv_percentile_52w
        if iv_pct is None:
            iv_pct = scored.iv_percentile_30d
        if iv_pct is None:
            iv_pct = 0.0
        if iv_pct < self.iv_percentile_min:
            failures.append(
                f"iv_pct {iv_pct:.1f} < min {self.iv_percentile_min}"
            )

        scored.filter_failures = failures
        scored.passed_all_filters = len(failures) == 0
        return scored

    # ------------------------------------------------------------------
    # Scoring pipeline (shared by live and backtest)
    # ------------------------------------------------------------------

    def _score_and_filter(self, contracts: list) -> list:
        """Score and filter a list of OptionContract objects."""
        results = []
        for contract in contracts:
            scored = self.scorer.score(contract)
            scored = self.apply_filters(scored)
            log.debug(
                "%s strike=%.2f dte=%d score=%.6f passed=%s failures=%s",
                scored.symbol, scored.strike, scored.dte,
                scored.composite_score, scored.passed_all_filters,
                scored.filter_failures,
            )
            results.append(scored)
        return results

    # ------------------------------------------------------------------
    # Public: run() — live mode
    # ------------------------------------------------------------------

    def run(self, underlying_ids: list = None) -> list:
        """
        Live screening run. Uses the latest snapshot_time per underlying.

        If underlying_ids is None, queries underlyings table for all active=true rows.

        Returns a list of ScoredContract sorted by composite_score descending.
        Both passing and failing contracts are included — caller filters as needed.
        """
        import time as _time
        t_start = _time.time()

        if underlying_ids is None:
            underlying_ids = self._get_active_underlying_ids()

        log.info(
            "Screener starting — %d underlying(s) to process",
            len(underlying_ids),
        )

        all_results = []
        processed = 0

        for uid in underlying_ids:
            try:
                snap_time = self.get_latest_snapshot_time(uid)
                if snap_time is None:
                    log.warning(
                        "underlying_id=%d has no snapshot data — skipping",
                        uid,
                    )
                    continue

                contracts = self.load_contracts(uid, snap_time)
                log.debug(
                    "underlying_id=%d snapshot=%s contracts_loaded=%d",
                    uid, snap_time, len(contracts),
                )

                results = self._score_and_filter(contracts)
                all_results.extend(results)
                processed += 1

            except psycopg2.Error:
                log.error(
                    "Database error processing underlying_id=%d:\n%s",
                    uid, traceback.format_exc(),
                )
            except Exception:
                log.error(
                    "Unexpected error processing underlying_id=%d:\n%s",
                    uid, traceback.format_exc(),
                )

        all_results.sort(key=lambda c: c.composite_score, reverse=True)

        passing = sum(1 for c in all_results if c.passed_all_filters)
        elapsed = _time.time() - t_start

        log.info(
            "Screener complete — underlyings_processed=%d contracts_scored=%d "
            "passing_all_filters=%d elapsed=%.2fs",
            processed, len(all_results), passing, elapsed,
        )

        return all_results

    # ------------------------------------------------------------------
    # Public: run_backtest() — backtest mode
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        start_date: date,
        end_date: date,
        underlying_ids: list = None,
    ) -> list:
        """
        Backtest screening run. Iterates over all distinct snapshot_time values
        between start_date and end_date for the given underlyings.

        Uses identical scoring logic to run(). The only difference is the data source.

        Returns the full list of ScoredContract across all snapshots in the date range.
        Note: for large date ranges this list can be very large (see spec Section 5.6).
        The paper trading simulator must process results by snapshot_time in batches.

        If underlying_ids is None, queries underlyings table for all active=true rows.
        """
        import time as _time
        t_start = _time.time()

        if underlying_ids is None:
            underlying_ids = self._get_active_underlying_ids()

        snap_times = self._get_all_snapshot_times(underlying_ids, start_date, end_date)

        log.info(
            "Backtest starting — %d underlying(s), %d snapshot times, "
            "range=%s to %s",
            len(underlying_ids), len(snap_times), start_date, end_date,
        )

        all_results = []
        processed_snaps = 0

        for snap_time in snap_times:
            for uid in underlying_ids:
                try:
                    contracts = self.load_contracts(uid, snap_time)
                    if not contracts:
                        continue
                    results = self._score_and_filter(contracts)
                    all_results.extend(results)
                except psycopg2.Error:
                    log.error(
                        "DB error at snapshot=%s underlying_id=%d:\n%s",
                        snap_time, uid, traceback.format_exc(),
                    )
                except Exception:
                    log.error(
                        "Error at snapshot=%s underlying_id=%d:\n%s",
                        snap_time, uid, traceback.format_exc(),
                    )
            processed_snaps += 1

        elapsed = _time.time() - t_start
        passing = sum(1 for c in all_results if c.passed_all_filters)

        log.info(
            "Backtest complete — snapshots_processed=%d total_contracts=%d "
            "passing=%d elapsed=%.2fs",
            processed_snaps, len(all_results), passing, elapsed,
        )

        return all_results
