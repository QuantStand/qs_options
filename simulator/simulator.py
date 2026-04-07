"""
simulator.py — Orchestration layer for the QuantStand paper trading simulator.

PaperTradingSimulator coordinates the screener, rules engine, and position
manager. Reads from options_chain_snapshots; writes only to trade_log.
"""

import logging
import traceback
import yaml
from datetime import datetime, date, timezone
from typing import Optional

import psycopg2.pool

from engine.screener import OptionScreener
from engine.scorer import OptionScorer
from .models import PaperPosition
from .position_manager import PositionManager
from .rules import Rules

log = logging.getLogger(__name__)


def _load_config(config_path: str) -> dict:
    with open(config_path, "r") as fh:
        return yaml.safe_load(fh)


def _create_connection_pool(db_cfg: dict) -> psycopg2.pool.ThreadedConnectionPool:
    try:
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=db_cfg["host"],
            port=db_cfg["port"],
            dbname=db_cfg["name"],
            user=db_cfg["user"],
            password=db_cfg["password"],
        )
    except psycopg2.Error as exc:
        raise RuntimeError(
            f"Failed to connect to DB at "
            f"{db_cfg['host']}:{db_cfg['port']}/{db_cfg['name']}: {exc}"
        ) from exc


class PaperTradingSimulator:
    """
    Top-level coordinator for the paper trading simulator.

    Calls the scoring engine every cycle, evaluates entry/exit rules,
    and writes trade events to trade_log.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = _load_config(config_path)
        self.pool = _create_connection_pool(self.config["database"])
        self.screener = OptionScreener(self.pool, self.config)
        self.scorer = OptionScorer()
        self.position_manager = PositionManager(self.pool)
        self.rules = Rules(self.config)
        self.open_positions: list = []
        self._load_open_positions()

    def _load_open_positions(self) -> None:
        """Restore open paper positions from DB on startup."""
        self.open_positions = self.position_manager.load_open_positions()

    def _current_threshold_params(self) -> dict:
        """
        Return the current scoring thresholds from config.
        Stored with each trade as ML training inputs.
        """
        sc = self.config["scoring"]
        return {
            "threshold_min_score": sc["min_composite_score"],
            "threshold_min_roi":   sc["min_annualised_roi"],
            "threshold_min_pop":   sc["min_probability_of_profit"],
            "threshold_min_oi":    sc["min_open_interest"],
        }

    # ------------------------------------------------------------------
    # run_cycle — live paper mode
    # ------------------------------------------------------------------

    def run_cycle(self) -> dict:
        """
        Core method. Called every 15 minutes during market hours.

        Step 1 — Get latest scored contracts from screener
        Step 2 — Monitor and close open positions
        Step 3 — Evaluate new entries (max 1 per cycle)
        Step 4 — Return cycle summary

        Returns a summary dict for logging and testing.
        """
        cycle_start = datetime.now(timezone.utc)
        log.info("=== Cycle start %s ===", cycle_start.isoformat())

        closes_this_cycle = 0
        new_entries = 0

        # ----------------------------------------------------------
        # Step 1 — Score
        # ----------------------------------------------------------
        try:
            results = self.screener.run()
        except Exception:
            log.error("Screener failed:\n%s", traceback.format_exc())
            results = []

        # ----------------------------------------------------------
        # Step 2 — Monitor and close existing positions
        # ----------------------------------------------------------
        positions_to_remove = []

        for position in list(self.open_positions):
            try:
                latest = self.position_manager.find_matching_contract(
                    position, results
                )
                position = self.position_manager.update_position(position, latest)

                should_close, reason = self.rules.should_close_position(
                    position, latest, self.config
                )

                if should_close:
                    exit_price = float(latest.mid) if latest is not None else 0.0
                    position = self.position_manager.close_position(
                        position, exit_price, reason, datetime.now(timezone.utc)
                    )
                    positions_to_remove.append(position)
                    closes_this_cycle += 1
                    log.info(
                        "Closed %s %.2fP exp=%s — %s — P&L $%.2f (%.1f%% of premium)",
                        position.symbol, position.strike, position.expiry_date,
                        reason, position.realised_pnl or 0.0,
                        (position.pnl_pct_of_premium or 0.0) * 100,
                    )
                else:
                    # Update position in the live list (it was mutated in place)
                    idx = self.open_positions.index(
                        next(p for p in self.open_positions
                             if p.trade_id == position.trade_id)
                    )
                    self.open_positions[idx] = position

            except Exception:
                log.error(
                    "Error processing position trade_id=%d:\n%s",
                    position.trade_id, traceback.format_exc(),
                )

        for pos in positions_to_remove:
            self.open_positions = [
                p for p in self.open_positions if p.trade_id != pos.trade_id
            ]

        # ----------------------------------------------------------
        # Step 3 — Evaluate new entries
        # ----------------------------------------------------------
        passing = [c for c in results if c.passed_all_filters]
        passing.sort(key=lambda c: c.composite_score, reverse=True)

        for candidate in passing:
            can_open, reason = self.rules.can_open_new_position(
                candidate, self.open_positions, self.config
            )
            log.debug(
                "Entry candidate %s %.2fP dte=%d score=%.4f — can_open=%s reason=%s",
                candidate.symbol, candidate.strike, candidate.dte,
                candidate.composite_score, can_open, reason or "ok",
            )
            if can_open:
                try:
                    position = self.position_manager.open_position(
                        candidate,
                        self.config["position_management"]["contracts_per_trade"],
                        self._current_threshold_params(),
                    )
                    self.open_positions.append(position)
                    new_entries += 1
                    log.info(
                        "Opened %s %.2fP exp=%s — score=%.4f roi=%.1f%% dte=%d",
                        candidate.symbol, candidate.strike, candidate.expiry_date,
                        candidate.composite_score, candidate.annualised_roi * 100,
                        candidate.dte,
                    )
                    # One entry per cycle maximum
                    break
                except Exception:
                    log.error(
                        "Failed to open position for %s:\n%s",
                        candidate.symbol, traceback.format_exc(),
                    )

        # ----------------------------------------------------------
        # Step 4 — Log cycle summary
        # ----------------------------------------------------------
        elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        summary = {
            "cycle_time": cycle_start.isoformat(),
            "open_positions_count": len(self.open_positions),
            "contracts_scored": len(results),
            "passing_count": len(passing),
            "new_entries": new_entries,
            "closes_this_cycle": closes_this_cycle,
            "elapsed_s": elapsed,
        }
        log.info(
            "=== Cycle end — open=%d scored=%d passing=%d entries=%d closes=%d (%.2fs) ===",
            summary["open_positions_count"],
            summary["contracts_scored"],
            summary["passing_count"],
            summary["new_entries"],
            summary["closes_this_cycle"],
            summary["elapsed_s"],
        )
        return summary

    # ------------------------------------------------------------------
    # run_backtest — backtest mode
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        start_date: date,
        end_date: date,
        underlying_ids: Optional[list] = None,
        write_to_db: bool = False,
    ) -> dict:
        """
        Replay historical snapshots across a date range.

        Applies identical entry/exit logic to run_cycle() but at historical
        prices. Returns a summary dict.

        write_to_db=False (default): positions are simulated in memory only,
            nothing written to trade_log. Use for validation and parameter testing.
        write_to_db=True: writes all trades to trade_log with trade_mode='PAPER'.
            Used by the ML layer to generate training data from historical snapshots.
        """
        log.info(
            "Backtest starting — %s to %s  write_to_db=%s",
            start_date, end_date, write_to_db,
        )

        if underlying_ids is None:
            underlying_ids = self.screener._get_active_underlying_ids()

        snap_times = self.screener._get_all_snapshot_times(
            underlying_ids, start_date, end_date
        )

        log.info(
            "Found %d snapshot times across %d underlying(s)",
            len(snap_times), len(underlying_ids),
        )

        # Backtest uses a local position list — never touches self.open_positions
        bt_positions: list = []
        all_closed: list = []
        trade_id_counter = [1]  # for in-memory only mode

        for snap_time in snap_times:
            # Collect scored contracts for all underlyings at this snapshot time
            results = []
            for uid in underlying_ids:
                try:
                    contracts = self.screener.load_contracts(uid, snap_time)
                    scored = self.screener._score_and_filter(contracts)
                    results.extend(scored)
                except Exception:
                    log.error(
                        "Error loading contracts uid=%d snap=%s:\n%s",
                        uid, snap_time, traceback.format_exc(),
                    )

            # Step 2 — Monitor and close open positions
            positions_to_remove = []

            for position in list(bt_positions):
                latest = self.position_manager.find_matching_contract(
                    position, results
                )
                position = self.position_manager.update_position(position, latest)

                should_close, reason = self.rules.should_close_position(
                    position, latest, self.config
                )

                if should_close:
                    exit_price = float(latest.mid) if latest is not None else 0.0
                    exit_time = snap_time

                    realised_pnl = (
                        (position.entry_price - exit_price)
                        * 100.0
                        * position.contracts
                    )
                    pnl_pct = (
                        realised_pnl / position.premium_collected
                        if position.premium_collected != 0
                        else 0.0
                    )
                    was_assigned = reason == "ASSIGNED"

                    if write_to_db:
                        try:
                            position = self.position_manager.close_position(
                                position, exit_price, reason, exit_time
                            )
                        except Exception:
                            log.error(
                                "DB write failed closing trade_id=%d:\n%s",
                                position.trade_id, traceback.format_exc(),
                            )
                    else:
                        # In-memory close
                        position.exit_time = exit_time
                        position.exit_price = exit_price
                        position.exit_reason = reason
                        position.realised_pnl = realised_pnl
                        position.pnl_pct_of_premium = pnl_pct
                        position.was_assigned = was_assigned
                        position.assigned_cost_basis = (
                            position.strike - position.entry_price
                            if was_assigned else None
                        )

                    positions_to_remove.append(position)
                    all_closed.append(position)
                    log.info(
                        "[BT] Closed %s %.2fP — %s — P&L $%.2f",
                        position.symbol, position.strike,
                        reason, position.realised_pnl or 0.0,
                    )
                else:
                    idx = next(
                        (i for i, p in enumerate(bt_positions)
                         if p.trade_id == position.trade_id),
                        None,
                    )
                    if idx is not None:
                        bt_positions[idx] = position

            for pos in positions_to_remove:
                bt_positions = [
                    p for p in bt_positions if p.trade_id != pos.trade_id
                ]

            # Step 3 — Evaluate new entries
            passing = [c for c in results if c.passed_all_filters]
            passing.sort(key=lambda c: c.composite_score, reverse=True)

            for candidate in passing:
                can_open, reason = self.rules.can_open_new_position(
                    candidate, bt_positions, self.config
                )
                if can_open:
                    try:
                        if write_to_db:
                            position = self.position_manager.open_position(
                                candidate,
                                self.config["position_management"]["contracts_per_trade"],
                                self._current_threshold_params(),
                            )
                        else:
                            # In-memory open — no DB write
                            contracts = self.config["position_management"]["contracts_per_trade"]
                            tp = self._current_threshold_params()
                            iv_pct = (
                                candidate.iv_percentile_52w
                                if candidate.iv_percentile_52w is not None
                                else (candidate.iv_percentile_30d or 0.0)
                            )
                            position = PaperPosition(
                                trade_id=trade_id_counter[0],
                                underlying_id=candidate.underlying_id,
                                symbol=candidate.symbol,
                                expiry_date=candidate.expiry_date,
                                strike=candidate.strike,
                                option_type=candidate.option_type,
                                contracts=contracts,
                                entry_time=snap_time,
                                entry_price=candidate.mid,
                                entry_underlying_price=candidate.underlying_price,
                                entry_delta=candidate.delta,
                                entry_theta=candidate.theta,
                                entry_iv=candidate.implied_vol,
                                entry_iv_percentile=iv_pct,
                                entry_dte=candidate.dte,
                                entry_composite_score=candidate.composite_score,
                                entry_ann_roi=candidate.annualised_roi,
                                premium_collected=candidate.mid * 100.0 * contracts,
                                collateral_assigned=candidate.strike * 100.0 * contracts,
                                threshold_min_score=tp["threshold_min_score"],
                                threshold_min_roi=tp["threshold_min_roi"],
                                threshold_min_pop=tp["threshold_min_pop"],
                                threshold_min_oi=tp["threshold_min_oi"],
                                current_price=candidate.mid,
                                current_underlying_price=candidate.underlying_price,
                                current_delta=candidate.delta,
                                current_dte=candidate.dte,
                                unrealised_pnl=0.0,
                                exit_time=None,
                                exit_price=None,
                                exit_reason=None,
                                realised_pnl=None,
                                pnl_pct_of_premium=None,
                                was_assigned=False,
                                assigned_cost_basis=None,
                            )
                            trade_id_counter[0] += 1

                        bt_positions.append(position)
                        log.info(
                            "[BT] Opened %s %.2fP exp=%s — score=%.4f dte=%d",
                            candidate.symbol, candidate.strike,
                            candidate.expiry_date, candidate.composite_score,
                            candidate.dte,
                        )
                        # One entry per cycle
                        break
                    except Exception:
                        log.error(
                            "[BT] Failed to open %s:\n%s",
                            candidate.symbol, traceback.format_exc(),
                        )

        # Any positions still open at end of backtest are counted but not force-closed
        all_trades = all_closed + bt_positions

        total_pnl = sum(
            p.realised_pnl for p in all_closed if p.realised_pnl is not None
        )
        winning = sum(
            1 for p in all_closed
            if p.realised_pnl is not None and p.realised_pnl > 0
        )
        avg_pnl_pct = (
            sum(p.pnl_pct_of_premium for p in all_closed
                if p.pnl_pct_of_premium is not None)
            / len(all_closed)
            if all_closed else 0.0
        )

        summary = {
            "total_trades":            len(all_trades),
            "winning_trades":          winning,
            "total_pnl":               total_pnl,
            "avg_pnl_pct_of_premium":  avg_pnl_pct,
            "assigned_count":          sum(1 for p in all_closed if p.was_assigned),
            "early_close_count":       sum(1 for p in all_closed if p.exit_reason == "EARLY_CLOSE"),
            "expired_worthless_count": sum(1 for p in all_closed if p.exit_reason == "EXPIRED"),
            "still_open_at_end":       len(bt_positions),
        }

        log.info(
            "Backtest complete — trades=%d wins=%d pnl=$%.2f avg_pnl_pct=%.1f%% "
            "assigned=%d early_close=%d expired=%d still_open=%d",
            summary["total_trades"], summary["winning_trades"],
            summary["total_pnl"], summary["avg_pnl_pct_of_premium"] * 100,
            summary["assigned_count"], summary["early_close_count"],
            summary["expired_worthless_count"], summary["still_open_at_end"],
        )
        return summary

    def close(self):
        """Release DB connection pool."""
        if self.pool:
            self.pool.closeall()
