"""
simulator/__init__.py — Public interface for the QuantStand paper trading simulator.

External code calls exactly two functions:
    run_paper_session() — live paper trading on a schedule
    run_backtest()      — historical replay over a date range
"""

import logging
import sys
from datetime import date
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .simulator import PaperTradingSimulator

log = logging.getLogger(__name__)


def run_paper_session(config_path: str = "config/config.yaml") -> None:
    """
    Start the live paper trading session.

    Runs run_cycle() every 15 minutes during US market hours (09:30–16:00 ET,
    Mon–Fri). Blocking call — runs until interrupted.

    Scheduler parameters are read from config.yaml simulator: section.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [paper_trader] %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    sim = PaperTradingSimulator(config_path)
    sim_cfg = sim.config.get("simulator", {})

    interval = sim_cfg.get("cycle_interval_minutes", 15)
    tz = "America/New_York"

    # Fire every `interval` minutes during 09:30–16:00 ET Mon–Fri.
    # An additional market hours guard inside run_cycle handles the 09:00–09:30 gap.
    cron_minute = f"*/{interval}"
    trigger = CronTrigger(
        minute=cron_minute,
        hour="9-15",
        day_of_week="mon-fri",
        timezone=tz,
        misfire_grace_time=3600,
        coalesce=True,
    )

    sched = BlockingScheduler(timezone=tz)

    def _cycle_wrapper():
        try:
            sim.run_cycle()
        except Exception:
            log.error("Unhandled exception in run_cycle:\n%s",
                      __import__("traceback").format_exc())

    sched.add_job(
        _cycle_wrapper,
        trigger,
        id="paper_cycle",
        max_instances=1,
    )

    log.info(
        "Paper trader starting — cycle every %d min, 09:30–16:00 ET Mon–Fri",
        interval,
    )
    log.info("Open positions restored: %d", len(sim.open_positions))

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown requested — stopping paper trader.")
        sched.shutdown(wait=False)
        sim.close()
        logging.shutdown()


def run_backtest(
    start_date: date,
    end_date: date,
    config_path: str = "config/config.yaml",
    underlying_ids: Optional[list] = None,
    write_to_db: bool = False,
) -> dict:
    """
    Run the simulator over historical chain snapshots.

    Returns a summary dict:
        total_trades, winning_trades, total_pnl, avg_pnl_pct_of_premium,
        assigned_count, early_close_count, expired_worthless_count,
        still_open_at_end

    write_to_db=True causes all simulated trades to be written to trade_log
    with trade_mode='PAPER'. Used by the ML layer to generate training data.
    """
    sim = PaperTradingSimulator(config_path)
    try:
        return sim.run_backtest(
            start_date, end_date,
            underlying_ids=underlying_ids,
            write_to_db=write_to_db,
        )
    finally:
        sim.close()
