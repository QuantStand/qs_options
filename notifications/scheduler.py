"""
scheduler.py — APScheduler integration for the notification service.

Uses AsyncIOScheduler so all jobs run in the same event loop as
the python-telegram-bot Application, avoiding threading issues.
Jobs are registered here; the scheduler is started/stopped via
Application.post_init / post_shutdown hooks in run_notifier.py.
"""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import Application

from notifications.alerts import check_all_alerts, send_morning_check
from notifications.briefing import send_daily_briefing

log = logging.getLogger(__name__)


def build_scheduler(app: Application) -> AsyncIOScheduler:
    """
    Create and configure the AsyncIOScheduler.
    All job functions receive pool and config via closure over app.bot_data.
    Returns the configured (not yet started) scheduler.
    """
    pool   = app.bot_data["pool"]
    config = app.bot_data["config"]
    bot    = app.bot
    tz     = config.get("telegram", {}).get("timezone", "America/New_York")

    scheduler = AsyncIOScheduler(timezone=tz)

    # ── Rule checks every 5 min during market hours ───────────────────────────
    scheduler.add_job(
        check_all_alerts,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9-15",
        minute="*/5",
        kwargs={"pool": pool, "config": config, "bot": bot},
        id="check_all_alerts",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # ── Morning position check — 09:35 ET ─────────────────────────────────────
    scheduler.add_job(
        send_morning_check,
        trigger="cron",
        day_of_week="mon-fri",
        hour=9,
        minute=35,
        kwargs={"pool": pool, "config": config, "bot": bot},
        id="morning_check",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # ── Daily briefing — 16:30 ET ─────────────────────────────────────────────
    scheduler.add_job(
        send_daily_briefing,
        trigger="cron",
        day_of_week="mon-fri",
        hour=16,
        minute=30,
        kwargs={"pool": pool, "config": config, "bot": bot},
        id="daily_briefing",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    log.info("Scheduler configured — 3 jobs registered")
    return scheduler
