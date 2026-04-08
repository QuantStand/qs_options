"""
QuantStand Notification Service — Entry Point

Starts the Telegram bot and APScheduler in the same async event loop.
The scheduler is created inside post_init so it has access to app.bot
before starting — this is the correct python-telegram-bot v20 pattern.

Usage:
    python3 run_notifier.py

PM2:
    Managed as 'qs-notifier' in ecosystem.config.js.

Prerequisites (one-time setup):
    1. Create bot via @BotFather (see notifications spec §3)
    2. Add bot_token + chat_id to config/config.yaml under 'telegram:'
    3. pip install python-telegram-bot==20.7
"""

import logging
import sys
import os

# ── sys.path: ensure repo root is importable ─────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml
import psycopg2.pool
from telegram.ext import Application, ApplicationBuilder

from notifications.bot import register_handlers
from notifications.scheduler import build_scheduler

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [qs-notifier] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "config", "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def create_pool(config: dict) -> psycopg2.pool.ThreadedConnectionPool:
    db = config["database"]
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    config = load_config()

    tg_cfg = config.get("telegram", {})
    if not tg_cfg.get("bot_token") or tg_cfg["bot_token"] == "YOUR_BOT_TOKEN":
        log.error(
            "Telegram bot_token is not configured.\n"
            "Complete BotFather setup (spec §3) and add token to config/config.yaml."
        )
        sys.exit(1)

    log.info("=" * 60)
    log.info("QuantStand Notification Service starting")
    log.info("  Chat ID : %s", tg_cfg.get("chat_id", "NOT SET"))
    log.info("=" * 60)

    pool = create_pool(config)

    # ── Lifecycle hooks ───────────────────────────────────────────────────────
    # post_init fires after the bot is initialised but before polling starts.
    # app.bot is available here — safe to pass to the scheduler.

    async def post_init(app: Application) -> None:
        # Store shared state in bot_data so all command handlers can reach it
        app.bot_data["pool"]   = pool
        app.bot_data["config"] = config
        # Build scheduler now that app.bot is available
        scheduler = build_scheduler(app)
        scheduler.start()
        app.bot_data["scheduler"] = scheduler
        log.info("Scheduler started — jobs: %s",
                 [j.id for j in scheduler.get_jobs()])

    async def post_shutdown(app: Application) -> None:
        scheduler = app.bot_data.get("scheduler")
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            log.info("Scheduler stopped")
        pool.closeall()
        log.info("DB pool closed")

    # ── Build Application ─────────────────────────────────────────────────────
    app = (
        ApplicationBuilder()
        .token(tg_cfg["bot_token"])
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    register_handlers(app)

    log.info("Starting bot polling...")
    app.run_polling(
        allowed_updates=["message"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
