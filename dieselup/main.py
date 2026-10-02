"""
DieselUp NJ — process entrypoint.

Wires together:
  1. Configuration. Importing dieselup.config validates every required env var;
     a bad config sys.exits(1) before we ever reach this module's code.
  2. asyncpg pool. First call to get_pool() also applies schema.sql.
  3. python-telegram-bot Application with handlers from dieselup.bot.handlers.
  4. APScheduler AsyncIOScheduler in America/New_York — empty for now; will hold
     load-sync, compliance-resolution, and weekly-report jobs in later phases.

Run:  python -m dieselup.main
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
import sys

from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import BotCommand, Update
from telegram.ext import ApplicationBuilder

from dieselup.bot.admin import price_upload_reminder, register_admin_handlers
from dieselup.bot.group_link import register_group_link_handlers
from dieselup.bot.handlers import register_handlers
from dieselup.bot.messaging_policy import MessagingPolicyRequest
from dieselup.config import settings
from dieselup.core.driver_assignments import refresh_driver_assignments
from dieselup.core.compliance import resolve_pending_events
from dieselup.core.dlq_retry import retry_failed_alerts
from dieselup.core.fuel_brain import capture_fleet_state
from dieselup.core.load_sync import sync_active_loads
from dieselup.db import close_pool, get_pool
from dieselup.health_server import start_health_server
from dieselup.reports.weekly import run_weekly_report

log = logging.getLogger("dieselup")

# One bot token can have only one active getUpdates poller. The same is true
# operationally for this app's scheduled jobs: duplicate containers would send
# duplicate/competing alerts. A Postgres advisory lock gives all app instances
# a shared, automatically released leader latch.
APP_SINGLETON_LOCK_ID = 9101912026


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)


async def _run() -> None:
    _configure_logging()
    log.info(
        "%s starting — TMS %s, home state %s, Pilot account %s",
        settings.COMPANY_NAME,
        settings.TMS_PROVIDER,
        settings.IFTA_HOME_STATE,
        settings.PILOT_ACCOUNT_NUMBER,
    )

    await get_pool()
    log.info("Postgres pool ready.")

    start_health_server()

    if settings.BOT_MODE == "bootstrap":
        log.warning(
            "%s mode — database and health server are ready; "
            "Telegram polling and all scheduled jobs are disabled.",
            settings.BOT_MODE.upper(),
        )
        try:
            await asyncio.Event().wait()
        finally:
            await close_pool()
        return

    singleton_conn = await _acquire_singleton_lock()

    application = (ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN)
                   .request(MessagingPolicyRequest()).build())
    log.info("Telegram messaging mode: %s", settings.TELEGRAM_MESSAGING_MODE)
    log.info("Operating scope: trucks=%s auto_link=%s", settings.TEST_TRUCK_UNITS or 'full fleet', settings.AUTO_LINK_ENABLED)
    register_handlers(application)
    register_admin_handlers(application)
    register_group_link_handlers(application)
    log.info("Telegram handlers registered.")

    scheduler = AsyncIOScheduler(timezone="America/New_York")
    scheduler.add_job(
        price_upload_reminder,
        CronTrigger(hour=8, minute=0, timezone="America/New_York"),
        args=[application.bot],
        id="price_upload_reminder",
        replace_existing=True,
    )
    scheduler.add_job(
        sync_active_loads,
        IntervalTrigger(minutes=15),
        args=[application.bot],
        id="load_sync",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        resolve_pending_events,
        IntervalTrigger(minutes=5),
        args=[application.bot],
        id="compliance_resolver",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        run_weekly_report,
        CronTrigger(day_of_week="sat", hour=6, minute=0, timezone="America/New_York"),
        args=[application.bot],
        id="weekly_report",
        replace_existing=True,
    )
    scheduler.add_job(
        retry_failed_alerts,
        IntervalTrigger(minutes=10),
        args=[application.bot],
        id="dlq_retry",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        capture_fleet_state,
        IntervalTrigger(minutes=5),
        args=[application.bot],
        id="fuel_brain",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    # Assignment metadata stays current even when fuel-alert jobs are paused.
    scheduler.add_job(
        refresh_driver_assignments,
        IntervalTrigger(minutes=5),
        args=[application.bot],
        id="driver_assignments",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=15),
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.start()
    log.info(
        "APScheduler started — load_sync 15m, compliance 5m, fuel_brain 5m, "
        "dlq_retry 10m, price reminder 08:00 EST, weekly report Sat 06:00 EST."
    )

    # Expose scheduler to admin command handlers via bot_data so /pause and
    # /resume can pause/resume jobs without needing a global variable.
    application.bot_data["scheduler"] = scheduler
    application.bot_data["bot_paused"] = False
    application.bot_data["pause_reason"] = None
    application.bot_data["paused_at"] = None

    try:
        await application.initialize()
        await application.bot.set_my_commands([
            BotCommand("start",    "Check bot is running"),
            BotCommand("status",   "Current truck + load + next fuel stop"),
            BotCommand("briefing", "Re-send latest fueling briefing"),
            BotCommand("myweek",   "Your savings/losses this week"),
        ])
        log.info("Bot commands registered with Telegram.")
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        log.info("Bot polling — %s is live. Ctrl-C to stop.", settings.COMPANY_NAME)

        log.info("Routing engine: private Valhalla only (fallbacks disabled).")

        stop = asyncio.Event()
        await stop.wait()
    finally:
        log.info("Shutting down...")
        try:
            if application.updater and application.updater.running:
                await application.updater.stop()
            if application.running:
                await application.stop()
            await application.shutdown()
        except Exception as exc:  # noqa: BLE001 — log everything during shutdown
            log.warning("Telegram shutdown raised: %s", exc)
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await _release_singleton_lock(singleton_conn)
        await close_pool()
        log.info("Shutdown complete.")


async def _acquire_singleton_lock() -> Any:
    """Wait until this container is the only scheduler/polling leader."""
    pool = await get_pool()
    while True:
        conn = await pool.acquire()
        try:
            acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", APP_SINGLETON_LOCK_ID)
        except Exception:
            await pool.release(conn)
            raise
        if acquired:
            log.info("Acquired app singleton lock %s.", APP_SINGLETON_LOCK_ID)
            return conn

        await pool.release(conn)
        log.warning(
            "Another DieselUp instance already holds app singleton lock %s; "
            "this container will not poll Telegram or run scheduled jobs yet.",
            APP_SINGLETON_LOCK_ID,
        )
        await asyncio.sleep(15)


async def _release_singleton_lock(conn: Any) -> None:
    try:
        await conn.execute("SELECT pg_advisory_unlock($1)", APP_SINGLETON_LOCK_ID)
    except Exception as exc:  # noqa: BLE001 — shutdown should continue
        log.warning("Failed to release app singleton lock: %s", exc)
    finally:
        pool = await get_pool()
        await pool.release(conn)


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
