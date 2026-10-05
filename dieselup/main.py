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
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import sys

from typing import Any
from urllib.parse import urlparse

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import BotCommand, Update
from telegram.ext import ApplicationBuilder

from dieselup.bot.admin import price_upload_reminder, register_admin_handlers
from dieselup.bot.group_link import register_group_link_handlers
from dieselup.bot.handlers import register_handlers
from dieselup.bot.messaging_policy import MessagingPolicyRequest, _SILENT_ALLOWED
from dieselup import metrics
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
LEADER_CHECK_INTERVAL_SECONDS = 2.0
LEADER_CHECK_TIMEOUT_SECONDS = 3.0
ACTIVE_JOB_CANCEL_TIMEOUT_SECONDS = 5.0
TELEGRAM_SHUTDOWN_TIMEOUT_SECONDS = 10.0

_SINGLETON_OWNERSHIP_SQL = """
SELECT EXISTS (
    SELECT 1 FROM pg_locks
    WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
      AND classid::bigint = (($1::bigint >> 32) & 4294967295)
      AND objid::bigint = ($1::bigint & 4294967295) AND objsubid = 1
)
"""


class SingletonLeadershipLost(RuntimeError):
    """This process must exit before the supervisor can start a new leader."""


class _SingletonLeadership:
    def __init__(self, conn: Any) -> None:
        self.conn = conn
        self.reason: str | None = None
        self._lost = asyncio.Event()
        self._verified = False
        self._watcher: asyncio.Task | None = None
        self._listener = self._terminated
        self._registered = False

    @property
    def active(self) -> bool:
        return self._verified and not self._lost.is_set()

    def _lose(self, reason: str) -> None:
        if not self._lost.is_set():
            self.reason = reason
            self._lost.set()
        metrics.gauge("singleton_leader_active", 0)

    def _terminated(self, conn: Any) -> None:
        self._lose("connection_terminated")

    async def start(self) -> None:
        metrics.gauge("singleton_leader_active", 0)
        self.conn.add_termination_listener(self._listener)
        self._registered = True
        await self._check()
        if self._lost.is_set():
            raise SingletonLeadershipLost(self.reason or "leadership_unverified")
        self._verified = True
        metrics.gauge("singleton_leader_active", 1)
        self._watcher = asyncio.create_task(self._watch(), name="singleton-leadership-monitor")

    async def _check(self) -> None:
        try:
            if self.conn.is_closed():
                self._lose("connection_closed")
                return
            async with asyncio.timeout(LEADER_CHECK_TIMEOUT_SECONDS):
                owned = await self.conn.fetchval(
                    _SINGLETON_OWNERSHIP_SQL, APP_SINGLETON_LOCK_ID,
                    timeout=LEADER_CHECK_TIMEOUT_SECONDS,
                )
            if owned is not True:
                self._lose("lock_ownership_lost")
        except TimeoutError:
            self._lose("ownership_check_timeout")
        except Exception as exc:
            self._lose("ownership_check_failed_" + type(exc).__name__)

    async def _watch(self) -> None:
        try:
            while not self._lost.is_set():
                await asyncio.sleep(LEADER_CHECK_INTERVAL_SECONDS)
                await self._check()
        except asyncio.CancelledError:
            if self.active:
                self._lose("leader_monitor_cancelled")
        except Exception as exc:
            self._lose("leader_monitor_failed_" + type(exc).__name__)

    async def wait_lost(self) -> None:
        await self._lost.wait()

    def begin_shutdown(self) -> None:
        self._lose("normal_shutdown")

    async def close(self) -> None:
        self.begin_shutdown()
        if self._registered:
            with suppress(Exception):
                self.conn.remove_termination_listener(self._listener)
            self._registered = False
        if self._watcher is not None:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)
            self._watcher = None


class _LeadershipPolicyRequest(MessagingPolicyRequest):
    """Keep existing recipient policy, and stop output when leadership is lost."""
    def __init__(self, leader: _SingletonLeadership) -> None:
        super().__init__()
        self._leader = leader

    async def do_request(self, url: str, method: str, **kwargs: Any) -> tuple[int, bytes]:
        endpoint = url.rsplit("/", 1)[-1].split("?", 1)[0]
        file_download = method.upper() == "GET" and urlparse(url).path.startswith("/file/bot")
        if endpoint in _SILENT_ALLOWED or file_download:
            return await super().do_request(url=url, method=method, **kwargs)
        if not self._leader.active:
            return 403, json.dumps({"ok": False, "error_code": 403,
                "description": "Telegram output is disabled without verified bot leadership"}).encode()
        request = asyncio.create_task(super().do_request(url=url, method=method, **kwargs))
        lost = asyncio.create_task(self._leader.wait_lost())
        try:
            done, _ = await asyncio.wait({request, lost}, return_when=asyncio.FIRST_COMPLETED)
            if request in done:
                # Preserve an accepted response/message ID even if a loss
                # notification arrived in the same turn of the event loop.
                return await request
            # A cancelled in-flight send has an uncertain outcome. Propagate
            # cancellation, never manufacture a known failed send for replay.
            raise asyncio.CancelledError("singleton leadership lost during output")
        finally:
            for task in (request, lost):
                if not task.done(): task.cancel()
            await asyncio.gather(request, lost, return_exceptions=True)


def _force_fail_closed_exit() -> None:
    log.critical("Bot work could not stop safely; terminating this process before leadership can restart.")
    os._exit(1)


class _ActiveJobs:
    def __init__(self, leader: _SingletonLeadership) -> None:
        self.leader = leader
        self.accepting = True
        self.tasks: set[asyncio.Task] = set()

    async def run(self, fn: Any, *args: Any) -> Any:
        if not self.accepting or not self.leader.active:
            return None
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            return await fn(*args)
        finally:
            self.tasks.discard(task)

    async def stop(self, scheduler: Any) -> None:
        self.accepting = False
        if scheduler.running:
            scheduler.pause()
            scheduler.shutdown(wait=False)
        # AsyncIOScheduler queues executor shutdown on the event loop. Let it
        # cancel queued futures before checking every tracked active job.
        await asyncio.sleep(0)
        pending = {task for task in self.tasks if not task.done()}
        for task in pending:
            if not task.cancelling(): task.cancel()
        if pending:
            _, pending = await asyncio.wait(pending, timeout=ACTIVE_JOB_CANCEL_TIMEOUT_SECONDS)
            if pending:
                _force_fail_closed_exit()


async def _telegram_shutdown(application: Any) -> None:
    operations = []
    if application.updater and application.updater.running:
        operations.append(application.updater.stop)
    if application.running:
        operations.append(application.stop)
    operations.append(application.shutdown)
    for operation in operations:
        try:
            async with asyncio.timeout(TELEGRAM_SHUTDOWN_TIMEOUT_SECONDS):
                await operation()
        except Exception as exc:
            log.warning("Telegram shutdown step failed: %s", type(exc).__name__)
            # PTB marks running=False before all update/handler tasks finish.
            # A timed-out/failed stop therefore cannot prove those tasks have
            # stopped, even when its public running flag is already false.
            _force_fail_closed_exit()
    if application.running or (application.updater and application.updater.running):
        _force_fail_closed_exit()


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

    singleton_conn = None
    leader = None
    service = lost = None
    metrics.gauge("singleton_leader_active", 0)
    try:
        singleton_conn = await _acquire_singleton_lock()
        leader = _SingletonLeadership(singleton_conn)
        await leader.start()
        service = asyncio.create_task(_serve_application(leader), name="singleton-bot-service")
        lost = asyncio.create_task(leader.wait_lost())
        done, _ = await asyncio.wait({service, lost}, return_when=asyncio.FIRST_COMPLETED)
        if lost in done:
            log.critical("Bot singleton leadership lost: %s; stopping all active work.", leader.reason)
            service.cancel()
            await asyncio.gather(service, return_exceptions=True)
            raise SingletonLeadershipLost(leader.reason or "leadership_unverified")
        await service
    finally:
        if leader is not None:
            leader.begin_shutdown()
        if lost is not None:
            lost.cancel()
            await asyncio.gather(lost, return_exceptions=True)
        if service is not None and not service.done():
            service.cancel()
            await asyncio.gather(service, return_exceptions=True)
        if leader is not None:
            await leader.close()
        if singleton_conn is not None:
            await _release_singleton_lock(singleton_conn)
        try:
            async with asyncio.timeout(TELEGRAM_SHUTDOWN_TIMEOUT_SECONDS):
                await close_pool()
        except Exception as exc:
            log.warning("Database shutdown failed: %s", type(exc).__name__)
            _force_fail_closed_exit()
        log.info("Shutdown complete.")


async def _serve_application(leader: _SingletonLeadership) -> None:
    application = (ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN)
                   .request(_LeadershipPolicyRequest(leader)).build())
    log.info("Telegram messaging mode: %s", settings.TELEGRAM_MESSAGING_MODE)
    log.info("Operating scope: trucks=%s auto_link=%s", settings.TEST_TRUCK_UNITS or 'full fleet', settings.AUTO_LINK_ENABLED)
    register_handlers(application)
    register_admin_handlers(application)
    register_group_link_handlers(application)
    log.info("Telegram handlers registered.")

    scheduler = AsyncIOScheduler(timezone="America/New_York")
    active_jobs = _ActiveJobs(leader)
    scheduler.add_job(
        active_jobs.run,
        CronTrigger(hour=8, minute=0, timezone="America/New_York"),
        args=[price_upload_reminder, application.bot],
        id="price_upload_reminder",
        replace_existing=True,
    )
    scheduler.add_job(
        active_jobs.run,
        IntervalTrigger(minutes=15),
        args=[sync_active_loads, application.bot],
        id="load_sync",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        active_jobs.run,
        IntervalTrigger(minutes=5),
        args=[resolve_pending_events, application.bot],
        id="compliance_resolver",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        active_jobs.run,
        CronTrigger(day_of_week="sat", hour=6, minute=0, timezone="America/New_York"),
        args=[run_weekly_report, application.bot],
        id="weekly_report",
        replace_existing=True,
    )
    scheduler.add_job(
        active_jobs.run,
        IntervalTrigger(minutes=10),
        args=[retry_failed_alerts, application.bot],
        id="dlq_retry",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
        active_jobs.run,
        IntervalTrigger(minutes=5),
        args=[capture_fleet_state, application.bot],
        id="fuel_brain",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    # Assignment metadata stays current even when fuel-alert jobs are paused.
    scheduler.add_job(
        active_jobs.run,
        IntervalTrigger(minutes=5),
        args=[refresh_driver_assignments, application.bot],
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
        await active_jobs.stop(scheduler)
        await _telegram_shutdown(application)


async def _acquire_singleton_lock() -> Any:
    """Wait until this container is the only scheduler/polling leader."""
    pool = await get_pool()
    while True:
        conn = await pool.acquire(timeout=LEADER_CHECK_TIMEOUT_SECONDS)
        try:
            acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", APP_SINGLETON_LOCK_ID,
                                           timeout=LEADER_CHECK_TIMEOUT_SECONDS)
        except BaseException:
            try:
                await pool.release(conn, timeout=LEADER_CHECK_TIMEOUT_SECONDS)
            except Exception:
                with suppress(Exception): conn.terminate()
            raise
        if acquired:
            log.info("Acquired app singleton lock %s.", APP_SINGLETON_LOCK_ID)
            return conn

        await pool.release(conn, timeout=LEADER_CHECK_TIMEOUT_SECONDS)
        log.warning(
            "Another DieselUp instance already holds app singleton lock %s; "
            "this container will not poll Telegram or run scheduled jobs yet.",
            APP_SINGLETON_LOCK_ID,
        )
        await asyncio.sleep(15)


async def _release_singleton_lock(conn: Any) -> None:
    try:
        if not conn.is_closed():
            # asyncpg can wait for a previous cancellation before its query
            # timeout starts. Bound the whole operation as well as the query.
            async with asyncio.timeout(LEADER_CHECK_TIMEOUT_SECONDS):
                await conn.execute("SELECT pg_advisory_unlock($1)", APP_SINGLETON_LOCK_ID,
                                   timeout=LEADER_CHECK_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — shutdown should continue
        log.warning("Failed to release app singleton lock: %s", type(exc).__name__)
        with suppress(Exception): conn.terminate()
    finally:
        pool = await get_pool()
        try:
            async with asyncio.timeout(LEADER_CHECK_TIMEOUT_SECONDS):
                await pool.release(conn, timeout=LEADER_CHECK_TIMEOUT_SECONDS)
        except Exception as exc:
            log.warning("Failed to return singleton connection: %s", type(exc).__name__)
            with suppress(Exception): conn.terminate()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
    except SingletonLeadershipLost as exc:
        log.critical("Bot stopped after singleton leadership loss: %s", exc)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
