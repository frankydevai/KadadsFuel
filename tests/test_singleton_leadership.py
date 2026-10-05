"""A lost database session must stop polling and scheduled work promptly."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from dieselup import main


@pytest.fixture(autouse=True)
def isolated_leadership_metrics(monkeypatch):
    monkeypatch.setattr(main.metrics, "gauge", Mock())


class Connection:
    def __init__(self):
        self.closed = False
        self.owned = True
        self.acquired = True
        self.listeners = set()
        self.queries = []
        self.hang = False

    def is_closed(self):
        return self.closed

    def add_termination_listener(self, callback):
        self.listeners.add(callback)

    def remove_termination_listener(self, callback):
        self.listeners.discard(callback)

    def terminate(self):
        self.close()

    def close(self, notify=True):
        self.closed = True
        if notify:
            for callback in tuple(self.listeners):
                callback(self)

    async def fetchval(self, sql, *args, **kwargs):
        self.queries.append((sql, args, kwargs))
        if "pg_try_advisory_lock" in sql:
            return self.acquired
        if self.hang:
            await asyncio.Event().wait()
        return self.owned

    async def execute(self, *args, **kwargs):
        if self.closed:
            raise RuntimeError("closed connection")
        self.owned = False


def fake_runtime(monkeypatch):
    events = []
    conn = Connection()
    pool = SimpleNamespace(acquire=AsyncMock(return_value=conn), release=AsyncMock())
    schedulers = []
    applications = []
    class Scheduler:
        def __init__(self, **kwargs):
            self.running = False
            self.jobs = {}
            schedulers.append(self)
        def add_job(self, fn, trigger, *, args, id, **kwargs):
            self.jobs[id] = (fn, args)
        def start(self):
            self.running = True
            events.append("scheduler_started")
        def pause(self):
            events.append("scheduler_paused")
        def shutdown(self, wait=False):
            self.running = False
            events.append("scheduler_stopped")
    class Application:
        def __init__(self):
            self.running = False
            self.bot_data = {}
            self.bot = SimpleNamespace(set_my_commands=AsyncMock())
            self.updater = SimpleNamespace(running=False, start_polling=self.poll, stop=self.unpoll)
            self.shutdown = AsyncMock(side_effect=lambda: events.append("telegram_shutdown"))
            self.polling = asyncio.Event()
            self.stop_gate = None
            applications.append(self)
        async def initialize(self):
            events.append("telegram_initialized")
        async def start(self):
            self.running = True
            events.append("telegram_started")
        async def poll(self, **kwargs):
            self.updater.running = True
            events.append("polling_started")
            self.polling.set()
        async def unpoll(self):
            self.updater.running = False
            events.append("polling_stopped")
            if self.stop_gate is not None:
                await self.stop_gate.wait()
        async def stop(self):
            self.running = False
            events.append("telegram_stopped")
    class Builder:
        def token(self, value): return self
        def request(self, value): return self
        def build(self): return Application()
    monkeypatch.setattr(main.settings, "BOT_MODE", "active")
    monkeypatch.setattr(main, "get_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(main, "close_pool", AsyncMock())
    monkeypatch.setattr(main, "start_health_server", lambda: None)
    monkeypatch.setattr(main, "_configure_logging", lambda: None)
    monkeypatch.setattr(main, "ApplicationBuilder", Builder)
    monkeypatch.setattr(main, "MessagingPolicyRequest", lambda: object())
    monkeypatch.setattr(main, "_LeadershipPolicyRequest", lambda guard: object(), raising=False)
    monkeypatch.setattr(main, "AsyncIOScheduler", Scheduler)
    for name in ("register_handlers", "register_admin_handlers", "register_group_link_handlers"):
        monkeypatch.setattr(main, name, lambda app: None)
    monkeypatch.setattr(main, "LEADER_CHECK_INTERVAL_SECONDS", 0.001, raising=False)
    monkeypatch.setattr(main, "LEADER_CHECK_TIMEOUT_SECONDS", 0.015, raising=False)
    return SimpleNamespace(conn=conn, pool=pool, events=events, applications=applications,
                           schedulers=schedulers)


async def wait_started(state):
    while not state.applications:
        await asyncio.sleep(0)
    await state.applications[0].polling.wait()


def test_closed_leader_connection_stops_existing_poller_and_scheduler(monkeypatch):
    state = fake_runtime(monkeypatch)
    async def run():
        task = asyncio.create_task(main._run())
        try:
            await wait_started(state)
            state.conn.close()
            done, _ = await asyncio.wait({task}, timeout=0.1)
            assert task in done, "The bot kept polling and scheduling after its leader session closed"
            assert not state.schedulers[0].running
            assert not state.applications[0].updater.running
            result = task.exception()
            assert isinstance(result, RuntimeError)
            assert sum("pg_try_advisory_lock" in q[0] for q in state.conn.queries) == 1
        finally:
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_healthy_watch_checks_owned_session_without_reentering_lock(monkeypatch):
    monkeypatch.setattr(main, "LEADER_CHECK_INTERVAL_SECONDS", 0.001)
    async def run():
        conn = Connection()
        guard = main._SingletonLeadership(conn)
        try:
            await guard.start()
            await asyncio.sleep(0.006)
            assert guard.active
            assert len(conn.queries) >= 2
            assert all("pg_locks" in q[0] and "pg_backend_pid()" in q[0]
                       and "objsubid = 1" in q[0] and "4294967295" in q[0]
                       and "pg_try_advisory_lock" not in q[0] for q in conn.queries)
            assert all(q[1] == (main.APP_SINGLETON_LOCK_ID,) for q in conn.queries)
        finally:
            await guard.close()
    asyncio.run(run())


@pytest.mark.parametrize("failure,reason", [("close", "connection_closed"),
                                             ("unlock", "lock_ownership_lost"),
                                             ("timeout", "ownership_check_timeout")])
def test_periodic_watch_loses_closed_live_unlocked_or_stalled_connection(monkeypatch, failure, reason):
    monkeypatch.setattr(main, "LEADER_CHECK_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(main, "LEADER_CHECK_TIMEOUT_SECONDS", 0.01)
    async def run():
        conn = Connection()
        guard = main._SingletonLeadership(conn)
        try:
            await guard.start()
            if failure == "close": conn.close(notify=False)
            elif failure == "unlock": conn.owned = False
            else: conn.hang = True
            await asyncio.wait_for(guard.wait_lost(), timeout=0.1)
            assert not guard.active
            assert guard.reason == reason
            main.metrics.gauge.assert_any_call("singleton_leader_active", 0)
        finally:
            await guard.close()
    asyncio.run(run())


def test_normal_shutdown_removes_listener_and_monitor_before_connection_release(monkeypatch):
    async def run():
        conn = Connection()
        guard = main._SingletonLeadership(conn)
        await guard.start()
        watcher = guard._watcher
        await guard.close()
        queries = len(conn.queries)
        conn.close()
        await asyncio.sleep(0)
        assert not conn.listeners
        assert watcher.done()
        assert guard._watcher is None
        assert guard.reason == "normal_shutdown"
        assert len(conn.queries) == queries
    asyncio.run(run())


def test_unexpected_monitor_cancellation_fails_closed(monkeypatch):
    async def run():
        conn = Connection()
        guard = main._SingletonLeadership(conn)
        try:
            await guard.start()
            await asyncio.sleep(0)
            guard._watcher.cancel()
            await asyncio.wait_for(guard.wait_lost(), timeout=0.1)
            assert guard.reason == "leader_monitor_cancelled"
            assert not guard.active
        finally:
            await guard.close()
    asyncio.run(run())


def test_unverified_startup_never_creates_scheduler_or_poller(monkeypatch):
    state = fake_runtime(monkeypatch)
    state.conn.owned = False
    async def run():
        with pytest.raises(main.SingletonLeadershipLost, match="lock_ownership_lost"):
            await main._run()
        assert not state.applications and not state.schedulers
        assert not state.conn.listeners
        state.pool.release.assert_awaited_once()
    asyncio.run(run())


def test_active_scheduled_job_cancels_before_slow_telegram_shutdown(monkeypatch):
    state = fake_runtime(monkeypatch)
    async def run():
        started, cancelled = asyncio.Event(), asyncio.Event()
        gate = asyncio.Event()
        async def active_job(bot):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        monkeypatch.setattr(main, "sync_active_loads", active_job)
        task = asyncio.create_task(main._run())
        job = None
        try:
            await wait_started(state)
            app = state.applications[0]
            app.stop_gate = gate
            fn, args = state.schedulers[0].jobs["load_sync"]
            job = asyncio.create_task(fn(*args))
            await started.wait()
            state.conn.close()
            while "polling_stopped" not in state.events:
                await asyncio.sleep(0)
            assert cancelled.is_set() and job.done()
            assert not state.schedulers[0].running
            assert not task.done(), "The Telegram cleanup gate should still be pending"
            assert state.events.index("scheduler_stopped") < state.events.index("polling_stopped")
            gate.set()
            with pytest.raises(main.SingletonLeadershipLost): await task
        finally:
            gate.set()
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if job is not None: await asyncio.gather(job, return_exceptions=True)
    asyncio.run(run())


def test_normal_runtime_shutdown_preserves_one_start_and_safe_release(monkeypatch):
    state = fake_runtime(monkeypatch)
    async def run():
        task = asyncio.create_task(main._run())
        await wait_started(state)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert state.events.count("polling_started") == 1
        assert state.events.index("scheduler_stopped") < state.events.index("polling_stopped")
        assert not state.conn.listeners
        state.pool.release.assert_awaited_once()
    asyncio.run(run())


def test_closed_connection_release_does_not_attempt_an_unlock(monkeypatch):
    conn = Connection()
    conn.close()
    conn.execute = AsyncMock()
    pool = SimpleNamespace(release=AsyncMock())
    monkeypatch.setattr(main, "get_pool", AsyncMock(return_value=pool))
    asyncio.run(main._release_singleton_lock(conn))
    conn.execute.assert_not_awaited()
    pool.release.assert_awaited_once_with(conn, timeout=main.LEADER_CHECK_TIMEOUT_SECONDS)


@pytest.mark.parametrize("endpoint,method", [("sendMessage", "POST"), ("sendMessage", "GET"),
                                           ("sendDocument", "POST"), ("unknownOutput", "POST")])
def test_lost_leadership_blocks_output_without_a_transport_request(monkeypatch, endpoint, method):
    from dieselup.bot.messaging_policy import MessagingPolicyRequest
    parent = AsyncMock(return_value=(200, b"accepted"))
    monkeypatch.setattr(MessagingPolicyRequest, "do_request", parent)
    async def run():
        guard = main._SingletonLeadership(Connection())
        request = main._LeadershipPolicyRequest(guard)
        try:
            status, body = await request.do_request("https://api.telegram.org/bot-test/" + endpoint, method)
            assert status == 403
            assert b"message_id" not in body
            parent.assert_not_awaited()
        finally:
            await request.shutdown()
    asyncio.run(run())


def test_polling_cleanup_reads_still_work_after_leadership_loss(monkeypatch):
    from dieselup.bot.messaging_policy import MessagingPolicyRequest
    parent = AsyncMock(return_value=(200, b"read"))
    monkeypatch.setattr(MessagingPolicyRequest, "do_request", parent)
    async def run():
        guard = main._SingletonLeadership(Connection())
        request = main._LeadershipPolicyRequest(guard)
        try:
            assert await request.do_request("https://api.telegram.org/bot-test/getUpdates", "POST") == (200, b"read")
            assert await request.do_request("https://api.telegram.org/file/bot-test/test.xlsx", "GET") == (200, b"read")
        finally:
            await request.shutdown()
    asyncio.run(run())


def test_leadership_loss_cancels_inflight_send_without_fabricating_failed_receipt(monkeypatch):
    from dieselup.bot.messaging_policy import MessagingPolicyRequest
    async def run():
        started, cancelled = asyncio.Event(), asyncio.Event()
        async def parent(*args, **kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        monkeypatch.setattr(MessagingPolicyRequest, "do_request", parent)
        guard = main._SingletonLeadership(Connection())
        await guard.start()
        request = main._LeadershipPolicyRequest(guard)
        try:
            task = asyncio.create_task(request.do_request("https://api.telegram.org/bot-test/sendMessage", "POST"))
            await started.wait()
            guard._lose("connection_terminated")
            with pytest.raises(asyncio.CancelledError): await task
            assert cancelled.is_set()
        finally:
            await guard.close()
            await request.shutdown()
    asyncio.run(run())


def test_accepted_response_is_preserved_when_loss_arrives_at_the_same_time(monkeypatch):
    from dieselup.bot.messaging_policy import MessagingPolicyRequest
    async def run():
        guard = main._SingletonLeadership(Connection())
        await guard.start()
        async def parent(*args, **kwargs):
            guard._lose("connection_terminated")
            return 200, b'{"ok":true,"result":{"message_id":42}}'
        monkeypatch.setattr(MessagingPolicyRequest, "do_request", parent)
        request = main._LeadershipPolicyRequest(guard)
        try:
            status, body = await request.do_request("https://api.telegram.org/bot-test/sendMessage", "POST")
            assert status == 200 and b'"message_id":42' in body
        finally:
            await guard.close()
            await request.shutdown()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["timeout", "flag_false_timeout", "raises", "still_running"])
def test_unproven_telegram_shutdown_terminates_before_lock_can_be_released(monkeypatch, kind):
    monkeypatch.setattr(main, "TELEGRAM_SHUTDOWN_TIMEOUT_SECONDS", 0.01)
    exit_process = Mock(side_effect=RuntimeError("fake process exit"))
    monkeypatch.setattr(main, "_force_fail_closed_exit", exit_process)
    async def run():
        updater = SimpleNamespace(running=True)
        async def stop_updater():
            if kind == "raises": raise OSError("private transport error")
            if kind == "flag_false_timeout": updater.running = False
            if kind in {"timeout", "flag_false_timeout"}: await asyncio.Event().wait()
        updater.stop = stop_updater
        app = SimpleNamespace(running=False, updater=updater, shutdown=AsyncMock())
        with pytest.raises(RuntimeError, match="fake process exit"):
            await main._telegram_shutdown(app)
        exit_process.assert_called_once()
    asyncio.run(run())


def test_job_swallowing_cancellation_cannot_outlive_leadership_cleanup(monkeypatch):
    monkeypatch.setattr(main, "ACTIVE_JOB_CANCEL_TIMEOUT_SECONDS", 0.01)
    exit_process = Mock(side_effect=RuntimeError("fake process exit"))
    monkeypatch.setattr(main, "_force_fail_closed_exit", exit_process)
    async def run():
        guard = main._SingletonLeadership(Connection())
        await guard.start()
        jobs = main._ActiveJobs(guard)
        gate, started = asyncio.Event(), asyncio.Event()
        async def stubborn():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await gate.wait()
        task = asyncio.create_task(jobs.run(stubborn))
        await started.wait()
        scheduler = SimpleNamespace(running=True, pause=Mock(), shutdown=Mock())
        try:
            with pytest.raises(RuntimeError, match="fake process exit"):
                await jobs.stop(scheduler)
            exit_process.assert_called_once()
            scheduler.pause.assert_called_once()
            scheduler.shutdown.assert_called_once_with(wait=False)
        finally:
            gate.set()
            await task
            await guard.close()
    asyncio.run(run())


def test_standby_releases_its_connection_before_waiting_for_the_only_lock(monkeypatch):
    standby, leader = Connection(), Connection()
    standby.acquired = False
    pool = SimpleNamespace(acquire=AsyncMock(side_effect=[standby, leader]), release=AsyncMock())
    pause = AsyncMock()
    monkeypatch.setattr(main, "get_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(main.asyncio, "sleep", pause)
    async def run():
        assert await main._acquire_singleton_lock() is leader
        assert pool.acquire.await_count == 2
        pool.release.assert_awaited_once_with(standby, timeout=main.LEADER_CHECK_TIMEOUT_SECONDS)
        pause.assert_awaited_once_with(15)
        assert len(standby.queries) == len(leader.queries) == 1
    asyncio.run(run())


def test_runtime_pool_cleanup_is_bounded_after_all_active_work_stops(monkeypatch):
    state = fake_runtime(monkeypatch)
    monkeypatch.setattr(main, "TELEGRAM_SHUTDOWN_TIMEOUT_SECONDS", 0.01)
    exit_process = Mock(side_effect=RuntimeError("fake process exit"))
    monkeypatch.setattr(main, "_force_fail_closed_exit", exit_process)
    async def stuck_close(): await asyncio.Event().wait()
    monkeypatch.setattr(main, "close_pool", stuck_close)
    async def run():
        task = asyncio.create_task(main._run())
        await wait_started(state)
        state.conn.close()
        with pytest.raises(RuntimeError, match="fake process exit"): await task
        assert not state.schedulers[0].running
        assert not state.applications[0].running
        assert not state.applications[0].updater.running
        assert not state.conn.listeners
        exit_process.assert_called_once()
    asyncio.run(run())


def test_executor_cancellation_is_not_injected_again_into_job_cleanup(monkeypatch):
    async def run():
        guard = main._SingletonLeadership(Connection())
        await guard.start()
        jobs = main._ActiveJobs(guard)
        started, cleaning, gate, cleaned = (asyncio.Event() for _ in range(4))
        async def job():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await gate.wait()
                cleaned.set()
        task = asyncio.create_task(jobs.run(job))
        await started.wait()
        scheduler = SimpleNamespace(running=True, pause=Mock(),
                                    shutdown=Mock(side_effect=lambda **kwargs: task.cancel()))
        stop = asyncio.create_task(jobs.stop(scheduler))
        try:
            await cleaning.wait()
            await asyncio.sleep(0)
            assert task.cancelling() == 1
            gate.set()
            await stop
            assert cleaned.is_set()
        finally:
            gate.set()
            await asyncio.gather(task, stop, return_exceptions=True)
            await guard.close()
    asyncio.run(run())
