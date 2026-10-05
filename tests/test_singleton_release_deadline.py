"""A pending asyncpg cancellation handshake cannot delay leader shutdown."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from dieselup import main


def test_unlock_deadline_covers_wait_before_query_timeout(monkeypatch):
    async def check():
        gate = asyncio.Event()
        terminated = Mock()

        async def execute(*args, **kwargs):
            # asyncpg waits for a previous cancellation before starting the query
            # timeout. This is that pre-query wait, not a slow SQL statement.
            await gate.wait()

        conn = SimpleNamespace(is_closed=lambda: False, execute=execute, terminate=terminated)
        pool = SimpleNamespace(release=AsyncMock())
        monkeypatch.setattr(main, "get_pool", AsyncMock(return_value=pool))
        monkeypatch.setattr(main, "LEADER_CHECK_TIMEOUT_SECONDS", 0.01)
        task = asyncio.create_task(main._release_singleton_lock(conn))
        try:
            done, _ = await asyncio.wait({task}, timeout=0.1)
            assert task in done, "Unlock must finish without the prior cancellation handshake"
            terminated.assert_called_once()
            pool.release.assert_awaited_once()
        finally:
            gate.set()
            await task

    asyncio.run(check())

def test_ownership_timeout_already_blocks_during_prior_handshake(monkeypatch):
    async def check():
        gate = asyncio.Event()
        conn = SimpleNamespace(is_closed=lambda: False)

        async def fetchval(*args, **kwargs):
            await gate.wait()

        conn.fetchval = fetchval
        monkeypatch.setattr(main.metrics, "gauge", Mock())
        monkeypatch.setattr(main, "LEADER_CHECK_TIMEOUT_SECONDS", 0.01)
        leader = main._SingletonLeadership(conn)
        leader._verified = True
        task = asyncio.create_task(leader._check())
        try:
            done, _ = await asyncio.wait({task}, timeout=0.1)
            assert task in done
            assert not leader.active
            assert leader.reason == "ownership_check_timeout"
        finally:
            gate.set()
            await task

    asyncio.run(check())


def test_pool_return_deadline_covers_reset_wait(monkeypatch):
    async def check():
        gate = asyncio.Event()
        conn = SimpleNamespace(is_closed=lambda: False, execute=AsyncMock(), terminate=Mock())

        async def release(*args, **kwargs):
            await gate.wait()

        pool = SimpleNamespace(release=release)
        monkeypatch.setattr(main, "get_pool", AsyncMock(return_value=pool))
        monkeypatch.setattr(main, "LEADER_CHECK_TIMEOUT_SECONDS", 0.01)
        task = asyncio.create_task(main._release_singleton_lock(conn))
        try:
            done, _ = await asyncio.wait({task}, timeout=0.1)
            assert task in done, "Pool cleanup must not prevent a safe leader restart"
            conn.terminate.assert_called_once()
        finally:
            gate.set()
            await task

    asyncio.run(check())
