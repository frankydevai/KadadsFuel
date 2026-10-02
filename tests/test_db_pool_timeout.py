"""Pool contention must not occupy a scheduled job indefinitely."""
import asyncio
from contextlib import asynccontextmanager, suppress
from unittest.mock import AsyncMock

import pytest

from dieselup import db


@pytest.mark.parametrize("helper, method", [
    ("fetch_one", "fetchrow"), ("fetch_all", "fetch"), ("execute", "execute"),
])
def test_saturated_pool_wait_is_bounded_before_query_execution(monkeypatch, helper, method):
    invoked = AsyncMock()
    released = []

    class SaturatedPool:
        @asynccontextmanager
        async def acquire(self, *, timeout=None):
            await asyncio.wait_for(asyncio.Event().wait(), timeout=timeout)
            try:
                yield type("Connection", (), {method: invoked})()
            finally:
                released.append(True)

    monkeypatch.setattr(db, "get_pool", AsyncMock(return_value=SaturatedPool()))
    monkeypatch.setattr(db, "POOL_ACQUIRE_TIMEOUT_SECONDS", 0.02)

    async def run():
        task = asyncio.create_task(getattr(db, helper)("SELECT $1", 42))
        try:
            done, _ = await asyncio.wait({task}, timeout=0.2)
            assert task in done, "Pool contention still has no acquisition deadline"
            with pytest.raises(TimeoutError):
                await task
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError, TimeoutError):
                await task

    asyncio.run(run())
    invoked.assert_not_awaited()
    assert released == []  # No connection was acquired or leaked.


@pytest.mark.parametrize("helper, method", [
    ("fetch_one", "fetchrow"), ("fetch_all", "fetch"), ("execute", "execute"),
])
def test_cancelling_query_exits_connection_context(monkeypatch, helper, method):
    released = []

    async def run():
        entered = asyncio.Event()

        async def query(*args):
            entered.set()
            await asyncio.Event().wait()

        class Pool:
            @asynccontextmanager
            async def acquire(self, *, timeout=None):
                try:
                    yield type("Connection", (), {method: staticmethod(query)})()
                finally:
                    released.append(True)

        monkeypatch.setattr(db, "get_pool", AsyncMock(return_value=Pool()))
        task = asyncio.create_task(getattr(db, helper)("SELECT $1", 42))
        await asyncio.wait_for(entered.wait(), timeout=0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert released == [True]

    asyncio.run(run())
