import asyncio

import pytest

from dieselup.circuit_breaker import CircuitBreaker, CircuitOpenError


def test_half_open_allows_only_one_concurrent_probe():
    breaker = CircuitBreaker("test_single_probe", fail_threshold=1, cooldown=0.0)

    async def fail_once():
        raise RuntimeError("upstream down")

    with pytest.raises(RuntimeError):
        asyncio.run(breaker.call(fail_once))

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_probe():
        probe_started.set()
        await release_probe.wait()
        return "recovered"

    async def exercise():
        first = asyncio.create_task(breaker.call(slow_probe))
        await probe_started.wait()
        with pytest.raises(CircuitOpenError):
            await breaker.call(slow_probe)
        release_probe.set()
        assert await first == "recovered"

    asyncio.run(exercise())
    assert breaker.state() == "closed"


def test_cancelled_half_open_probe_does_not_stick_forever():
    breaker = CircuitBreaker("test_cancelled_probe", fail_threshold=1, cooldown=0.0)

    async def fail_once():
        raise RuntimeError("upstream down")

    with pytest.raises(RuntimeError):
        asyncio.run(breaker.call(fail_once))

    async def exercise():
        async def cancelled_probe():
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await breaker.call(cancelled_probe)

        async def recovered_probe():
            return "ok"

        assert await breaker.call(recovered_probe) == "ok"

    asyncio.run(exercise())
    assert breaker.state() == "closed"
