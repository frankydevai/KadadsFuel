"""A stalled compliance read must release the scheduler slot without fake health."""
import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock

import pytest

from dieselup import metrics
from dieselup.core import compliance, fuel_replan


def test_hanging_initial_read_times_out_and_next_sweep_can_complete(monkeypatch, caplog):
    observed = {}
    counted = []
    cancelled = []

    async def hanging_read(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    monkeypatch.setattr(compliance, "COMPLIANCE_CYCLE_TIMEOUT_SECONDS", 0.02, raising=False)
    monkeypatch.setattr(compliance, "fetch_all", hanging_read)
    monkeypatch.setattr(fuel_replan, "run_requested_replans", AsyncMock())
    monkeypatch.setattr(metrics, "gauge", lambda key, value: observed.update({key: value}))
    monkeypatch.setattr(metrics, "incr", lambda key: counted.append(key))

    async def run():
        task = asyncio.create_task(compliance.resolve_pending_events(object()))
        try:
            done, _ = await asyncio.wait({task}, timeout=0.2)
            assert task in done, "The stuck read still occupies the compliance scheduler slot"
            with pytest.raises(TimeoutError):
                await task
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError, TimeoutError):
                await task
        assert cancelled == [True]
        assert "compliance_last_heartbeat_mono" not in observed
        assert "compliance_cycle_timeouts_total" in counted
        assert "resolution sweep timed out" in caplog.text

        monkeypatch.setattr(compliance, "fetch_all", AsyncMock(return_value=[]))
        await compliance.resolve_pending_events(object())
        assert observed["compliance_last_heartbeat_mono"] > 0

    asyncio.run(run())


def test_failed_post_fueling_check_does_not_mark_whole_sweep_healthy(monkeypatch):
    observed = {}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(compliance, "fetch_all", AsyncMock(return_value=[{"truck_unit": "6682"}]))
    monkeypatch.setattr(compliance, "SamsaraClient", Client)
    monkeypatch.setattr(compliance, "make_tms_client", Client)
    monkeypatch.setattr(compliance, "_resolve_one", AsyncMock(return_value=None))
    monkeypatch.setattr(compliance, "_verify_fueling_deltas", AsyncMock(side_effect=RuntimeError("verification failed")))
    monkeypatch.setattr(fuel_replan, "run_requested_replans", AsyncMock())
    monkeypatch.setattr(metrics, "gauge", lambda key, value: observed.update({key: value}))

    with pytest.raises(RuntimeError, match="verification failed"):
        asyncio.run(compliance.resolve_pending_events(object()))
    assert "compliance_last_heartbeat_mono" not in observed


def test_idle_replan_is_inside_sweep_deadline(monkeypatch):
    observed = {}
    cancelled = []

    async def hanging_replan(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    monkeypatch.setattr(compliance, "COMPLIANCE_CYCLE_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(compliance, "fetch_all", AsyncMock(return_value=[]))
    monkeypatch.setattr(fuel_replan, "run_requested_replans", hanging_replan)
    monkeypatch.setattr(metrics, "gauge", lambda key, value: observed.update({key: value}))
    with pytest.raises(TimeoutError):
        asyncio.run(compliance.resolve_pending_events(object()))
    assert cancelled == [True]
    assert "compliance_last_heartbeat_mono" not in observed


def test_inner_database_timeout_is_not_reported_as_cycle_deadline(monkeypatch, caplog):
    counted = []
    observed = {}
    monkeypatch.setattr(compliance, "fetch_all", AsyncMock(side_effect=TimeoutError("pool acquisition timed out")))
    monkeypatch.setattr(metrics, "incr", lambda key: counted.append(key))
    monkeypatch.setattr(metrics, "gauge", lambda key, value: observed.update({key: value}))

    with pytest.raises(TimeoutError, match="pool acquisition timed out"):
        asyncio.run(compliance.resolve_pending_events(object()))
    assert "compliance_cycle_timeouts_total" not in counted
    assert "resolution sweep timed out" not in caplog.text
    assert "compliance_last_heartbeat_mono" not in observed
