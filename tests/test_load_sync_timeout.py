from __future__ import annotations

import asyncio

from dieselup import metrics
from dieselup.core import load_sync
from dieselup.clients.samsara import VehicleSummary
from dieselup.clients.quickmanage import QuickManageError


class _HangingTms:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def iter_orders(self, *, max_pages=None):
        yield {"id": "trip-1", "status": "active", "truck_unit_number": "5145"}
        await asyncio.sleep(10)


class _FakeSamsara:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class _SlowDeliveredQuickManage:
    async def get_order(self, order_id):
        return {"id": order_id, "status": "dispatched", "truck_unit_number": "5145"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def iter_orders(self, *, filters=None, max_pages=None):
        statuses = (filters or [{}])[0].get("value") or []
        if "completed" in statuses or "delivered" in statuses:
            await asyncio.sleep(10)
            return
        yield {"id": "active-1", "status": "dispatched", "truck_unit_number": "5145"}


def test_sync_active_loads_records_heartbeat_when_order_enumeration_times_out(monkeypatch):
    async def fake_refresh_groups(_bot):
        return {}

    async def fake_build_samsara_unit_map(_samsara):
        return {"5145": [VehicleSummary("v5145", "5145 - Driver", "5145")]}

    async def fake_process_one_load(*args, **kwargs):
        return "skipped"

    monkeypatch.setattr(load_sync, "make_tms_client", lambda: _HangingTms())
    monkeypatch.setattr(load_sync, "SamsaraClient", lambda: _FakeSamsara())
    monkeypatch.setattr(load_sync, "_build_samsara_unit_map", fake_build_samsara_unit_map)
    monkeypatch.setattr(load_sync, "_process_one_load", fake_process_one_load)
    monkeypatch.setattr(load_sync, "refresh_and_verify_linked_groups", fake_refresh_groups)
    monkeypatch.setattr(load_sync, "ORDER_ENUMERATION_TIMEOUT_SECONDS", 0.01)

    asyncio.run(load_sync.sync_active_loads(bot=object()))

    snap = metrics.snapshot()
    assert snap["counters"]["load_sync_order_enumeration_timed_out"] >= 1
    assert snap["gauges"]["load_sync_last_cycle_processed"] >= 1
    assert snap["gauges"]["load_sync_last_heartbeat_mono"] > 0


def test_quickmanage_delivered_scan_does_not_block_active_processing(monkeypatch):
    processed_orders: list[str] = []

    async def fake_refresh_groups(_bot):
        return {}

    async def fake_build_samsara_unit_map(_samsara):
        return {"5145": [VehicleSummary("v5145", "5145 - Driver", "5145")]}

    async def fake_process_one_load(order, **kwargs):
        processed_orders.append(order["id"])
        return "briefed"

    monkeypatch.setattr(load_sync.settings, "TMS_PROVIDER", "quickmanage")
    monkeypatch.setattr(load_sync, "make_tms_client", lambda: _SlowDeliveredQuickManage())
    monkeypatch.setattr(load_sync, "SamsaraClient", lambda: _FakeSamsara())
    monkeypatch.setattr(load_sync, "_build_samsara_unit_map", fake_build_samsara_unit_map)
    monkeypatch.setattr(load_sync, "_process_one_load", fake_process_one_load)
    monkeypatch.setattr(load_sync, "refresh_and_verify_linked_groups", fake_refresh_groups)
    monkeypatch.setattr(load_sync, "ORDER_ENUMERATION_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(load_sync, "DELIVERED_ENUMERATION_TIMEOUT_SECONDS", 0.01)

    asyncio.run(load_sync.sync_active_loads(bot=object()))

    snap = metrics.snapshot()
    assert processed_orders == ["active-1"]
    assert snap["counters"]["load_sync_delivered_order_enumeration_timed_out"] >= 1
    assert snap["gauges"]["load_sync_last_cycle_briefed"] >= 1
    assert snap["gauges"]["load_sync_last_heartbeat_mono"] > 0


def test_incomplete_history_preserves_active_result_and_holds_followups(monkeypatch):
    class CappedHistory(_SlowDeliveredQuickManage):
        async def iter_orders(self, *, filters=None, max_pages=None):
            if 'completed' in filters[0]['value']:
                yield {'id':'partial-history','status':'completed','truck_unit_number':'5145'}
                raise QuickManageError('Trip search reached the page limit before proving completion')
            yield {'id':'active-1','status':'dispatched','truck_unit_number':'5145'}
    from unittest.mock import AsyncMock
    process=AsyncMock(return_value='briefed');followup=AsyncMock();admin=AsyncMock()
    monkeypatch.setattr(load_sync.settings,'TMS_PROVIDER','quickmanage')
    monkeypatch.setattr(load_sync,'make_tms_client',CappedHistory)
    monkeypatch.setattr(load_sync,'SamsaraClient',_FakeSamsara)
    monkeypatch.setattr(load_sync,'refresh_and_verify_linked_groups',AsyncMock(return_value={}))
    monkeypatch.setattr(load_sync,'_build_samsara_unit_map',AsyncMock(return_value={
        '5145':[VehicleSummary('v5145','5145 - Driver','5145')]}))
    monkeypatch.setattr(load_sync,'_process_one_load',process)
    monkeypatch.setattr(load_sync,'_send_standalone_delivery_complete',followup)
    monkeypatch.setattr(load_sync,'_safe_send_admin',admin)
    before=metrics.snapshot()['counters'].get('load_sync_aborted_datatruck',0)
    asyncio.run(load_sync.sync_active_loads(object()))
    assert process.await_count==1
    followup.assert_not_awaited();admin.assert_not_awaited()
    snapshot=metrics.snapshot()
    assert snapshot['counters'].get('load_sync_aborted_datatruck',0)==before
    assert snapshot['counters']['load_sync_delivered_history_held']>=1
    assert snapshot['gauges']['load_sync_last_cycle_briefed']==1
