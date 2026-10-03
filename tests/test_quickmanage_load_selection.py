import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.samsara import VehicleSummary
from dieselup.core import load_sync


def trip(trip_id, status):
    return {'id': trip_id, 'tms_order_id': trip_id, 'load_number': trip_id,
            'tms_provider': 'quickmanage', 'raw_status': status, 'status': status,
            'truck_unit_number': '6682', 'driver_full_name': 'Jane Driver'}


def test_reserved_is_retained_as_next_load_without_replacing_current_trip():
    rows = [trip('next', 'reserved'), trip('shipper', 'dispatched'), trip('delivery', 'in_transit')]
    fleet = {'6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}
    assert [row['id'] for row in load_sync._select_current_loads(rows, fleet)] == ['delivery']
    assert [row['id'] for row in load_sync._select_reserved_loads(rows, fleet)] == ['next']


def test_reserved_only_has_no_current_route_and_waits_for_dispatched_transition():
    fleet = {'6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}
    assert load_sync._select_current_loads([trip('next', 'reserved')], fleet) == []
    assert [row['id'] for row in load_sync._select_current_loads([trip('next', 'dispatched')], fleet)] == ['next']


def test_normalized_upcoming_is_never_a_current_dispatched_trip():
    queued = {**trip('next', 'upcoming'), 'status': 'dispatched'}
    assert not load_sync._is_current(queued)


def test_phase_changed_pending_plan_is_replaced_even_when_pump_and_quantity_match():
    previous = {'plan': {'route_evidence': {'model': 'remaining_route_v1',
                 'route_context_sha256': 'old-shipper-phase'}}, 'distance_miles': 10}
    fresh = {'plan': {'route_evidence': {'model': 'remaining_route_v1',
              'route_context_sha256': 'new-delivery-phase'}}, 'distance_miles': 10}
    assert not load_sync._same_pending_plan(
        {'recommended_site_id': 1, 'gallons': 80, 'candidates': [previous]},
        {'recommended_site_id': 1, 'gallons': 80, 'candidates_json': json.dumps([fresh])},
        {'tms_provider': 'quickmanage'})


def test_conflicting_reserved_assignments_do_not_poison_a_valid_current_trip():
    rows = [trip('current', 'in_transit'), trip('next-one', 'reserved'), trip('next-two', 'reserved')]
    fleet = {'6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}
    assert [row['id'] for row in load_sync._select_current_loads(rows, fleet)] == ['current']
    assert load_sync._select_reserved_loads(rows, fleet) == []


def test_wrong_reserved_driver_holds_only_next_assignment():
    rows = [trip('current', 'in_transit'), trip('next', 'reserved'),
            {**trip('conflicting-next', 'reserved'), 'driver_full_name': 'Other Driver'}]
    fleet = {'6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}
    assert [row['id'] for row in load_sync._select_current_loads(rows, fleet)] == ['current']
    assert load_sync._select_reserved_loads(rows, fleet) == []


def test_reserved_queue_keeps_only_authorized_trucks(monkeypatch):
    monkeypatch.setattr(load_sync.settings, 'TEST_TRUCK_UNITS', '6682,8089,8217')
    rows = [trip('authorized', 'reserved'), {**trip('outside', 'reserved'), 'truck_unit_number': '7777'}]
    fleet = {'6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')],
             '7777': [VehicleSummary('v2', '7777 - Jane Driver', '7777')]}
    assert [row['id'] for row in load_sync._select_reserved_loads(rows, fleet)] == ['authorized']


@pytest.mark.parametrize('status', ['reserved', 'upcoming', 'delivered', 'unknown'])
def test_quickmanage_non_current_phases_are_not_current_even_if_normalized_active(status):
    assert not load_sync._is_current({**trip('next', status), 'status': 'dispatched'})


def test_reserved_is_recorded_in_history_without_becoming_a_route_or_message(monkeypatch):
    class Tms:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def iter_orders(self, *, filters=None, max_pages=None):
            if 'reserved' in filters[0]['value']:
                yield trip('next', 'reserved')

    class Samsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass

    recorder = AsyncMock()
    processor = AsyncMock()
    sender = AsyncMock()
    monkeypatch.setattr(load_sync.settings, 'TMS_PROVIDER', 'quickmanage')
    monkeypatch.setattr(load_sync, 'make_tms_client', Tms)
    monkeypatch.setattr(load_sync, 'SamsaraClient', Samsara)
    monkeypatch.setattr(load_sync, 'refresh_and_verify_linked_groups', AsyncMock(return_value={'6682': -1006682}))
    monkeypatch.setattr(load_sync, '_build_samsara_unit_map', AsyncMock(return_value={
        '6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}))
    monkeypatch.setattr(load_sync.advice_audit, 'record', recorder)
    monkeypatch.setattr(load_sync, '_process_one_load', processor)
    monkeypatch.setattr(load_sync, 'safe_send', sender)
    asyncio.run(load_sync.sync_active_loads(object()))
    recorder.assert_awaited_once()
    assert recorder.call_args.args[0] == 'next_load_reserved'
    assert recorder.call_args.kwargs['details']['tms_order_id'] == 'next'
    processor.assert_not_awaited()
    sender.assert_not_awaited()


def test_fresh_detail_failure_is_audited_without_private_provider_error_text(monkeypatch):
    from dieselup.clients.quickmanage import QuickManageError

    class Tms:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def iter_orders(self, *, filters=None, max_pages=None):
            if 'in_transit' in filters[0]['value']:
                yield trip('current', 'in_transit')
        async def get_order(self, *args):
            raise QuickManageError('PRIVATE ADDRESS OR PROVIDER AUTH DETAILS')

    class Samsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass

    recorder = AsyncMock()
    processor = AsyncMock()
    monkeypatch.setattr(load_sync.settings, 'TMS_PROVIDER', 'quickmanage')
    monkeypatch.setattr(load_sync, 'make_tms_client', Tms)
    monkeypatch.setattr(load_sync, 'SamsaraClient', Samsara)
    monkeypatch.setattr(load_sync, 'refresh_and_verify_linked_groups', AsyncMock(return_value={'6682': -1006682}))
    monkeypatch.setattr(load_sync, '_build_samsara_unit_map', AsyncMock(return_value={
        '6682': [VehicleSummary('v1', '6682 - Jane Driver', '6682')]}))
    monkeypatch.setattr(load_sync.advice_audit, 'record', recorder)
    monkeypatch.setattr(load_sync, '_process_one_load', processor)
    monkeypatch.setattr(load_sync, '_safe_send_admin', AsyncMock())
    asyncio.run(load_sync.sync_active_loads(object()))
    held = [call for call in recorder.call_args_list if call.args[0] == 'plan_held']
    assert len(held) == 1
    assert held[0].kwargs['details'] == {'reason': 'QuickManageError', 'stage': 'current_trip_detail'}
    processor.assert_not_awaited()
