"""Run the real per-load workflow with isolated database and delivery sinks."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.routing import RoutingError
from dieselup.clients.samsara import SamsaraError, VehicleSummary
from dieselup.core import load_sync
from dieselup.core.optimizer import StaleFuelPricesError


@pytest.mark.parametrize('scenario,driver_sends,dispatch_sends,expected', [
    ('verified', 1, 1, 'briefed'),
    ('driver_mismatch', 0, 0, load_sync.LoadContextError),
    ('group_mismatch', 0, 1, 'briefed'),
    ('unlinked', 0, 1, 'briefed'),
    ('paused_preflight', 0, 1, 'briefed'),
    ('old_gps', 0, 0, load_sync.LoadContextError),
    ('existing_pending', 0, 0, 'skipped'),
    ('passed_pending', 1, 1, 'briefed'),
    ('repeat_sweep', 1, 1, 'briefed'),
    ('missing_fuel', 0, 0, load_sync.LoadContextError),
    ('routing_outage', 0, 0, RoutingError),
    ('stale_prices', 0, 0, StaleFuelPricesError),
    ('enough_fuel', 0, 0, 'skipped'),
])
def test_no_send_workflow_gates(monkeypatch, scenario, driver_sends, dispatch_sends, expected):
    async def run():
        row = {'id': 1, 'driver_full_name': 'Jane Driver', 'driver_telegram_id': -101,
               'samsara_vehicle_id': 'vehicle-1'}
        order = {'id': 'test-load', 'load_number': 'SIMULATION', 'truck_unit_number': '100',
                 'driver_full_name': 'Wrong Driver' if scenario == 'driver_mismatch' else 'Jane Driver',
                 'stops': [{'type': 'delivery', 'latitude': 40, 'longitude': -80, 'city': 'Test', 'state': 'PA'}]}
        verified = {'100': -101}
        if scenario == 'unlinked': row['driver_telegram_id'] = None
        if scenario == 'group_mismatch': verified = {'100': -999}
        if scenario == 'paused_preflight': verified = {}
        pending = scenario in {'existing_pending', 'passed_pending'}
        accepted_driver_id = 1 if scenario == 'existing_pending' else None
        event_candidates = json.dumps([{'site_id': 1, 'distance_miles': 10, 'plan': {'route_evidence': {'model': 'remaining_route_v1'}}}])
        inserts = []

        async def fetch_one(sql, *args):
            nonlocal pending
            if 'FROM trucks_drivers' in sql: return dict(row)
            if sql.lstrip().startswith('INSERT INTO stop_events'):
                inserts.append(args); pending = True; return {'id': 1}
            if 'FROM stop_events' in sql:
                return {"id": 1, "driver_id": -101, "samsara_vehicle_id": "vehicle-1", "recommended_site_id": 1, "gallons": 80, "candidates": event_candidates, "briefing_driver_msg_id": accepted_driver_id} if "status = 'pending'" in sql and pending else None
            raise AssertionError('Unexpected database access in isolated simulation')

        monkeypatch.setattr(load_sync, 'fetch_one', fetch_one)
        async def execute(sql, *args):
            nonlocal accepted_driver_id
            if 'COALESCE($2, briefing_driver_msg_id)' in sql:
                accepted_driver_id = args[1]
        monkeypatch.setattr(load_sync, 'execute', execute)
        monkeypatch.setattr(load_sync, '_claim_driver_briefing_fingerprint', AsyncMock(return_value=True))
        monkeypatch.setattr(load_sync, '_claim_admin_alert_fingerprint', AsyncMock(return_value=True))
        monkeypatch.setattr(load_sync, '_safe_send_admin', AsyncMock())
        monkeypatch.setattr(load_sync, 'fuel_plan_keyboard', lambda **kwargs: None)
        monkeypatch.setattr(load_sync.settings, 'TELEGRAM_DISPATCH_CHAT_ID', -202)
        sink = AsyncMock(return_value=1)
        monkeypatch.setattr(load_sync, 'safe_send', sink)
        stats = SimpleNamespace(lat=39, lng=-80, fuel_gallons=60, mpg_rolling=6.5,
                                heading=0, speed_mph=55,
                                fuel_age_minutes=1, gps_age_minutes=120 if scenario == 'old_gps' else 1)
        samsara = SimpleNamespace(get_vehicle_stats=AsyncMock(return_value=stats))
        if scenario == 'missing_fuel': samsara.get_vehicle_stats.side_effect = SamsaraError('Missing fuel')
        leg = {'recommended_site_id': 2 if scenario == 'passed_pending' else 1, 'recommended_true_cost': 3,
               'worst_true_cost': 4, 'gallons': 80, 'candidates_json': event_candidates,
               'briefing_text': 'SIMULATION ONLY', 'alert_kind': 'briefing', 'degraded': None}
        planner = AsyncMock(return_value=None if scenario == 'enough_fuel' else leg)
        if scenario == 'routing_outage': planner.side_effect = RoutingError('Routing unavailable')
        if scenario == 'stale_prices': planner.side_effect = StaleFuelPricesError('Stale feed')
        monkeypatch.setattr(load_sync, '_build_routed_leg', planner)
        kwargs = dict(samsara=samsara, samsara_by_unit={"100": [VehicleSummary("vehicle-1", "100 - Jane Driver", "100")]}, bot=object(), verified_driver_links=verified, current_trip_verified=True)
        if isinstance(expected, type):
            with pytest.raises(expected): await load_sync._process_one_load(order, **kwargs)
            assert not inserts
        else:
            assert await load_sync._process_one_load(order, **kwargs) == expected
        if scenario == 'repeat_sweep':
            assert await load_sync._process_one_load(order, **kwargs) == 'skipped'
            assert len(inserts) == 1
        chats = [call.kwargs['chat_id'] for call in sink.call_args_list]
        assert chats.count(-101) == driver_sends
        assert chats.count(-202) == dispatch_sends
    asyncio.run(run())
