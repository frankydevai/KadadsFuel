"""Samsara reports percentage points; low readings must never be scaled up."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.samsara import SamsaraClient, SamsaraError
from dieselup.config import settings
from datetime import datetime,timezone,timedelta
from dieselup.clients import samsara as samsara_module


@pytest.mark.parametrize('percent', [0, 0.5, 1, 1.01, 5, 50, 100])
def test_fuel_feed_percentage_points_reach_the_planner_unchanged(percent):
    async def run():
        async with SamsaraClient() as client:
            client._get_json = AsyncMock(return_value={
                'data': [{'id': 'vehicle', 'fuelPercents': [
                    {'time': '2026-10-01T12:00:00Z', 'value': percent},
                ]}],
            })
            gallons = await client.get_vehicle_fuel('vehicle')
            assert gallons == pytest.approx(percent / 100 * settings.TANK_CAPACITY_GALLONS)
    asyncio.run(run())


@pytest.mark.parametrize('percent', [-1, 101, float('nan'), float('inf'), None, 'invalid'])
def test_missing_or_invalid_fuel_never_becomes_a_tank_estimate(percent):
    async def run():
        async with SamsaraClient() as client:
            client._get_json = AsyncMock(return_value={
                'data': [{'id': 'vehicle', 'fuelPercents': [{'value': percent}]}],
            })
            with pytest.raises(SamsaraError):
                await client.get_vehicle_fuel('vehicle')
    asyncio.run(run())


def test_fuel_timestamp_is_retained_separately_from_fresh_gps():
    async def run():
        async with SamsaraClient() as client:
            now=datetime.now(timezone.utc)
            async def response(path,**kwargs):
                if path.endswith('/locations'):
                    return {'data':[{'id':'vehicle','location':{'latitude':40,'longitude':-100,'time':now.isoformat()}}]}
                if path.endswith('/feed'):
                    return {'data':[{'id':'vehicle','fuelPercents':[{'value':50,'time':(now-timedelta(minutes=75)).isoformat()}]}]}
                return {'data':{'vehicleReports':[]}}
            client._get_json=response
            stats=await client.get_vehicle_stats('vehicle')
            assert stats.gps_age_minutes<1
            assert stats.fuel_age_minutes==pytest.approx(75,abs=.1)
    asyncio.run(run())


def test_cached_position_refreshes_during_a_long_sweep(monkeypatch):
    async def run():
        async with SamsaraClient() as client:
            clock=[100.0];calls=[]
            monkeypatch.setattr(samsara_module.time,'monotonic',lambda:clock[0])
            async def response(path,**kwargs):
                calls.append(path)
                return {'data':[{'id':'vehicle','location':{'latitude':40,'longitude':-100+len(calls),'time':datetime.now(timezone.utc).isoformat()}}]}
            client._get_json=response
            first=await client.get_vehicle_location('vehicle')
            assert (await client.get_vehicle_location('vehicle')).lng==first.lng
            clock[0]+=31
            assert (await client.get_vehicle_location('vehicle')).lng!=first.lng
            assert len(calls)==2
    asyncio.run(run())
