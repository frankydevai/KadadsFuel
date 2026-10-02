import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.samsara import SamsaraClient, SamsaraError


def report(vid, mpg):
    return {'vehicle': {'id': vid}, 'distanceTraveledMeters': mpg * 10 * 1609.344,
            'fuelConsumedMl': 10 * 3785.411784, 'efficiencyMpge': 999}


def test_report_uses_documented_dates_units_and_every_page():
    async def run():
        async with SamsaraClient() as client:
            client._get_json = AsyncMock(side_effect=[
                {'data': {'vehicleReports': [report('a', 5.5)]},
                 'pagination': {'hasNextPage': True, 'endCursor': 'next'}},
                {'data': {'vehicleReports': [report('b', 8)]},
                 'pagination': {'hasNextPage': False}},
            ])
            assert await client._fetch_all_mpg() == pytest.approx({'a': 5.5, 'b': 8})
            first, second = client._get_json.call_args_list
            params = first.kwargs['params']
            assert set(params) == {'startDate', 'endDate'}
            end = datetime.fromisoformat(params['endDate'].replace('Z', '+00:00'))
            start = datetime.fromisoformat(params['startDate'].replace('Z', '+00:00'))
            assert end <= datetime.now(timezone.utc) - timedelta(days=3)
            assert (end-start).days == 6
            assert second.kwargs['params']['after'] == 'next'
    asyncio.run(run())


@pytest.mark.parametrize('value', [0, -1, float('nan'), float('inf'), None, 'invalid'])
def test_invalid_consumption_does_not_create_a_usable_mpg(value):
    async def run():
        async with SamsaraClient() as client:
            row = report('a', 6)
            row['fuelConsumedMl'] = value
            client._get_json = AsyncMock(return_value={'data': {'vehicleReports': [row]}})
            assert await client._fetch_all_mpg() == {}
    asyncio.run(run())


def test_repeating_report_cursor_cannot_loop_or_return_partial_fleet():
    async def run():
        async with SamsaraClient() as client:
            client._get_json = AsyncMock(return_value={
                'data': {'vehicleReports': [report('a', 6)]},
                'pagination': {'hasNextPage': True, 'endCursor': 'same'},
            })
            with pytest.raises(SamsaraError, match='did not advance'):
                await client._fetch_all_mpg()
            assert client._get_json.await_count == 2
    asyncio.run(run())


def test_bad_report_shape_is_an_error_and_falls_back_explicitly():
    async def run():
        async with SamsaraClient() as client:
            client._get_json = AsyncMock(return_value={'data': []})
            assert await client._ensure_mpg() == {}
    asyncio.run(run())
