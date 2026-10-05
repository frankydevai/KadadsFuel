"""Bulk trip normalization cannot load ZIP data or invent navigable points."""
import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from dieselup.clients import quickmanage
from dieselup.core.stop_locations import match_samsara_address
from dieselup.core.trip_context import TripContextError, remaining_stops


def test_all_pages_skip_zip_lookup_preserve_address_and_require_verified_point(monkeypatch):
    def forbidden_zip_lookup(value):
        raise AssertionError('Trip enumeration must not load or download ZIP data')

    monkeypatch.setattr(quickmanage, '_geocode_zip', forbidden_zip_lookup)
    address = {'address_line_1': '100 West Example Street', 'city': 'Testville',
               'state': 'NJ', 'zip_code': '07001'}
    current = {'id': 'current', 'status': 'in_transit', 'truck_number': '6682',
               'stops': [{'id': 'delivery', 'pickup': False,
                          'company_name': 'Synthetic Warehouse', 'address': address}]}
    future = {'id': 'future', 'status': 'reserved', 'truck_number': '6682',
              'stops': [{'id': 'pickup', 'pickup': True, 'lat': 41.0, 'lng': -75.0,
                         'address': address},
                        {'id': 'delivery', 'pickup': False, 'address': address}]}

    async def check():
        async with quickmanage.QuickManageClient() as client:
            client._post = AsyncMock(side_effect=[
                {'data': {'items': [current], 'count': 2, 'page_size': 1}},
                {'data': {'items': [future], 'count': 2, 'page_size': 1}},
            ])
            orders = [row async for row in client.iter_orders(max_pages=3)]
            assert client._post.await_count == 2
            assert [call.args[1]['page'] for call in client._post.await_args_list] == [0, 1]
            return orders

    orders = asyncio.run(check())
    assert [row['id'] for row in orders] == ['current', 'future']
    unresolved = orders[0]['stops'][0]
    for key, value in address.items():
        assert unresolved[key] == value
    assert unresolved['company_name'] == 'Synthetic Warehouse'
    assert unresolved['latitude'] is None and unresolved['longitude'] is None
    assert unresolved['coordinate_source'] == 'missing'
    native = orders[1]['stops'][0]
    assert (native['latitude'], native['longitude'], native['coordinate_source']) == (41.0, -75.0, 'exact')
    assert orders[1]['stops'][1]['coordinate_source'] == 'missing'

    with pytest.raises(TripContextError, match='no exact coordinates'):
        remaining_stops(orders[0])
    with pytest.raises(TripContextError, match='Reserved next load'):
        remaining_stops(orders[1])

    # Exact full-address matching remains the existing path from missing
    # provider points to coordinates suitable for the fuel route.
    approved = match_samsara_address(unresolved, [{
        'id': 'saved-facility',
        'formattedAddress': '100 W Example St, Testville, NJ 07001, USA',
        'latitude': 40.5, 'longitude': -74.3,
    }])
    assert approved is not None and approved['coordinate_source'] == 'samsara_address'
    resolved = deepcopy(orders[0])
    resolved['stops'][0].update(approved)
    target = remaining_stops(resolved)[0]
    assert (target['latitude'], target['longitude']) == (40.5, -74.3)
    assert target['coordinate_verified_for'] == 'fuel_route'
    assert unresolved['coordinate_source'] == 'missing'
