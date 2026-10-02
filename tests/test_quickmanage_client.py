from __future__ import annotations

import asyncio

from dieselup.clients.quickmanage import QuickManageClient, _extract_items
from dieselup.clients.quickmanage import QuickManageError
import pytest
from dieselup.core.load_sync import _load_context


def test_quickmanage_trip_normalizes_for_shipper_to_delivery_planner():
    trip = {
        "id": "52041c16-cb42-4fc4-91af-2009a8a10fe0",
        "trip_num": 119,
        "ref_number": "REF001",
        "status": "in_transit",
        "stops": [
            {
                "pickup": True,
                "lat": 40.7128,
                "lng": -74.0060,
                "address": {"city": "New York", "state": "NY", "zip_code": "10001"},
                "assigned_truck": {"id": "truck-1", "unit": "5145"},
                "assigned_driver": {"first_name": "Jane", "last_name": "Driver"},
            },
            {
                "pickup": False,
                "lat": 41.8781,
                "lng": -87.6298,
                "address": {"city": "Chicago", "state": "IL", "zip_code": "60601"},
            },
        ],
    }

    async def run():
        client = QuickManageClient()
        try:
            return await client._normalize_trip(trip)
        finally:
            await client.close()

    normalized = asyncio.run(run())
    ctx = _load_context(normalized)

    assert normalized["status"] == "in_transit"
    assert normalized["truck_unit_number"] == "5145"
    assert normalized["driver_full_name"] == "Jane Driver"
    assert ctx["load_id"] == "REF001"
    assert ctx["datatruck_order_id"] is None
    assert ctx["tms_order_id"] == trip["id"]
    assert ctx["origin_lat"] == 40.7128
    assert ctx["destination_lat"] == 41.8781


def test_quickmanage_statuses_map_to_existing_active_and_delivered_flow():
    async def normalize(status: str):
        client = QuickManageClient()
        try:
            return await client._normalize_trip({
                "id": "trip-1",
                "status": status,
                "stops": [
                    {"pickup": True, "lat": 35.0, "lng": -90.0,
                     "assigned_truck": {"unit": "A1"}},
                    {"pickup": False, "lat": 36.0, "lng": -89.0},
                ],
            })
        finally:
            await client.close()

    assert asyncio.run(normalize("dispatching"))["status"] == "dispatched"
    assert asyncio.run(normalize("completed"))["status"] == "completed"


def test_quickmanage_observed_response_envelopes_are_supported():
    trip = {"id": "trip-1"}
    for payload in (
        {"data": {"items": [trip]}},
        {"data": {"trips": [trip]}},
        {"items": [trip]},
        {"trips": [trip]},
        {"data": [trip]},
    ):
        assert _extract_items(payload) == [trip]


def test_quickmanage_trip_level_truck_identity_is_normalized():
    async def normalize():
        client = QuickManageClient()
        try:
            return await client._normalize_trip({
                "id": "trip-2",
                "status": "in_transit",
                "truck_number": "9363",
                "driver": {"first_name": "Anthony", "last_name": "James"},
                "stops": [
                    {"pickup": True, "lat": 35.0, "lng": -90.0},
                    {"pickup": False, "lat": 36.0, "lng": -89.0},
                ],
            })
        finally:
            await client.close()

    row = asyncio.run(normalize())
    assert row["truck_unit_number"] == "9363"
    assert row["driver_full_name"] == "Anthony James"


def test_quickmanage_stop_truck_id_lookup_uses_all_search_envelopes():
    async def normalize():
        client = QuickManageClient()

        async def fake_post(path, body):
            assert path == "/x/trucks/search"
            assert body["filters"][0]["value"] == "truck-uuid-1"
            return {"items": [{"id": "truck-uuid-1", "unit": "777N"}]}

        client._post = fake_post
        try:
            return await client._normalize_trip({
                "id": "trip-3",
                "status": "in_transit",
                "stops": [
                    {
                        "pickup": True,
                        "lat": 35.0,
                        "lng": -90.0,
                        "assigned_truck": {"id": "truck-uuid-1"},
                        "assigned_drivers": [{"first_name": "Hector", "last_name": "Martin"}],
                    },
                    {"pickup": False, "lat": 36.0, "lng": -89.0},
                ],
            })
        finally:
            await client.close()

    row = asyncio.run(normalize())
    assert row["truck_unit_number"] == "777N"
    assert row["driver_full_name"] == "Hector Martin"


def test_quickmanage_pagination_stops_when_api_repeats_first_page():
    async def run():
        client = QuickManageClient()
        calls = 0

        async def repeated_page(_path, _body):
            nonlocal calls
            calls += 1
            return {"data": {"items": [{"id": "trip-1"}, {"id": "trip-2"}]}}

        async def passthrough(item):
            return item

        client._post = repeated_page
        client._normalize_trip = passthrough
        try:
            rows = []
            with pytest.raises(QuickManageError, match="repeated a page"):
                async for row in client.iter_orders(max_pages=50): rows.append(row)
            return calls, rows
        finally:
            await client.close()

    calls, rows = asyncio.run(run())
    assert calls == 2
    assert [row["id"] for row in rows] == ["trip-1", "trip-2"]


def test_quickmanage_iter_orders_sends_filter_list():
    async def run():
        client = QuickManageClient()
        seen = []

        async def fake_post(_path, body):
            seen.append(body)
            return {"data": {"items": [{"id": "trip-1", "status": "in_transit"}], "count": 1, "page_size": 100}}

        async def passthrough(item):
            return item

        client._post = fake_post
        client._normalize_trip = passthrough
        try:
            rows = [
                row
                async for row in client.iter_orders(
                    filters=[{"field": "status", "operator": "in", "value": ["in_transit"]}],
                    max_pages=50,
                )
            ]
            return seen, rows
        finally:
            await client.close()

    seen, rows = asyncio.run(run())
    assert rows == [{"id": "trip-1", "status": "in_transit"}]
    assert seen[0]["filters"] == [{"field": "status", "operator": "in", "value": ["in_transit"]}]
