import asyncio

import pytest

from dieselup.clients.quickmanage import QuickManageClient, QuickManageError
from dieselup.clients.samsara import VehicleSummary
from dieselup.core.load_sync import (
    _select_current_loads,
    _assigned_vehicle,
    LoadContextError,
)
from dieselup.core.trip_context import remaining_stops, TripContextError


def order(id, status="in_transit", unit="100", driver="Jane Driver"):
    return {
        "id": id,
        "status": status,
        "truck_unit_number": unit,
        "driver_full_name": driver,
    }


def test_current_in_transit_load_wins_over_upcoming_and_dispatched():
    assert [
        o["id"]
        for o in _select_current_loads(
            [order("future", "upcoming"), order("queued", "dispatched"), order("now")],
            {"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]},
        )
    ] == ["now"]


def test_conflicting_current_trips_are_held_instead_of_first_page_winning():
    assert (
        _select_current_loads(
            [order("one"), order("two")],
            {"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]},
        )
        == []
    )


def test_unit_aliases_are_deduplicated_by_physical_vehicle():
    v = VehicleSummary("v1", "SUBUNIT 100 (200) - Jane Driver", "100")
    assert (
        _select_current_loads(
            [order("one"), order("two", unit="200")], {"100": [v], "200": [v]}
        )
        == []
    )


def test_driver_match_cannot_retarget_a_trip_to_another_truck():
    unit_map = {
        "100": [VehicleSummary("v1", "100 - Other Driver", "100")],
        "200": [VehicleSummary("v2", "200 - Jane Driver", "200")],
    }
    with pytest.raises(LoadContextError):
        _assigned_vehicle(order("trip"), unit_map)


def test_numeric_samsara_label_does_not_invent_a_driver_mismatch():
    vehicle=VehicleSummary('v1','100','100')
    assert _assigned_vehicle(order('trip'),{'100':[vehicle]}) is vehicle


def test_numeric_labels_do_not_disambiguate_duplicate_trucks():
    with pytest.raises(LoadContextError,match='unique'):
        _assigned_vehicle(order('trip'),{'100':[
            VehicleSummary('v1','100','100'),VehicleSummary('v2','100','100')]})


@pytest.mark.parametrize(
    "change",
    [
        {"completed": None},
        {"coordinate_source": "zip_centroid"},
        {"latitude": None},
        {"longitude": float("nan")},
    ],
)
def test_missing_progress_or_exact_location_is_a_hold(change):
    stop = {
        "id": "s1",
        "type": "pickup",
        "completed": False,
        "latitude": 40,
        "longitude": -100,
    }
    stop.update(change)
    with pytest.raises(TripContextError):
        remaining_stops({"stops": [stop]})


def test_remaining_stops_preserve_order_and_drop_only_explicit_completed_prefix():
    stops = [
        {
            "id": str(i),
            "completed": i == 0,
            "latitude": 40 + i,
            "longitude": -100,
            "type": "pickup" if i == 1 else "delivery",
        }
        for i in range(4)
    ]
    assert [s["id"] for s in remaining_stops({"stops": stops})] == ["1", "2", "3"]


def test_out_of_sequence_completion_is_a_hold():
    with pytest.raises(TripContextError):
        remaining_stops({"stops": [{"completed": False}, {"completed": True}]})


def test_quickmanage_preserves_exact_progress_and_flags_conflicting_trucks():
    async def run():
        async with QuickManageClient() as client:
            return await client._normalize_trip(
                {
                    "id": "trip",
                    "status": "in_transit",
                    "stops": [
                        {
                            "id": "past",
                            "pickup": True,
                            "completed": True,
                            "lat": 40,
                            "lng": -100,
                            "assigned_truck": {"unit": "100"},
                        },
                        {
                            "id": "next",
                            "pickup": False,
                            "status": "pending",
                            "lat": 41,
                            "lng": -100,
                            "assigned_truck": {"unit": "200"},
                        },
                    ],
                }
            )

    row = asyncio.run(run())
    assert row["assignment_conflict"]
    assert [s["completed"] for s in row["stops"]] == [True, False]
    assert [s["coordinate_source"] for s in row["stops"]] == ["exact", "exact"]


def test_detail_search_cannot_return_a_different_trip_when_filter_is_ignored():
    async def run():
        async with QuickManageClient() as client:

            async def ignored(*_):
                return {"items": [{"id": "wrong"}]}

            client._post = ignored
            with pytest.raises(QuickManageError):
                await client.get_order("requested")

    asyncio.run(run())


def test_missing_fleet_identity_never_falls_back_to_driver_name():
    with pytest.raises(LoadContextError, match="fleet identity"):
        _assigned_vehicle(order("trip"), {})


def test_page_limit_holds_assignment_instead_of_processing_a_partial_fleet():
    async def run():
        async with QuickManageClient() as client:

            async def incomplete(*_):
                return {
                    "data": {"items": [{"id": "trip"}], "count": 200, "page_size": 100}
                }

            async def identity(row):
                return row

            client._post = incomplete
            client._normalize_trip = identity
            with pytest.raises(QuickManageError, match="page limit"):
                rows = [row async for row in client.iter_orders(max_pages=1)]

    asyncio.run(run())


def test_completed_trip_with_old_driver_does_not_poison_current_assignment():
    rows = [order("history", "completed", driver="Former Driver"), order("now")]
    unit_map = {"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]}
    assert [row["id"] for row in _select_current_loads(rows, unit_map)] == ["now"]


def test_current_trip_with_missing_unit_holds_same_driver_truck():
    unit_map = {"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]}
    assert (
        _select_current_loads(
            [order("unknown", unit=""), order("possible", "dispatched")], unit_map
        )
        == []
    )
