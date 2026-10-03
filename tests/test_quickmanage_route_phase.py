"""Owner-defined QuickManage phases guide navigation without inventing stop history."""
import asyncio
import copy

import pytest

from dieselup.clients.quickmanage import QuickManageClient
from dieselup.core.trip_context import TripContextError, remaining_stops, route_context_signature


def normalized(status, stops=None):
    async def run():
        async with QuickManageClient() as client:
            return await client._normalize_trip({
                "id": "trip-current", "status": status, "truck_number": "6682",
                "stops": stops or [
                    {"id": "pickup", "pickup": True, "lat": 40, "lng": -100},
                    {"id": "delivery", "pickup": False, "lat": 41, "lng": -99},
                ],
            })
    return asyncio.run(run())


def test_in_transit_navigates_only_to_delivery_without_marking_pickup_completed():
    order = normalized("in_transit")
    assert [stop["id"] for stop in remaining_stops(order)] == ["delivery"]
    assert [stop["completed"] for stop in order["stops"]] == [None, None]
    assert order["route_phase"] == "delivery_only"
    assert order["route_context_source"] == "quickmanage_status"
    assert order["trip_metadata"]["route_phase_status"] == "in_transit"


@pytest.mark.parametrize("status", ["dispatched", "dispatching"])
def test_dispatched_navigates_ordered_pickups_then_delivery_without_fake_progress(status):
    order = normalized(status, [
        {"id": "pickup-one", "pickup": True, "lat": 40, "lng": -100},
        {"id": "pickup-two", "pickup": True, "lat": 40.5, "lng": -99.5},
        {"id": "delivery", "pickup": False, "lat": 41, "lng": -99},
    ])
    assert [stop["id"] for stop in remaining_stops(order)] == ["pickup-one", "pickup-two", "delivery"]
    assert all(stop["completed"] is None for stop in order["stops"])
    assert order["route_phase"] == "pickup_then_delivery"


@pytest.mark.parametrize("status", ["reserved", "upcoming"])
def test_reserved_next_load_cannot_supply_current_route(status):
    order = normalized(status)
    assert order["route_phase"] == "reserved"
    with pytest.raises(TripContextError, match="Reserved"):
        remaining_stops(order)


@pytest.mark.parametrize("status", ["active", "enroute", "completed", "delivered", "cancelled", "rejected", "", "unrecognized"])
def test_unrecognized_current_route_status_is_held_even_with_stop_progress(status):
    order = normalized(status, [
        {"id": "delivery", "pickup": False, "completed": False, "lat": 41, "lng": -99},
    ])
    assert order["route_phase"] == "unknown"
    with pytest.raises(TripContextError, match="does not authorize"):
        remaining_stops(order)


def test_in_transit_multiple_unknown_deliveries_are_held():
    order = normalized("in_transit", [
        {"id": "pickup", "pickup": True, "lat": 40, "lng": -100},
        {"id": "first-delivery", "pickup": False, "lat": 41, "lng": -99},
        {"id": "last-delivery", "pickup": False, "lat": 42, "lng": -98},
    ])
    with pytest.raises(TripContextError, match="Multiple delivery"):
        remaining_stops(order)


def test_completed_delivery_prefix_leaves_one_authorized_unknown_endpoint():
    order = normalized("in_transit", [
        {"id": "pickup", "pickup": True, "lat": 40, "lng": -100},
        {"id": "past-delivery", "pickup": False, "completed": True, "lat": 41, "lng": -99},
        {"id": "last-delivery", "pickup": False, "lat": 42, "lng": -98},
    ])
    before = copy.deepcopy(order)
    assert [stop["id"] for stop in remaining_stops(order)] == ["last-delivery"]
    assert order == before


def test_explicit_pending_deliveries_preserve_their_order():
    order = normalized("in_transit", [
        {"id": "pickup", "pickup": True, "completed": True, "lat": 40, "lng": -100},
        {"id": "first-delivery", "pickup": False, "completed": False, "lat": 41, "lng": -99},
        {"id": "last-delivery", "pickup": False, "completed": False, "lat": 42, "lng": -98},
    ])
    assert [stop["id"] for stop in remaining_stops(order)] == ["first-delivery", "last-delivery"]


@pytest.mark.parametrize("first_state", [False, None])
def test_completed_delivery_after_unverified_or_pending_delivery_is_held(first_state):
    order = normalized("in_transit", [
        {"id": "first-delivery", "pickup": False, "completed": first_state, "lat": 41, "lng": -99},
        {"id": "last-delivery", "pickup": False, "completed": True, "lat": 42, "lng": -98},
    ])
    with pytest.raises(TripContextError, match="out of sequence"):
        remaining_stops(order)


@pytest.mark.parametrize("status, stop, reason", [
    ("in_transit", {"pickup": True, "completed": False}, "Uncompleted pickup"),
    ("dispatched", {"pickup": True, "completed": True}, "Completed stop"),
    ("dispatching", {"pickup": False, "completed": True}, "Completed stop"),
])
def test_explicit_progress_contradicting_phase_is_held(status, stop, reason):
    stops = [
        {"id": "pickup", "pickup": True, "lat": 40, "lng": -100},
        {"id": "delivery", "pickup": False, "lat": 41, "lng": -99},
    ]
    stops[0 if stop["pickup"] else 1].update(stop)
    order = normalized(status, stops)
    with pytest.raises(TripContextError, match=reason):
        remaining_stops(order)


@pytest.mark.parametrize("change", [
    {"coordinate_source": "zip_centroid"}, {"latitude": None},
    {"latitude": float("nan")}, {"longitude": 181},
])
def test_status_authorization_still_requires_exact_valid_locations(change):
    order = normalized("in_transit")
    order["stops"][1].update(change)
    with pytest.raises(TripContextError):
        remaining_stops(order)


@pytest.mark.parametrize("change", [
    {"route_context_source": "guessed"}, {"route_phase": "pickup_then_delivery"},
    {"route_phase_status": "dispatched"},
])
def test_inconsistent_phase_provenance_is_held(change):
    order = normalized("in_transit")
    order.update(change)
    with pytest.raises(TripContextError, match="phase is missing or inconsistent"):
        remaining_stops(order)


def test_non_quickmanage_trip_cannot_use_quickmanage_status_as_progress():
    order = normalized("in_transit")
    order["tms_provider"] = "datatruck"
    with pytest.raises(TripContextError, match="progress is missing"):
        remaining_stops(order)


def test_route_context_signature_changes_on_phase_and_endpoint_changes():
    dispatched = normalized("dispatched")
    in_transit = normalized("in_transit")
    assert route_context_signature(dispatched) != route_context_signature(in_transit)
    first = route_context_signature(in_transit)
    in_transit["stops"][1]["longitude"] = -98.5
    assert route_context_signature(in_transit) != first


def test_generic_route_signature_is_stable_and_tracks_ordered_remaining_endpoints():
    order = {"status": "in_transit", "tms_provider": "datatruck", "stops": [
        {"id": "past", "type": "pickup", "completed": True, "latitude": 40, "longitude": -100},
        {"id": "next", "type": "delivery", "completed": False, "latitude": 41, "longitude": -99},
        {"id": "last", "type": "delivery", "completed": False, "latitude": 42, "longitude": -98},
    ]}
    first = route_context_signature(order)
    order["stops"][0]["latitude"] = 39  # Completed history cannot change navigation.
    order["stops"][1]["city"] = "Display label"
    assert route_context_signature(order) == first
    order["stops"][1:] = list(reversed(order["stops"][1:]))
    assert route_context_signature(order) != first


def test_street_route_proof_preserves_accuracy_without_inventing_completion():
    order = normalized("in_transit")
    order["stops"][1].update({
        "coordinate_source": "census_address_range",
        "coordinate_accuracy": "address_range_interpolation",
        "coordinate_verified_for": "fuel_route",
        "address_fingerprint": "synthetic-address-hash",
        "coordinate_provider": "US Census Bureau",
    })
    target = remaining_stops(order)[0]
    assert target["coordinate_accuracy"] == "address_range_interpolation"
    assert target["coordinate_verified_for"] == "fuel_route"
    assert target["coordinate_provider"] == "US Census Bureau"
    assert order["stops"][1]["completed"] is None
    previous = route_context_signature(order)
    order["stops"][1].update({"coordinate_source": "samsara_address",
                              "coordinate_accuracy": "saved_facility_point"})
    assert route_context_signature(order) != previous


def test_route_context_detects_assigned_driver_and_truck_changes():
    order = normalized("in_transit")
    order["driver_full_name"] = "Example Driver"
    first = route_context_signature(order)
    order["truck_unit_number"] = "06682"
    order["driver_full_name"] = " EXAMPLE   DRIVER "
    assert route_context_signature(order) == first
    order["driver_full_name"] = "Other Driver"
    assert route_context_signature(order) != first
    order["driver_full_name"] = "Example Driver"
    order["truck_unit_number"] = "8089"
    assert route_context_signature(order) != first
