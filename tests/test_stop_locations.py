import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from dieselup.core.stop_locations import (
    CensusStreetAddressResolver, StopLocationError, StopLocationResolver,
    match_samsara_address, parse_census_address,
)


def stop(**changes):
    value = {"id": "stop-1", "type": "delivery", "completed": None,
             "address_line_1": "100 West Example Street", "city": "Testville",
             "state": "NJ", "zip_code": "07001", "latitude": 40.1,
             "longitude": -74.1, "coordinate_source": "zip_centroid"}
    return {**value, **changes}


def facility(**changes):
    return {"id": "facility-1", "formattedAddress":
            "100 W Example St, Testville, NJ 07001, USA",
            "latitude": 40.5, "longitude": -74.3, **changes}


def page(records, next_page=False, cursor="cursor-1"):
    return {"data": records, "pagination": {"hasNextPage": next_page, "endCursor": cursor}}


def order(status="dispatched", **changes):
    return {"id": "trip-1", "tms_provider": "quickmanage", "truck_unit_number": "8089",
            "raw_status": status, "status": status, "route_phase_status": status,
            "route_context_source": "quickmanage_status",
            "route_phase": {"dispatched": "pickup_then_delivery", "in_transit": "delivery_only",
                            "reserved": "reserved"}.get(status, "unknown"),
            "stops": [stop(id="pickup", type="pickup"), stop(id="delivery")], **changes}


def census_payload(**changes):
    match = {"matchedAddress": "100 W EXAMPLE ST, TESTVILLE, NJ 07001",
             "coordinates": {"x": -74.3, "y": 40.5},
             "addressComponents": {"preDirection": "W", "streetName": "EXAMPLE",
                                   "suffixType": "ST", "city": "TESTVILLE",
                                   "state": "NJ", "zip": "07001"}, **changes}
    return {"result": {"addressMatches": [match]}}


@pytest.fixture
def selected_scope(monkeypatch):
    from dieselup.core.operating_scope import settings
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "6682,8089,8217")


def test_full_street_match_resolves_a_saved_facility():
    result = match_samsara_address(stop(), [facility()])
    assert result["latitude"] == 40.5
    assert result["coordinate_source"] == "samsara_address"
    assert result["address_id"] == "facility-1"
    assert result["coordinate_verified_for"] == "fuel_route"
    assert len(result["address_fingerprint"]) == 64
    assert "completed" not in result


@pytest.mark.parametrize("change", [
    {"address_line_1": "101 West Example Street"},
    {"address_line_1": "100 East Example Street"},
    {"address_line_1": "100 West Different Street"},
    {"address_line_1": "100 West Example Street Suite 2"},
    {"address_line_1": "PO Box 100"}, {"address_line_1": ""},
    {"city": "Other City"}, {"state": "NY"}, {"zip_code": "07002"},
    {"zip_code": None},
])
def test_partial_or_different_addresses_do_not_match(change):
    assert match_samsara_address(stop(**change), [facility()]) is None


def test_full_state_spelling_and_zip_plus_four_are_controlled_variants():
    assert match_samsara_address(stop(state="New Jersey", zip_code="07001-1234"),
                                 [facility()]) is not None


def test_house_number_punctuation_and_unit_information_are_not_discarded():
    assert match_samsara_address(stop(address_line_1="100-2 West Example Street"),
                                 [facility(formattedAddress="1002 W Example St, Testville, NJ 07001")]) is None


def test_duplicate_facilities_remain_ambiguous_even_when_coordinates_agree():
    assert match_samsara_address(stop(), [facility(), facility(id="facility-2")]) is None


@pytest.mark.parametrize("change", [
    {"latitude": float("nan")}, {"longitude": float("inf")},
    {"latitude": 91}, {"longitude": -181}, {"latitude": True},
    {"latitude": 0, "longitude": 0}, {"latitude": None}, {"id": ""},
])
def test_invalid_saved_coordinates_or_identity_are_rejected(change):
    assert match_samsara_address(stop(), [facility(**change)]) is None


def test_top_level_coordinates_override_the_circle_and_invalid_override_stays_held():
    circle = {"circle": {"latitude": 41, "longitude": -75}}
    assert match_samsara_address(stop(), [facility(geofence=circle)])["latitude"] == 40.5
    assert match_samsara_address(stop(), [facility(latitude=91, geofence=circle)]) is None


def test_circle_coordinates_can_supply_a_missing_top_level_point():
    match = match_samsara_address(stop(), [facility(latitude=None, longitude=None,
        geofence={"circle": {"latitude": 41, "longitude": -75}})])
    assert (match["latitude"], match["longitude"]) == (41, -75)


def test_polygon_vertices_do_not_create_an_invented_centroid():
    assert match_samsara_address(stop(), [facility(latitude=None, longitude=None,
        geofence={"polygon": {"vertices": [{"latitude": 41, "longitude": -75}]}})]) is None


def test_complete_paging_continues_through_empty_intermediate_pages_and_caches():
    client = SimpleNamespace(_get_json=AsyncMock(side_effect=[
        page([facility()], True, "a"), page([], True, "b"), page([], False)]))
    async def run():
        resolver = StopLocationResolver(client)
        first = await resolver.addresses()
        first[0]["latitude"] = 0
        return await resolver.addresses()
    assert asyncio.run(run())[0]["latitude"] == 40.5
    assert client._get_json.call_args_list == [
        call("/addresses", params={"limit": 512}),
        call("/addresses", params={"limit": 512, "after": "a"}),
        call("/addresses", params={"limit": 512, "after": "b"})]


def test_address_cache_expires_and_shares_concurrent_fetches(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("dieselup.core.stop_locations.time.monotonic", lambda: now[0])
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    async def run():
        resolver = StopLocationResolver(client, cache_seconds=10)
        await asyncio.gather(resolver.addresses(), resolver.addresses())
        assert client._get_json.await_count == 1
        now[0] = 111
        await resolver.addresses()
        assert client._get_json.await_count == 2
    asyncio.run(run())


@pytest.mark.parametrize("payload", [
    {"data": [], "pagination": {"hasNextPage": True}},
    {"data": [], "pagination": {"hasNextPage": "false"}},
    {"data": []}, {"data": {}}, page([None]),
])
def test_incomplete_address_indexes_are_not_cached(payload):
    client = SimpleNamespace(_get_json=AsyncMock(side_effect=[payload, page([facility()])]))
    async def run():
        resolver = StopLocationResolver(client)
        with pytest.raises(StopLocationError):
            await resolver.addresses()
        assert len(await resolver.addresses()) == 1
    asyncio.run(run())
    assert client._get_json.await_count == 2


def test_repeated_cursor_and_page_budget_fail_closed():
    async def run():
        repeated = SimpleNamespace(_get_json=AsyncMock(return_value=page([], True, "same")))
        with pytest.raises(StopLocationError, match="incomplete"):
            await StopLocationResolver(repeated).addresses()
        limited = SimpleNamespace(_get_json=AsyncMock(return_value=page([], True, "next")))
        with pytest.raises(StopLocationError, match="limit"):
            await StopLocationResolver(limited, max_pages=1).addresses()
    asyncio.run(run())


def test_provider_failure_has_a_safe_reason_and_never_caches_a_partial_index():
    client = SimpleNamespace(_get_json=AsyncMock(side_effect=RuntimeError("private upstream value")))
    async def run():
        resolver = StopLocationResolver(client)
        with pytest.raises(StopLocationError) as exc:
            await resolver.addresses()
        assert str(exc.value) == "Saved facility addresses are unavailable"
        assert resolver._addresses is None
    asyncio.run(run())


def test_dispatched_targets_are_enriched_without_mutating_input_or_completion(selected_scope):
    original = order()
    before = deepcopy(original)
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(original))
    assert original == before
    assert [s["coordinate_source"] for s in result["stops"]] == ["samsara_address"] * 2
    assert all(s["completed"] is None for s in result["stops"])
    assert client._get_json.await_count == 1


def test_in_transit_only_enriches_the_remaining_delivery(selected_scope):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(order("in_transit")))
    assert result["stops"][0]["coordinate_source"] == "zip_centroid"
    assert result["stops"][1]["coordinate_source"] == "samsara_address"


@pytest.mark.parametrize("changes", [
    {"truck_unit_number": "9999"}, {"truck_unit_number": None},
    {"tms_provider": "other"}, {"route_context_source": "unknown"},
    {"route_phase": "reserved"},
])
def test_unapproved_truck_provider_or_phase_never_calls_address_api(selected_scope, changes):
    client = SimpleNamespace(_get_json=AsyncMock())
    original = order(**changes)
    assert asyncio.run(StopLocationResolver(client).enrich_order(original)) == original
    client._get_json.assert_not_awaited()


@pytest.mark.parametrize("status", ["reserved", "unknown"])
def test_no_current_route_means_no_location_requests(selected_scope, status):
    client = SimpleNamespace(_get_json=AsyncMock())
    original = order(status)
    assert asyncio.run(StopLocationResolver(client).enrich_order(original)) == original
    client._get_json.assert_not_awaited()


def test_explicit_coordinates_are_preserved_and_need_no_index(selected_scope):
    original = order(stops=[stop(coordinate_source="exact")])
    client = SimpleNamespace(_get_json=AsyncMock())
    assert asyncio.run(StopLocationResolver(client).enrich_order(original)) == original
    client._get_json.assert_not_awaited()


@pytest.mark.parametrize("changes", [
    {"assignment_conflict": True},
    {"stops": [stop(assigned_truck_unit="9999")]},
    {"stops": [stop(assigned_truck_unit="8217")]},
    {"stops": [stop(assigned_truck_unit="invalid")]},
])
def test_conflicting_trip_or_required_stop_assignment_never_queries_providers(selected_scope, changes):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([])))
    census = SimpleNamespace(resolve_stop=AsyncMock(return_value=None))
    original = order(**changes)
    result = asyncio.run(StopLocationResolver(client).enrich_order(
        original, street_address_resolver=census))
    assert result == original
    client._get_json.assert_not_awaited()
    census.resolve_stop.assert_not_awaited()


def test_required_stop_unit_alias_matches_the_current_truck(selected_scope):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(
        order(stops=[stop(assigned_truck_unit="08089")])))
    assert result["stops"][0]["coordinate_source"] == "samsara_address"


@pytest.mark.parametrize("changes", [
    {"truck_id": "truck-a", "stops": [stop(assigned_truck_id="truck-b", assigned_truck_unit="8089")]},
    {"stops": [stop(assigned_truck_id="unresolved-truck")]},
    {"assigned_truck_ids": ["truck-a", "truck-b"]},
    {"assigned_truck_ids": "truck-a"},
    {"stops": [stop(assigned_truck_ids=["truck-a", "truck-b"], assigned_truck_unit="8089")]},
    {"stops": [stop(assigned_truck_ids="truck-a", assigned_truck_unit="8089")]},
])
def test_unverified_or_mixed_truck_ids_never_query_location_providers(selected_scope, changes):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([])))
    census = SimpleNamespace(resolve_stop=AsyncMock(return_value=None))
    original = order(**changes)
    assert asyncio.run(StopLocationResolver(client).enrich_order(
        original, street_address_resolver=census)) == original
    client._get_json.assert_not_awaited()
    census.resolve_stop.assert_not_awaited()


def test_known_trip_id_proves_same_required_stop_assignment(selected_scope):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    original = order(truck_id="truck-a", assigned_truck_ids=["truck-a"],
                     stops=[stop(assigned_truck_id="truck-a")])
    result = asyncio.run(StopLocationResolver(client).enrich_order(original))
    assert result["stops"][0]["coordinate_source"] == "samsara_address"


def test_only_phase_required_stop_unit_is_checked_without_rewriting_history(selected_scope):
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    original = order("in_transit", stops=[
        stop(type="pickup", assigned_truck_unit="9999"),
        stop(type="delivery", assigned_truck_unit="8089"),
    ])
    result = asyncio.run(StopLocationResolver(client).enrich_order(original))
    assert result["stops"][0] == original["stops"][0]
    assert result["stops"][1]["coordinate_source"] == "samsara_address"


def test_disappeared_facility_does_not_keep_old_derived_coordinates(selected_scope):
    original = order(stops=[stop(coordinate_source="samsara_address", address_id="old")])
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(original))
    assert result["stops"][0]["coordinate_source"] == "missing"
    assert result["stops"][0]["latitude"] is None
    assert "address_id" not in result["stops"][0]


def test_optional_census_is_never_enabled_by_default_and_saved_facilities_win(selected_scope):
    census = SimpleNamespace(resolve_stop=AsyncMock(return_value=None))
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([facility()])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(order(), street_address_resolver=census))
    assert result["stops"][0]["coordinate_source"] == "samsara_address"
    census.resolve_stop.assert_not_awaited()
    empty = SimpleNamespace(_get_json=AsyncMock(return_value=page([])))
    result = asyncio.run(StopLocationResolver(empty).enrich_order(order()))
    assert result["stops"][0]["coordinate_source"] == "zip_centroid"


def test_census_cannot_override_an_ambiguous_saved_facility(selected_scope):
    census = SimpleNamespace(resolve_stop=AsyncMock(return_value=parse_census_address(stop(), census_payload())))
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([
        facility(), facility(id="conflicting", latitude=41)])))
    result = asyncio.run(StopLocationResolver(client).enrich_order(order(), street_address_resolver=census))
    assert result["stops"][0]["coordinate_source"] == "zip_centroid"
    census.resolve_stop.assert_not_awaited()


def test_census_parses_a_full_address_as_route_estimate_with_correct_axes():
    result = parse_census_address(stop(), census_payload())
    assert (result["latitude"], result["longitude"]) == (40.5, -74.3)
    assert result["coordinate_source"] == "census_address_range"
    assert result["coordinate_verified_for"] == "fuel_route"
    assert result["coordinate_accuracy"] == "address_range_interpolation"
    assert "address_id" not in result and "completed" not in result


@pytest.mark.parametrize("field,value", [
    ("matchedAddress", "101 W EXAMPLE ST, TESTVILLE, NJ 07001"),
    ("matchedAddress", "100 E EXAMPLE ST, TESTVILLE, NJ 07001"),
    ("matchedAddress", "100 W EXAMPLE ST, OTHER CITY, NJ 07001"),
    ("matchedAddress", "100 W EXAMPLE ST, TESTVILLE, NY 07001"),
    ("matchedAddress", "100 W EXAMPLE ST, TESTVILLE, NJ 07002"),
    ("coordinates", {"x": float("nan"), "y": 40.5}),
    ("coordinates", {"x": -74.3, "y": True}),
    ("coordinates", {"x": 10, "y": 50}),
    ("coordinates", {"x": -74.3, "y": 80}),
    ("coordinates", {"x": 0, "y": 0}),
    ("addressComponents", {}),
])
def test_census_partial_mismatched_or_invalid_results_remain_unresolved(field, value):
    assert parse_census_address(stop(), census_payload(**{field: value})) is None


@pytest.mark.parametrize("change", [
    {"streetName": "OTHER"}, {"preDirection": "E"}, {"city": "OTHER"},
    {"state": "NY"}, {"zip": "07002"},
])
def test_census_components_must_confirm_the_full_address(change):
    payload = census_payload()
    payload["result"]["addressMatches"][0]["addressComponents"].update(change)
    assert parse_census_address(stop(), payload) is None


def test_census_missing_or_multiple_matches_are_not_first_match_wins():
    assert parse_census_address(stop(), {"result": {"addressMatches": []}}) is None
    payload = census_payload()
    payload["result"]["addressMatches"].append(deepcopy(payload["result"]["addressMatches"][0]))
    assert parse_census_address(stop(), payload) is None


def test_census_request_uses_official_structured_parameters_timeout_and_bounded_cache():
    response = Mock()
    response.json.return_value = census_payload()
    client = SimpleNamespace(get=AsyncMock(return_value=response))
    async def run():
        resolver = CensusStreetAddressResolver(client, max_cache_entries=1)
        first = await resolver.resolve_stop(stop())
        first["latitude"] = 0
        assert (await resolver.resolve_stop(stop()))["latitude"] == 40.5
        await resolver.resolve_stop(stop(address_line_1="101 West Example Street"))
        assert len(resolver._cache) == 1
        await resolver.resolve_stop(stop())
    asyncio.run(run())
    assert client.get.await_count == 3
    assert client.get.call_args_list[0] == call(CensusStreetAddressResolver.ENDPOINT,
        params={"street": "100 West Example Street", "city": "Testville", "state": "NJ",
                "zip": "07001", "benchmark": "Public_AR_Current", "format": "json"}, timeout=8.0)


def test_invalid_address_never_triggers_public_lookup_and_failures_are_not_cached():
    client = SimpleNamespace(get=AsyncMock(side_effect=RuntimeError("private address URL")))
    async def run():
        resolver = CensusStreetAddressResolver(client)
        assert await resolver.resolve_stop(stop(address_line_1="PO Box 100")) is None
        client.get.assert_not_awaited()
        with pytest.raises(StopLocationError, match="^Street address lookup is unavailable$"):
            await resolver.resolve_stop(stop())
        assert not resolver._cache
    asyncio.run(run())


def test_census_cache_expires_and_empty_matches_are_cached(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("dieselup.core.stop_locations.time.monotonic", lambda: now[0])
    response = Mock()
    response.json.return_value = {"result": {"addressMatches": []}}
    client = SimpleNamespace(get=AsyncMock(return_value=response))
    async def run():
        resolver = CensusStreetAddressResolver(client, cache_seconds=10)
        assert await resolver.resolve_stop(stop()) is None
        assert await resolver.resolve_stop(stop()) is None
        assert client.get.await_count == 1
        now[0] = 111
        assert await resolver.resolve_stop(stop()) is None
        assert client.get.await_count == 2
    asyncio.run(run())


def test_explicit_census_opt_in_only_enriches_required_stops_and_changes_no_evidence(selected_scope):
    census = SimpleNamespace(resolve_stop=AsyncMock(return_value=parse_census_address(stop(), census_payload())))
    client = SimpleNamespace(_get_json=AsyncMock(return_value=page([])))
    original = order("in_transit")
    result = asyncio.run(StopLocationResolver(client).enrich_order(original, street_address_resolver=census))
    assert census.resolve_stop.await_count == 1
    assert result["stops"][1]["coordinate_source"] == "census_address_range"
    assert result["stops"][0] == original["stops"][0]
    assert result["stops"][1]["completed"] is None
