"""External street lookup stays opt-in and separate from general fleet scope."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.quickmanage import QuickManageClient
from dieselup.config import settings


def order(unit="6682", phase="pickup_then_delivery"):
    return {"truck_unit_number": unit, "route_phase": phase, "stops": [
        {"type": "pickup", "coordinate_source": "zip_centroid",
         "address_line_1": "100 Example Road"},
        {"type": "delivery", "coordinate_source": "zip_centroid",
         "address_line_1": "200 Example Road"},
    ]}


def test_census_disabled_never_creates_or_supplies_public_lookup(monkeypatch):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "6682,8089,8217")
    monkeypatch.setattr(settings, "CENSUS_GEOCODING_ENABLED", False)
    def forbidden():
        pytest.fail("Disabled public geocoder was instantiated")
    monkeypatch.setattr("dieselup.core.stop_locations.CensusStreetAddressResolver", forbidden)
    async def run():
        async with QuickManageClient() as client:
            client._stop_locations = resolver = AsyncMock()
            original = order()
            resolver.enrich_order.return_value = original
            assert await client._resolve_stop_locations(original) is original
            resolver.enrich_order.assert_awaited_once_with(original, street_address_resolver=None)
    asyncio.run(run())


def test_public_lookup_scope_stays_three_trucks_even_when_general_scope_broadens(monkeypatch):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "")
    monkeypatch.setattr(settings, "CENSUS_GEOCODING_ENABLED", True)
    monkeypatch.setattr(settings, "CENSUS_GEOCODING_TRUCK_UNITS", "6682,8089,8217")
    def forbidden():
        pytest.fail("Unapproved truck would send an address to Census")
    monkeypatch.setattr("dieselup.core.stop_locations.CensusStreetAddressResolver", forbidden)
    async def run():
        async with QuickManageClient() as client:
            client._stop_locations = resolver = AsyncMock()
            original = order("9999")
            resolver.enrich_order.return_value = original
            await client._resolve_stop_locations(original)
            resolver.enrich_order.assert_awaited_once_with(original, street_address_resolver=None)
    asyncio.run(run())


@pytest.mark.parametrize("original", [order("9999"), order(phase="reserved"),
                                      {**order(), "assignment_conflict": True}])
def test_out_of_scope_and_reserved_orders_never_lookup_locations(monkeypatch, original):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "6682,8089,8217")
    monkeypatch.setattr(settings, "CENSUS_GEOCODING_ENABLED", True)
    def forbidden(*args):
        pytest.fail("Non-current or unauthorized trip queried location services")
    monkeypatch.setattr("dieselup.core.stop_locations.StopLocationResolver", forbidden)
    async def run():
        async with QuickManageClient() as client:
            assert await client._resolve_stop_locations(original) is original
    asyncio.run(run())


def test_in_transit_does_not_lookup_only_missing_pickup(monkeypatch):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "6682,8089,8217")
    monkeypatch.setattr(settings, "CENSUS_GEOCODING_ENABLED", True)
    original = order(phase="delivery_only")
    original["stops"][1]["coordinate_source"] = "exact"
    def forbidden(*args):
        pytest.fail("Historical pickup was sent for geocoding")
    monkeypatch.setattr("dieselup.core.stop_locations.StopLocationResolver", forbidden)
    async def run():
        async with QuickManageClient() as client:
            assert await client._resolve_stop_locations(original) is original
    asyncio.run(run())
