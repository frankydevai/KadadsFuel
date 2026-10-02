"""
Tests for the fueling optimizer (rank_candidates, NoValidStopError, haversine).

Key scenarios covered:
  * Fuel too high (193.6 gal / 220 tank = 88%) → all stops rejected by ceiling → NoValidStopError
  * Fuel too low → all stops below safety floor → NoValidStopError
  * Valid stop selection — cheapest your_price chosen
  * Forward-progress filter — stops behind the truck rejected
  * Delivery-reserve filter — stops that leave insufficient fuel at delivery rejected
  * Brand filter — ONE9 / affiliate stops excluded
  * Both rankings stored (your_price and IFTA)
"""
import asyncio

import pytest
from unittest.mock import patch

from dieselup.core import optimizer
from dieselup.core.optimizer import (
    CandidateStop,
    FuelPlan,
    NoValidStopError,
    RankedStop,
    get_truck_distance,
    haversine_miles,
    is_pilot_flying_j,
    rank_candidates,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stop(
    *,
    site_id: int = 1,
    station_name: str = "Pilot Travel Center",
    city: str = "Trenton",
    state: str = "NJ",
    latitude: float = 40.0,
    longitude: float = -74.5,
    your_price: float = 3.50,
    retail_price: float = 4.00,
) -> CandidateStop:
    return CandidateStop(
        site_id=site_id,
        station_name=station_name,
        address="1 Main St",
        city=city,
        state=state,
        latitude=latitude,
        longitude=longitude,
        your_price=your_price,
        retail_price=retail_price,
    )


def _rank(
    candidates: list[CandidateStop],
    *,
    truck_lat: float = 40.7,
    truck_lng: float = -74.0,
    dest_lat: float = 39.0,
    dest_lng: float = -77.0,
    fuel: float = 90.0,
    mpg: float = 6.5,
) -> FuelPlan:
    return rank_candidates(
        candidates,
        truck_lat=truck_lat,
        truck_lng=truck_lng,
        destination_lat=dest_lat,
        destination_lng=dest_lng,
        current_fuel_gallons=fuel,
        truck_mpg=mpg,
        truck_unit="TEST_TRUCK",
        load_id="TEST_LOAD",
    )


# ---------------------------------------------------------------------------
# is_pilot_flying_j brand filter
# ---------------------------------------------------------------------------

class TestBrandFilter:
    def test_pilot_passes(self):
        assert is_pilot_flying_j("Pilot Travel Center")

    def test_flying_j_passes(self):
        assert is_pilot_flying_j("Flying J Travel Center")

    def test_one9_excluded(self):
        assert not is_pilot_flying_j("ONE9 Travel Center")

    def test_love_excluded(self):
        assert not is_pilot_flying_j("Love's Travel Stop")

    def test_ta_excluded(self):
        assert not is_pilot_flying_j("TA Travel Center")

    def test_case_insensitive(self):
        assert is_pilot_flying_j("PILOT TRAVEL CENTER")
        assert is_pilot_flying_j("flying j")


# ---------------------------------------------------------------------------
# haversine_miles
# ---------------------------------------------------------------------------

class TestHaversine:
    def test_same_point_is_zero(self):
        d = haversine_miles(40.0, -74.0, 40.0, -74.0)
        assert d == pytest.approx(0.0, abs=0.01)

    def test_known_distance_newark_to_philly(self):
        # Newark NJ to Philadelphia PA ≈ 83 miles
        d = haversine_miles(40.7357, -74.1724, 39.9526, -75.1652)
        assert 75 < d < 95

    def test_symmetry(self):
        a = haversine_miles(40.0, -74.0, 39.0, -75.0)
        b = haversine_miles(39.0, -75.0, 40.0, -74.0)
        assert a == pytest.approx(b, rel=1e-6)


def test_get_truck_distance_reads_precomputed_graph(monkeypatch):
    async def fake_fetch_one(_query, *args):
        assert args == (10, 20)
        return {"distance_miles": "17.25"}

    monkeypatch.setattr("dieselup.core.optimizer.fetch_one", fake_fetch_one)

    assert asyncio.run(get_truck_distance(10, 20)) == 17.25


def test_get_truck_distance_falls_back_to_inflated_haversine(monkeypatch):
    async def fake_fetch_one(_query, *_args):
        return None

    monkeypatch.setattr("dieselup.core.optimizer.fetch_one", fake_fetch_one)

    miles = asyncio.run(
        get_truck_distance(
            10,
            20,
            fallback_coords=((40.0, -80.0), (40.0, -80.1)),
        )
    )

    assert miles == pytest.approx(
        haversine_miles(40.0, -80.0, 40.0, -80.1) * 1.15
    )


# ---------------------------------------------------------------------------
# NoValidStopError — the 193.6 gal scenario and related edge cases
# ---------------------------------------------------------------------------

class TestNoValidStopError:
    def test_fuel_too_high_raises_no_valid_stop(self):
        """
        Truck 567667: fuel = 193.6 gal out of 220 → 88% full.
        MAX_ARRIVAL_FUEL_GALLONS = 80 gal.
        Any stop close enough for the truck to arrive still has fuel > 80 → rejected.
        Result: NoValidStopError.
        """
        # Stop 50 miles ahead; at 6.5 mpg the truck burns ~7.7 gal → arrives with 185.9 gal
        # 185.9 > 80 → ceiling rejection
        stop = _stop(latitude=40.3, longitude=-74.5, your_price=3.40)

        with pytest.raises(NoValidStopError) as exc_info:
            _rank([stop], fuel=193.6)

        err = exc_info.value
        assert err.truck_unit == "TEST_TRUCK"
        assert err.load_id == "TEST_LOAD"
        assert err.current_fuel_gallons == pytest.approx(193.6)
        assert err.reason == "arrival_fuel_too_high"

    def test_fuel_too_low_raises_no_valid_stop(self):
        """
        Truck starts with only 15 gal — below SAFETY_FLOOR_GALLONS=20.
        Stop is 100 miles away; truck would arrive with 15 - 100/6.5 ≈ -0.4 gal → rejected.
        """
        stop = _stop(latitude=39.0, longitude=-75.0)  # ~60 miles from truck

        with pytest.raises(NoValidStopError):
            _rank([stop], fuel=10.0)

    def test_empty_candidates_raises_no_valid_stop(self):
        with pytest.raises(NoValidStopError):
            _rank([])

    def test_all_behind_truck_raises_no_valid_stop(self):
        # Stop is NORTH-EAST of the truck while destination is SOUTH-WEST
        # → stop is farther from destination than truck → rejected
        stop = _stop(latitude=41.0, longitude=-73.0)  # NE of truck at 40.7,-74.0
        # destination is SW at 39.0, -77.0

        with pytest.raises(NoValidStopError):
            _rank([stop])

    def test_one9_only_raises_no_valid_stop(self):
        stop = _stop(station_name="ONE9 Travel Center", latitude=40.0, longitude=-75.0)
        with pytest.raises(NoValidStopError):
            _rank([stop])


# ---------------------------------------------------------------------------
# Valid stop selection
# ---------------------------------------------------------------------------

class TestValidStopSelection:
    def test_selects_cheapest_your_price(self):
        cheap = _stop(site_id=1, latitude=40.0, longitude=-75.0, your_price=3.40, station_name="Pilot Travel Center")
        expensive = _stop(site_id=2, latitude=40.1, longitude=-75.1, your_price=3.80, station_name="Pilot Travel Center")

        plan = _rank([cheap, expensive])
        assert plan.selected.site_id == cheap.site_id

    def test_fuel_at_arrival_above_floor(self):
        stop = _stop(latitude=40.0, longitude=-75.0, your_price=3.50)
        plan = _rank([stop])
        assert plan.selected.fuel_at_arrival >= 20.0  # SAFETY_FLOOR_GALLONS

    def test_fuel_at_arrival_below_ceiling(self):
        stop = _stop(latitude=40.0, longitude=-75.0, your_price=3.50)
        plan = _rank([stop])
        assert plan.selected.fuel_at_arrival <= 80.0  # MAX_ARRIVAL_FUEL_GALLONS

    def test_gallons_to_pump_fills_to_tank(self):
        stop = _stop(latitude=40.0, longitude=-75.0, your_price=3.50)
        plan = _rank([stop])
        # gallons_to_pump = int(TANK_CAPACITY - fuel_at_arrival)
        expected = int(220 - plan.selected.fuel_at_arrival)
        assert plan.selected.gallons_to_pump == expected

    def test_mpg_fallback_flag_set_when_no_mpg(self):
        stop = _stop(latitude=40.0, longitude=-75.0)
        plan = _rank([stop], mpg=None)
        assert "mpg_fallback_used" in plan.flags

    def test_no_flag_when_mpg_provided(self):
        stop = _stop(latitude=40.0, longitude=-75.0)
        plan = _rank([stop], mpg=7.0)
        assert "mpg_fallback_used" not in plan.flags

    def test_both_rankings_populated(self):
        s1 = _stop(site_id=1, latitude=40.0, longitude=-75.0, your_price=3.40, station_name="Pilot Travel Center")
        s2 = _stop(site_id=2, latitude=40.1, longitude=-75.1, your_price=3.80, station_name="Flying J Travel Center")
        plan = _rank([s1, s2])
        assert len(plan.ranked_your_price) >= 1
        assert len(plan.ranked_ifta) >= 1

    def test_worst_candidate_true_cost_is_highest(self):
        s1 = _stop(site_id=1, latitude=40.0, longitude=-75.0, your_price=3.40, state="NJ", station_name="Pilot Travel Center")
        s2 = _stop(site_id=2, latitude=40.1, longitude=-75.1, your_price=3.80, state="NJ", station_name="Flying J Travel Center")
        plan = _rank([s1, s2])
        assert plan.worst_candidate_true_cost >= plan.selected.true_cost_per_gallon

    def test_savings_per_gallon_is_retail_minus_your_price(self):
        stop = _stop(latitude=40.0, longitude=-75.0, your_price=3.50, retail_price=4.10)
        plan = _rank([stop])
        assert plan.selected.savings_per_gallon == pytest.approx(4.10 - 3.50, abs=0.01)

    def test_projected_tank_at_delivery_above_reserve(self):
        stop = _stop(latitude=40.0, longitude=-75.0)
        plan = _rank([stop])
        # DELIVERY_RESERVE_PCT = 20 → 220 * 0.20 = 44 gal reserve
        assert plan.selected.projected_tank_at_delivery >= 44.0

    def test_forward_progress_required(self):
        """A stop that is farther from destination than the truck is must be rejected."""
        # truck at 40.7,-74.0, dest at 39.0,-77.0
        # stop at 40.9,-73.0 → farther from dest than truck → rejected
        behind = _stop(latitude=40.9, longitude=-73.0, station_name="Pilot Travel Center")
        ahead = _stop(site_id=2, latitude=40.0, longitude=-75.0, station_name="Pilot Travel Center")

        plan = _rank([behind, ahead])
        selected_ids = {s.site_id for s in plan.ranked_your_price}
        assert behind.site_id not in selected_ids
        assert ahead.site_id in selected_ids


def test_corridor_candidates_join_locations_by_pilot_site_id(monkeypatch):
    captured = {}

    async def fake_freshness(*_args):
        return {"effective_date": "2026-09-27", "age_days": 0}

    async def fake_fetch_all(query, *args):
        captured["query"] = query
        return []

    monkeypatch.setattr(optimizer, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(optimizer, "fetch_one", fake_freshness)
    result = asyncio.run(
        optimizer.fetch_corridor_candidates(
            truck_lat=32.2,
            truck_lng=-101.6,
            destination_lat=31.8,
            destination_lng=-106.5,
        )
    )

    assert result == []
    normalized = " ".join(captured["query"].split()).lower()
    assert "join fuel_stops fs on fs.pilot_site_id = cp.site_id" in normalized
    assert "lower(cp.city) = lower(fs.city)" not in normalized
