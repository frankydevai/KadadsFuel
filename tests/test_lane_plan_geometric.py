"""Tests for the geometric and Valhalla-backed lane planners."""
import asyncio

import pytest

from dieselup.core.fuel_plan import NoFeasibleFuelPlan, Stop
from dieselup.core.lane_plan import (
    GEOMETRIC_CORRIDOR_MILES,
    build_buy_plan_geometric,
    build_buy_plan_tomtom,
    build_buy_plan_valhalla,
)
from dieselup.core.optimizer import CandidateStop


def _cand(site_id, lat, lng, price=3.50, state="PA"):
    return CandidateStop(
        site_id=site_id,
        station_name=f"Pilot {site_id}",
        address=None,
        city="X",
        state=state,
        latitude=lat,
        longitude=lng,
        your_price=price,
        retail_price=price + 0.40,
    )


# Roughly NJ → Chicago: heading WNW. 1° longitude ≈ 53 mi at 40°N.
SHIPPER = (40.0, -74.5)
DELIVERY = (41.8, -87.6)

ON_ROUTE_MID = _cand(1, 41.08, -81.0, price=3.30)      # near the great-circle lane midline
BEHIND = _cand(2, 39.5, -70.0, price=2.90)             # east of shipper (behind)
FAR_OFF = _cand(3, 33.0, -81.0, price=2.90)            # ~500 mi south of lane
PAST_DELIVERY = _cand(4, 42.5, -93.0, price=2.90)      # beyond Chicago


def _plan(candidates, start_fuel=120.0):
    return build_buy_plan_geometric(
        candidates=candidates,
        shipper_lat=SHIPPER[0],
        shipper_lng=SHIPPER[1],
        destination_lat=DELIVERY[0],
        destination_lng=DELIVERY[1],
        tank_capacity_gal=200.0,
        start_fuel_gal=start_fuel,
        reserve_gal=30.0,
        mpg=6.5,
        cost_per_mile=0.55,
        stop_time_penalty=20.0,
        min_purchase_gal=30.0,
        terminal_reserve_gal=20.0,
    )


def test_plans_buy_at_on_route_stop():
    lane = _plan([ON_ROUTE_MID])
    assert len(lane.legs) == 1
    assert lane.legs[0].candidate.site_id == 1
    assert lane.legs[0].gallons > 0
    # mile marker is within the lane span (×1.15 road factor applied)
    assert 0 < lane.legs[0].distance_from_truck_mi < 800 * 1.15


def test_behind_and_off_lane_and_past_delivery_stops_dropped():
    lane = _plan([ON_ROUTE_MID, BEHIND, FAR_OFF, PAST_DELIVERY])
    used_ids = {leg.candidate.site_id for leg in lane.legs}
    assert used_ids == {1}  # cheaper but invalid stops never chosen


def test_geometric_fallback_drops_far_detour_stop_on_long_az_to_ny_lane():
    # Reproduces the BigRig screenshot class: a cheap stop near Cumberland Gap
    # can be "ahead" of an AZ→NY lane but is hundreds of miles off the real
    # interstate corridor. In fallback geometry mode, it must be dropped rather
    # than creating a bogus STOP 12/12 recommendation.
    off_route_ky = _cand(321, 36.61, -83.72, price=2.70, state="KY")
    lane = build_buy_plan_geometric(
        candidates=[off_route_ky],
        shipper_lat=33.45,
        shipper_lng=-112.259,
        destination_lat=43.22,
        destination_lng=-78.386,
        tank_capacity_gal=200.0,
        start_fuel_gal=134.0,
        reserve_gal=30.0,
        mpg=6.5,
        cost_per_mile=0.55,
        stop_time_penalty=20.0,
        min_purchase_gal=50.0,
        terminal_reserve_gal=20.0,
    )
    assert GEOMETRIC_CORRIDOR_MILES <= 35.0
    assert lane.legs == []


def test_no_candidates_returns_empty_plan():
    lane = _plan([])
    assert lane.legs == []
    assert lane.worst_true_cost == 0.0


def test_no_buy_when_tank_covers_lane():
    # Full tank: 200 gal × 6.5 mpg = 1300 mi range > ~770 mi lane
    lane = _plan([ON_ROUTE_MID], start_fuel=200.0)
    assert lane.legs == []


def test_valhalla_matrix_feeds_same_dp_planner():
    class FakeValhalla:
        def __init__(self):
            self.points = None

        async def distance_matrix_miles(self, points):
            self.points = points
            # [shipper, delivery, stop]. Stop is exactly on-route at mile 200.
            return [
                [0.0, 500.0, 200.0],
                [500.0, 0.0, 300.0],
                [200.0, 300.0, 0.0],
            ]

    router = FakeValhalla()
    lane = asyncio.run(
        build_buy_plan_valhalla(
            candidates=[ON_ROUTE_MID],
            shipper_lat=SHIPPER[0],
            shipper_lng=SHIPPER[1],
            destination_lat=DELIVERY[0],
            destination_lng=DELIVERY[1],
            tank_capacity_gal=200.0,
            start_fuel_gal=70.0,
            reserve_gal=30.0,
            mpg=6.5,
            cost_per_mile=0.55,
            stop_time_penalty=20.0,
            min_purchase_gal=30.0,
            terminal_reserve_gal=20.0,
            router=router,
        )
    )

    assert router.points == [SHIPPER, DELIVERY, (ON_ROUTE_MID.latitude, ON_ROUTE_MID.longitude)]
    assert len(lane.legs) == 1
    assert lane.legs[0].candidate.site_id == ON_ROUTE_MID.site_id
    assert lane.legs[0].distance_from_truck_mi == 200.0


def test_valhalla_no_candidates_still_checks_delivery_reachability():
    class FakeValhalla:
        async def distance_matrix_miles(self, points):
            assert points == [SHIPPER, DELIVERY]
            return [[0.0, 500.0], [500.0, 0.0]]

    with pytest.raises(NoFeasibleFuelPlan):
        asyncio.run(build_buy_plan_valhalla(
            candidates=[],
            shipper_lat=SHIPPER[0],
            shipper_lng=SHIPPER[1],
            destination_lat=DELIVERY[0],
            destination_lng=DELIVERY[1],
            tank_capacity_gal=200.0,
            start_fuel_gal=40.0,
            reserve_gal=30.0,
            mpg=6.5,
            cost_per_mile=0.55,
            stop_time_penalty=20.0,
            min_purchase_gal=50.0,
            terminal_reserve_gal=20.0,
            router=FakeValhalla(),
        ))


def test_tomtom_matrix_feeds_same_dp_planner():
    class FakeTomTom:
        def __init__(self):
            self.points = None

        async def distance_matrix_miles(self, points):
            self.points = points
            return [
                [0.0, 500.0, 200.0],
                [500.0, 0.0, 300.0],
                [200.0, 300.0, 0.0],
            ]

    router = FakeTomTom()
    lane = asyncio.run(
        build_buy_plan_tomtom(
            candidates=[ON_ROUTE_MID],
            shipper_lat=SHIPPER[0],
            shipper_lng=SHIPPER[1],
            destination_lat=DELIVERY[0],
            destination_lng=DELIVERY[1],
            tank_capacity_gal=200.0,
            start_fuel_gal=70.0,
            reserve_gal=30.0,
            mpg=6.5,
            cost_per_mile=0.55,
            stop_time_penalty=20.0,
            min_purchase_gal=30.0,
            terminal_reserve_gal=20.0,
            router=router,
        )
    )

    assert router.points == [SHIPPER, DELIVERY, (ON_ROUTE_MID.latitude, ON_ROUTE_MID.longitude)]
    assert len(lane.legs) == 1
    assert lane.legs[0].candidate.site_id == ON_ROUTE_MID.site_id
    assert lane.legs[0].distance_from_truck_mi == 200.0


def test_valhalla_matrix_drops_high_detour_stop():
    class FakeValhalla:
        async def distance_matrix_miles(self, points):
            # [shipper, delivery, valid_stop, off_route_stop].
            # The off-route stop projects into the lane at mile 200 but adds
            # 90 round-trip miles, i.e. 45 one-way detour. It is cheaper, so
            # without the hard detour guard the optimizer would choose it.
            return [
                [0.0, 500.0, 200.0, 245.0],
                [500.0, 0.0, 300.0, 345.0],
                [200.0, 300.0, 0.0, 90.0],
                [245.0, 345.0, 90.0, 0.0],
            ]

    valid = _cand(100, 40.5, -80.0, price=3.60)
    off_route_cheaper = _cand(101, 39.5, -80.0, price=2.00)
    lane = asyncio.run(
        build_buy_plan_valhalla(
            candidates=[valid, off_route_cheaper],
            shipper_lat=SHIPPER[0],
            shipper_lng=SHIPPER[1],
            destination_lat=DELIVERY[0],
            destination_lng=DELIVERY[1],
            tank_capacity_gal=200.0,
            start_fuel_gal=70.0,
            reserve_gal=30.0,
            mpg=6.5,
            cost_per_mile=0.55,
            stop_time_penalty=20.0,
            min_purchase_gal=50.0,
            terminal_reserve_gal=20.0,
            router=FakeValhalla(),
        )
    )

    assert len(lane.legs) == 1
    assert lane.legs[0].candidate.site_id == valid.site_id


def test_valhalla_matrix_drops_stop_more_than_ten_miles_off_route():
    class FakeValhalla:
        async def distance_matrix_miles(self, points):
            # Valid stop is exactly on route. The cheaper candidate adds 24
            # round-trip miles, i.e. a 12-mile one-way route deviation.
            return [
                [0.0, 500.0, 200.0, 212.0],
                [500.0, 0.0, 300.0, 312.0],
                [200.0, 300.0, 0.0, 24.0],
                [212.0, 312.0, 24.0, 0.0],
            ]

    valid = _cand(110, 40.5, -80.0, price=3.60)
    off_route_cheaper = _cand(111, 39.5, -80.0, price=2.00)
    lane = asyncio.run(
        build_buy_plan_valhalla(
            candidates=[valid, off_route_cheaper],
            shipper_lat=SHIPPER[0],
            shipper_lng=SHIPPER[1],
            destination_lat=DELIVERY[0],
            destination_lng=DELIVERY[1],
            tank_capacity_gal=200.0,
            start_fuel_gal=70.0,
            reserve_gal=30.0,
            mpg=6.5,
            cost_per_mile=0.55,
            stop_time_penalty=20.0,
            min_purchase_gal=90.0,
            terminal_reserve_gal=20.0,
            router=FakeValhalla(),
        )
    )

    assert len(lane.legs) == 1
    assert lane.legs[0].candidate.site_id == valid.site_id


# ---------------------------------------------------------------------------
# Relaxation ladder — never dead-end while a stop ahead is reachable
# ---------------------------------------------------------------------------

from dieselup.core.lane_plan import _run_dp_with_relaxation


def _run(stops, start_fuel, reserve=30.0, terminal=20.0, min_purchase=30.0):
    return _run_dp_with_relaxation(
        stops,
        tank_capacity_gal=200.0,
        start_fuel_gal=start_fuel,
        reserve_gal=reserve,
        mpg=6.5,
        cost_per_mile=0.55,
        stop_time_penalty=20.0,
        min_purchase_gal=min_purchase,
        terminal_reserve_gal=terminal,
    )


def test_strict_plan_not_degraded():
    stops = [Stop(300.0, 3.50, 0.0), Stop(900.0, 0.0, 0.0)]  # last = delivery
    plan, degraded = _run(stops, start_fuel=100.0)
    assert degraded is None
    assert plan


def test_reserve_relaxation_used_when_strict_infeasible():
    # Stop at mile 500: strict needs 30 gal on arrival → 500/6.5 + 30 ≈ 107 gal.
    # Start with 95: strict fails, 15-gal rung succeeds (500/6.5+15 ≈ 92).
    stops = [Stop(500.0, 3.50, 0.0), Stop(1200.0, 0.0, 0.0)]
    plan, degraded = _run(stops, start_fuel=95.0)
    assert degraded in ("reduced_reserve_15", "emergency_reserve_10")
    assert plan and plan[0][0].mile_marker == 500.0


def test_partial_lane_when_delivery_unreachable():
    # Delivery 2500 mi out — beyond full-tank range from the only stop at 300.
    # Full lane infeasible at every reserve rung → fill full at the reachable
    # stop and let the next replan continue.
    stops = [Stop(300.0, 3.50, 0.0), Stop(2500.0, 0.0, 0.0)]
    plan, degraded = _run(stops, start_fuel=100.0)
    assert degraded == "partial_lane_best_reachable"
    assert len(plan) == 1
    stop, gallons = plan[0]
    assert stop.mile_marker == 300.0
    assert gallons > 0


def test_partial_lane_picks_cheapest_reachable():
    stops = [
        Stop(200.0, 4.20, 0.0),
        Stop(350.0, 3.10, 0.0),   # cheaper and still reachable with 100 gal
        Stop(2500.0, 0.0, 0.0),
    ]
    plan, degraded = _run(stops, start_fuel=100.0)
    assert degraded == "partial_lane_best_reachable"
    assert plan[0][0].mile_marker == 350.0


def test_raises_only_when_nothing_reachable():
    # Only stop is 800 mi out; 50 gal ≈ 325 mi range → truly unreachable.
    stops = [Stop(800.0, 3.50, 0.0), Stop(2500.0, 0.0, 0.0)]
    with pytest.raises(NoFeasibleFuelPlan):
        _run(stops, start_fuel=50.0)
