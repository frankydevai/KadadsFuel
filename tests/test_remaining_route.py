import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.routing import RoutingError
from dieselup.clients.valhalla import ValhallaClient
from dieselup.config import settings
from dieselup.core import remaining_route as route
from dieselup.core.fuel_plan import NoFeasibleFuelPlan, Stop
from dieselup.core.optimizer import CandidateStop
from dieselup.core.road_fuel_plan import plan_road_fuel


def encode(points):
    result = []
    previous = (0, 0)
    for point in points:
        values = tuple(round(v * 1e6) for v in point)
        for value, old in zip(values, previous):
            delta = value - old
            delta = ~(delta << 1) if delta < 0 else delta << 1
            while delta >= 32:
                result.append(chr((32 | (delta & 31)) + 63))
                delta >>= 5
            result.append(chr(delta + 63))
        previous = values
    return "".join(result)


def candidate(site, lon, price=4, lat=40):
    return CandidateStop(
        site, "Pilot", None, "Test", "NJ", lat, lon, price, price + 0.5
    )


class RoadRouter:
    def __init__(self):
        self.locations = []
        self.routes = []
        self.matrices = []

    async def route(self, locations):
        self.locations = locations
        self.routes.append(locations)
        points = [(p["lat"], p["lon"]) for p in locations]
        return {
            "trip": {
                "legs": [
                    {
                        "shape": encode([a, b]),
                        "summary": {"length": self.distance(a, b)},
                    }
                    for a, b in zip(points, points[1:])
                ]
            }
        }

    @staticmethod
    def distance(a, b):
        return (abs(a[0] - b[0]) + abs(a[1] - b[1])) * 100

    async def distance_matrix_miles_batched(self, points):
        self.matrices.append(points)
        return [[self.distance(a, b) for b in points] for a in points]


@pytest.fixture
def policy(monkeypatch):
    for key, value in {
        "TANK_CAPACITY_GALLONS": 100,
        "SAFETY_FLOOR_GALLONS": 10,
        "DELIVERY_RESERVE_PCT": 20,
        "MIN_FUEL_PURCHASE_GALLONS": 5,
        "BRIDGE_MIN_FUEL_PURCHASE_GALLONS": 5,
    }.items():
        monkeypatch.setattr(settings, key, value)


@pytest.mark.parametrize(
    "heading,speed,start_fuel", [(0, 60, 30), (None, 60, 30), (0, 0, 30), (None, 0, 15)]
)
def test_passed_cheaper_stop_is_never_selected_even_parked_or_critical(
    monkeypatch, policy, heading, speed, start_fuel
):
    behind = candidate(1, -100.05, 1)
    ahead = candidate(2, -99.96, 4)
    monkeypatch.setattr(
        route, "fetch_corridor_candidates", AsyncMock(return_value=[behind, ahead])
    )
    router = RoadRouter()
    stats = SimpleNamespace(
        lat=40, lng=-100, heading=heading, speed_mph=speed, fuel_gallons=start_fuel
    )
    lane = asyncio.run(
        route.plan_remaining_route(
            stats=stats,
            waypoints=[{"latitude": 40, "longitude": -99.2}],
            mpg=1,
            router=router,
        )
    )
    assert [leg.candidate.site_id for leg in lane.legs] == [2]
    assert all(
        (behind.latitude, behind.longitude) not in points for points in router.matrices
    )
    assert lane.legs[0].distance_from_truck_mi == pytest.approx(4)


def test_route_bend_accepts_station_ahead_on_road_even_behind_compass():
    # Northbound first, then east, then south: downstream station is south
    # of the truck but has not been passed on this remaining road route.
    leg = route.RouteLeg([(40, -100), (41, -100), (41, -99), (39, -99)], 400)
    station = candidate(1, -99, lat=39.5)
    assert route.place_candidates([station], [leg])[0][0][0] == station


@pytest.mark.parametrize("delta,full", [(0.75,True),(-0.75,True),(0.75,False)])
def test_full_tank_instruction_uses_actual_arrival_space_partial_remains_whole_gallons(monkeypatch,policy,delta,full):
    monkeypatch.setattr(route,"fetch_corridor_candidates",AsyncMock(return_value=[candidate(1,-99.936)]))
    monkeypatch.setattr(route,"plan_road_fuel",lambda stops,**kwargs:[(stops[0],76.4 if full else 50)])
    class ChangedApproach(RoadRouter):
        async def route(self, locations):
            result=await super().route(locations)
            if len(self.routes)>1:
                result["trip"]["legs"][0]["summary"]["length"]+=delta
            return result
    stats=SimpleNamespace(lat=40,lng=-100,heading=None,speed_mph=0,fuel_gallons=30)
    lane=asyncio.run(route.plan_remaining_route(stats=stats,waypoints=[{"latitude":40,"longitude":-99.7}],mpg=1,router=ChangedApproach()))
    assert lane.legs[0].fill_to_full is full
    assert lane.legs[0].gallons == pytest.approx(76.4+delta if full else 50)


def test_mandatory_pickup_spur_is_routed_before_later_fuel_stop(monkeypatch, policy):
    a, b = candidate(1, -100, lat=40.2), candidate(2, -99.2, lat=41)
    monkeypatch.setattr(
        route, "fetch_corridor_candidates", AsyncMock(return_value=[a, b])
    )
    router = RoadRouter()
    stats = SimpleNamespace(
        lat=40, lng=-100, heading=None, speed_mph=0, fuel_gallons=40
    )
    waypoints = [
        {"id": "pickup", "latitude": 41, "longitude": -100},
        {"id": "delivery", "latitude": 41, "longitude": -99},
    ]
    lane = asyncio.run(
        route.plan_remaining_route(
            stats=stats, waypoints=waypoints, mpg=3, router=router
        )
    )
    assert [(p["lat"], p["lon"]) for p in router.routes[0]] == [
        (40, -100),
        (41, -100),
        (41, -99),
    ]
    assert [(p["lat"], p["lon"]) for p in router.routes[1]] == [
        (40, -100),
        (40.2, -100),
        (41, -100),
        (41, -99),
    ]
    assert [x.candidate.site_id for x in lane.legs] == [1]
    assert lane.legs[0].distance_from_truck_mi == pytest.approx(20)
    # Independent fuel replay must include the 100-mile pickup leg.
    assert 40 - 200 / 3 + lane.legs[0].gallons >= 20
    assert lane.route_evidence["remaining_stops"] == waypoints


def test_shortcut_cannot_skip_required_customer_even_without_fuel_stops(
    monkeypatch, policy
):
    monkeypatch.setattr(route, "fetch_corridor_candidates", AsyncMock(return_value=[]))
    stats = SimpleNamespace(
        lat=40, lng=-100, heading=None, speed_mph=0, fuel_gallons=60
    )
    # Direct delivery only 10 mi, but required pickup makes the trip 210 mi.
    with pytest.raises(NoFeasibleFuelPlan):
        asyncio.run(
            route.plan_remaining_route(
                stats=stats,
                waypoints=[
                    {"latitude": 41, "longitude": -100},
                    {"latitude": 40, "longitude": -99.9},
                ],
                mpg=2,
                router=RoadRouter(),
            )
        )


def test_required_customer_node_does_not_sell_free_fuel():
    stops = [Stop(100, 0, 0, required=True), Stop(200, 0, 0, required=True)]
    with pytest.raises(NoFeasibleFuelPlan):
        plan_road_fuel(
            stops,
            road_miles=[[0, 100, 1], [100, 0, 100], [1, 100, 0]],
            tank_capacity_gal=100,
            start_fuel_gal=100,
            reserve_gal=10,
            mpg=1,
            cost_per_mile=0,
            stop_time_penalty=0,
            terminal_reserve_gal=10,
        )


def test_every_candidate_survives_request_limit_without_price_shortlisting(
    monkeypatch, policy
):
    stations = [candidate(i, -100 + i * 0.01, 5 if i < 55 else 1) for i in range(1, 61)]
    monkeypatch.setattr(
        route, "fetch_corridor_candidates", AsyncMock(return_value=stations)
    )
    monkeypatch.setattr(settings, "VALHALLA_MAX_MATRIX_LOCATIONS", 4)
    router = RoadRouter()
    stats = SimpleNamespace(
        lat=40, lng=-100, heading=None, speed_mph=0, fuel_gallons=80
    )
    lane = asyncio.run(
        route.plan_remaining_route(
            stats=stats,
            waypoints=[{"latitude": 40, "longitude": -99}],
            mpg=1,
            router=router,
        )
    )
    assert lane.route_evidence["routed_candidates"] == 60
    assert len(router.matrices[0]) == 62
    assert lane.legs[0].candidate.site_id >= 55


def test_batched_matrix_checks_all_directed_cells_and_preserves_null(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_MAX_MATRIX_LOCATIONS", 4)

    async def run():
        client = ValhallaClient()
        calls = []

        async def actor(method, payload):
            assert method == "matrix"
            assert len(payload["sources"]) + len(payload["targets"]) <= 4
            calls.append(payload)
            return {
                "sources_to_targets": [
                    [
                        {
                            "distance": None
                            if s["lat"] == 2 and t["lat"] == 4
                            else s["lat"] * 10 + t["lat"]
                        }
                        for t in payload["targets"]
                    ]
                    for s in payload["sources"]
                ]
            }

        client._actor_call = actor
        result = await client.distance_matrix_miles_batched([(i, 0) for i in range(7)])
        assert result[2][4] is None
        assert result[6][1] == 61 and result[1][6] == 16
        assert sum(len(c["sources"]) * len(c["targets"]) for c in calls) == 49

    asyncio.run(run())


def test_missing_matrix_block_rejects_entire_plan(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_MAX_MATRIX_LOCATIONS", 4)

    async def run():
        client = ValhallaClient()
        client._actor_call = AsyncMock(return_value={"sources_to_targets": []})
        with pytest.raises(RoutingError, match="incomplete"):
            await client.distance_matrix_miles_batched([(i, 0) for i in range(8)])

    asyncio.run(run())


@pytest.mark.parametrize("shape", ["", "?", "????", "~"])
def test_invalid_route_shape_blocks(shape):
    with pytest.raises(RoutingError):
        route.decode_shape(shape)


def test_actual_approach_that_backtracks_behind_truck_is_held(monkeypatch, policy):
    monkeypatch.setattr(
        route,
        "fetch_corridor_candidates",
        AsyncMock(return_value=[candidate(1, -99.96)]),
    )

    class BacktrackingRouter(RoadRouter):
        async def route(self, locations):
            response = await super().route(locations)
            if len(self.routes) == 2:
                target = (locations[1]["lat"], locations[1]["lon"])
                response["trip"]["legs"][0]["shape"] = encode(
                    [(40, -100), (40, -100.1), target]
                )
            return response

    with pytest.raises(RoutingError, match="backtracks"):
        asyncio.run(
            route.plan_remaining_route(
                stats=SimpleNamespace(
                    lat=40, lng=-100, heading=90, speed_mph=60, fuel_gallons=30
                ),
                waypoints=[{"latitude": 40, "longitude": -99.2}],
                mpg=1,
                router=BacktrackingRouter(),
            )
        )


def test_actual_road_route_longer_than_matrix_cannot_violate_reserve(
    monkeypatch, policy
):
    monkeypatch.setattr(route, "fetch_corridor_candidates", AsyncMock(return_value=[]))

    class LongerRouter(RoadRouter):
        async def route(self, locations):
            result = await super().route(locations)
            result["trip"]["legs"][0]["summary"]["length"] = 150
            return result

    with pytest.raises(RoutingError, match="violate the fuel reserve"):
        asyncio.run(
            route.plan_remaining_route(
                stats=SimpleNamespace(
                    lat=40, lng=-100, heading=None, speed_mph=0, fuel_gallons=80
                ),
                waypoints=[{"latitude": 40, "longitude": -99.9}],
                mpg=1,
                router=LongerRouter(),
            )
        )
