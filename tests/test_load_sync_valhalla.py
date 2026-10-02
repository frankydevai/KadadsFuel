import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.routing import RoutingError
from dieselup.core import load_sync
from dieselup.core.lane_plan import BuyLeg, LaneBuyPlan
from dieselup.core.optimizer import CandidateStop
from dieselup.core.fuel_plan import NoFeasibleFuelPlan


def context():
    order = {
        "id": "L1",
        "truck_unit_number": "100",
        "stops": [
            {
                "id": "past",
                "type": "pickup",
                "completed": True,
                "latitude": 35,
                "longitude": -90,
            },
            {
                "id": "next",
                "type": "delivery",
                "completed": False,
                "latitude": 39,
                "longitude": -85,
            },
        ],
    }
    return load_sync._load_context(order)


def stats(**changes):
    values = dict(
        lat=37,
        lng=-85,
        heading=None,
        speed_mph=0,
        fuel_gallons=120,
        mpg_rolling=6.5,
        gps_age_minutes=1,
        fuel_age_minutes=1,
    )
    values.update(changes)
    return SimpleNamespace(**values)


def test_first_plan_and_replan_both_use_current_truck_and_only_remaining_stops(
    monkeypatch,
):
    stop = CandidateStop(1, "Pilot", None, "X", "KY", 38, -85, 3.5, 4)
    planner = AsyncMock(
        return_value=LaneBuyPlan(
            [BuyLeg(stop, 80, 42, 0, 3.5)],
            3.5,
            route_evidence={"model": "remaining_route_v1"},
        )
    )
    monkeypatch.setattr(load_sync, "plan_remaining_route", planner)
    for first in (False, True):
        result = asyncio.run(
            load_sync._build_routed_leg(
                ctx=context(), stats=stats(), is_first_plan=first
            )
        )
        call = planner.call_args.kwargs
        assert (call["stats"].lat, call["stats"].lng) == (37, -85)
        assert [s["id"] for s in call["waypoints"]] == ["next"]
        assert json.loads(result["candidates_json"])[0]["distance_miles"] == 42


@pytest.mark.parametrize("age", [None, -1, 6, 120])
def test_stale_or_missing_gps_blocks_routing(monkeypatch, age):
    planner = AsyncMock()
    monkeypatch.setattr(load_sync, "plan_remaining_route", planner)
    with pytest.raises(load_sync.LoadContextError, match="Fresh truck GPS"):
        asyncio.run(
            load_sync._build_routed_leg(ctx=context(), stats=stats(gps_age_minutes=age))
        )
    planner.assert_not_called()


def test_missing_progress_blocks_instead_of_guessing_pickup_was_passed(monkeypatch):
    ctx = context()
    ctx["order"]["stops"][0].pop("completed")
    with pytest.raises(load_sync.LoadContextError, match="progress is missing"):
        asyncio.run(load_sync._build_routed_leg(ctx=ctx, stats=stats()))


def test_routing_failure_never_falls_back_to_geometric_advice(monkeypatch):
    monkeypatch.setattr(
        load_sync,
        "plan_remaining_route",
        AsyncMock(side_effect=RoutingError("private server failed")),
    )
    with pytest.raises(RoutingError):
        asyncio.run(load_sync._build_routed_leg(ctx=context(), stats=stats()))


def test_degraded_empty_plan_remains_blocked(monkeypatch):
    monkeypatch.setattr(
        load_sync,
        "plan_remaining_route",
        AsyncMock(return_value=LaneBuyPlan([], 0, "reduced_reserve_15")),
    )
    with pytest.raises(NoFeasibleFuelPlan):
        asyncio.run(load_sync._build_routed_leg(ctx=context(), stats=stats()))


@pytest.mark.parametrize("age", [None, -1, 31, 100])
def test_stale_fuel_is_a_hold_even_with_fresh_gps(monkeypatch, age):
    planner = AsyncMock()
    monkeypatch.setattr(load_sync, "plan_remaining_route", planner)
    with pytest.raises(load_sync.LoadContextError, match="Fresh truck fuel"):
        asyncio.run(
            load_sync._build_routed_leg(
                ctx=context(), stats=stats(fuel_age_minutes=age)
            )
        )
    planner.assert_not_called()
