"""
Lane fuel-plan orchestration: candidates + truck routing -> fuel buy plan.

Glue between the price/candidate layer, TomTom/Valhalla/ORS road mileage, and
the bounded road-fuel search. This is the only place the three meet:

  1. The selected truck-routing engine gives the road distances needed to
     locate each stop on the shipper→delivery lane.
  2. route_position filters/orders candidates; directed matrix edges retain
     the actual fuel burn between pumps. Rank by the configured pump/IFTA cost.
  3. plan_road_fuel finds a feasible, low-cost purchase plan over those stops.
     Bucket pruning makes this approximate; legacy geometric callers retain
     the older distance-projection planner, but the live bot uses Valhalla only.

Kept thin and side-effect-free apart from the optional routing-engine call.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

from dieselup.clients.routing import OpenRouteServiceClient, RoutingError
from dieselup.clients.tomtom import TomTomClient
from dieselup.clients.valhalla import ValhallaClient
from dieselup.config import settings
from dieselup.core.fuel_plan import NoFeasibleFuelPlan, Stop, plan_fuel, route_position
from dieselup.core.road_fuel_plan import plan_road_fuel
from dieselup.core.ifta import true_cost_per_gallon
from dieselup.core.optimizer import CandidateStop, haversine_miles

log = logging.getLogger(__name__)

_EPS = 1e-9

# Geometric (no-routing-API) lane planning. Straight-line distances are
# inflated by the standard road factor; a stop's lane position comes from
# bearing projection: along-track = progress down the shipper→delivery axis,
# cross-track = one-way off-route distance (used as the detour estimate).
# Same approach proven in the sister project's route_planner.py — zero
# external calls, zero quota, zero routing errors.
GEOMETRIC_ROAD_FACTOR = 1.15
GEOMETRIC_CORRIDOR_MILES = settings.MAX_STOP_DETOUR_MILES


@dataclass(frozen=True)
class BuyLeg:
    """One purchase in the plan.

    distance_from_truck_mi is the stop's road mileage ahead of the truck (the
    planning origin), used for the briefing. detour_miles and net_price are
    carried so the whole plan — routing + IFTA economics — can be persisted to
    stop_events.candidates for audit, not just recomputed and discarded.
    """
    candidate: CandidateStop
    gallons: float
    distance_from_truck_mi: float
    detour_miles: float
    net_price: float
    fill_to_full: bool = False


@dataclass(frozen=True)
class LaneBuyPlan:
    """Result of routing + DP. `worst_true_cost` is the max IFTA true cost among
    the reachable forward candidates — compliance needs it for the saved-dollar
    formula (worst - recommended) * gallons.

    `degraded` is None for a strict plan, otherwise names the relaxation rung
    that produced it ('reduced_reserve_15', 'emergency_reserve_10',
    'partial_lane_best_reachable') so briefings and admin can flag it.
    """
    legs: list[BuyLeg]
    worst_true_cost: float
    degraded: str | None = None
    route_evidence: dict = field(default_factory=dict)


def _run_dp_with_relaxation(
    stops: list[Stop],
    *,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None,
    road_miles: list[list[float | None]] | None = None,
) -> tuple[list[tuple[Stop, float]], str | None]:
    """plan_fuel with a progressive relaxation ladder — never dead-ends while
    ANY contracted stop ahead is physically reachable.

    Rungs:
      1. strict (caller's reserves)            → degraded=None
      2. keep reserves, relax purchase floor  → 'purchase_floor_relaxed'
      3. reserve 15 gal everywhere             → 'reduced_reserve_15'
      4. reserve 10 gal, no min purchase       → 'emergency_reserve_10'
      5. plan to the cheapest REACHABLE stop,
         fill the tank there (delivery handled
         by the next replan from that point)   → 'partial_lane_best_reachable'
    Raises NoFeasibleFuelPlan only when no stop ahead is reachable at all —
    the truck genuinely needs dispatch intervention.
    """
    bridge_min = (
        min_purchase_gal
        if bridge_min_purchase_gal is None
        else max(min_purchase_gal, bridge_min_purchase_gal)
    )
    ladder: list[tuple[str | None, float, float, float, float]] = [
        (None, reserve_gal, terminal_reserve_gal
         if terminal_reserve_gal is not None else reserve_gal,
         min_purchase_gal, bridge_min),
        # A small fill is preferable to lowering the safety reserve solely
        # to satisfy a commercial minimum purchase size.
        ("purchase_floor_relaxed", reserve_gal, terminal_reserve_gal
         if terminal_reserve_gal is not None else reserve_gal, 0.0, 0.0),
        ("reduced_reserve_15", 15.0, 15.0,
         min_purchase_gal, bridge_min),
        ("emergency_reserve_10", 10.0, 10.0, 0.0, 0.0),
    ]
    for label, rung_reserve, rung_terminal, rung_min, rung_bridge_min in ladder:
        if start_fuel_gal < rung_reserve:
            continue  # plan_fuel would reject the seed outright
        try:
            planner = plan_fuel if road_miles is None else plan_road_fuel
            plan = planner(
                stops,
                tank_capacity_gal=tank_capacity_gal,
                start_fuel_gal=start_fuel_gal,
                reserve_gal=rung_reserve,
                mpg=mpg,
                cost_per_mile=cost_per_mile,
                stop_time_penalty=stop_time_penalty,
                min_purchase_gal=rung_min,
                bridge_min_purchase_gal=rung_bridge_min,
                terminal_reserve_gal=rung_terminal,
                **({"road_miles": road_miles} if road_miles is not None else {}),
            )
            return plan, label
        except NoFeasibleFuelPlan:
            continue

    # Last rung: delivery is unreachable even at emergency reserve — bridge as far
    # as possible. Pick the cheapest stop the truck can physically reach with
    # >= 10 gal on arrival (counting its out-and-back detour burn) and fill the
    # tank; the sequential replan continues the lane from there.
    fuel_stops = stops[:-1]  # last element is the delivery sentinel
    reachable: list[tuple[Stop, float]] = []
    for index, s in enumerate(fuel_stops, start=1):
        # In routed mode the incoming road leg ends at the pump, so its fuel
        # is burned BEFORE buying. Never subtract an invented return detour.
        distance = road_miles[0][index] if road_miles is not None else s.mile_marker + s.detour_miles * 2.0
        if distance is None:
            continue
        arrival = start_fuel_gal - distance / mpg
        if arrival >= 10.0:
            reachable.append((s, arrival))
    if not reachable:
        raise NoFeasibleFuelPlan(
            "no contracted stop ahead is reachable with >= 10 gal on arrival"
        )
    best, arrival = min(reachable, key=lambda t: t[0].net_price)
    gallons = max(tank_capacity_gal - arrival, 0.0)
    if gallons <= 0.0:
        raise NoFeasibleFuelPlan("reachable stop but tank already full — nothing to plan")
    return [(best, gallons)], "partial_lane_best_reachable"


async def build_buy_plan(
    *,
    candidates: list[CandidateStop],
    shipper_lat: float,
    shipper_lng: float,
    destination_lat: float,
    destination_lng: float,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float = 0.0,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None = None,
    router: OpenRouteServiceClient | None = None,
) -> LaneBuyPlan:
    """Cheapest fuel buy plan for the lane.

    `router` is injectable for tests; when None we open (and close) our own ORS
    client. Raises RoutingError on routing failure and
    fuel_plan.NoFeasibleFuelPlan when delivery is unreachable under reserve.
    """
    if not candidates:
        return LaneBuyPlan(legs=[], worst_true_cost=0.0)

    points = [
        (shipper_lat, shipper_lng),
        (destination_lat, destination_lng),
        *[(c.latitude, c.longitude) for c in candidates],
    ]

    own_router = router is None
    rt = router or OpenRouteServiceClient()
    try:
        matrix = await rt.distance_matrix_miles(points)
    finally:
        if own_router:
            await rt.close()

    return _build_buy_plan_from_matrix(
        candidates=candidates,
        matrix=matrix,
        engine_name="ORS",
        tank_capacity_gal=tank_capacity_gal,
        start_fuel_gal=start_fuel_gal,
        reserve_gal=reserve_gal,
        mpg=mpg,
        cost_per_mile=cost_per_mile,
        stop_time_penalty=stop_time_penalty,
        min_purchase_gal=min_purchase_gal,
        bridge_min_purchase_gal=(
            min_purchase_gal
            if bridge_min_purchase_gal is None
            else bridge_min_purchase_gal
        ),
        terminal_reserve_gal=terminal_reserve_gal,
    )


async def build_buy_plan_valhalla(
    *,
    candidates: list[CandidateStop],
    shipper_lat: float,
    shipper_lng: float,
    destination_lat: float,
    destination_lng: float,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float = 0.0,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None = None,
    router: ValhallaClient | None = None,
) -> LaneBuyPlan:
    """Cheapest fuel buy plan using self-hosted Valhalla truck road miles."""
    points = [
        (shipper_lat, shipper_lng),
        (destination_lat, destination_lng),
        *[(c.latitude, c.longitude) for c in candidates],
    ]

    own_router = router is None
    rt = router or ValhallaClient()
    try:
        matrix = await rt.distance_matrix_miles(points)
    finally:
        if own_router:
            await rt.close()
    if matrix is None:
        raise RoutingError("Valhalla matrix failed")

    return _build_buy_plan_from_matrix(
        candidates=candidates,
        matrix=matrix,
        engine_name="Valhalla",
        tank_capacity_gal=tank_capacity_gal,
        start_fuel_gal=start_fuel_gal,
        reserve_gal=reserve_gal,
        mpg=mpg,
        cost_per_mile=cost_per_mile,
        stop_time_penalty=stop_time_penalty,
        min_purchase_gal=min_purchase_gal,
        bridge_min_purchase_gal=(
            min_purchase_gal
            if bridge_min_purchase_gal is None
            else bridge_min_purchase_gal
        ),
        terminal_reserve_gal=terminal_reserve_gal,
    )


async def build_buy_plan_tomtom(
    *,
    candidates: list[CandidateStop],
    shipper_lat: float,
    shipper_lng: float,
    destination_lat: float,
    destination_lng: float,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float = 0.0,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None = None,
    router: TomTomClient | None = None,
) -> LaneBuyPlan:
    """Cheapest fuel buy plan using TomTom truck road miles."""
    if not candidates:
        return LaneBuyPlan(legs=[], worst_true_cost=0.0)

    points = [
        (shipper_lat, shipper_lng),
        (destination_lat, destination_lng),
        *[(c.latitude, c.longitude) for c in candidates],
    ]

    own_router = router is None
    rt = router or TomTomClient()
    try:
        matrix = await rt.distance_matrix_miles(points)
    finally:
        if own_router:
            await rt.close()

    return _build_buy_plan_from_matrix(
        candidates=candidates,
        matrix=matrix,
        engine_name="TomTom",
        tank_capacity_gal=tank_capacity_gal,
        start_fuel_gal=start_fuel_gal,
        reserve_gal=reserve_gal,
        mpg=mpg,
        cost_per_mile=cost_per_mile,
        stop_time_penalty=stop_time_penalty,
        min_purchase_gal=min_purchase_gal,
        bridge_min_purchase_gal=(
            min_purchase_gal
            if bridge_min_purchase_gal is None
            else bridge_min_purchase_gal
        ),
        terminal_reserve_gal=terminal_reserve_gal,
    )


def _build_buy_plan_from_matrix(
    *,
    candidates: list[CandidateStop],
    matrix: list[list[float | None]],
    engine_name: str,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float,
    bridge_min_purchase_gal: float,
    terminal_reserve_gal: float | None,
) -> LaneBuyPlan:
    """Convert a truck-road distance matrix into DP stops, then run fuel math."""
    expected = len(candidates) + 2
    if len(matrix) != expected or any(len(row) != expected for row in matrix):
        raise RoutingError(f"{engine_name} matrix response was incomplete")
    if any(v is not None and (not math.isfinite(v) or v < 0)
           for row in matrix for v in row):
        raise RoutingError(f"{engine_name} matrix contains invalid road distances")

    d_shipper_delivery = matrix[0][1]
    if d_shipper_delivery is None:
        raise RoutingError(f"{engine_name} could not route shipper -> delivery")

    indexed_stops: list[tuple[Stop, int]] = []
    for offset, c in enumerate(candidates, start=2):
        if offset >= len(matrix) or offset >= len(matrix[0]):
            log.warning(
                "lane_plan: %s matrix missing stop site_id=%s",
                engine_name,
                c.site_id,
            )
            continue
        d_shipper_stop = matrix[0][offset]
        d_stop_delivery = matrix[offset][1]
        if d_shipper_stop is None or d_stop_delivery is None:
            # Stop is unroutable for a truck — skip rather than guess.
            log.warning(
                "lane_plan: %s dropping unroutable stop site_id=%s",
                engine_name,
                c.site_id,
            )
            continue
        # Forward-progress guard: the stop must be closer to delivery by road
        # than the truck's current position. Mirrors the legacy rank_candidates
        # check (straight_dist_to_dest >= truck_to_dest → reject).
        #
        # The route_position mile_marker formula alone is insufficient: ORS road
        # distances are not perfectly additive, so a stop that is geographically
        # *behind* the truck (e.g. north when the truck is heading south KY→GA)
        # can still yield a positive mile_marker if ORS finds a more direct
        # highway from that stop to the destination that bypasses the truck's
        # current position. This explicit distance check closes that gap.
        if d_stop_delivery >= d_shipper_delivery:
            log.warning(
                "lane_plan: %s dropping behind-truck stop site_id=%s "
                "d_stop_delivery=%.1f mi >= d_shipper_delivery=%.1f mi",
                engine_name,
                c.site_id,
                d_stop_delivery,
                d_shipper_delivery,
            )
            continue
        mile_marker, detour_miles = route_position(
            d_shipper_delivery, d_shipper_stop, d_stop_delivery
        )
        if detour_miles > settings.MAX_STOP_DETOUR_MILES:
            log.warning(
                "lane_plan: %s dropping off-route stop site_id=%s "
                "detour=%.1f mi > max=%.1f mi",
                engine_name,
                c.site_id,
                detour_miles,
                settings.MAX_STOP_DETOUR_MILES,
            )
            continue
        if mile_marker <= _EPS or mile_marker >= d_shipper_delivery - _EPS:
            log.warning(
                "lane_plan: %s dropping out-of-lane stop site_id=%s mile_marker=%.1f delivery=%.1f",
                engine_name,
                c.site_id,
                mile_marker,
                d_shipper_delivery,
            )
            continue
        indexed_stops.append((
            Stop(
                mile_marker=mile_marker,
                net_price=(c.your_price if settings.RANK_STRATEGY == "your_price"
                           else true_cost_per_gallon(c.your_price, c.state)),
                detour_miles=detour_miles,
                ref=c,
            ), offset,
        ))

    indexed_stops.sort(key=lambda item: (item[0].mile_marker, item[0].ref.site_id))
    stops = [s for s, _ in indexed_stops]
    matrix_indices = [0, *[index for _, index in indexed_stops], 1]
    road_miles = [[matrix[i][j] for j in matrix_indices] for i in matrix_indices]

    # Delivery sentinel: never bought at; plan_fuel only needs its mile_marker.
    stops.append(
        Stop(mile_marker=d_shipper_delivery, net_price=0.0, detour_miles=0.0, ref=None)
    )

    plan, degraded = _run_dp_with_relaxation(
        stops,
        tank_capacity_gal=tank_capacity_gal,
        start_fuel_gal=start_fuel_gal,
        reserve_gal=reserve_gal,
        mpg=mpg,
        cost_per_mile=cost_per_mile,
        stop_time_penalty=stop_time_penalty,
        min_purchase_gal=min_purchase_gal,
        bridge_min_purchase_gal=bridge_min_purchase_gal,
        terminal_reserve_gal=terminal_reserve_gal,
        road_miles=road_miles,
    )

    # Worst reachable forward true cost (delivery sentinel has ref=None) — the
    # baseline compliance compares the chosen stop against for saved dollars.
    worst_true_cost = max(
        (true_cost_per_gallon(s.ref.your_price, s.ref.state)
         for s in stops if s.ref is not None), default=0.0
    )
    road_index = {id(s): index for index, s in enumerate(stops, start=1)}
    # Report distance along the selected itinerary, including previous stops.
    cumulative_miles = 0.0
    previous_index = 0
    distances: dict[int, float] = {}
    full_fills: dict[int, bool] = {}
    fuel = start_fuel_gal
    for stop, gallons in plan:
        current_index = road_index[id(stop)]
        leg_miles = road_miles[previous_index][current_index]
        cumulative_miles += leg_miles
        fuel = fuel - leg_miles / mpg + gallons
        full_fills[id(stop)] = abs(fuel - tank_capacity_gal) <= 1e-7
        distances[id(stop)] = cumulative_miles
        previous_index = current_index
    legs = [
        BuyLeg(
            candidate=stop.ref,
            gallons=gallons,
            distance_from_truck_mi=distances[id(stop)],
            detour_miles=stop.detour_miles,
            net_price=true_cost_per_gallon(stop.ref.your_price, stop.ref.state),
            fill_to_full=full_fills[id(stop)],
        )
        for stop, gallons in plan
    ]
    return LaneBuyPlan(legs=legs, worst_true_cost=worst_true_cost, degraded=degraded)


def _bearing_degrees(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Initial great-circle bearing from point 1 to point 2 (0-360)."""
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlng = math.radians(lng2 - lng1)
    x = math.sin(dlng) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlng)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _angle_diff(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return d if d <= 180.0 else 360.0 - d


def build_buy_plan_geometric(
    *,
    candidates: list[CandidateStop],
    shipper_lat: float,
    shipper_lng: float,
    destination_lat: float,
    destination_lng: float,
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float = 0.0,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None = None,
) -> LaneBuyPlan:
    """Same min-cost DP lane plan as build_buy_plan, but with NO routing API.

    Stop positions come from pure geometry (haversine + bearing projection):
      along  = distance from shipper projected onto the lane axis (→ mile_marker
               after the ×1.15 road factor)
      cross  = perpendicular off-route distance (→ one-way detour estimate)
    Stops behind the shipper, beyond the delivery, more than 90° off the lane
    axis, outside the corridor, or not making forward progress are dropped —
    the same filters the road-routed path enforces with real distances.

    Less precise than ORS road miles (~15% straight-line inflation, no
    mountain/river awareness) but it cannot fail: no key, no quota, no HTTP.
    Used automatically when ORS is unconfigured or errors out, so the fleet
    NEVER degrades to single-stop nearby planning.
    """
    total_straight = haversine_miles(
        shipper_lat, shipper_lng, destination_lat, destination_lng
    )
    if not candidates or total_straight <= 1.0:
        return LaneBuyPlan(legs=[], worst_true_cost=0.0)

    lane_bearing = _bearing_degrees(
        shipper_lat, shipper_lng, destination_lat, destination_lng
    )

    stops: list[Stop] = []
    for c in candidates:
        d_shipper_stop = haversine_miles(shipper_lat, shipper_lng, c.latitude, c.longitude)
        if d_shipper_stop <= _EPS:
            continue
        adiff = _angle_diff(
            lane_bearing,
            _bearing_degrees(shipper_lat, shipper_lng, c.latitude, c.longitude),
        )
        if adiff > 90.0:
            continue  # behind the lane axis
        along = d_shipper_stop * math.cos(math.radians(adiff))
        cross = abs(d_shipper_stop * math.sin(math.radians(adiff)))
        if along <= _EPS or along >= total_straight - _EPS:
            continue  # outside the shipper→delivery span
        if cross > GEOMETRIC_CORRIDOR_MILES:
            continue  # too far off the lane
        # Forward progress: stop must be closer to delivery than the shipper is.
        if haversine_miles(c.latitude, c.longitude, destination_lat, destination_lng) >= total_straight:
            continue
        stops.append(
            Stop(
                mile_marker=along * GEOMETRIC_ROAD_FACTOR,
                net_price=true_cost_per_gallon(c.your_price, c.state),
                detour_miles=cross,
                ref=c,
            )
        )

    if not stops:
        return LaneBuyPlan(legs=[], worst_true_cost=0.0)

    delivery_mile = total_straight * GEOMETRIC_ROAD_FACTOR
    stops.append(Stop(mile_marker=delivery_mile, net_price=0.0, detour_miles=0.0, ref=None))

    plan, degraded = _run_dp_with_relaxation(
        stops,
        tank_capacity_gal=tank_capacity_gal,
        start_fuel_gal=start_fuel_gal,
        reserve_gal=reserve_gal,
        mpg=mpg,
        cost_per_mile=cost_per_mile,
        stop_time_penalty=stop_time_penalty,
        min_purchase_gal=min_purchase_gal,
        bridge_min_purchase_gal=(
            min_purchase_gal
            if bridge_min_purchase_gal is None
            else bridge_min_purchase_gal
        ),
        terminal_reserve_gal=terminal_reserve_gal,
    )

    worst_true_cost = max(
        (s.net_price for s in stops if s.ref is not None), default=0.0
    )
    legs = [
        BuyLeg(
            candidate=stop.ref,
            gallons=gallons,
            distance_from_truck_mi=stop.mile_marker,
            detour_miles=stop.detour_miles,
            net_price=stop.net_price,
        )
        for stop, gallons in plan
    ]
    return LaneBuyPlan(legs=legs, worst_true_cost=worst_true_cost, degraded=degraded)
