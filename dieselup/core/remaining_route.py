"""Fuel advice along the verified remaining truck route, with mandatory stops."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any

from dieselup.clients.routing import RoutingError
from dieselup.clients.valhalla import ValhallaClient
from dieselup.config import settings
from dieselup.core.fuel_plan import Stop
from dieselup.core.ifta import true_cost_per_gallon
from dieselup.core.lane_plan import BuyLeg, LaneBuyPlan
from dieselup.core.optimizer import (
    CandidateStop,
    fetch_corridor_candidates,
    haversine_miles,
)
from dieselup.core.road_fuel_plan import plan_road_fuel


def decode_shape(encoded: str) -> list[tuple[float, float]]:
    """Valhalla JSON shapes use encoded polyline with six-digit precision."""
    if not isinstance(encoded, str) or not encoded:
        raise RoutingError("Valhalla route has no geometry")
    values = []
    index = 0
    lat = lon = 0
    try:
        while index < len(encoded):
            deltas = []
            for _ in range(2):
                value = shift = 0
                while True:
                    byte = ord(encoded[index]) - 63
                    index += 1
                    if byte < 0 or byte > 63 or shift > 30:
                        raise ValueError
                    value |= (byte & 31) << shift
                    shift += 5
                    if byte < 32:
                        break
                deltas.append(~(value >> 1) if value & 1 else value >> 1)
            lat += deltas[0]
            lon += deltas[1]
            point = (lat / 1e6, lon / 1e6)
            if not -90 <= point[0] <= 90 or not -180 <= point[1] <= 180:
                raise ValueError
            values.append(point)
    except (IndexError, ValueError) as exc:
        raise RoutingError("Valhalla route geometry is invalid") from exc
    if len(values) < 2 or len(set(values)) < 2:
        raise RoutingError("Valhalla route geometry is incomplete")
    return values


@dataclass(frozen=True)
class RouteLeg:
    shape: list[tuple[float, float]]
    road_miles: float

    def project(self, point: tuple[float, float]) -> tuple[float, float, bool]:
        """(progress, distance from route, outside endpoint). Geometry only
        selects/order stations; all fuel burn uses directed road distances.
        """
        best = None
        walked = 0.0
        for index, (a, b) in enumerate(zip(self.shape, self.shape[1:])):
            length = haversine_miles(*a, *b)
            if length <= 1e-9:
                continue
            scale = math.cos(math.radians((a[0] + b[0]) / 2))
            dx, dy = (b[1] - a[1]) * scale, b[0] - a[0]
            px, py = (point[1] - a[1]) * scale, point[0] - a[0]
            t = (px * dx + py * dy) / (dx * dx + dy * dy)
            clipped = min(1.0, max(0.0, t))
            q = (a[0] + clipped * (b[0] - a[0]), a[1] + clipped * (b[1] - a[1]))
            distance = haversine_miles(*point, *q)
            progress = walked + clipped * length
            outside = (index == 0 and t < 0) or (index == len(self.shape) - 2 and t > 1)
            # Stops whose closest point is the departure endpoint are not
            # ahead, unless the truck is already at that station.
            outside = outside or (progress <= 1e-6 and distance > 0.05)
            value = (distance, progress, outside)
            if best is None or value[:2] < best[:2]:
                best = value
            walked += length
        if best is None:
            raise RoutingError("Valhalla route geometry has no usable segments")
        return best[1], best[0], best[2]


async def remaining_geometry(
    router: ValhallaClient, stats: Any, waypoints: list[dict]
) -> list[RouteLeg]:
    start = {"lat": stats.lat, "lon": stats.lng, "type": "break"}
    heading = getattr(stats, "heading", None)
    if (
        heading is not None
        and (getattr(stats, "speed_mph", None) or 0) >= settings.DRIVER_REST_SPEED_MPH
    ):
        start.update(heading=heading, heading_tolerance=60)
    points = [
        (stats.lat, stats.lng),
        *[(s["latitude"], s["longitude"]) for s in waypoints],
    ]
    data = await router.route(
        [start, *[{"lat": p[0], "lon": p[1], "type": "break"} for p in points[1:]]]
    )
    trip = data.get("trip") if isinstance(data, dict) else None
    raw = trip.get("legs") if isinstance(trip, dict) else None
    if not isinstance(raw, list) or len(raw) != len(waypoints):
        raise RoutingError("Valhalla did not route every remaining required stop")
    legs = []
    for i, leg in enumerate(raw):
        shape = decode_shape(leg.get("shape"))
        try:
            miles = float(leg["summary"]["length"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RoutingError("Valhalla route length is missing") from exc
        if not math.isfinite(miles) or miles < 0:
            raise RoutingError("Valhalla route length is invalid")
        if (
            haversine_miles(*points[i], *shape[0]) > 0.5
            or haversine_miles(*points[i + 1], *shape[-1]) > 0.5
        ):
            raise RoutingError(
                "Valhalla route endpoints are too far from verified locations"
            )
        legs.append(RouteLeg(shape, miles))
    return legs


def place_candidates(
    candidates: list[CandidateStop], legs: list[RouteLeg]
) -> list[list[tuple[CandidateStop, float]]]:
    groups: list[list[tuple[CandidateStop, float]]] = [[] for _ in legs]
    for candidate in candidates:
        projections = [
            (leg.project((candidate.latitude, candidate.longitude)), index)
            for index, leg in enumerate(legs)
        ]
        (progress, distance, outside), index = min(
            projections, key=lambda item: (item[0][1], item[1])
        )
        if outside or distance > settings.MAX_STOP_DETOUR_MILES:
            continue
        groups[index].append((candidate, progress))
    for group in groups:
        group.sort(key=lambda item: (item[1], item[0].site_id))
    return groups


async def plan_remaining_route(
    *, stats: Any, waypoints: list[dict], mpg: float, router=None
) -> LaneBuyPlan:
    rt = router or ValhallaClient()
    own = router is None
    try:
        legs = await remaining_geometry(rt, stats, waypoints)
        shape = [p for leg in legs for p in leg.shape]
        candidates = await fetch_corridor_candidates(
            truck_lat=min(p[0] for p in shape),
            truck_lng=min(p[1] for p in shape),
            destination_lat=max(p[0] for p in shape),
            destination_lng=max(p[1] for p in shape),
        )
        groups = place_candidates(candidates, legs)
        stops: list[Stop] = []
        local_matrices = []
        offset = 0.0
        node = 0
        for index, (leg, group) in enumerate(zip(legs, groups)):
            origin = (
                (stats.lat, stats.lng)
                if index == 0
                else (
                    waypoints[index - 1]["latitude"],
                    waypoints[index - 1]["longitude"],
                )
            )
            target = (waypoints[index]["latitude"], waypoints[index]["longitude"])
            raw = await rt.distance_matrix_miles_batched(
                [origin, target, *[(c.latitude, c.longitude) for c, _ in group]]
            )
            if raw[0][1] is None:
                raise RoutingError("A required remaining road leg is unreachable")
            selected = []
            for pos, (candidate, progress) in enumerate(group, start=2):
                incoming, outgoing = raw[0][pos], raw[pos][1]
                if incoming is None or outgoing is None:
                    continue
                detour = max(0.0, incoming + outgoing - raw[0][1]) / 2
                if detour > settings.MAX_STOP_DETOUR_MILES:
                    continue
                selected.append(pos)
                price = (
                    candidate.your_price
                    if settings.RANK_STRATEGY == "your_price"
                    else true_cost_per_gallon(candidate.your_price, candidate.state)
                )
                stops.append(Stop(offset + progress, price, detour, candidate))
            # Geometry lengths set forward ordering only. Matrix edges are
            # the sole fuel-distance source; every required customer remains.
            shape_length = sum(
                haversine_miles(*a, *b) for a, b in zip(leg.shape, leg.shape[1:])
            )
            offset += shape_length
            stops.append(Stop(offset, 0, 0, None, required=True))
            ids = [0, *selected, 1]
            nodes = list(range(node, node + len(ids)))
            local_matrices.append((nodes, [[raw[i][j] for j in ids] for i in ids]))
            node += len(ids) - 1
        size = len(stops) + 1
        matrix = [[None] * size for _ in range(size)]
        for nodes, local in local_matrices:
            for i, source in enumerate(nodes):
                for j, target in enumerate(nodes):
                    matrix[source][target] = local[i][j]
        buys = plan_road_fuel(
            stops,
            road_miles=matrix,
            tank_capacity_gal=settings.TANK_CAPACITY_GALLONS,
            start_fuel_gal=stats.fuel_gallons,
            reserve_gal=settings.SAFETY_FLOOR_GALLONS,
            mpg=mpg,
            cost_per_mile=settings.COST_PER_MILE,
            stop_time_penalty=settings.STOP_TIME_PENALTY,
            min_purchase_gal=settings.MIN_FUEL_PURCHASE_GALLONS,
            bridge_min_purchase_gal=settings.BRIDGE_MIN_FUEL_PURCHASE_GALLONS,
            terminal_reserve_gal=settings.TANK_CAPACITY_GALLONS
            * settings.DELIVERY_RESERVE_PCT
            / 100,
        )
        indices = {id(s): i for i, s in enumerate(stops, start=1)}
        purchases = {indices[id(s)]: gallons for s, gallons in buys}
        visited = sorted(
            set(purchases) | {i for i, s in enumerate(stops, start=1) if s.required}
        )
        model_fuel = stats.fuel_gallons
        previous_node = 0
        full_purchases = set()
        for index in visited:
            model_fuel -= matrix[previous_node][index] / mpg
            if index in purchases:
                model_fuel += purchases[index]
                if abs(model_fuel - settings.TANK_CAPACITY_GALLONS) < 1e-7:
                    full_purchases.add(index)
            previous_node = index
        itinerary = []
        for index in visited:
            stop = stops[index - 1]
            if stop.required:
                itinerary.append(waypoints[sum(s.required for s in stops[:index]) - 1])
            else:
                itinerary.append(
                    {"latitude": stop.ref.latitude, "longitude": stop.ref.longitude}
                )
        # Verify the actual ordered instruction route, including the moving
        # truck's departure heading. Matrix shortcuts cannot certify fuel.
        driven = await remaining_geometry(rt, stats, itinerary)
        if buys:
            first_pump = indices[id(buys[0][0])]
            approach = driven[visited.index(first_pump)]
            for point in approach.shape:
                progress, distance, outside = legs[0].project(point)
                if outside and progress <= 1e-6 and distance > 0.1:
                    raise RoutingError(
                        "Fuel-stop approach backtracks behind the current truck"
                    )
        result = []
        fuel = stats.fuel_gallons
        miles = 0.0
        for index, driven_leg in zip(visited, driven):
            distance = driven_leg.road_miles
            miles += distance
            fuel -= distance / mpg
            floor = (
                settings.TANK_CAPACITY_GALLONS * settings.DELIVERY_RESERVE_PCT / 100
                if index == size - 1
                else settings.SAFETY_FLOOR_GALLONS
            )
            if fuel < floor - 1e-7:
                raise RoutingError(
                    "Actual instruction route would violate the fuel reserve"
                )
            if index in purchases:
                stop = stops[index - 1]
                # Preserve a fill-to-full instruction when the final route
                # differs from the matrix. Fractional tank space must never
                # become a rounded partial-purchase instruction.
                gallons = settings.TANK_CAPACITY_GALLONS - fuel if index in full_purchases else purchases[index]
                following = visited[visited.index(index) + 1]
                floor = settings.MIN_FUEL_PURCHASE_GALLONS
                if following != size-1 and not stops[following-1].required:
                    floor = max(floor,settings.BRIDGE_MIN_FUEL_PURCHASE_GALLONS)
                if gallons < floor - 1e-7:
                    raise RoutingError("Actual instruction route would violate the purchase floor")
                fuel += gallons
                if fuel > settings.TANK_CAPACITY_GALLONS + 1e-7:
                    raise RoutingError(
                        "Actual instruction route would overfill the tank"
                    )
                result.append(
                    BuyLeg(
                        stop.ref,
                        gallons,
                        miles,
                        stop.detour_miles,
                        true_cost_per_gallon(stop.ref.your_price, stop.ref.state),
                        index in full_purchases,
                    )
                )
        evidence = {
            "model": "remaining_route_v1",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "remaining_stops": waypoints,
            "truck_latitude": stats.lat,
            "truck_longitude": stats.lng,
            "contracted_candidates_scanned": len(candidates),
            "on_route_candidates": sum(map(len, groups)),
            "routed_candidates": sum(not s.required for s in stops),
            "complete_candidate_coverage": True,
            "route_sha256": hashlib.sha256(json.dumps(shape).encode()).hexdigest(),
        }
        if buys:
            # Keep the verified driven route, including customers, to monitor
            # passage by road progress instead of compass/longitude guesses.
            monitor_shape = []
            first_progress = None
            walked = 0.0
            first_pump = indices[id(buys[0][0])]
            for index, driven_leg in zip(visited, driven):
                monitor_shape.extend(driven_leg.shape if not monitor_shape else driven_leg.shape[1:])
                walked += sum(haversine_miles(*a, *b) for a, b in zip(driven_leg.shape, driven_leg.shape[1:]))
                if index == first_pump:
                    first_progress = walked
            evidence["monitor"] = {"shape": monitor_shape, "first_fuel_progress_miles": first_progress}
        return LaneBuyPlan(
            result,
            max(
                (
                    true_cost_per_gallon(s.ref.your_price, s.ref.state)
                    for s in stops
                    if s.ref
                ),
                default=0,
            ),
            route_evidence=evidence,
        )
    finally:
        if own:
            await rt.close()
