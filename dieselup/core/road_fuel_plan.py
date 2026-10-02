"""Bounded fuel-cost search over directed, truck-routed road legs.

Stops are supplied in forward lane order; the final node is delivery. The
matrix order is [truck, *stops]. Missing edges stay unreachable. Fuel buckets
bound the search, but each label retains its EXACT physical fuel and immutable
parent, so pruning can affect cost optimality, never the reserve guarantee.
The result is an approximation over the supplied candidates, not a claim of
global optimality over the entire road network.
"""
from __future__ import annotations

from dataclasses import dataclass
import math

from dieselup.core.fuel_plan import BUCKET_GAL, NoFeasibleFuelPlan, Stop

_EPS = 1e-9


@dataclass(frozen=True)
class _Label:
    node: int
    fuel: float
    cost: float
    parent: _Label | None = None
    purchase: float = 0.0  # bought at parent.node


def plan_road_fuel(
    stops: list[Stop],
    *,
    road_miles: list[list[float | None]],
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,
    mpg: float,
    cost_per_mile: float,
    stop_time_penalty: float,
    min_purchase_gal: float = 0.0,
    bridge_min_purchase_gal: float | None = None,
    terminal_reserve_gal: float | None = None,
) -> list[tuple[Stop, float]]:
    """Return physically feasible purchases using actual directed road miles.

    Positive purchases are required at visited fuel nodes. Skipping a station
    uses a direct matrix edge, never a chain through unreported waypoints.
    Stop detours are already included in the road legs and are not burned twice.
    """
    terminal_reserve = reserve_gal if terminal_reserve_gal is None else terminal_reserve_gal
    bridge_floor = max(min_purchase_gal, bridge_min_purchase_gal or 0.0)
    values = [tank_capacity_gal, start_fuel_gal, reserve_gal, terminal_reserve,
              mpg, cost_per_mile, stop_time_penalty, min_purchase_gal, bridge_floor]
    if any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("fuel planning inputs must be finite and non-negative")
    if mpg <= 0 or tank_capacity_gal <= reserve_gal or terminal_reserve > tank_capacity_gal:
        raise ValueError("invalid MPG, tank capacity or reserve")
    if start_fuel_gal > tank_capacity_gal + _EPS:
        raise ValueError("start fuel exceeds tank capacity")
    if not stops:
        raise ValueError("stops must include delivery")
    if any(not math.isfinite(s.mile_marker) or s.mile_marker < 0 for s in stops):
        raise ValueError("stop positions must be finite and non-negative")
    if any(a.mile_marker > b.mile_marker for a, b in zip(stops, stops[1:])):
        raise ValueError("stops and matrix must be in forward lane order")
    if any(not math.isfinite(s.net_price) or s.net_price < 0 for s in stops[:-1]):
        raise ValueError("prices must be finite and non-negative")
    n = len(stops) + 1
    if len(road_miles) != n or any(len(row) != n for row in road_miles):
        raise ValueError("road matrix must include truck, stops and delivery")
    if any(v is not None and (not math.isfinite(v) or v < 0)
           for row in road_miles for v in row):
        raise ValueError("road distances must be finite and non-negative")

    terminal = n - 1
    required = {i for i, stop in enumerate(stops, start=1) if stop.required} | {terminal}
    labels: list[dict[int, _Label]] = [{} for _ in range(n)]
    labels[0][0] = _Label(0, start_fuel_gal, 0.0)

    def floor_at(node: int) -> float:
        return terminal_reserve if node == terminal else reserve_gal

    for i in range(terminal):
        next_required = min(j for j in required if j > i)
        edges = [(j, road_miles[i][j] / mpg) for j in range(i + 1, next_required + 1)
                 if road_miles[i][j] is not None
                 and road_miles[i][j] / mpg + floor_at(j) <= tank_capacity_gal + _EPS]
        for label in list(labels[i].values()):
            can_buy = i > 0 and i not in required
            departures = {tank_capacity_gal} if can_buy else {label.fuel}
            if can_buy:
                # Include minimum buys, exact next-reserve targets, and a small
                # departure grid to retain useful partial fills for later legs.
                departures.update((label.fuel + min_purchase_gal, label.fuel + bridge_floor))
                departures.update(burn + floor_at(j) for j, burn in edges)
                first = math.ceil((label.fuel + min_purchase_gal) / BUCKET_GAL)
                departures.update(b * BUCKET_GAL for b in range(first, int(tank_capacity_gal / BUCKET_GAL) + 1))
                # Driver instructions use whole gallons. Round purchases UP
                # inside the model, where the tank limit and downstream burns
                # can still be checked. A fractional final increment is a
                # fill-to-full instruction, not a rounded gallon target.
                departures = {min(tank_capacity_gal, label.fuel + math.ceil(dep - label.fuel - _EPS))
                              for dep in departures if dep >= label.fuel - _EPS}
            for dep in departures:
                gallons = dep - label.fuel
                if dep > tank_capacity_gal + _EPS or gallons < -_EPS:
                    continue
                if can_buy and (gallons <= _EPS or gallons < min_purchase_gal - _EPS):
                    continue
                purchase_cost = gallons * stops[i - 1].net_price + stop_time_penalty if can_buy else 0.0
                for j, burn in edges:
                    if can_buy and j != terminal and j not in required and gallons < bridge_floor - _EPS:
                        continue
                    arrival = dep - burn
                    if arrival < floor_at(j) - _EPS:
                        continue
                    # Telescope extra road miles against each node's direct
                    # distance to delivery; unavailable baselines cannot invent
                    # an edge and receive no speculative detour estimate.
                    remaining_i = road_miles[i][next_required]
                    remaining_j = 0.0 if j == next_required else road_miles[j][next_required]
                    extra = (max(0.0, road_miles[i][j] + remaining_j - remaining_i)
                             if remaining_i is not None and remaining_j is not None else 0.0)
                    cost = label.cost + purchase_cost + extra * cost_per_mile
                    bucket = 0 if j == terminal else int(math.floor((arrival - reserve_gal + _EPS) / BUCKET_GAL))
                    old = labels[j].get(bucket)
                    if old is None or cost < old.cost - _EPS or (abs(cost - old.cost) <= _EPS and arrival > old.fuel):
                        labels[j][bucket] = _Label(j, arrival, cost, label, gallons)

    end = labels[terminal].get(0)
    if end is None:
        raise NoFeasibleFuelPlan("no truck-road fuel plan satisfies the reserve and purchase constraints")
    plan: list[tuple[Stop, float]] = []
    while end.parent is not None:
        if end.parent.node > 0 and end.parent.node not in required:
            plan.append((stops[end.parent.node - 1], end.purchase))
        end = end.parent
    plan.reverse()
    return plan
