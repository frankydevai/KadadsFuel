"""
Min-cost-path fuel-buy planner.

Replaces the old "pick one stop, fill to full at the cheapest" heuristic with a
dynamic program that returns the globally cheapest buy plan for a lane — the
exact gallons to purchase at each stop — trading per-gallon savings against
detour deadhead and per-stop driver time.

Why a DP and not a rule
-----------------------
Stops are given in mile order (shipper -> delivery), so the reachability graph
is a DAG: a single forward Bellman pass, no cycles. Both "one big fill" and
"several partial buys" fall out of the SAME optimization based on where cheap
fuel sits relative to tank range — they are never coded as separate cases. The
DP also correctly *skips* a nominally-cheaper stop when its detour or stop-time
outweighs the per-gallon saving, which "fill at cheapest" cannot do.

State & fuel handling
---------------------
State is (node_index, fuel_on_arrival). Fuel is physically continuous; here it
is discretized into BUCKET_GAL buckets anchored at the reserve floor, so a
floored bucket is always >= reserve (never an under-reserve false positive).
This is near-optimal and simple. See the `# TODO: PWL` marker for the exact
piecewise-linear cost-to-go upgrade.

Contract
--------
`stops` must be ordered by mile_marker and the LAST element must be the
delivery point. plan_fuel never buys at the delivery node (its net_price /
detour are ignored); reaching it with on-hand fuel >= reserve is the objective.
The shipper (mile 0, fuel = start_fuel_gal) is modeled internally — do not pass
it in.

`net_price` is the existing IFTA value (card_price + home_state_rate -
stop_state_rate, i.e. core.ifta.true_cost_per_gallon). The price *source*
(card vs. relay) is chosen upstream — this function is price-source agnostic.

Pure: no DB, no Telegram, no clock.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

# Fuel discretization granularity (gallons). Smaller = closer to optimal, more
# states. 2 gal is near-optimal for a 200-ish-gallon tank.
# TODO: PWL — carry a piecewise-linear cost-to-go per node instead of buckets
# to remove the snap error entirely and get the exact continuous optimum.
BUCKET_GAL = 2.0

_EPS = 1e-9


@dataclass(frozen=True)
class Stop:
    """One point on the lane, in mile order.

    Required fields are exactly mile_marker / net_price / detour_miles. `ref` is
    an opaque caller handle (e.g. the original CandidateStop) so the returned
    plan can be mapped back to a real station; it never affects the math.
    """
    mile_marker: float        # miles from the shipper along the planned route
    net_price: float          # IFTA-adjusted $/gal (true_cost_per_gallon output)
    detour_miles: float       # one-way off-route miles to reach the pump
    ref: Any = field(default=None, compare=False)
    required: bool = False    # mandatory pickup/delivery; never buy fuel here


class NoFeasibleFuelPlan(Exception):
    """Delivery cannot be reached while keeping on-hand fuel >= reserve."""


def route_position(
    d_shipper_delivery: float,
    d_shipper_stop: float,
    d_stop_delivery: float,
) -> tuple[float, float]:
    """Derive (mile_marker, detour_miles) for a stop from three road distances.

    Pure distance-space decomposition — no geometry/polyline needed:

      mile_marker  = (d(sh→dl) + d(sh→s) − d(s→dl)) / 2
          the stop's progress *along* the lane. On-route stops satisfy
          d(sh→s)+d(s→dl) ≈ d(sh→dl); the formula projects onto that axis.

      detour_miles = max(0, d(sh→s) + d(s→dl) − d(sh→dl)) / 2
          one-way off-route deadhead. The bracket is the round-trip deviation
          (out + back); halving gives the one-way figure, and plan_fuel charges
          it ×2 again — so the total cost reflects the full out-and-back.
    """
    mile_marker = (d_shipper_delivery + d_shipper_stop - d_stop_delivery) / 2.0
    round_trip_extra = d_shipper_stop + d_stop_delivery - d_shipper_delivery
    detour_miles = max(0.0, round_trip_extra) / 2.0
    return max(0.0, mile_marker), detour_miles


def plan_fuel(
    stops: list[Stop],          # ordered shipper->delivery; last element is delivery
    tank_capacity_gal: float,
    start_fuel_gal: float,
    reserve_gal: float,         # on-hand fuel must never drop below this
    mpg: float,                 # per-truck, from Samsara
    cost_per_mile: float,       # TUNABLE config — deadhead fuel + wear per detour mile
    stop_time_penalty: float,   # TUNABLE config — $ cost of making one stop (HOS/driver time)
    min_purchase_gal: float = 0.0,  # minimum gallons per stop (0 = no floor)
    bridge_min_purchase_gal: float | None = None,  # floor when another fuel stop follows
    terminal_reserve_gal: float | None = None,  # reserve at DELIVERY only (None = reserve_gal)
) -> list[tuple[Stop, float]]:  # (stop, gallons_to_buy); 0-gallon stops are not returned
    """Globally cheapest buy plan for the lane. See module docstring for contract.

    Returns the buy plan in mile order: a list of (stop, gallons_to_buy) for the
    stops where fuel should actually be purchased. Raises NoFeasibleFuelPlan when
    delivery is unreachable under the reserve constraint.

    Two reserve floors: `reserve_gal` applies on arrival at every FUEL STOP
    (safety floor — e.g. 30 gal); `terminal_reserve_gal` applies at DELIVERY
    only (delivery reserve — e.g. 10% of tank = 20 gal). Spec: CLAUDE.md "Fuel
    Math Rules". Defaults to reserve_gal for back-compat.

    Detour fuel is burned, not just costed: buying at a stop with detour_miles
    consumes detour×2/mpg gallons from the tank in addition to the on-route
    burn — previously detours were charged $ but the model thought the fuel
    was still on board, eroding the reserve guarantee.
    """
    if mpg <= 0:
        raise ValueError("mpg must be positive")
    if tank_capacity_gal <= reserve_gal:
        raise ValueError("tank_capacity_gal must exceed reserve_gal")
    if not stops:
        raise ValueError("stops must include at least the delivery node")

    delivery = stops[-1]
    fuel_stops = stops[:-1]
    if delivery.mile_marker < 0:
        raise ValueError("delivery mile_marker must be non-negative")
    for stop in fuel_stops:
        if stop.mile_marker < 0:
            raise ValueError("fuel stop mile_marker must be non-negative")
        if stop.mile_marker > delivery.mile_marker + _EPS:
            raise ValueError("fuel stops must not be beyond delivery")

    # --- Build augmented node arrays: node 0 = shipper, 1..= stops (last=delivery).
    # The last caller-provided element is the delivery sentinel by contract. Sort
    # fuel stops only, then append delivery so a beyond-route stop can never
    # become the terminal objective by accident.
    ordered = sorted(fuel_stops, key=lambda s: s.mile_marker) + [delivery]
    mile = [0.0] + [s.mile_marker for s in ordered]
    price = [0.0] + [s.net_price for s in ordered]      # shipper price unused (forced g=0)
    detour = [0.0] + [s.detour_miles for s in ordered]
    n = len(mile)                       # number of augmented nodes
    terminal = n - 1                    # delivery node index

    if start_fuel_gal < reserve_gal - _EPS:
        raise NoFeasibleFuelPlan(
            f"start fuel {start_fuel_gal:.1f} gal already below reserve {reserve_gal:.1f}"
        )

    # Terminal (delivery) reserve may differ from the per-stop safety floor.
    t_reserve = reserve_gal if terminal_reserve_gal is None else terminal_reserve_gal
    bridge_floor = (
        min_purchase_gal
        if bridge_min_purchase_gal is None
        else max(min_purchase_gal, bridge_min_purchase_gal)
    )

    def floor_at(j: int) -> float:
        return t_reserve if j == terminal else reserve_gal

    # Reserve-anchored bucket grid: bucket 0 == reserve. Negative buckets are
    # valid and only ever occur at the terminal when t_reserve < reserve_gal —
    # the terminal is a sink, so flooring there never violates a downstream floor.
    def to_bucket(fuel: float) -> int:
        return int(math.floor((fuel - reserve_gal + _EPS) / BUCKET_GAL))

    def from_bucket(b: int) -> float:
        return reserve_gal + b * BUCKET_GAL

    # Precompute forward reachability (mile order => only j > i). Range to the
    # terminal uses the (possibly lower) delivery reserve.
    reachable: list[list[int]] = [[] for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if mile[j] - mile[i] <= (tank_capacity_gal - floor_at(j)) * mpg + _EPS:
                reachable[i].append(j)

    # --- Forward DP. best[(node, bucket)] = min cost to arrive there.
    #     trace[state] = (parent_state, gallons_bought_at_parent_node).
    best: dict[tuple[int, int], float] = {}
    trace: dict[tuple[int, int], tuple[tuple[int, int] | None, float]] = {}

    seed = (0, to_bucket(start_fuel_gal))
    best[seed] = 0.0
    trace[seed] = (None, 0.0)

    for i in range(terminal):  # delivery node is a sink — never expanded
        # Gather this node's live states (sparse).
        node_states = [(b, c) for (node, b), c in best.items() if node == i]
        if not node_states:
            continue
        burns = {j: (mile[j] - mile[i]) / mpg for j in reachable[i]}

        # Out-and-back detour burn (gallons) paid whenever we actually buy here.
        detour_burn_gal = detour[i] * 2.0 / mpg

        for b, base_cost in node_states:
            fuel_arrival = from_bucket(b)

            # Candidate departure fuels. Buying is only ever useful to either
            # (a) reach some reachable stop j arriving exactly at its floor, or
            # (b) fill the tank to carry cheap fuel forward. Plus g=0 (coast).
            # This is the complete optimal candidate set, far smaller than
            # scanning every bucket. The shipper (node 0) may not buy.
            #
            # min_purchase_gal floor: if we buy at this stop we buy AT LEAST
            # that many gallons. Buying 0 (coast through) is always available —
            # the floor only applies when we actually decide to stop.
            departures: set[float] = {fuel_arrival}
            if i != 0:
                for j in reachable[i]:
                    # Buying implies the detour burn, so the "arrive exactly at
                    # floor" target must cover it too.
                    need = floor_at(j) + burns[j] + detour_burn_gal
                    if need <= tank_capacity_gal + _EPS:
                        # Apply the minimum-purchase floor: if going to buy,
                        # buy at least min_purchase_gal.
                        purchase_floor = min_purchase_gal if j == terminal else bridge_floor
                        floored = max(need, fuel_arrival + purchase_floor)
                        if floored <= tank_capacity_gal + _EPS:
                            departures.add(min(floored, tank_capacity_gal))
                departures.add(tank_capacity_gal)

            for dep in departures:
                if dep < fuel_arrival - _EPS or dep > tank_capacity_gal + _EPS:
                    continue
                gallons = dep - fuel_arrival
                edge_cost = gallons * price[i]
                burn_extra = 0.0
                if gallons > _EPS:
                    # Charge detour + stop time only when we actually stop to
                    # buy — and BURN the detour fuel, don't just cost it.
                    edge_cost += detour[i] * 2.0 * cost_per_mile + stop_time_penalty
                    burn_extra = detour_burn_gal

                for j in reachable[i]:
                    # A purchase that hands off to another fuel stop is a
                    # bridge fill and must meet the larger bridge floor. A
                    # final/main purchase going directly to delivery keeps the
                    # normal economic floor.
                    if gallons > _EPS and j != terminal and gallons < bridge_floor - _EPS:
                        continue
                    arrival_j = dep - burns[j] - burn_extra
                    if arrival_j < floor_at(j) - _EPS:
                        continue
                    arrival_j = min(arrival_j, tank_capacity_gal)
                    bj = to_bucket(arrival_j)
                    state_j = (j, bj)
                    new_cost = base_cost + edge_cost
                    if new_cost < best.get(state_j, math.inf) - _EPS:
                        best[state_j] = new_cost
                        trace[state_j] = ((i, b), gallons)

    # --- Pick the cheapest way to have arrived at delivery (any fuel >= reserve).
    end_state: tuple[int, int] | None = None
    end_cost = math.inf
    for (node, b), c in best.items():
        if node == terminal and c < end_cost:
            end_cost, end_state = c, (node, b)

    if end_state is None:
        raise NoFeasibleFuelPlan(
            "no buy plan reaches delivery without dropping below reserve"
        )

    # --- Reconstruct: walk parents back, recording gallons bought at each node.
    plan_by_node: dict[int, float] = {}
    state: tuple[int, int] | None = end_state
    while state is not None:
        parent, gallons_at_parent = trace[state]
        if parent is not None and gallons_at_parent > _EPS:
            plan_by_node[parent[0]] = plan_by_node.get(parent[0], 0.0) + gallons_at_parent
        state = parent

    # Map augmented node indices (1..) back to the ordered Stop objects, mile order.
    return [
        (ordered[node - 1], plan_by_node[node])
        for node in sorted(plan_by_node)
        if plan_by_node[node] > _EPS
    ]
