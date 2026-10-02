"""
Tests for the min-cost-path DP in core/fuel_plan.py.

The DP is pure (no I/O, no settings). Tests verify:
  * Single-stop optimal buy
  * Multi-stop plan when one fill is insufficient
  * Infeasible plan raises NoFeasibleFuelPlan
  * Delivery sentinel is never bought at
  * Reserve is never violated
  * route_position decomposition
"""
import math
import pytest

from dieselup.core.fuel_plan import (
    BUCKET_GAL,
    NoFeasibleFuelPlan,
    Stop,
    plan_fuel,
    route_position,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stop(mile: float, price: float = 3.50, detour: float = 0.0) -> Stop:
    return Stop(mile_marker=mile, net_price=price, detour_miles=detour)


def _delivery(mile: float) -> Stop:
    return Stop(mile_marker=mile, net_price=0.0, detour_miles=0.0)


TANK = 220.0
RESERVE = 20.0
MPG = 6.5

# Max range on full tank without dipping below reserve
MAX_RANGE = (TANK - RESERVE) * MPG  # 200 * 6.5 = 1300 miles


# ---------------------------------------------------------------------------
# route_position
# ---------------------------------------------------------------------------

class TestRoutePosition:
    def test_on_route_stop_has_zero_detour(self):
        # Perfect on-route stop: d(S→D) + d(Sh→S) == d(Sh→D)
        d_sh_dl = 500.0
        d_sh_s = 200.0
        d_s_dl = 300.0
        mile, detour = route_position(d_sh_dl, d_sh_s, d_s_dl)
        assert mile == pytest.approx(200.0, abs=1.0)
        assert detour == pytest.approx(0.0, abs=0.1)

    def test_off_route_stop_has_positive_detour(self):
        d_sh_dl = 500.0
        d_sh_s = 220.0   # 20 miles extra to reach
        d_s_dl = 300.0   # 20 miles extra to return
        mile, detour = route_position(d_sh_dl, d_sh_s, d_s_dl)
        assert detour == pytest.approx(10.0, abs=1.0)  # round-trip / 2

    def test_detour_never_negative(self):
        # Numerical noise could push round_trip_extra slightly negative
        mile, detour = route_position(100.0, 50.0, 51.0)
        assert detour >= 0.0

    def test_mile_marker_never_negative(self):
        mile, detour = route_position(100.0, 0.1, 99.9)
        assert mile >= 0.0


# ---------------------------------------------------------------------------
# plan_fuel — single stop
# ---------------------------------------------------------------------------

class TestPlanFuelSingleStop:
    def test_buys_fuel_at_single_stop(self):
        stops = [_stop(500.0, price=3.50), _delivery(1000.0)]
        plan = plan_fuel(stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0)
        assert len(plan) == 1
        stop, gallons = plan[0]
        assert gallons > 0
        assert stop.mile_marker == pytest.approx(500.0)

    def test_does_not_buy_at_delivery(self):
        stops = [_stop(500.0), _delivery(1000.0)]
        plan = plan_fuel(stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0)
        for stop, gallons in plan:
            assert stop.mile_marker < 1000.0

    def test_reserve_never_violated(self):
        stops = [_stop(300.0), _stop(600.0), _delivery(900.0)]
        plan = plan_fuel(stops, TANK, 80.0, RESERVE, MPG, 0.55, 12.0)
        # Reconstruct fuel levels and verify reserve holds
        fuel = 80.0
        buy_dict = {s.mile_marker: g for s, g in plan}
        prev_mile = 0.0
        for stop, gallons in sorted(plan, key=lambda x: x[0].mile_marker):
            burn = (stop.mile_marker - prev_mile) / MPG
            fuel -= burn
            assert fuel >= RESERVE - 0.5, f"Reserve violated at mile {stop.mile_marker}"
            fuel += gallons
            assert fuel <= TANK + 0.5
            prev_mile = stop.mile_marker

    def test_tank_not_overfilled(self):
        stops = [_stop(50.0), _delivery(1000.0)]
        plan = plan_fuel(stops, TANK, 210.0, RESERVE, MPG, 0.55, 12.0)
        # 50 miles burns 50/6.5 ≈ 7.7 gal → arrives with 202.3 → almost full already
        # A buy that would push over 220 must be capped
        for stop, gallons in plan:
            fuel_on_arrival = 210.0 - stop.mile_marker / MPG
            assert fuel_on_arrival + gallons <= TANK + 1.0

    def test_no_buy_needed_when_enough_fuel(self):
        # Delivery at 100 miles, start with 200 gal → burns 15.4 gal → arrives with 184.6 > reserve
        stops = [_delivery(100.0)]
        plan = plan_fuel(stops, TANK, 200.0, RESERVE, MPG, 0.55, 12.0)
        assert len(plan) == 0

    def test_cheaper_stop_preferred_over_expensive(self):
        cheap = _stop(200.0, price=3.20)
        expensive = _stop(400.0, price=4.50)
        stops = [cheap, expensive, _delivery(800.0)]
        plan = plan_fuel(stops, TANK, 60.0, RESERVE, MPG, 0.55, 12.0)
        stop_miles = {s.mile_marker for s, _ in plan}
        # The cheap stop should be used
        assert 200.0 in stop_miles


# ---------------------------------------------------------------------------
# plan_fuel — multi-stop (distance exceeds single-tank range)
# ---------------------------------------------------------------------------

class TestPlanFuelMultiStop:
    def test_partial_buy_to_reach_cheaper_fuller_stop(self):
        expensive_bridge = _stop(500.0, price=5.00)
        cheap_later = _stop(900.0, price=3.00)
        stops = [expensive_bridge, cheap_later, _delivery(1400.0)]

        plan = plan_fuel(stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0)

        by_mile = {s.mile_marker: gallons for s, gallons in plan}
        assert 500.0 in by_mile
        assert 900.0 in by_mile
        assert by_mile[500.0] < 100.0
        assert by_mile[900.0] > by_mile[500.0]

    def test_bridge_buy_respects_90_gallon_minimum(self):
        # Same shape as the partial-buy case, but with the live 90-gal purchase
        # floor: the bridge fill must be a real >=90 gal buy (no nuisance top-ups),
        # then a larger full-tank buy at the cheaper later stop.
        expensive_bridge = _stop(500.0, price=5.00)
        cheap_later = _stop(900.0, price=3.00)
        stops = [expensive_bridge, cheap_later, _delivery(1400.0)]

        plan = plan_fuel(
            stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0,
            min_purchase_gal=50.0,
            bridge_min_purchase_gal=90.0,
        )

        by_mile = {s.mile_marker: gallons for s, gallons in plan}
        assert 500.0 in by_mile, "expected a bridge fill at the closer stop"
        assert 900.0 in by_mile, "expected the full fill at the cheaper stop"
        assert by_mile[500.0] >= 90.0, "bridge fill must honor the 90-gal floor"
        assert by_mile[900.0] >= 50.0, "main stop keeps the normal purchase floor"

    def test_two_stop_plan_for_long_route(self):
        # Route is 1800 miles — needs at least 2 stops
        stops = [
            _stop(400.0, price=3.50),
            _stop(900.0, price=3.30),  # cheaper — should prefer
            _stop(1300.0, price=3.70),
            _delivery(1800.0),
        ]
        plan = plan_fuel(stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0)
        assert len(plan) >= 2

    def test_infeasible_raises(self):
        # Single stop at 2000 miles, tank range only ~1300 miles
        stops = [_stop(2000.0), _delivery(3000.0)]
        with pytest.raises(NoFeasibleFuelPlan):
            plan_fuel(stops, TANK, 100.0, RESERVE, MPG, 0.55, 12.0)


# ---------------------------------------------------------------------------
# plan_fuel — terminal (delivery) reserve and detour fuel burn
# ---------------------------------------------------------------------------

class TestTerminalReserveAndDetourBurn:
    def test_lower_delivery_reserve_buys_fewer_gallons(self):
        """With a 20-gal delivery reserve vs the 30-gal stop floor, the plan
        should let the truck arrive at delivery with less fuel — buying fewer
        gallons overall."""
        stops = [_stop(300.0, price=3.50), _delivery(900.0)]
        plan_high = plan_fuel(stops, TANK, 80.0, 30.0, MPG, 0.55, 12.0)
        plan_low = plan_fuel(
            stops, TANK, 80.0, 30.0, MPG, 0.55, 12.0, terminal_reserve_gal=20.0
        )
        total_high = sum(g for _, g in plan_high)
        total_low = sum(g for _, g in plan_low)
        assert total_low <= total_high
        assert total_low < total_high or total_high == 0.0

    def test_terminal_reserve_default_matches_old_behavior(self):
        stops = [_stop(300.0, price=3.50), _delivery(900.0)]
        plan_default = plan_fuel(stops, TANK, 80.0, RESERVE, MPG, 0.55, 12.0)
        plan_explicit = plan_fuel(
            stops, TANK, 80.0, RESERVE, MPG, 0.55, 12.0,
            terminal_reserve_gal=RESERVE,
        )
        assert [(s.mile_marker, pytest.approx(g)) for s, g in plan_default] == [
            (s.mile_marker, pytest.approx(g)) for s, g in plan_explicit
        ]

    def test_detour_fuel_is_burned_not_just_costed(self):
        """A stop with a big detour must account for the out-and-back burn.

        Tank range from full is 1300 mi. Delivery 1290 mi from the only stop
        at mile 10. With a 30-mile one-way detour, buying there burns an extra
        60/6.5 ≈ 9.2 gal, so a full tank can no longer make delivery >= reserve
        — the plan must be infeasible. The old model (cost-only detour)
        wrongly accepted it."""
        detour_stop = _stop(10.0, price=3.50, detour=30.0)
        stops = [detour_stop, _delivery(1300.0)]
        # start fuel just enough to reach the stop, forcing a buy there
        with pytest.raises(NoFeasibleFuelPlan):
            plan_fuel(stops, TANK, 25.0, RESERVE, MPG, 0.55, 12.0)

    def test_zero_detour_unaffected_by_burn_change(self):
        stops = [_stop(10.0, price=3.50, detour=0.0), _delivery(1300.0)]
        plan = plan_fuel(stops, TANK, 25.0, RESERVE, MPG, 0.55, 12.0)
        assert len(plan) == 1


# ---------------------------------------------------------------------------
# plan_fuel — validation
# ---------------------------------------------------------------------------

class TestPlanFuelValidation:
    def test_invalid_mpg_raises(self):
        stops = [_delivery(100.0)]
        with pytest.raises(ValueError):
            plan_fuel(stops, TANK, 100.0, RESERVE, 0.0, 0.55, 12.0)

    def test_tank_less_than_reserve_raises(self):
        stops = [_delivery(100.0)]
        with pytest.raises(ValueError):
            plan_fuel(stops, 20.0, 100.0, 30.0, MPG, 0.55, 12.0)

    def test_empty_stops_raises(self):
        with pytest.raises(ValueError):
            plan_fuel([], TANK, 100.0, RESERVE, MPG, 0.55, 12.0)

    def test_start_fuel_below_reserve_raises(self):
        stops = [_stop(100.0), _delivery(500.0)]
        with pytest.raises(NoFeasibleFuelPlan):
            plan_fuel(stops, TANK, 5.0, RESERVE, MPG, 0.55, 12.0)
