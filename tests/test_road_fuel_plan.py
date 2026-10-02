"""Physical replay and regressions for the directed truck-road optimizer."""
import random

import pytest

from dieselup.core.fuel_plan import NoFeasibleFuelPlan, Stop
from dieselup.core.road_fuel_plan import plan_road_fuel


def solve(stops, matrix, **changes):
    args = dict(tank_capacity_gal=100.0, start_fuel_gal=40.0,
                reserve_gal=10.0, mpg=5.0, cost_per_mile=0.55,
                stop_time_penalty=20.0, min_purchase_gal=0.0)
    args.update(changes)
    return plan_road_fuel(stops, road_miles=matrix, **args)


def replay(stops, matrix, plan, *, tank=100, start=40, reserve=10, terminal=10, mpg=5, minimum=0, bridge=0):
    fuel, previous = start, 0
    for index, (stop, gallons) in enumerate(plan):
        node = next(i for i, s in enumerate(stops, start=1) if s is stop)
        assert matrix[previous][node] is not None
        fuel -= matrix[previous][node] / mpg
        assert fuel >= reserve - 1e-8
        assert gallons > 0
        assert gallons >= (max(minimum, bridge) if index < len(plan)-1 else minimum) - 1e-8
        fuel += gallons
        assert fuel <= tank + 1e-8
        previous = node
    assert matrix[previous][-1] is not None
    fuel -= matrix[previous][-1] / mpg
    assert fuel >= terminal - 1e-8


def test_unreachable_directed_pair_cannot_be_invented_from_mile_markers():
    stops = [Stop(100, 3, 0), Stop(500, 3, 0), Stop(900, 0, 0)]
    matrix = [[0,100,500,900],[100,0,None,800],[500,400,0,400],[900,800,400,0]]
    with pytest.raises(NoFeasibleFuelPlan):
        solve(stops,matrix)


def test_long_road_leg_rejects_false_feasibility_from_projection():
    stops = [Stop(100, 3, 0), Stop(500, 3, 0), Stop(900, 0, 0)]
    matrix = [[0,100,500,900],[100,0,475,800],[500,400,0,400],[900,800,400,0]]
    with pytest.raises(NoFeasibleFuelPlan):
        solve(stops,matrix)


def test_incoming_detour_is_burned_before_reaching_pump():
    stops = [Stop(150, 3, 10), Stop(400, 0, 0)]
    matrix = [[0,160,400],[160,0,260],[400,260,0]]
    with pytest.raises(NoFeasibleFuelPlan):
        solve(stops,matrix)  # 40 - 160/5 = 8, below 10-gal reserve


def test_detour_not_burned_again_after_routed_leg():
    stops = [Stop(100, 3, 10), Stop(540, 0, 0)]
    matrix = [[0,110,540],[110,0,450],[540,450,0]]
    plan = solve(stops,matrix)
    assert len(plan) == 1
    replay(stops,matrix,plan)


def test_fractional_arrival_never_overfills_tank():
    stops = [Stop(100, 1, 0), Stop(500, 9, 0), Stop(900, 0, 0)]
    matrix = [[0,101,501,901],[101,0,400,800],[501,400,0,400],[901,800,400,0]]
    plan = solve(stops,matrix,start_fuel_gal=40.37)
    assert len(plan) == 2
    replay(stops,matrix,plan,start=40.37)


def test_full_tank_exception_cannot_bypass_minimum_purchase():
    stops = [Stop(5,3,0), Stop(400,0,0)]
    matrix = [[0,5,400],[5,0,395],[400,395,0]]
    with pytest.raises(NoFeasibleFuelPlan):
        solve(stops,matrix,start_fuel_gal=80,min_purchase_gal=50)


def test_no_purchase_when_delivery_already_reachable():
    assert solve([Stop(100,0,0)],[[0,100],[100,0]]) == []


def test_delivery_reserve_is_independent_of_stop_reserve():
    stops = [Stop(100,3,0), Stop(400,0,0)]
    matrix = [[0,100,400],[100,0,300],[400,300,0]]
    plan = solve(stops,matrix,terminal_reserve_gal=25)
    replay(stops,matrix,plan,terminal=25)


def test_cheaper_price_does_not_justify_excess_detour_cost():
    stops=[Stop(100,3.0,10),Stop(120,3.1,0),Stop(400,0,0)]
    matrix=[[0,110,120,400],[110,0,40,310],[120,40,0,280],[400,310,280,0]]
    plan=solve(stops,matrix,cost_per_mile=5)
    assert [s for s,_ in plan] == [stops[1]]


@pytest.mark.parametrize('bad',[-1,float('nan'),float('inf')])
def test_invalid_distance_rejected(bad):
    with pytest.raises(ValueError,match='distances'):
        solve([Stop(100,0,0)],[[0,bad],[100,0]])


@pytest.mark.parametrize('changes',[
    {'mpg':float('nan')},{'start_fuel_gal':101},{'reserve_gal':-1},
    {'terminal_reserve_gal':101},{'min_purchase_gal':-1},
])
def test_invalid_fuel_inputs_rejected(changes):
    with pytest.raises(ValueError):
        solve([Stop(100,0,0)],[[0,100],[100,0]],**changes)


def test_incomplete_matrix_rejected():
    with pytest.raises(ValueError,match='matrix'):
        solve([Stop(100,0,0)],[[0,100]])


def test_randomized_physical_replay():
    rng=random.Random(910191)
    feasible=0
    for _ in range(100):
        miles=[0,80,200,360,520,650]
        stops=[Stop(m,rng.uniform(2.5,5.5),0) for m in miles[1:]]
        matrix=[[0 if i==j else abs(a-b)+rng.uniform(0,15) for j,b in enumerate(miles)] for i,a in enumerate(miles)]
        for i in range(1,5):
            if rng.random()<0.2: matrix[i][i+1]=None
        start=rng.uniform(30,95)
        try:
            plan=solve(stops,matrix,start_fuel_gal=start,min_purchase_gal=15,bridge_min_purchase_gal=25)
        except NoFeasibleFuelPlan:
            continue
        feasible+=1
        replay(stops,matrix,plan,start=start,minimum=15,bridge=25)
    assert feasible>80
