from datetime import date
import asyncio

import pytest

from dieselup.config import settings
from dieselup.core import lane_plan, optimizer
from dieselup.core.fuel_plan import Stop
from dieselup.core.optimizer import CandidateStop, StaleFuelPricesError


def candidate(site, price, state):
    return CandidateStop(site, f'Pilot {site}', None, 'City', state, 40, -80, price, price+.5)


def matrix_plan(candidates,matrix,**changes):
    args=dict(candidates=candidates,matrix=matrix,engine_name='Valhalla',tank_capacity_gal=100,
              start_fuel_gal=40,reserve_gal=10,mpg=5,cost_per_mile=.55,
              stop_time_penalty=20,min_purchase_gal=0,bridge_min_purchase_gal=0,
              terminal_reserve_gal=10)
    args.update(changes)
    return lane_plan._build_buy_plan_from_matrix(**args)


@pytest.mark.parametrize('strategy,expected',[('your_price',1),('ifta_adjusted',2)])
def test_ranking_setting_is_honored_while_true_cost_is_retained(monkeypatch,strategy,expected):
    monkeypatch.setattr(settings,'RANK_STRATEGY',strategy)
    monkeypatch.setattr(lane_plan,'true_cost_per_gallon',lambda price,state: 4 if state=='NJ' else 2)
    a,b=candidate(1,3,'NJ'),candidate(2,3.2,'PA')
    lane=matrix_plan([b,a],[[0,400,140,100],[400,0,260,300],[140,260,0,40],[100,300,40,0]])
    assert lane.legs[0].candidate.site_id==expected
    assert lane.legs[0].net_price==(4 if expected==1 else 2)
    assert lane.worst_true_cost==4


def test_displayed_distance_reaches_the_pump_including_detour():
    lane=matrix_plan([candidate(1,3,'NJ')],[[0,400,110],[400,0,310],[110,310,0]])
    assert lane.legs[0].distance_from_truck_mi==110  # projected marker is only 100


def test_partial_lane_fallback_does_not_invent_a_missing_leg():
    a,b=candidate(1,3,'NJ'),candidate(2,3,'NJ')
    lane=matrix_plan([a,b],[[0,900,100,500],[900,0,800,400],[100,800,0,None],[500,400,400,0]])
    assert lane.degraded=='partial_lane_best_reachable'
    assert len(lane.legs)==1
    assert lane.legs[0].fill_to_full
    assert lane.legs[0].gallons==80


def test_relax_purchase_minimum_before_safety_reserve():
    stops=[Stop(100,3,0),Stop(440,0,0)]
    plan,label=lane_plan._run_dp_with_relaxation(
        stops,road_miles=[[0,100,440],[100,0,340],[440,340,0]],
        tank_capacity_gal=100,start_fuel_gal=90,reserve_gal=10,mpg=5,
        cost_per_mile=.55,stop_time_penalty=20,min_purchase_gal=50,
        bridge_min_purchase_gal=90,terminal_reserve_gal=10)
    assert label=='purchase_floor_relaxed'
    assert plan[0][1]==8


@pytest.mark.parametrize('age',[None,-1,4,40])
def test_stale_or_missing_price_feed_blocks_recommendations(monkeypatch,age):
    async def freshness(*_): return {'effective_date':date(2026,8,18),'age_days':age}
    async def unexpected(*_): raise AssertionError('must not select stale candidates')
    monkeypatch.setattr(optimizer,'fetch_one',freshness)
    monkeypatch.setattr(optimizer,'fetch_all',unexpected)
    monkeypatch.setattr(settings,'MAX_FUEL_PRICE_AGE_DAYS',3)
    with pytest.raises(StaleFuelPricesError):
        asyncio.run(optimizer.fetch_corridor_candidates(truck_lat=40,truck_lng=-80,destination_lat=41,destination_lng=-81))


def test_recent_price_feed_allows_candidate_query(monkeypatch):
    async def freshness(*_): return {'effective_date':date(2026,9,27),'age_days':0}
    async def rows(*_): return []
    monkeypatch.setattr(optimizer,'fetch_one',freshness)
    monkeypatch.setattr(optimizer,'fetch_all',rows)
    assert asyncio.run(optimizer.fetch_corridor_candidates(truck_lat=40,truck_lng=-80,destination_lat=41,destination_lng=-81))==[]


def test_full_fill_in_driver_message_cannot_become_a_rounded_gallon_target():
    from dieselup.bot.messages import sequential_fuel_plan_message
    message=sequential_fuel_plan_message(
        load_id='L1',truck_unit='100',origin_label='Origin',destination_label='Delivery',
        current_fuel_gallons=40.37,stop={'station_name':'Pilot','fill_to_full':True},
        gallons_to_buy=80,stop_number=1,stop_count=2,is_final_leg=False)
    assert 'Fill to full (about 80 gal)' in message
    assert 'Buy: 80 gal' not in message


def test_partial_lane_message_does_not_claim_delivery_is_covered():
    from dieselup.bot.messages import sequential_fuel_plan_message
    message=sequential_fuel_plan_message(
        load_id='L1',truck_unit='100',origin_label='Origin',destination_label='Delivery',
        current_fuel_gallons=40,stop={'station_name':'Pilot','plan':{'degraded':'partial_lane_best_reachable'}},
        gallons_to_buy=80,stop_number=1,stop_count=1,is_final_leg=True)
    assert 'to delivery + reserve' not in message
    assert 'Contact dispatch' in message
