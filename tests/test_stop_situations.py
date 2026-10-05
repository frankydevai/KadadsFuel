"""Simulations of stop behavior, fresh telemetry and honest price comparisons."""
import asyncio
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from dieselup.core import compliance,fuel_brain,observed_fueling,stationary_context
from dieselup.core.fueling_analysis import compare_fueling
from dieselup.core.stop_visits import observation,dwell_evidence

NOW=datetime.now(timezone.utc)

@pytest.mark.parametrize('speed,age,expected',[(0,0,True),(4,1,True),(55,0,False),(0,10,False),(None,0,False)])
def test_automatic_visit_requires_slow_truck_and_fresh_gps(speed,age,expected):
    loc=SimpleNamespace(lat=32,lng=-97,speed_mph=speed,gps_time=NOW-timedelta(minutes=age))
    assert bool(observation(loc,{'latitude':32,'longitude':-97},NOW))==expected

@pytest.mark.parametrize('gap,expected',[(0,False),(60,False),(120,True),(300,True),(700,False)])
def test_polling_same_gps_never_creates_a_visit(gap,expected):
    assert bool(dwell_evidence({'gps_time':NOW.isoformat()},{'gps_time':(NOW+timedelta(seconds=gap)).isoformat()}))==expected


def samples(minutes=75):
    return [{'latitude':32,'longitude':-97,'speed_mph':0,'fuel_pct':50,
      'taken_at':NOW-timedelta(minutes=i),'gps_observed_at':NOW-timedelta(minutes=i),
      'fuel_observed_at':NOW-timedelta(minutes=i)} for i in range(0,minutes+1,5)]

@pytest.mark.parametrize('case,expected', [('rest',True),('short',False),('fuel',False),('moving',False),('stale_fuel',False),('stale_gps',False),('repeat',False),('outage',False)])
def test_hour_stop_without_fueling_is_context_and_not_wrong_fueling(case,expected):
    rows=samples(30 if case=='short' else 75)
    if case=='fuel':rows[0]['fuel_pct']=80
    if case=='moving':rows[0]['speed_mph']=60
    if case=='stale_fuel':rows[0]['fuel_observed_at']=NOW-timedelta(hours=1)
    if case=='stale_gps':rows[0]['gps_observed_at']=NOW-timedelta(hours=1)
    if case=='repeat':
        for row in rows:row['gps_observed_at']=NOW-timedelta(minutes=75)
    if case=='outage':del rows[2:8]
    result=stationary_context.assess_stationary(rows,NOW)
    assert bool(result)==expected
    if result:assert result['fueling_detected'] is False and 'unconfirmed' in result['interpretation']

@pytest.mark.parametrize('actual,offset,loss,saving',[(3.8,0,24,0),(3.2,0,0,24),(3.5,0,0,0),(None,0,None,None),(3.8,10,None,None),(3.8,-1,None,None)])
def test_cost_uses_observed_gallons_and_dated_actual_price(actual,offset,loss,saving):
    date=NOW.date();actual_date=date-timedelta(days=offset)
    result=compare_fueling(80,3.5,actual,date,actual_date,as_of=NOW)
    assert result['extra_cost']==loss and result['saving']==saving
    if loss is None:assert result['price_status']=='pending'

@pytest.mark.parametrize('qty',[None,float('nan'),-1,0])
def test_invalid_quantity_cannot_produce_a_dollar_loss(qty):
    assert compare_fueling(qty,3.5,3.8,NOW.date(),NOW.date())['extra_cost'] is None

@pytest.mark.parametrize('actual,expected_alert',[(None,False),(3.2,False),(3.8,True)])
def test_other_stop_only_alerts_on_confirmed_more_expensive_fueling(monkeypatch,actual,expected_alert):
    date=NOW.date().isoformat()
    facts=compare_fueling(80,3.5,actual,date,date)
    facts.update(fueling_confirmed=True,classification='contracted_other',fueling_at=NOW.isoformat())
    monkeypatch.setattr(observed_fueling,'analysis',AsyncMock(return_value=(facts,{'site_id':2} if actual else None)))
    marked=AsyncMock();alert=AsyncMock()
    monkeypatch.setattr(compliance,'_mark_resolved',marked);monkeypatch.setattr(compliance,'_stamp_fuel_delta',AsyncMock());monkeypatch.setattr(compliance,'_send_wrong_stop_alert',alert)
    event={'id':10,'briefing_driver_msg_id':900,'candidates':[{'your_price':3.5,'price_date':date}]}
    asyncio.run(compliance._resolve_observed_other_fueling(event,{'gallons':80,'fuel_pct_end':90,'site_id':2},None,None))
    assert bool(alert.call_count)==expected_alert
    assert marked.call_args.kwargs['dollar_impact']==(24 if actual==3.2 else -24 if actual==3.8 else 0)

@pytest.mark.parametrize('repeat,stale',[(True,False),(False,True)])
def test_fuel_rise_with_replayed_or_stale_reading_never_opens_fill(monkeypatch,repeat,stale):
    now=datetime.now(timezone.utc);previous=now-timedelta(minutes=5)
    client=SimpleNamespace(get_vehicle_location=AsyncMock(return_value=SimpleNamespace(lat=32,lng=-97,gps_age_minutes=0)),
        get_vehicle_fuel_reading=AsyncMock(return_value=(180,previous if repeat else now-timedelta(hours=1))))
    monkeypatch.setattr(fuel_brain,'_maybe_write_snapshot',AsyncMock(return_value=True))
    monkeypatch.setattr(stationary_context,'record_stationary_context',AsyncMock())
    handle=AsyncMock();monkeypatch.setattr(fuel_brain,'_handle_fuel_rise',handle)
    asyncio.run(fuel_brain._process_vehicle(vehicle=SimpleNamespace(id='v1',name='100'),samsara=client,bot=None,
        prev={'fuel_pct':50,'fuel_observed_at':previous},open_event=None,truck_row={'truck_unit':'100'},pending_by_unit={}))
    handle.assert_not_called()


@pytest.mark.parametrize('case',['moving','competing_station','old_sensor'])
def test_uncertain_pump_location_is_not_attributed_to_advised_stop(monkeypatch,case):
    now=datetime.now(timezone.utc)
    loc=SimpleNamespace(lat=32,lng=-97,speed_mph=55 if case=='moving' else 0,gps_time=now)
    other={'site_id':2,'latitude':32,'longitude':-97,'your_price':3.8,'state':'TX','station_name':'Adjacent station'}
    monkeypatch.setattr(fuel_brain,'_nearby_priced_stop',AsyncMock(return_value=other if case=='competing_station' else None))
    pending={'recommended_site_id':1,'candidates':[{'site_id':1,'latitude':32,'longitude':-97}]}
    result=asyncio.run(fuel_brain._classify_fuel_location(location=loc,prev=None,pending=pending,
        fuel_observed_at=now-timedelta(minutes=20) if case=='old_sensor' else now))
    assert result.nearby is None and result.classification=='off_network'
