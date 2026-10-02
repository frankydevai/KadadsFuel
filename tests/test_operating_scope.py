from types import SimpleNamespace
import asyncio
from functools import wraps
from unittest.mock import AsyncMock
import pytest
from pydantic import ValidationError
from dieselup.config import Settings, settings
from dieselup.core import operating_scope as scope
from dieselup.core import driver_assignments as assignments


def run_async(fn):
    @wraps(fn)
    def run(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))
    return run


@pytest.fixture
def restricted(monkeypatch):
    monkeypatch.setattr(settings, 'TEST_TRUCK_UNITS', '6682,8089,8217')
    monkeypatch.setattr(settings, 'AUTO_LINK_ENABLED', False)


@pytest.mark.parametrize('unit,expected', [('6682',True),('08217',True),('8089',True),
    ('9999',False),(None,False),('unknown',False),('8089 driver',False)])
def test_scope_is_exact_and_normalizes_leading_zeroes(restricted,unit,expected):
    assert scope.allows(unit) is expected


def test_empty_scope_retains_normal_fleet_mode(monkeypatch):
    monkeypatch.setattr(settings,'TEST_TRUCK_UNITS','')
    assert scope.allows('9999')
    assert scope.allowed_units() is None


def test_scope_config_rejects_malformed_list():
    assert Settings(TEST_TRUCK_UNITS=' 06682,8089,6682 ').TEST_TRUCK_UNITS=='6682,8089'
    for value in ('6682,', '*', '6682 OR true', '6682,truck8089'):
        with pytest.raises(ValidationError): Settings(TEST_TRUCK_UNITS=value)


@pytest.mark.parametrize('reason',['auto','service_message','title_change'])
@run_async
async def test_disabled_automatic_link_never_looks_up_or_writes(restricted,monkeypatch,reason):
    from dieselup.bot import group_link
    lookup=AsyncMock(); send=AsyncMock()
    monkeypatch.setattr(group_link,'_existing_truck_mapping',lookup)
    monkeypatch.setattr(group_link,'_send_group',send)
    await group_link._attempt_link(bot=object(),chat_id=-1,chat_title='6682 Good Driver',reason=reason)
    lookup.assert_not_awaited(); send.assert_not_awaited()


def row(unit,chat):
    return dict(truck_unit=unit,driver_telegram_id=chat,driver_full_name='Good Driver',
        samsara_vehicle_id='v'+unit,assignment_status='ready',alerts_paused=False)


@pytest.mark.parametrize('title,verified', [('6682 Good Driver',{'6682':-1}),
    ('8217 New Driver',{}),('6682 Changed Driver',{}),('9999 Good Driver',{})])
def test_refresh_does_not_change_active_assignments(restricted,title,verified):
    rows=[row('6682',-1),row('8217',None),row('9999',-9)]
    groups={-1:title}
    vehicles=[{'id':'v'+r['truck_unit'],'name':r['truck_unit']} for r in rows]
    plan=assignments.plan_assignments(rows,groups,vehicles)
    actual=assignments.controlled_assignment_plan(rows,groups,plan)
    assert actual['changes']==[]
    assert actual['verified']==verified


def test_selected_inactive_safety_unlink_is_preserved(restricted):
    rows=[row('6682',-1),row('9999',-9)]
    groups={-1:'Home Time 6682 Good Driver',-9:'Terminated 9999 Good Driver'}
    plan=assignments.plan_assignments(rows,groups,[])
    actual=assignments.controlled_assignment_plan(rows,groups,plan)
    assert [r['truck_unit'] for r in actual['changes']]==['6682']
    assert actual['changes'][0]['driver_telegram_id'] is None


def test_existing_link_can_recover_verification_without_relinking(restricted):
    old=row('6682',-1);old['assignment_status']='conflict'
    groups={-1:'6682 Good Driver'}
    plan=assignments.plan_assignments([old],groups,[{'id':'v6682','name':'6682'}])
    actual=assignments.controlled_assignment_plan([old],groups,plan)
    assert actual['verified']=={'6682':-1}
    assert actual['changes']==[{**old,'assignment_status':'ready'}]


@run_async
async def test_capture_skips_other_trucks_and_scopes_retention(restricted,monkeypatch):
    from dieselup.core import fuel_brain as brain
    vehicles=[SimpleNamespace(id='v6682',name='6682 Good Driver'),
        SimpleNamespace(id='v9999',name='9999 Other Driver'),
        SimpleNamespace(id='unknown',name='8217 Unlinked Driver')]
    class Samsara:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def list_vehicles(self):return vehicles
    process=AsyncMock(return_value=(True,False));write=AsyncMock()
    monkeypatch.setattr(brain,'SamsaraClient',Samsara)
    monkeypatch.setattr(brain,'_last_snapshots',AsyncMock(return_value={}))
    monkeypatch.setattr(brain,'_open_fuel_events',AsyncMock(return_value={}))
    monkeypatch.setattr(brain,'_trucks_by_vehicle_id',AsyncMock(return_value={
        'v6682':row('6682',-1),'v9999':row('9999',-9)}))
    monkeypatch.setattr(brain,'_pending_events_by_unit',AsyncMock(return_value={}))
    monkeypatch.setattr(brain,'_process_vehicle',process)
    monkeypatch.setattr(brain,'execute',write)
    await brain.capture_fleet_state(None)
    assert process.await_count==1
    assert process.call_args.kwargs['vehicle'].id=='v6682'
    assert write.call_args.args[2]==['6682','8089','8217']


@run_async
async def test_periodic_refresh_only_reads_selected_groups(restricted,monkeypatch):
    from contextlib import asynccontextmanager
    from dieselup.bot import group_link
    rows=[row('6682',-1),row('9999',-9)]
    class Pool:
        async def fetch(self,*args):return rows
        @asynccontextmanager
        async def acquire(self):yield object()
    class Samsara:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def list_vehicles(self):return [SimpleNamespace(id='v6682',name='6682 Good Driver')]
    from dieselup.clients import samsara
    read=AsyncMock(return_value='6682 Good Driver');apply=AsyncMock(return_value=0)
    monkeypatch.setattr(group_link,'_read_current_group_title',read)
    monkeypatch.setattr(assignments,'get_pool',AsyncMock(return_value=Pool()))
    monkeypatch.setattr(assignments,'apply_assignments',apply)
    monkeypatch.setattr(samsara,'SamsaraClient',Samsara)
    assert await assignments._refresh_driver_assignments(None)=={'6682':-1}
    read.assert_awaited_once_with(None,-1)
    assert apply.call_args.args[2]['changes']==[]


@run_async
async def test_disabled_auto_onboard_keeps_unlinked_truck_unchanged(restricted,monkeypatch):
    from dieselup.core import load_sync
    read=AsyncMock(return_value=None);write=AsyncMock()
    monkeypatch.setattr(load_sync,'fetch_one',read)
    monkeypatch.setattr(load_sync,'execute',write)
    assert await load_sync._ensure_truck_onboarded(bot=object(),truck_unit='6682',
        samsara_by_unit={},notified_units=set()) is None
    write.assert_not_awaited()


@run_async
async def test_outside_scope_direct_entrypoints_do_no_work(restricted,monkeypatch):
    from dieselup.core import load_sync,compliance,fuel_replan,fuel_brain,dlq_retry
    read=AsyncMock();write=AsyncMock();samsara=SimpleNamespace(get_vehicle_location=AsyncMock())
    monkeypatch.setattr(compliance,'fetch_one',read)
    monkeypatch.setattr(load_sync,'fetch_one',read)
    monkeypatch.setattr(dlq_retry,'execute',write)
    assert await load_sync._process_one_load({'truck_unit':'9999'})=='skipped'
    assert await compliance._resolve_one({'truck_unit':'9999'},samsara=samsara,datatruck=None,bot=None) is None
    assert not await fuel_replan.replan_truck(None,'9999',1)
    assert await fuel_brain._process_vehicle(vehicle=SimpleNamespace(id='v9999'),samsara=samsara,
        bot=None,prev=None,open_event=None,truck_row={'truck_unit':'9999'},pending_by_unit={})==(False,False)
    assert await dlq_retry._retry_one(None,{'truck_unit':'9999'})=='suppressed'
    read.assert_not_awaited();write.assert_not_awaited();samsara.get_vehicle_location.assert_not_awaited()


@run_async
async def test_scope_is_applied_before_replan_limit(restricted,monkeypatch):
    from dieselup.core import fuel_replan
    read=AsyncMock(return_value=[])
    monkeypatch.setattr(fuel_replan,'fetch_all',read)
    await fuel_replan.run_requested_replans(None)
    query, units = read.call_args.args
    assert 'ANY($1)' in query and query.index('ANY($1)')<query.index('LIMIT 20')
    assert units==['6682','8089','8217']
