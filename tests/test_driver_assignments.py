import asyncio
from copy import deepcopy
from contextlib import asynccontextmanager
import pytest
from dieselup.core.driver_assignments import plan_assignments, parse_title, apply_assignments, FIELDS

def row(unit, chat=None, paused=False):
    return dict(truck_unit=unit,driver_telegram_id=chat,driver_full_name='Old Driver',
                samsara_vehicle_id='v'+unit,telegram_group_name=None,
                assignment_status='ready',alerts_paused=paused,updated_at=None)

def plan(rows, groups, vehicles=None):
    return plan_assignments(rows,groups,vehicles or [{'id':'v'+r['truck_unit'],'name':r['truck_unit']} for r in rows])

@pytest.mark.parametrize('title,expected',[
    ('0568 Steve Charitable I 30%',('0568','Steve Charitable')),
    ('0470 Peterson Joram 75 CPM',('0470','Peterson Joram')),
    ('OWNER OP 122 Max Anderson Louis | 12%',('122','Max Anderson Louis')),
    ('402651 OWNER OP Cefoua Estael',('402651','Cefoua Estael')),
    ('*Terminated* 3664 Mauro |30%',None),('Home Time 0568 Old Driver',None),
    ('Kadads || DieselUp',None),('Driver Name |30%',None),('8143',None),
])
def test_title(title,expected): assert parse_title(title)==expected

def test_new_truck_move_uses_vehicle_identity():
    p=plan([row('0814',-10)],{-10:'630862 New Driver |85%'},[{'id':'new-vehicle','name':'630862'}])
    changes={r['truck_unit']:r for r in p['changes']}
    assert changes['0814']['driver_telegram_id'] is None
    assert changes['630862']['samsara_vehicle_id']=='new-vehicle'
    assert p['verified']=={'630862':-10}

def test_active_driver_replaces_terminated_group():
    p=plan([row('100',-1),row('200',-2)],{-1:'*Terminated* 100 Old Driver',-2:'100 New Driver'})
    changes={r['truck_unit']:r for r in p['changes']}
    assert changes['100']['driver_telegram_id']==-2
    assert changes['200']['driver_telegram_id'] is None
    assert p['verified']=={'100':-2}

def test_two_active_claims_are_not_linked():
    p=plan([row('100',-1),row('200',-2)],{-1:'100 Alpha Driver',-2:'100 Beta Driver'})
    assert not p['verified']
    assert {r['driver_telegram_id'] for r in p['changes']}=={-1,-2}
    assert all(r['assignment_status']=='conflict' for r in p['changes'])

def test_swaps_are_order_independent():
    rows=[row('100',-1),row('200',-2)]
    assert plan(rows,{-1:'200 Alpha Driver',-2:'100 Beta Driver'})['verified']==plan(rows,{-2:'100 Beta Driver',-1:'200 Alpha Driver'})['verified']=={'200':-1,'100':-2}

def test_failed_dependency_does_not_evict_incumbent():
    p=plan([row('100',-1),row('200',-2)],{-1:'200 Alpha Driver',-2:'999 Beta Driver'})
    assert not p['verified']
    assert {r['driver_telegram_id'] for r in p['changes']}=={-1,-2}

def test_unreadable_group_is_retained_for_review():
    p=plan([row('100',-1)],{-1:None})
    assert p['changes'][0]['driver_telegram_id']==-1
    assert p['changes'][0]['assignment_status']=='conflict'
    assert not p['verified']

def test_existing_pause_and_no_fuel_title_are_respected():
    p=plan([row('100',-1,True),row('200',-2)],{-1:'100 Alpha Driver',-2:'200 Beta Driver | NO GAS'})
    assert not p['verified']
    assert all(r['alerts_paused'] for r in p['changes'])

def test_duplicate_samsara_names_keep_existing_identity():
    p=plan([row('100',-1)],{-1:'100 Alpha Driver'},[{'id':'v100','name':'100'},{'id':'other','name':'100'}])
    assert p['verified']=={'100':-1}
    assert p['changes'][0]['samsara_vehicle_id']=='v100'

def test_unknown_truck_with_duplicate_vehicles_is_blocked():
    p=plan([row('100',-1)],{-1:'200 Alpha Driver'},[{'id':'v1','name':'200'},{'id':'v2','name':'200'}])
    assert not p['verified']
    assert p['issues'][0]['reason']=='vehicle_not_unique'

def test_unchanged_refresh_is_idempotent():
    p=plan([row('100',-1)],{-1:'100 Alpha Driver'})
    assert plan(p['changes'],{-1:'100 Alpha Driver'})['changes']==[]

class Connection:
    def __init__(self, rows): self.rows=deepcopy(rows);self.operations=[]
    @asynccontextmanager
    async def transaction(self):
        before=deepcopy(self.rows)
        try: yield
        except Exception:
            self.rows=before
            raise
    async def fetch(self,sql): return self.rows
    async def execute(self,sql,*args):
        self.operations.append(sql)
        if sql.startswith('UPDATE'):
            next(r for r in self.rows if r['truck_unit']==args[0])['driver_telegram_id']=None
        else:
            new=dict(zip(FIELDS,args))
            assert not any(r['driver_telegram_id']==new['driver_telegram_id'] for r in self.rows if new['driver_telegram_id'] is not None)
            self.rows=[r for r in self.rows if r['truck_unit']!=new['truck_unit']]+[new]

def test_atomic_apply_clears_all_links_before_swapping():
    rows=[row('100',-1),row('200',-2)];conn=Connection(rows)
    assert asyncio.run(apply_assignments(conn,rows,plan(rows,{-1:'200 Alpha Driver',-2:'100 Beta Driver'})))==2
    assert all(s.startswith('UPDATE') for s in conn.operations[:2])

def test_concurrent_edit_is_not_overwritten():
    rows=[row('100',-1)];conn=Connection(rows);conn.rows[0]['driver_full_name']='Dispatcher Edit'
    with pytest.raises(RuntimeError,match='changed during refresh'):
        asyncio.run(apply_assignments(conn,rows,plan(rows,{-1:'100 Alpha Driver'})))
    assert not conn.operations

def test_new_unit_insert_never_overwrites_concurrent_assignment():
    rows=[];conn=Connection(rows)
    p=plan(rows,{-1:'100 New Driver'},[{'id':'v100','name':'100'}])
    async def execute(sql,*args):conn.operations.append(sql)
    conn.execute=execute
    asyncio.run(apply_assignments(conn,rows,p))
    assert 'ON CONFLICT' not in conn.operations[-1]

def test_two_new_unit_aliases_cannot_claim_same_vehicle():
    p=plan([],{-1:'100 Alpha Driver',-2:'200 Beta Driver'},[{'id':'same','name':'SUBUNIT 100 (200)'}])
    assert not p['verified']
    assert not p['changes']

def test_refresh_loop_persists_titles_and_never_sends_messages(monkeypatch):
    from types import SimpleNamespace
    from dieselup.core import driver_assignments as module
    rows=[row('100',-1)];conn=Connection(rows)
    class Pool:
        async def fetch(self,*args):return deepcopy(rows)
        @asynccontextmanager
        async def acquire(self):yield conn
    class Bot:
        async def get_chat(self,chat_id):return SimpleNamespace(title='100 Current Driver |30%')
    class Samsara:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def list_vehicles(self):return [SimpleNamespace(id='v100',name='100')]
    async def pool():return Pool()
    monkeypatch.setattr(module,'get_pool',pool)
    from dieselup.clients import samsara
    monkeypatch.setattr(samsara,'SamsaraClient',Samsara)
    assert asyncio.run(module.refresh_driver_assignments(Bot()))=={'100':-1}
    assert conn.rows[0]['telegram_group_name']=='100 Current Driver |30%'
    assert conn.rows[0]['driver_full_name']=='Current Driver'

@pytest.mark.parametrize('title', ['*Home Time* 100 Old Driver', '* Terminated * 100 Old Driver', 'HOME-TIME', 'HOME_TIME'])
def test_inactive_title_unlinks_and_is_excluded_from_verified_connections(title):
    p = plan([row('100', -1)], {-1: title})
    assert p['verified'] == {}
    assert len(p['changes']) == 1
    changed = p['changes'][0]
    assert changed['driver_telegram_id'] is None
    assert changed['driver_full_name'] is None
    assert changed['assignment_status'] == 'unlinked'
    assert changed['telegram_group_name'] == title
    assert changed['samsara_vehicle_id'] == 'v100'
    assert plan(p['changes'], {-1: title})['changes'] == []


@pytest.mark.parametrize('title', ['Home Time', 'TERMINATED'])
def test_inactive_connection_is_removed_even_when_samsara_fails(monkeypatch, title):
    from types import SimpleNamespace
    from dieselup.core import driver_assignments as module
    from dieselup.clients import samsara
    rows = [row('100', -1), row('200', -2)]
    conn = Connection(rows)
    class Pool:
        async def fetch(self, *args): return deepcopy(rows)
        @asynccontextmanager
        async def acquire(self): yield conn
    class Bot:
        async def get_chat(self, chat_id):
            return SimpleNamespace(title=title if chat_id == -1 else '200 Active Driver')
    class BrokenSamsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def list_vehicles(self): raise RuntimeError('Vehicle service unavailable')
    async def pool(): return Pool()
    monkeypatch.setattr(module, 'get_pool', pool)
    monkeypatch.setattr(samsara, 'SamsaraClient', BrokenSamsara)
    with pytest.raises(RuntimeError, match='Vehicle service unavailable'):
        asyncio.run(module.refresh_driver_assignments(Bot()))
    actual = {r['truck_unit']: r for r in conn.rows}
    assert actual['100']['driver_telegram_id'] is None
    assert actual['100']['assignment_status'] == 'unlinked'
    assert actual['200'] == rows[1]
