"""Install before the dashboard's StaticFiles mount, using its existing auth."""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from telegram import Bot
from dieselup.clients.samsara import SamsaraClient
from dieselup.config import settings
from dieselup.core.operating_scope import allows, status as operating_scope_status
from dieselup.core.driver_assignments import connect_driver_group, parse_title, key, refresh_driver_assignments

class ConnectionBody(BaseModel):
    telegram_group_id: int = Field(lt=0, ge=-(2**52), strict=True)


def install_driver_connections(app, check_auth, get_pool):
    router = APIRouter(dependencies=[Depends(check_auth)])

    @router.get('/api/drivers')
    async def drivers():
        pool = await get_pool()
        rows = await pool.fetch('''
            SELECT td.truck_unit AS truck_id, td.truck_unit AS unit_number,
                td.driver_full_name AS driver_name, td.driver_telegram_id AS telegram_group_id,
                td.telegram_group_name, td.samsara_vehicle_id, td.alerts_paused,
                td.assignment_status, td.updated_at AS assignment_updated_at,
                ts.fuel_pct, ts.speed_mph, ts.latitude, ts.longitude, ts.taken_at AS last_seen_at,
                se.candidates::jsonb -> 0 ->> 'station_name' AS stop_name
            FROM trucks_drivers td
            LEFT JOIN LATERAL (
                SELECT fuel_pct,speed_mph,latitude,longitude,taken_at FROM truck_snapshots
                WHERE truck_unit=td.truck_unit ORDER BY taken_at DESC LIMIT 1
            ) ts ON TRUE
            LEFT JOIN LATERAL (
                SELECT candidates FROM stop_events WHERE truck_unit=td.truck_unit AND status='pending'
                ORDER BY recommended_at DESC LIMIT 1
            ) se ON TRUE ORDER BY td.truck_unit
        ''')
        now = datetime.now(timezone.utc)
        data = []
        for raw in rows:
            r = dict(raw)
            seen = r['last_seen_at']
            r['telemetry_stale'] = not seen or (now-seen).total_seconds() > 1800
            r['status'] = 'Stale' if r['telemetry_stale'] else 'Rolling' if float(r['speed_mph'] or 0)>2 else 'Idle'
            r['automatic_processing_enabled'] = allows(r['unit_number'])
            data.append(r)
        return jsonable_encoder({'drivers': data, 'fetched_at': now, 'operating_scope': operating_scope_status()})

    @router.post('/api/drivers/refresh')
    async def refresh():
        try:
            async with Bot(settings.TELEGRAM_BOT_TOKEN) as bot:
                verified = await refresh_driver_assignments(bot)
        except Exception as exc:
            raise HTTPException(503, 'Group refresh could not finish. Inactive groups may already be disconnected. Refresh page data and retry shortly.') from exc
        return {'ok': True, 'verified_connections': len(verified)}

    @router.put('/api/drivers/{truck_unit}/connection')
    async def connect(truck_unit: str, body: ConnectionBody):
        pool = await get_pool()
        target = await pool.fetchrow('SELECT * FROM trucks_drivers WHERE truck_unit=$1',truck_unit)
        if target is None:
            raise HTTPException(404, 'Truck not found')
        try:
            async with Bot(settings.TELEGRAM_BOT_TOKEN) as bot:
                chat = await bot.get_chat(body.telegram_group_id)
                title = str(chat.title or '')
                parsed = parse_title(title)
                if not parsed or key(parsed[0])!=key(truck_unit):
                    raise HTTPException(409, 'Group title must contain this truck number and the driver name.')
                async with SamsaraClient() as samsara:
                    matches = await samsara.find_vehicles_by_unit(truck_unit)
                prior = [v for v in matches if str(v.id)==str(target['samsara_vehicle_id'])]
                selected = prior if len(prior)==1 else matches
                if len(selected)!=1:
                    raise HTTPException(409, 'Truck identity needs review in Samsara.')
                await connect_driver_group(bot, body.telegram_group_id, title, truck_unit, str(selected[0].id))
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(409,str(exc)) from exc
        except Exception as exc:
            raise HTTPException(503,'Could not verify this group and truck. No connection was made.') from exc
        return {'ok':True,'connected':True}

    @router.delete('/api/drivers/{truck_unit}/connection')
    async def disconnect(truck_unit: str):
        pool = await get_pool()
        result = await pool.execute('''UPDATE trucks_drivers
            SET driver_telegram_id=NULL,telegram_group_name=NULL,
                assignment_status='unlinked',updated_at=NOW() WHERE truck_unit=$1''',truck_unit)
        if result!='UPDATE 1':
            raise HTTPException(404,'Truck not found')
        return {'ok':True,'connected':False}

    paths={'/api/drivers','/api/drivers/refresh','/api/drivers/{truck_unit}/connection'}
    app.router.routes[:]=[r for r in app.router.routes if getattr(r,'path',None) not in paths]
    app.include_router(router)
