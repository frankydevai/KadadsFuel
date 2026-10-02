"""Explain a long non-fueling stop without inventing a driver's purpose."""
from datetime import datetime, timezone
import math

from dieselup.config import settings
from dieselup.core import advice_audit
from dieselup.core.optimizer import haversine_miles
from dieselup.db import fetch_all


def assess_stationary(rows, now=None):
    now = now or datetime.now(timezone.utc)
    samples=[]
    for row in rows:
        gps=row.get('gps_observed_at'); fuel=row.get('fuel_observed_at'); recorded=row.get('taken_at')
        if not all(isinstance(t,datetime) and t.tzinfo is not None for t in (gps,fuel,recorded)):
            break
        try:
            values=[float(row[k]) for k in ('fuel_pct','speed_mph','latitude','longitude')]
        except (ValueError,TypeError,KeyError):
            break
        if not all(math.isfinite(v) for v in values) or not 0<=values[0]<=100 or not -90<=values[2]<=90 or not -180<=values[3]<=180:
            break
        if not 0 <= (recorded-gps).total_seconds() <= 300 or not 0 <= (recorded-fuel).total_seconds() <= 1800:
            break
        if float(row['speed_mph'])>5 or float(row['speed_mph'])<0:
            break
        if samples:
            latest=samples[0]
            if haversine_miles(float(row['latitude']),float(row['longitude']),float(latest['latitude']),float(latest['longitude']))>.155343:
                break
            if not 0 < (samples[-1]['taken_at']-recorded).total_seconds()<=600:
                break
        samples.append(row)
    if len(samples)<3 or not 0<=(now-samples[0]['taken_at']).total_seconds()<=600:
        return None
    span=(samples[0]['taken_at']-samples[-1]['taken_at']).total_seconds()/60
    if span<60 or len({r['gps_observed_at'] for r in samples})<2 or len({r['fuel_observed_at'] for r in samples})<2:
        return None
    increase=(max(float(r['fuel_pct']) for r in samples)-min(float(r['fuel_pct']) for r in samples))/100*settings.TANK_CAPACITY_GALLONS
    if increase>=30:
        return None
    return {'method':'stationary_fresh_telemetry','duration_minutes':round(span),
        'fuel_increase_gallons':round(increase,1),'fueling_detected':False,
        'interpretation':'Possible rest or service visit; exact purpose is unconfirmed',
        'rule':'No fueling alert or driver penalty without confirmed fueling',
        'first_seen':samples[-1]['taken_at'].isoformat(),'last_seen':samples[0]['taken_at'].isoformat()}


async def record_stationary_context(vehicle_id, truck_unit, pending):
    rows=await fetch_all("""SELECT latitude,longitude,fuel_pct,speed_mph,taken_at,gps_observed_at,fuel_observed_at
        FROM truck_snapshots WHERE samsara_vehicle_id=$1 AND taken_at>NOW()-INTERVAL '85 minutes'
        ORDER BY taken_at DESC LIMIT 30""",vehicle_id)
    assessment=assess_stationary([dict(r) for r in rows])
    if assessment:
        from dieselup.core.fuel_brain import _advised_stop_from_pending
        stop=_advised_stop_from_pending(pending) or {}
        if stop.get('latitude') is not None and stop.get('longitude') is not None:
            assessment['at_advised_stop']=haversine_miles(float(rows[0]['latitude']),float(rows[0]['longitude']),float(stop['latitude']),float(stop['longitude']))<=.155343
        assessment['latitude']=float(rows[0]['latitude'])
        assessment['longitude']=float(rows[0]['longitude'])
        hour=int(datetime.now(timezone.utc).timestamp()//3600)
        await advice_audit.record('stationary_no_fuel',truck_unit=truck_unit,
            event_id=pending['id'] if pending else None,
            key=f'non_fueling:{vehicle_id}:{hour}',details=assessment)
