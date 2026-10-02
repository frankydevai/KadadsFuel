"""Record a physical visit separately from a confirmed fuel purchase."""
from datetime import datetime, timezone
import math

from dieselup.config import settings
from dieselup.core import advice_audit
from dieselup.core.optimizer import haversine_miles

VISIT_RADIUS_MILES = 0.155343  # 250 metres around the verified station location
MIN_DWELL_SECONDS = 120
MAX_OBSERVATION_GAP_SECONDS = 600


def observation(location, coords, now=None):
    now = now or datetime.now(timezone.utc)
    stamp = getattr(location, "gps_time", None)
    speed = getattr(location, "speed_mph", None)
    if not isinstance(stamp, datetime) or stamp.tzinfo is None or speed is None:
        return None
    age = (now-stamp).total_seconds()
    if not 0 <= age <= settings.MAX_ADVICE_GPS_AGE_MINUTES*60:
        return None
    values = [location.lat, location.lng, coords["latitude"], coords["longitude"], speed]
    if not all(isinstance(v, (int,float)) and math.isfinite(v) for v in values):
        return None
    if not (-90 <= values[0] <= 90 and -180 <= values[1] <= 180 and -90 <= values[2] <= 90 and -180 <= values[3] <= 180):
        return None
    if not 0 <= speed <= 5 or haversine_miles(*values[:4]) > VISIT_RADIUS_MILES:
        return None
    return {"gps_time": stamp.isoformat(), "latitude": location.lat, "longitude": location.lng,
            "speed_mph": speed, "radius_metres": 250}


def dwell_evidence(first, second):
    try:
        gap = (datetime.fromisoformat(second["gps_time"])-datetime.fromisoformat(first["gps_time"])).total_seconds()
    except (ValueError, KeyError, TypeError):
        return None
    if not MIN_DWELL_SECONDS <= gap <= MAX_OBSERVATION_GAP_SECONDS:
        return None
    return {"method": "fresh_gps_dwell", "first_gps_time": first["gps_time"],
            "last_gps_time": second["gps_time"], "dwell_seconds": round(gap), "radius_metres": 250,
            "fueling_confirmed": False}


async def observe_visit(event_id, location, coords):
    current = observation(location, coords)
    if current is None:
        return
    prior = await advice_audit.fetch_one("""SELECT details FROM fuel_advice_audit
        WHERE stop_event_id=$1 AND kind='visit_observed'
          AND created_at>NOW()-INTERVAL '10 minutes' ORDER BY id LIMIT 1""", event_id)
    await advice_audit.record("visit_observed", event_id=event_id,
        key=f"visit_observed:{event_id}:{current['gps_time']}", details=current)
    if prior:
        import json
        first = prior["details"]
        first = json.loads(first) if isinstance(first, str) else first
        evidence = dwell_evidence(first, current)
        if evidence:
            await advice_audit.record("stop_visited", event_id=event_id,
                                      key=f"visited:{event_id}", details=evidence)
