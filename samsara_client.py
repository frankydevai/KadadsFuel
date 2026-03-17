"""
samsara_client.py  -  Fetch vehicle locations and fuel levels from Samsara API.
"""

import requests
from config import SAMSARA_API_TOKEN, SAMSARA_BASE_URL

HEADERS = {
    "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
    "Content-Type":  "application/json",
}


def _get(endpoint: str, params: dict = None) -> dict:
    url  = f"{SAMSARA_BASE_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_vehicle_locations() -> list[dict]:
    """Fetch current vehicle locations from Samsara."""
    url  = "https://api.samsara.com/fleet/vehicles/locations"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    # Log any trucks with stale locations (>2 hours old)
    import logging
    log = logging.getLogger(__name__)
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    for v in data:
        loc = v.get("location", {})
        ts  = loc.get("time", "")
        if ts:
            try:
                t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_min = (now - t).total_seconds() / 60
                if age_min > 120:
                    log.warning(f"Truck {v.get('name')} GPS stale: {age_min:.0f} min old ({loc.get('reverseGeo',{}).get('formattedLocation','')})")
            except Exception:
                pass
    return data


def get_vehicle_stats() -> list[dict]:
    """Fetch current fuel levels — use /history endpoint with short window for latest reading."""
    url    = "https://api.samsara.com/fleet/vehicles/stats"
    params = {"types": "fuelPercents"}
    resp   = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", [])


def get_driver_for_vehicle(vehicle_id: str) -> dict | None:
    try:
        url  = f"https://api.samsara.com/fleet/vehicles/{vehicle_id}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.json().get("data", {}).get("currentDriver")
    except Exception:
        return None


def get_combined_vehicle_data() -> list[dict]:
    """
    Merge locations + fuel stats into one list per vehicle.
    Returns list of dicts with: vehicle_id, vehicle_name, lat, lng,
    heading, speed_mph, fuel_pct.
    """
    locations_raw = get_vehicle_locations()
    stats_raw     = get_vehicle_stats()

    # Index fuel stats by vehicle id
    stats_map = {}
    for s in stats_raw:
        vid = s.get("id")
        if not vid:
            continue
        # /stats endpoint returns fuelPercents as list with single current reading
        fuel_events = s.get("fuelPercents", [])
        if fuel_events:
            # Get most recent reading
            latest = max(fuel_events, key=lambda x: x.get("time", ""))
            val = latest.get("value")
            stats_map[vid] = float(val) if val is not None else 100.0
        else:
            stats_map[vid] = 100.0

    results = []
    for v in locations_raw:
        vid  = v.get("id")
        name = v.get("name", vid)
        loc  = v.get("location", {})
        lat  = loc.get("latitude")
        lng  = loc.get("longitude")

        if lat is None or lng is None:
            continue

        driver      = get_driver_for_vehicle(vid)
        driver_name = driver.get("name") if driver else None

        results.append({
            "vehicle_id":   vid,
            "vehicle_name": name,
            "driver_name":  driver_name,
            "lat":          float(lat),
            "lng":          float(lng),
            "heading":      float(loc.get("heading", 0)),
            "speed_mph":    float(loc.get("speed", 0)),
            "fuel_pct":     stats_map.get(vid, 100.0),
        })

    return results
