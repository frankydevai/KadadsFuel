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
    url  = "https://api.samsara.com/fleet/vehicles/locations"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", [])


def get_vehicle_stats() -> list[dict]:
    url    = "https://api.samsara.com/fleet/vehicles/stats/feed"
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
        vid         = s.get("id")
        fuel_events = s.get("fuelPercents", [])
        if vid and fuel_events:
            latest         = max(fuel_events, key=lambda x: x.get("time", ""))
            stats_map[vid] = float(latest.get("value", 100))
        elif vid:
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
