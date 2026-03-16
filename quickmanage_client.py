"""
quickmanage_client.py — QuickManage TMS integration

Fetches active trips from QuickManage API and maps them to truck numbers.
Used by FleetFuel AI to get the actual route (origin → delivery) for each truck
so fuel stops are recommended along the real route, not just GPS heading direction.

API endpoints used:
  POST /x/trips/search — search/list trips with filters
"""

import logging
import requests
from functools import lru_cache
from config import QUICKMANAGE_API_KEY, QUICKMANAGE_API_URL

log = logging.getLogger(__name__)

_HEADERS = {
    "Authorization": f"Bearer {QUICKMANAGE_API_KEY}",
    "Content-Type":  "application/json",
}

# Trip statuses where truck is actually moving
# upcoming   = load booked, truck not yet dispatched — skip
# dispatched = truck heading to first pickup
# in_transit = truck heading to next stop (pickup or delivery)
# delivered  = load done — skip
# canceled   = canceled — skip
_ACTIVE_STATUSES  = {"dispatched", "in_transit"}


# ---------------------------------------------------------------------------
# Geocoding — QM stops have address but no lat/lng
# ---------------------------------------------------------------------------

@lru_cache(maxsize=512)
def _geocode(city: str, state: str, zip_code: str) -> tuple[float, float] | None:
    """
    Convert city/state/zip to lat/lng using OpenStreetMap Nominatim (free, no key).
    Results are cached in memory for the process lifetime.
    """
    query = f"{zip_code}, {city}, {state}, US"
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1},
            headers={"User-Agent": "FleetFuelAI/1.0"},
            timeout=5,
        )
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        log.warning(f"Geocode failed for {query}: {e}")
    return None


def _stop_coords(stop: dict) -> tuple[float, float] | None:
    """Extract lat/lng from a stop — geocodes from address if not present."""
    # QM API doesn't return lat/lng in stop data — geocode from address
    addr = stop.get("address") or {}
    city     = addr.get("city", "").strip()
    state    = addr.get("state", "").strip()
    zip_code = addr.get("zip_code", "").strip()
    if city and state:
        return _geocode(city, state, zip_code)
    return None


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _search_trips(filters: list[dict], page_size: int = 50) -> list[dict]:
    """Call /x/trips/search and return list of trip items."""
    payload = {
        "query":     "",
        "filters":   filters,
        "page":      0,
        "page_size": page_size,
    }
    try:
        resp = requests.post(
            f"{QUICKMANAGE_API_URL}/x/trips/search",
            json=payload,
            headers=_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("items", [])
    except Exception as e:
        log.error(f"QuickManage API error: {e}")
        return []


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_active_trips() -> list[dict]:
    """
    Fetch all active trips (upcoming + in_progress).
    Returns list of raw trip dicts from QM API.
    """
    trips = _search_trips(filters=[])
    active = [t for t in trips if t.get("status", "").lower() in _ACTIVE_STATUSES]
    log.info(f"QuickManage: {len(trips)} total trips, {len(active)} active")
    return active


def get_route_for_truck(truck_number: str) -> dict | None:
    """
    Find the active trip for a given truck number and extract its route.

    Returns a route dict:
    {
        "trip_id":       str,
        "trip_num":      int,
        "ref_number":    str,
        "truck_number":  str,
        "status":        str,
        "stops": [
            {
                "pickup":       bool,
                "company_name": str,
                "city":         str,
                "state":        str,
                "zip":          str,
                "lat":          float | None,
                "lng":          float | None,
                "appt":         str,   # ISO datetime
            },
            ...
        ],
        "origin":      {"lat": float, "lng": float, "city": str, "state": str},
        "destination": {"lat": float, "lng": float, "city": str, "state": str},
    }
    Returns None if no active trip found for this truck.
    """
    trips = get_active_trips()

    for trip in trips:
        stops = trip.get("stops") or []
        for stop in stops:
            truck = stop.get("assigned_truck") or {}
            if str(truck.get("number", "")).strip() == str(truck_number).strip():
                return _build_route(trip, truck_number)

    log.info(f"QuickManage: no active trip found for truck {truck_number}")
    return None


def get_all_truck_routes() -> dict[str, dict]:
    """
    Returns a mapping of truck_number → route for all active trips.
    Used during each poll cycle to update route info for all trucks.
    """
    trips  = get_active_trips()
    routes = {}

    for trip in trips:
        stops = trip.get("stops") or []
        # Find truck number from first stop with a real truck assigned
        truck_number = None
        for stop in stops:
            truck = stop.get("assigned_truck") or {}
            num = str(truck.get("number", "")).strip()
            if num and num != "0" and truck.get("id") != "00000000-0000-0000-0000-000000000000":
                truck_number = num
                break
        if truck_number:
            route = _build_route(trip, truck_number)
            if route:
                routes[truck_number] = route

    log.info(f"QuickManage: built routes for {len(routes)} trucks")
    return routes


def _build_route(trip: dict, truck_number: str) -> dict | None:
    """Build a clean route dict from a raw QM trip."""
    stops_raw = trip.get("stops") or []
    if len(stops_raw) < 2:
        return None

    stops = []
    for s in stops_raw:
        addr  = s.get("address") or {}
        city  = addr.get("city", "").strip()
        state = addr.get("state", "").strip()
        zip_  = addr.get("zip_code", "").strip()
        coords = _stop_coords(s)

        stops.append({
            "pickup":       bool(s.get("pickup")),
            "company_name": s.get("company_name", ""),
            "city":         city,
            "state":        state,
            "zip":          zip_,
            "lat":          coords[0] if coords else None,
            "lng":          coords[1] if coords else None,
            "appt":         s.get("appointment_date", ""),
        })

    status = trip.get("status", "").lower()

    # Origin = first pickup stop with coords
    origin = next((s for s in stops if s["pickup"] and s["lat"]), None)

    # Destination depends on status:
    # dispatched  → truck heading to first pickup address
    # in_transit  → truck heading to next undelivered stop
    #               (could be a 2nd pickup or any delivery)
    #               We take the first stop that is NOT the first pickup
    if status == "dispatched":
        # Heading to first pickup
        dest = next((s for s in stops if s["pickup"] and s["lat"]), None)
    else:
        # in_transit — find next stop after first pickup
        # First pickup is the origin — next stop is the current destination
        pickup_passed = False
        dest = None
        for s in stops:
            if s["pickup"] and not pickup_passed:
                pickup_passed = True
                continue  # skip first pickup — already done
            if s["lat"]:
                dest = s
                break
        # Fallback — last stop with coords
        if not dest:
            dest = next((s for s in reversed(stops) if s["lat"]), None)

    if not origin or not dest:
        log.warning(f"Trip {trip.get('trip_num')} — could not resolve origin/destination coords")
        return None

    return {
        "trip_id":      trip.get("id", ""),
        "trip_num":     trip.get("trip_num"),
        "ref_number":   trip.get("ref_number", ""),
        "truck_number": truck_number,
        "status":       trip.get("status", ""),
        "stops":        stops,
        "origin": {
            "lat":   origin["lat"],
            "lng":   origin["lng"],
            "city":  origin["city"],
            "state": origin["state"],
        },
        "destination": {
            "lat":   dest["lat"],
            "lng":   dest["lng"],
            "city":  dest["city"],
            "state": dest["state"],
        },
    }
