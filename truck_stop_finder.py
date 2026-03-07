"""
truck_stop_finder.py  -  Find the best 2 diesel stops for a truck.

SCORING:
  Uses true cost formula — not just cheapest price or nearest stop.

  true_cost = (diesel_price × gallons_to_fill)
            + (detour_miles × 2 × diesel_price / mpg)

  This means a stop that is cheaper but far off-route might actually
  cost MORE than a slightly pricier stop that is directly on the route.

CORRIDOR:
  For MOVING trucks, only considers stops within CORRIDOR_WIDTH_MILES
  either side of the truck's heading direction, up to SEARCH_CORRIDOR_MILES.
  Stops behind the truck get a distance penalty instead of being excluded,
  to handle bad heading data from Samsara gracefully.

URGENCY TIERS:
  35–26%  ADVISORY   Search full corridor, price-optimized
  25–16%  WARNING    Shorter corridor, still price-optimized
  15–10%  CRITICAL   Nearest reachable stop, price ignored
  <10%    EMERGENCY  Absolute nearest stop, price ignored
"""

import math
import logging
from config import (
    SEARCH_CORRIDOR_MILES,
    CORRIDOR_WIDTH_MILES,
    BEHIND_PENALTY_MILES,
    DEFAULT_TANK_GAL,
    DEFAULT_MPG,
    SAFETY_RESERVE,
    FUEL_ALERT_THRESHOLD_PCT,
)
from database import get_all_diesel_stops

log = logging.getLogger(__name__)

EARTH_RADIUS_MILES  = 3958.8
_PARKED_SPEED_MPH   = 5
_AT_STOP_RADIUS     = 0.35   # miles — truck is in the lot
_AHEAD_ARC_DEGREES  = 120    # 60° left and right of heading


# -- Geo math -----------------------------------------------------------------

def haversine_miles(lat1, lng1, lat2, lng2) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return EARTH_RADIUS_MILES * 2 * math.asin(math.sqrt(a))


def bearing(lat1, lng1, lat2, lng2) -> float:
    """Compass bearing in degrees from point 1 to point 2 (0-360)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlam = math.radians(lng2 - lng1)
    x = math.sin(dlam) * math.cos(phi2)
    y = math.cos(phi1)*math.sin(phi2) - math.sin(phi1)*math.cos(phi2)*math.cos(dlam)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def angle_diff(a, b) -> float:
    """Smallest angle between two bearings (0-180)."""
    d = abs(a - b) % 360
    return d if d <= 180 else 360 - d


def perpendicular_distance(truck_lat, truck_lng, truck_heading,
                            stop_lat, stop_lng) -> float:
    """
    Approximate perpendicular (cross-track) distance from the truck's
    route line to the stop, in miles. Used as detour estimate.
    """
    dist = haversine_miles(truck_lat, truck_lng, stop_lat, stop_lng)
    bear = bearing(truck_lat, truck_lng, stop_lat, stop_lng)
    angle = math.radians(angle_diff(truck_heading, bear))
    return abs(dist * math.sin(angle))


# -- Urgency tiers ------------------------------------------------------------

def get_urgency(fuel_pct: float) -> str:
    if fuel_pct <= 10:
        return "EMERGENCY"
    if fuel_pct <= 15:
        return "CRITICAL"
    if fuel_pct <= 25:
        return "WARNING"
    return "ADVISORY"


def get_search_radius(urgency: str) -> float:
    return {
        "ADVISORY":  SEARCH_CORRIDOR_MILES,
        "WARNING":   150.0,
        "CRITICAL":  80.0,
        "EMERGENCY": 50.0,
    }[urgency]


# -- Usable range -------------------------------------------------------------

def usable_gallons(fuel_pct: float, tank_gal: float) -> float:
    """Gallons available above the safety reserve."""
    reserve = tank_gal * SAFETY_RESERVE
    available = (fuel_pct / 100.0) * tank_gal
    return max(0.0, available - reserve)


def gallons_to_fill(fuel_pct: float, tank_gal: float) -> float:
    """Gallons needed to fill tank to 100%."""
    current = (fuel_pct / 100.0) * tank_gal
    return max(0.0, tank_gal - current)


# -- True cost scoring --------------------------------------------------------

def true_cost(stop: dict, truck_lat: float, truck_lng: float,
              truck_heading: float, fuel_pct: float,
              tank_gal: float, mpg: float) -> float:
    """
    Total cost in dollars to fill up at this stop, including detour fuel penalty.

    true_cost = fill_cost + detour_penalty
    fill_cost = price × gallons_to_fill
    detour_penalty = detour_miles × 2 × price / mpg  (there and back)
    """
    price = stop.get("diesel_price")
    if not price or price <= 0:
        return float("inf")

    slat = float(stop["latitude"])
    slng = float(stop["longitude"])

    fill_gal     = gallons_to_fill(fuel_pct, tank_gal)
    fill_cost    = price * fill_gal

    detour_mi    = perpendicular_distance(truck_lat, truck_lng, truck_heading, slat, slng)
    detour_cost  = detour_mi * 2 * price / mpg

    return round(fill_cost + detour_cost, 2)


# -- Main finder --------------------------------------------------------------

def find_best_stops(
    truck_lat: float,
    truck_lng: float,
    truck_heading: float,
    speed_mph: float,
    fuel_pct: float,
    tank_gal: float = DEFAULT_TANK_GAL,
    mpg: float = DEFAULT_MPG,
) -> tuple[dict | None, dict | None]:
    """
    Find the best 2 diesel stops for a truck.

    Returns (best_stop, alt_stop) — either can be None.
    Each stop dict has extra keys: distance_miles, detour_miles,
    fill_cost, true_cost, google_maps_url.
    """
    all_stops = get_all_diesel_stops()
    if not all_stops:
        log.warning("No diesel stops in database.")
        return None, None

    parked   = speed_mph <= _PARKED_SPEED_MPH
    urgency  = get_urgency(fuel_pct)
    radius   = get_search_radius(urgency)
    price_matters = urgency in ("ADVISORY", "WARNING")

    log.info(f"Stop finder: urgency={urgency} radius={radius:.0f}mi "
             f"parked={parked} stops_in_db={len(all_stops)}")

    # -- Already at a stop? ---------------------------------------------------
    if parked:
        for stop in all_stops:
            dist = haversine_miles(truck_lat, truck_lng,
                                   float(stop["latitude"]), float(stop["longitude"]))
            if dist <= _AT_STOP_RADIUS:
                log.info(f"Truck already at {stop['store_name']} ({dist*5280:.0f} ft)")
                return None, None  # signal: already at stop, no alert needed

    # -- Score all stops in range --------------------------------------------
    candidates = []
    for stop in all_stops:
        slat = float(stop["latitude"])
        slng = float(stop["longitude"])

        dist = haversine_miles(truck_lat, truck_lng, slat, slng)
        if dist > radius:
            continue

        # For moving trucks: penalise stops behind rather than exclude
        # (handles bad heading data gracefully)
        ahead = True
        if not parked and truck_heading is not None:
            bear  = bearing(truck_lat, truck_lng, slat, slng)
            ahead = angle_diff(truck_heading, bear) <= _AHEAD_ARC_DEGREES

        detour_mi = perpendicular_distance(
            truck_lat, truck_lng, truck_heading or 0, slat, slng
        ) if not parked else 0.0

        fill_gal  = gallons_to_fill(fuel_pct, tank_gal)
        fill_cost = (stop["diesel_price"] or 0) * fill_gal
        tc        = true_cost(stop, truck_lat, truck_lng,
                              truck_heading or 0, fuel_pct, tank_gal, mpg)

        # Penalty for stops behind truck
        score = tc if price_matters else dist
        if not ahead:
            score += BEHIND_PENALTY_MILES * (stop.get("diesel_price") or 4.0)

        candidates.append({
            **stop,
            "distance_miles":  round(dist, 2),
            "detour_miles":    round(detour_mi, 2),
            "fill_cost":       round(fill_cost, 2),
            "true_cost":       tc,
            "_score":          score,
            "_ahead":          ahead,
            "google_maps_url": f"https://maps.google.com/?q={slat},{slng}",
        })

    if not candidates:
        log.warning(f"No stops found within {radius:.0f} miles.")
        return None, None

    # Sort by score (true cost for price-matters, distance for critical/emergency)
    candidates.sort(key=lambda s: s["_score"])

    best = candidates[0]

    log.info(f"Best: {best['store_name']} {best['distance_miles']:.1f}mi "
             f"${best.get('diesel_price','?')}/gal  true_cost=${best['true_cost']:.2f}")

    return best, None


def calc_savings(best: dict, alt: dict) -> float | None:
    return None


def is_near_stop(truck_lat, truck_lng, stop_lat, stop_lng,
                 radius_miles=None) -> bool:
    r = radius_miles or _AT_STOP_RADIUS
    return haversine_miles(truck_lat, truck_lng, stop_lat, stop_lng) <= r
