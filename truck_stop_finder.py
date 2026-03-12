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
_PARKED_SPEED_MPH   = 10   # trucks showing up to 10mph due to GPS drift while parked
_AT_STOP_RADIUS     = 0.5   # miles — truck is in the lot (raised from 0.35 for large truck stops)
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


def get_search_radius(urgency: str, fuel_range_miles: float = 0, fuel_pct: float = 100) -> float:
    """Search up to 80% of actual fuel range, capped by urgency.
    Below 30% fuel: hard cap at 80 miles — don't risk sending driver too far."""
    if fuel_pct < 30:
        return min(fuel_range_miles * 0.80, 100.0) if fuel_range_miles > 0 else 100.0

    max_by_urgency = {
        "ADVISORY":  250.0,   # 35-26% — search most of range
        "WARNING":   200.0,   # 25-16%
        "CRITICAL":  150.0,   # 15-10%
        "EMERGENCY": 100.0,   # <10% — nearest reachable only
    }[urgency]
    if fuel_range_miles > 0:
        return min(fuel_range_miles * 0.80, max_by_urgency)
    return max_by_urgency


def reachable_miles(fuel_pct: float, tank_gal: float, mpg: float) -> float:
    """How far the truck can actually drive on current fuel (minus 10% reserve).
    At CRITICAL/EMERGENCY, guarantee at least 50 miles so we always find nearby stops."""
    usable = usable_gallons(fuel_pct, tank_gal)
    calculated = usable * mpg
    if fuel_pct <= 15:
        return max(calculated, 50.0)  # CRITICAL/EMERGENCY — must find something
    return max(calculated, 30.0)


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

def find_current_stop(truck_lat: float, truck_lng: float) -> dict | None:
    """
    Check if truck is currently parked at a known fuel stop.
    Returns the CLOSEST stop within radius, or None.
    """
    all_stops = get_all_diesel_stops()
    best      = None
    best_dist = _AT_STOP_RADIUS + 1  # start outside radius

    for stop in all_stops:
        dist = haversine_miles(truck_lat, truck_lng,
                               float(stop["latitude"]), float(stop["longitude"]))
        if dist <= _AT_STOP_RADIUS and dist < best_dist:
            best_dist = dist
            slat = float(stop["latitude"])
            slng = float(stop["longitude"])
            best = {
                **stop,
                "distance_miles":  round(dist, 2),
                "detour_miles":    0.0,
                "fill_cost":       0.0,
                "true_cost":       0.0,
                "_score":          0.0,
                "_ahead":          True,
                "google_maps_url": f"https://maps.google.com/?q={slat},{slng}",
            }
    return best


_NEARBY_SEARCH_MILES = 20   # radius to compare prices when already at a stop
_MIN_SAVINGS_PER_GAL = 0.05 # only recommend nearby if saves at least 5 cents/gal


def find_cheaper_nearby(truck_lat: float, truck_lng: float,
                         current_stop: dict,
                         fuel_pct: float,
                         tank_gal: float = DEFAULT_TANK_GAL,
                         mpg: float = DEFAULT_MPG) -> dict | None:
    """
    When truck is already parked at a fuel stop, check if there is a
    cheaper stop within _NEARBY_SEARCH_MILES. Returns the cheaper stop
    if the savings are worth the detour, otherwise None.
    """
    current_price = current_stop.get("diesel_price")
    if not current_price or current_price <= 0:
        return None

    all_stops = get_all_diesel_stops()
    candidates = []

    for stop in all_stops:
        # Skip current stop itself
        if stop.get("id") == current_stop.get("id"):
            continue

        slat = float(stop["latitude"])
        slng = float(stop["longitude"])
        dist = haversine_miles(truck_lat, truck_lng, slat, slng)

        if dist > _NEARBY_SEARCH_MILES:
            continue

        price = stop.get("diesel_price")
        if not price or price <= 0:
            continue

        # Must be meaningfully cheaper
        if current_price - price < _MIN_SAVINGS_PER_GAL:
            continue

        fill_gal     = gallons_to_fill(fuel_pct, tank_gal)
        # true savings = price difference × gallons, minus detour fuel cost
        detour_cost  = dist * 2 * price / mpg   # drive there and back
        gross_saving = (current_price - price) * fill_gal
        net_saving   = gross_saving - detour_cost

        if net_saving <= 0:
            continue

        candidates.append({
            **stop,
            "distance_miles":  round(dist, 2),
            "detour_miles":    round(dist, 2),
            "fill_cost":       round(price * fill_gal, 2),
            "true_cost":       round(price * fill_gal + detour_cost, 2),
            "net_saving":      round(net_saving, 2),
            "_score":          -net_saving,   # best = highest net saving
            "_ahead":          True,
            "google_maps_url": f"https://maps.google.com/?q={slat},{slng}",
        })

    if not candidates:
        return None

    candidates.sort(key=lambda s: s["_score"])
    best = candidates[0]
    log.info(f"Cheaper nearby stop: {best['store_name']} ${best['diesel_price']:.3f} "
             f"vs current ${current_price:.3f} — saves ${best['net_saving']:.2f}")
    return best


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

    parked        = speed_mph <= _PARKED_SPEED_MPH
    urgency       = get_urgency(fuel_pct)
    max_range     = reachable_miles(fuel_pct, tank_gal, mpg)
    radius        = get_search_radius(urgency, max_range, fuel_pct)
    price_matters = urgency in ("ADVISORY", "WARNING")

    log.info(f"Stop finder: urgency={urgency} radius={radius:.0f}mi "
             f"range={max_range:.0f}mi parked={parked} stops_in_db={len(all_stops)}")

    # -- Already at a stop? — return it so alert can show current stop ---------
    if parked:
        current = find_current_stop(truck_lat, truck_lng)
        if current:
            log.info(f"Truck already at {current['store_name']} ({current['distance_miles']*5280:.0f} ft)")
            return current, None

    # -- Score all stops in range --------------------------------------------
    candidates = []
    for stop in all_stops:
        slat = float(stop["latitude"])
        slng = float(stop["longitude"])

        dist = haversine_miles(truck_lat, truck_lng, slat, slng)

        # Must be within search radius AND physically reachable
        if dist > radius:
            continue
        # CRITICAL/EMERGENCY: skip max_range filter — truck must find nearest stop
        if urgency not in ("CRITICAL", "EMERGENCY") and dist > max_range:
            continue

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

        # Parked: use true cost (price matters even when parked — truck has to drive there)
        # Critical/Emergency: nearest reachable regardless
        if price_matters:
            score = tc
        else:
            score = dist

        if not ahead and urgency not in ("EMERGENCY", "CRITICAL"):
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

    # If nothing found in urgency radius — expand by 30mi steps up to max_range
    if not candidates:
        expand_radius = radius + 30
        while not candidates and expand_radius <= max_range:
            log.warning(f"No stops in {expand_radius - 30:.0f}mi — expanding to {expand_radius:.0f}mi")
            for stop in all_stops:
                slat = float(stop["latitude"])
                slng = float(stop["longitude"])
                dist = haversine_miles(truck_lat, truck_lng, slat, slng)
                if dist > expand_radius:
                    continue
                ahead = True
                if not parked and truck_heading is not None:
                    bear  = bearing(truck_lat, truck_lng, slat, slng)
                    ahead = angle_diff(truck_heading, bear) <= _AHEAD_ARC_DEGREES
                detour_mi = perpendicular_distance(truck_lat, truck_lng, truck_heading or 0, slat, slng) if not parked else 0.0
                fill_gal  = gallons_to_fill(fuel_pct, tank_gal)
                fill_cost = (stop["diesel_price"] or 0) * fill_gal
                tc        = true_cost(stop, truck_lat, truck_lng, truck_heading or 0, fuel_pct, tank_gal, mpg)
                score     = dist if not ahead else (tc if price_matters else dist)
                if not ahead and urgency not in ("EMERGENCY", "CRITICAL"):
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
            expand_radius += 30

    if not candidates:
        log.warning(f"No reachable stops found within {max_range:.0f} miles.")
        return None, None

    # Sort by score (true cost for price-matters, distance for critical/emergency)
    candidates.sort(key=lambda s: s["_score"])
    best = candidates[0]

    # Find nearest stop (by distance) as alt for savings comparison
    nearest = min(candidates, key=lambda s: s["distance_miles"])
    alt = nearest if nearest["store_name"] != best["store_name"] else None

    log.info(f"Best: {best['store_name']} {best['distance_miles']:.1f}mi "
             f"${best.get('diesel_price','?')}/gal  true_cost=${best['true_cost']:.2f}")

    return best, alt


def calc_savings(best: dict, alt: dict) -> float | None:
    return None


def is_near_stop(truck_lat, truck_lng, stop_lat, stop_lng,
                 radius_miles=None) -> bool:
    r = radius_miles or _AT_STOP_RADIUS
    return haversine_miles(truck_lat, truck_lng, stop_lat, stop_lng) <= r
