"""
Top-off fueling optimizer — single best stop, dual rankings stored.

Given a truck's live position, fuel level, and the current load's destination,
pick THE cheapest Pilot/FJ stop on the route and return it together with both
ranking variants (your_price and IFTA-adjusted) for the weekly report.

Only Pilot and Flying J branded stops are eligible — affiliate brands such as
ONE9 Travel Center share the locations file but are not on the contract, so they
are filtered out both in SQL and again in rank_candidates. Candidates must also
make forward progress toward delivery (be closer to the destination than the
truck currently is); stops behind the truck fall inside the route bounding box
but would send the driver the wrong way, so they are rejected.

Algorithm:

  1. current_fuel_gallons is provided by the caller (Samsara).
  2. gallons_to_stop = distance_to_stop_miles / truck_mpg
       — falling back to settings.FLEET_DEFAULT_MPG if Samsara has no rolling MPG.
       That fallback raises the `mpg_fallback_used` flag on the result.
  3. fuel_at_arrival = current_fuel_gallons - gallons_to_stop
  4. Reject stops where fuel_at_arrival < SAFETY_FLOOR_GALLONS (30).
  5. gallons_to_pump = TANK_CAPACITY_GALLONS - fuel_at_arrival  (fill to full)
  6. projected_tank_at_delivery = TANK_CAPACITY_GALLONS - (dist_stop_to_dest / mpg)
       After a full fill the truck always leaves with TANK_CAPACITY gallons, so
       projected delivery fuel doesn't depend on fuel_at_arrival.
  7. Reject stops where projected_tank_at_delivery < TANK_CAPACITY * DELIVERY_RESERVE_PCT / 100
       (truck must reach delivery with 30% reserve).
  8. total_trip_cost = gallons_to_pump * true_cost_per_gallon
  9. Build two rankings of the remaining candidates:
       * by your_price (ascending pump-discounted price per gallon)
       * by IFTA-adjusted total_trip_cost (ascending true cost)
     The SELECTED stop is rank #1 of whichever strategy `settings.RANK_STRATEGY`
     names — driver-facing briefing only ever shows this one stop.

`worst_candidate_true_cost` (per-gallon) is preserved on the result for
core/compliance.py's dollar math.

NoValidStopError is raised when no candidate satisfies both the safety floor
and the delivery-reserve constraint.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from math import acos, asin, atan2, cos, degrees, radians, sin, sqrt
from typing import Iterable, Literal

from dieselup.config import settings
from dieselup import metrics
from dieselup.core.ifta import true_cost_per_gallon
from dieselup.db import fetch_all, fetch_one
from dieselup.core.price_sources import pilot_price_rows


EARTH_RADIUS_MILES = 3958.7613

# Sweet-spot arrival fuel — used only as a tiebreaker between candidates with
# matching primary sort key.
SWEET_SPOT_MIN_GALLONS = 40.0
SWEET_SPOT_MAX_GALLONS = 70.0

TOP_N = 3

# Legacy mode has no road polyline. Haversine miles understate the distance a
# truck must actually drive, so be conservative for fuel burn and displayed
# miles in the single-stop optimizer.
LEGACY_ROAD_DISTANCE_FACTOR = 1.15

# Geometry guard for the legacy bbox query (used as fallback when GPS heading
# is unavailable). A stop can be closer to delivery while still being far off
# the lane, so reject large heading/cross-track misses.
MAX_ROUTE_ANGLE_DEGREES = 45.0
MIN_CORRIDOR_MILES = 75.0
MAX_CORRIDOR_MILES = 175.0
CORRIDOR_ROUTE_FRACTION = 0.35

# Heading-based direction filter. A stop within ±AHEAD_ARC_DEGREES of the
# truck's live GPS heading is "ahead". Anything wider is behind the truck and
# is rejected — this directly eliminates the wrong-direction bug (e.g. KY→GA
# truck getting northward stops) without needing an external routing API.
AHEAD_ARC_DEGREES = 90.0

# Urgency tiers — fuel as % of tank capacity. The tier controls search
# behaviour: at ADVISORY/WARNING the system optimises for IFTA price; at
# CRITICAL/EMERGENCY it bypasses pricing and finds the nearest reachable stop.
EMERGENCY_FUEL_PCT = 10.0   # < 10%  — nearest stop, all direction filters off
CRITICAL_FUEL_PCT  = 15.0   # 10-15% — nearest reachable, price secondary
WARNING_FUEL_PCT   = 25.0   # 15-25% — price-optimised, tighter search
# > 25%  — ADVISORY: full corridor, normal IFTA-adjusted ranking

# Only Pilot and Flying J branded stops are on the contracted price sheet. The
# locations file also carries affiliate brands (e.g. "ONE9 Travel Center") that
# are NOT covered, so they must never be recommended to a driver.
PILOT_FJ_BRANDS = ("pilot", "flying j")


def is_pilot_flying_j(station_name: str | None) -> bool:
    """True only for Pilot / Flying J branded stops (excludes ONE9 etc.)."""
    name = (station_name or "").lower()
    return any(brand in name for brand in PILOT_FJ_BRANDS)

RankStrategy = Literal["your_price", "ifta_adjusted"]


@dataclass(frozen=True)
class CandidateStop:
    """A Pilot/FJ stop with today's contracted price, before the fuel math runs."""
    site_id: int
    station_name: str
    address: str | None
    city: str
    state: str
    latitude: float
    longitude: float
    your_price: float
    retail_price: float
    price_date: str | None = None


@dataclass(frozen=True)
class RankedStop:
    """A candidate that passed the safety floor and delivery-reserve checks."""
    site_id: int
    station_name: str
    address: str | None
    city: str
    state: str
    latitude: float
    longitude: float
    your_price: float
    retail_price: float
    distance_miles: float
    gallons_to_stop: float
    fuel_at_arrival: float
    gallons_to_pump: int
    true_cost_per_gallon: float
    total_trip_cost: float
    in_sweet_spot: bool
    savings_per_gallon: float
    total_savings: float
    distance_stop_to_destination_miles: float
    projected_tank_at_delivery: float


@dataclass(frozen=True)
class FuelPlan:
    """Optimizer output: one selected stop, plus both rankings for audit/report.

    `selected` is what the driver sees. `ranked_your_price` and `ranked_ifta`
    are the top-3 under each strategy — both stored in stop_events.candidates
    so the weekly report can compare "what we picked" vs "what IFTA would
    have picked" for the trial period.
    """
    selected: RankedStop
    ranked_your_price: list[RankedStop]
    ranked_ifta: list[RankedStop]
    flags: list[str] = field(default_factory=list)
    worst_candidate_true_cost: float = 0.0


# Backwards-compatible alias — older imports referencing `Recommendation` won't
# break during the in-flight transition.
Recommendation = FuelPlan


class NoValidStopError(Exception):
    """Raised when no candidate stop satisfies 30 <= fuel_at_arrival <= 80."""

    def __init__(
        self,
        truck_unit: str,
        load_id: str,
        current_fuel_gallons: float,
        reason: str = "no_valid_stop",
    ):
        self.truck_unit = truck_unit
        self.load_id = load_id
        self.current_fuel_gallons = current_fuel_gallons
        self.reason = reason
        super().__init__(
            f"no_valid_stop: truck={truck_unit} load={load_id} "
            f"current_fuel={current_fuel_gallons:.1f}gal reason={reason}"
        )


class StaleFuelPricesError(RuntimeError):
    """Contract prices are absent or too old to issue a driver recommendation."""


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in statute miles."""
    lat1_r, lat2_r = radians(lat1), radians(lat2)
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_MILES * asin(sqrt(a))


def bearing(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compass bearing in degrees from point 1 to point 2 (0–360)."""
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    dlam = radians(lng2 - lng1)
    x = sin(dlam) * cos(phi2)
    y = cos(phi1) * sin(phi2) - sin(phi1) * cos(phi2) * cos(dlam)
    return (degrees(atan2(x, y)) + 360) % 360


def angle_diff(a: float, b: float) -> float:
    """Smallest angle between two compass bearings (0–180)."""
    d = abs(a - b) % 360
    return d if d <= 180 else 360 - d


def _urgency(fuel_gallons: float) -> str:
    """Urgency tier based on current fuel as % of tank capacity."""
    pct = fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100.0
    if pct <= EMERGENCY_FUEL_PCT:
        return "EMERGENCY"
    if pct <= CRITICAL_FUEL_PCT:
        return "CRITICAL"
    if pct <= WARNING_FUEL_PCT:
        return "WARNING"
    return "ADVISORY"


def _estimated_road_miles(straight_line_miles: float) -> float:
    return straight_line_miles * LEGACY_ROAD_DISTANCE_FACTOR


async def get_truck_distance(
    from_stop_id: int,
    to_stop_id: int,
    *,
    fallback_coords: tuple[tuple[float, float], tuple[float, float]] | None = None,
) -> float | None:
    """Read precomputed Valhalla stop-to-stop miles, with geometry fallback.

    `fallback_coords` is ((from_lat, from_lng), (to_lat, to_lng)). It is only
    used when the graph pair is missing, preserving the old no-crash behavior.
    """
    row = await fetch_one(
        """
        SELECT distance_miles
        FROM stop_distances
        WHERE from_stop_id = $1 AND to_stop_id = $2
        """,
        from_stop_id,
        to_stop_id,
    )
    if row is not None:
        return float(row["distance_miles"])
    if fallback_coords is None:
        return None

    (from_lat, from_lng), (to_lat, to_lng) = fallback_coords
    return _estimated_road_miles(
        haversine_miles(from_lat, from_lng, to_lat, to_lng)
    )


def _route_geometry(
    *,
    truck_lat: float,
    truck_lng: float,
    destination_lat: float,
    destination_lng: float,
    stop_lat: float,
    stop_lng: float,
) -> tuple[float, float, float] | None:
    """Return progress ratio, cross-track miles, and heading angle in degrees."""
    mid_lat = radians((truck_lat + destination_lat) / 2.0)

    def xy(lat: float, lng: float) -> tuple[float, float]:
        return (
            (lng - truck_lng) * 69.172 * cos(mid_lat),
            (lat - truck_lat) * 69.0,
        )

    dest_x, dest_y = xy(destination_lat, destination_lng)
    stop_x, stop_y = xy(stop_lat, stop_lng)
    route_len = sqrt(dest_x * dest_x + dest_y * dest_y)
    stop_len = sqrt(stop_x * stop_x + stop_y * stop_y)
    if route_len <= 0.0 or stop_len <= 0.0:
        return None

    dot = stop_x * dest_x + stop_y * dest_y
    progress_ratio = dot / (route_len * route_len)
    projected_x = progress_ratio * dest_x
    projected_y = progress_ratio * dest_y
    cross_track = sqrt(
        (stop_x - projected_x) * (stop_x - projected_x)
        + (stop_y - projected_y) * (stop_y - projected_y)
    )
    cosine = max(-1.0, min(1.0, dot / (route_len * stop_len)))
    angle = degrees(acos(cosine))
    return progress_ratio, cross_track, angle


def _inside_legacy_route_corridor(
    *,
    truck_lat: float,
    truck_lng: float,
    destination_lat: float,
    destination_lng: float,
    stop_lat: float,
    stop_lng: float,
    truck_to_dest_miles: float,
) -> bool:
    geometry = _route_geometry(
        truck_lat=truck_lat,
        truck_lng=truck_lng,
        destination_lat=destination_lat,
        destination_lng=destination_lng,
        stop_lat=stop_lat,
        stop_lng=stop_lng,
    )
    if geometry is None:
        return False

    progress_ratio, cross_track, angle = geometry
    corridor = max(
        MIN_CORRIDOR_MILES,
        min(MAX_CORRIDOR_MILES, truck_to_dest_miles * CORRIDOR_ROUTE_FRACTION),
    )
    return (
        0.0 < progress_ratio < 1.0
        and cross_track <= corridor
        and angle <= MAX_ROUTE_ANGLE_DEGREES
    )


def _sort_by_your_price(r: RankedStop) -> tuple[float, bool]:
    return (r.your_price, not r.in_sweet_spot)


def _sort_by_ifta(r: RankedStop) -> tuple[float, bool]:
    return (r.total_trip_cost, not r.in_sweet_spot)


def rank_candidates(
    candidates: Iterable[CandidateStop],
    *,
    truck_lat: float,
    truck_lng: float,
    destination_lat: float,
    destination_lng: float,
    current_fuel_gallons: float,
    truck_mpg: float | None,
    truck_unit: str,
    load_id: str,
    strategy: RankStrategy | None = None,
    truck_heading: float | None = None,
) -> FuelPlan:
    """Steps 2-8 of the algorithm. Pure; no I/O.

    Builds both rankings, selects rank 1 from whichever strategy is requested
    (defaulting to settings.RANK_STRATEGY). Raises NoValidStopError if no
    candidate satisfies both the safety floor and the tank ceiling.
    """
    chosen_strategy: RankStrategy = strategy or settings.RANK_STRATEGY  # type: ignore[assignment]

    mpg_fallback_used = not (truck_mpg and truck_mpg > 0)
    mpg = truck_mpg if not mpg_fallback_used else settings.FLEET_DEFAULT_MPG
    floor = settings.SAFETY_FLOOR_GALLONS
    tank = settings.TANK_CAPACITY_GALLONS          # full tank capacity
    max_arrival = settings.MAX_ARRIVAL_FUEL_GALLONS  # reject if truck too full to bother (80 gal)
    reserve_gallons = tank * settings.DELIVERY_RESERVE_PCT / 100

    # Urgency tier — drives how strictly we filter direction and arrival fuel.
    urgency = _urgency(current_fuel_gallons)

    # Remaining miles from the truck's current position straight to delivery.
    truck_to_dest = haversine_miles(
        truck_lat, truck_lng, destination_lat, destination_lng
    )

    valid: list[RankedStop] = []
    rejected_too_full = 0
    rejected_too_low = 0
    otherwise_eligible = 0
    for c in candidates:
        # Brand gate: Pilot / Flying J only. ONE9 and other affiliates are not
        # on the contract and must never reach a driver.
        if not is_pilot_flying_j(c.station_name):
            continue

        straight_distance = haversine_miles(
            truck_lat, truck_lng, c.latitude, c.longitude
        )
        distance = _estimated_road_miles(straight_distance)
        gallons_to_stop = distance / mpg
        fuel_at_arrival = current_fuel_gallons - gallons_to_stop

        straight_dist_to_dest = haversine_miles(
            c.latitude, c.longitude, destination_lat, destination_lng
        )
        dist_to_dest = _estimated_road_miles(straight_dist_to_dest)

        # ── Direction filter ────────────────────────────────────────────────
        # CRITICAL/EMERGENCY: skip direction filters entirely — truck is nearly
        # empty and needs the nearest reachable stop regardless of direction.
        if urgency not in ("CRITICAL", "EMERGENCY"):
            if truck_heading is not None:
                # Primary: GPS heading from Samsara. A stop must be within
                # ±AHEAD_ARC_DEGREES (90°) of the truck's live compass bearing.
                # This is the ground-truth fix for the wrong-direction bug: a
                # truck heading south (bearing ~180°) at KY can never receive a
                # northward stop — angle_diff would be ~180° > 90°.
                stop_bear = bearing(truck_lat, truck_lng, c.latitude, c.longitude)
                if angle_diff(truck_heading, stop_bear) > AHEAD_ARC_DEGREES:
                    continue
                # Secondary: forward-progress guard vs destination ensures the
                # stop is actually closer to delivery, not just "ahead" in the
                # truck's momentary heading direction.
                if straight_dist_to_dest >= truck_to_dest:
                    continue
            else:
                # No heading available (parked, GPS gap) — fall back to the
                # destination-based corridor geometry.
                if straight_dist_to_dest >= truck_to_dest:
                    continue
                if not _inside_legacy_route_corridor(
                    truck_lat=truck_lat,
                    truck_lng=truck_lng,
                    destination_lat=destination_lat,
                    destination_lng=destination_lng,
                    stop_lat=c.latitude,
                    stop_lng=c.longitude,
                    truck_to_dest_miles=truck_to_dest,
                ):
                    continue

        # Truck leaves with full tank; must still reach delivery with reserve.
        projected_tank = tank - (dist_to_dest / mpg)
        if projected_tank < reserve_gallons:
            continue
        otherwise_eligible += 1

        # ── Urgency-aware safety checks ──────────────────────────────────────
        # Safety floor: truck must not arrive near empty.
        if fuel_at_arrival < floor:
            rejected_too_low += 1
            continue
        # Ceiling: skip if truck arrives too full to bother filling —
        # relaxed for CRITICAL/EMERGENCY so a nearly-empty truck is never
        # left without a stop just because it started with a high tank.
        if urgency not in ("CRITICAL", "EMERGENCY") and fuel_at_arrival > max_arrival:
            rejected_too_full += 1
            continue

        # Always fill to full tank
        gallons_to_pump = int(tank - fuel_at_arrival)
        tcg = true_cost_per_gallon(c.your_price, c.state)

        savings_per_gallon = c.retail_price - c.your_price

        valid.append(
            RankedStop(
                site_id=c.site_id,
                station_name=c.station_name,
                address=c.address,
                city=c.city,
                state=c.state,
                latitude=c.latitude,
                longitude=c.longitude,
                your_price=c.your_price,
                retail_price=c.retail_price,
                distance_miles=distance,
                gallons_to_stop=gallons_to_stop,
                fuel_at_arrival=fuel_at_arrival,
                gallons_to_pump=gallons_to_pump,
                true_cost_per_gallon=tcg,
                total_trip_cost=gallons_to_pump * tcg,
                in_sweet_spot=SWEET_SPOT_MIN_GALLONS <= fuel_at_arrival <= SWEET_SPOT_MAX_GALLONS,
                savings_per_gallon=savings_per_gallon,
                total_savings=savings_per_gallon * gallons_to_pump,
                distance_stop_to_destination_miles=dist_to_dest,
                projected_tank_at_delivery=projected_tank,
            )
        )

    if not valid:
        reason = (
            "arrival_fuel_too_high"
            if otherwise_eligible > 0
            and rejected_too_full == otherwise_eligible
            and rejected_too_low == 0
            else "no_valid_stop"
        )
        raise NoValidStopError(
            truck_unit=truck_unit,
            load_id=load_id,
            current_fuel_gallons=current_fuel_gallons,
            reason=reason,
        )

    ranked_yp = sorted(valid, key=_sort_by_your_price)[:TOP_N]
    ranked_ifta = sorted(valid, key=_sort_by_ifta)[:TOP_N]
    selected = ranked_yp[0] if chosen_strategy == "your_price" else ranked_ifta[0]

    # Worst-of-top-3 from the SELECTED ranking — keeps compliance.py's
    # (worst - recommended) * gallons math meaningful under either strategy.
    source_top = ranked_yp if chosen_strategy == "your_price" else ranked_ifta
    worst = max(r.true_cost_per_gallon for r in source_top)

    flags: list[str] = []
    if mpg_fallback_used:
        flags.append("mpg_fallback_used")

    return FuelPlan(
        selected=selected,
        ranked_your_price=ranked_yp,
        ranked_ifta=ranked_ifta,
        flags=flags,
        worst_candidate_true_cost=worst,
    )


async def fetch_corridor_candidates(
    *,
    truck_lat: float,
    truck_lng: float,
    destination_lat: float,
    destination_lng: float,
    corridor_buffer_degrees: float = 1.0,
) -> list[CandidateStop]:
    """Today's priced Pilot/FJ stops inside the truck->destination bbox.

    corridor_buffer_degrees pads the bbox in each direction (~69 mi per latitude
    degree) so stops slightly off the straight line still qualify. Shared by
    recommend_stops (legacy ranking) and the routed min-cost planner.
    """
    freshness = await fetch_one(
        f"""WITH prices AS ({pilot_price_rows('$1','$2')})
        SELECT MAX(effective_date) AS effective_date,
               ((NOW() AT TIME ZONE 'America/New_York')::date) - MAX(effective_date) AS age_days
        FROM prices WHERE truck_accessible IS DISTINCT FROM FALSE
          AND effective_date<=((NOW() AT TIME ZONE 'America/New_York')::date)
        """, settings.FTS_PRICE_CUSTOMER,settings.PILOT_ACCOUNT_NUMBER
    )
    if freshness is None or freshness["age_days"] is None:
        metrics.gauge("fuel_prices_stale", 1)
        raise StaleFuelPricesError("No contracted fuel price feed is available")
    age_days = int(freshness["age_days"])
    metrics.gauge("fuel_price_age_days", age_days)
    if age_days < 0 or age_days > settings.MAX_FUEL_PRICE_AGE_DAYS:
        metrics.gauge("fuel_prices_stale", 1)
        raise StaleFuelPricesError(
            f"Contract fuel prices dated {freshness['effective_date']} are outside "
            f"the allowed age of {settings.MAX_FUEL_PRICE_AGE_DAYS} days; upload current prices"
        )
    metrics.gauge("fuel_prices_stale", 0)

    min_lat = min(truck_lat, destination_lat) - corridor_buffer_degrees
    max_lat = max(truck_lat, destination_lat) + corridor_buffer_degrees
    min_lng = min(truck_lng, destination_lng) - corridor_buffer_degrees
    max_lng = max(truck_lng, destination_lng) + corridor_buffer_degrees

    rows = await fetch_all(
        f"""WITH prices AS ({pilot_price_rows('$5','$6')})
        SELECT DISTINCT ON(site_id) site_id,station_name,address,city,state,latitude,longitude,
               your_price,retail_price,effective_date,price_provider
        FROM prices
        WHERE ((NOW() AT TIME ZONE 'America/New_York')::date)-effective_date BETWEEN 0 AND $7
          AND truck_accessible IS DISTINCT FROM FALSE
          AND latitude BETWEEN $1 AND $2 AND longitude BETWEEN $3 AND $4
        ORDER BY site_id,effective_date DESC,uploaded_at DESC,fuel_stop_id
        """,
        min_lat,max_lat,min_lng,max_lng,settings.FTS_PRICE_CUSTOMER,
        settings.PILOT_ACCOUNT_NUMBER,settings.MAX_FUEL_PRICE_AGE_DAYS,
    )

    return [
        CandidateStop(
            site_id=row["site_id"],
            station_name=row["station_name"],
            address=row["address"],
            city=row["city"],
            state=row["state"],
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            your_price=float(row["your_price"]),
            retail_price=float(row["retail_price"]) if row["retail_price"] is not None else float(row["your_price"]),
            price_date=str(row.get("effective_date")) if row.get("effective_date") else None,
        )
        for row in rows
    ]


async def recommend_stops(
    *,
    truck_unit: str,
    load_id: str,
    truck_lat: float,
    truck_lng: float,
    destination_lat: float,
    destination_lng: float,
    current_fuel_gallons: float,
    truck_mpg: float | None,
    strategy: RankStrategy | None = None,
    corridor_buffer_degrees: float = 1.0,
    truck_heading: float | None = None,
) -> FuelPlan:
    """Query today's prices joined with fuel_stops in the route bbox, then rank."""
    candidates = await fetch_corridor_candidates(
        truck_lat=truck_lat,
        truck_lng=truck_lng,
        destination_lat=destination_lat,
        destination_lng=destination_lng,
        corridor_buffer_degrees=corridor_buffer_degrees,
    )

    return rank_candidates(
        candidates,
        truck_lat=truck_lat,
        truck_lng=truck_lng,
        destination_lat=destination_lat,
        destination_lng=destination_lng,
        current_fuel_gallons=current_fuel_gallons,
        truck_mpg=truck_mpg,
        truck_unit=truck_unit,
        load_id=load_id,
        strategy=strategy,
        truck_heading=truck_heading,
    )
