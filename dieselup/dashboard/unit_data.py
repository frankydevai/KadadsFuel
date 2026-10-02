"""Source real data for the unit dashboard and assemble the payload.

Pulls live GPS/fuel (Samsara), the active load (QuickManage), and today's contracted prices joined
to fuel-stop coordinates (Postgres via the shared pool), runs ONE Valhalla route + ONE matrix call
per unit, then hands everything to the pure dashboard_unit_view.build_unit_view.

Route geometry is cached 5 min per unit (it changes only when the load changes); GPS, fuel, and the
truck→stop matrix are refreshed on every request.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from dieselup.clients.quickmanage import (
    QuickManageClient,
    _extract_delivery_coords,
    _extract_destination_label,
    _extract_load_id,
    _extract_origin_label,
    _extract_shipper_coords,
    _extract_truck_unit,
)
from dieselup.clients.samsara import SamsaraClient, SamsaraError
from dieselup.config import settings
from dieselup.core.ifta import true_cost_per_gallon
from dieselup.db import get_pool
from dieselup.metrics import Timer, gauge, incr

from .corridor_stops import decode_polyline, encode_polyline, haversine_miles, split_corridor

# Crow-flies → road miles fudge factor for the no-Valhalla "approximate" mode (typical US average).
_APPROX_ROAD_FACTOR = 1.17
from .dashboard_unit_view import build_unit_view
from .valhalla_http import ValhallaHTTP, extract_shape_and_miles

log = logging.getLogger(__name__)

_ROUTE_TTL_SECONDS = 300
_route_cache: dict[str, dict[str, Any]] = {}   # unit -> {key, shape, miles, polyline, ts}


class UnitDashboardError(RuntimeError):
    """A unit dashboard can't be built (no vehicle, no active load, routing unavailable…)."""


def _ifta_adjusted(price: float | None, state: str | None) -> float | None:
    if price is None or not state:
        return None
    try:
        return true_cost_per_gallon(float(price), state)
    except (ValueError, TypeError):
        return None


def _extract_time(trip: dict, *keys: str) -> str | None:
    """Best-effort appointment/scheduled time from a QM stop/trip dict."""
    for k in keys:
        v = trip.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for kk in ("appointment", "scheduled", "scheduled_at", "time", "date"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv.strip():
                    return vv.strip()
    return None


async def _resolve_vehicle(pool, unit: str) -> tuple[str, str | None]:
    """Return (samsara_vehicle_id, driver_name) for a unit, via trucks_drivers then Samsara."""
    row = await pool.fetchrow(
        "SELECT samsara_vehicle_id, driver_full_name FROM trucks_drivers WHERE truck_unit = $1",
        unit,
    )
    vehicle_id = row["samsara_vehicle_id"] if row else None
    driver = row["driver_full_name"] if row else None
    if not vehicle_id:
        async with SamsaraClient() as sams:
            matches = await sams.find_vehicles_by_unit(unit)
        if matches:
            vehicle_id = matches[0].id
    if not vehicle_id:
        raise UnitDashboardError(f"No Samsara vehicle mapped for unit {unit}")
    return vehicle_id, driver


async def _active_load(unit: str) -> dict[str, Any]:
    """Return the active QM trip facts for a unit, or raise if none has pickup+delivery coords."""
    async with QuickManageClient() as qm:
        trips = await qm.list_active_trips()
    target_unit = unit.strip().lower()
    for trip in trips:
        if str(_extract_truck_unit(trip) or "").strip().lower() != target_unit:
            continue
        pickup = _extract_shipper_coords(trip)
        delivery = _extract_delivery_coords(trip)
        if pickup and delivery:
            return {
                "load_id": _extract_load_id(trip),
                "pickup_coords": pickup,
                "delivery_coords": delivery,
                "pickup": {"address": _extract_origin_label(trip),
                           "time": _extract_time(trip, "pickup", "origin", "shipper")},
                "delivery": {"address": _extract_destination_label(trip),
                             "time": _extract_time(trip, "delivery", "destination", "dropoff")},
            }
    raise UnitDashboardError(f"No active load with pickup+delivery for unit {unit}")


async def _route_for(unit: str, pickup: tuple, delivery: tuple) -> dict[str, Any]:
    """Cached 5-min truck-legal route for the unit's current load."""
    key = (round(pickup[0], 4), round(pickup[1], 4), round(delivery[0], 4), round(delivery[1], 4))
    cached = _route_cache.get(unit)
    if cached and cached["key"] == key and (time.monotonic() - cached["ts"]) < _ROUTE_TTL_SECONDS:
        incr("unit_dashboard_route_cache_hit")
        return cached
    incr("unit_dashboard_route_cache_miss")

    shape = miles = None
    mode = "approximate"
    val = ValhallaHTTP()
    if val.available:
        with Timer("unit_dashboard_valhalla_route"):
            resp = await val.route(
                [{"lat": pickup[0], "lon": pickup[1]}, {"lat": delivery[0], "lon": delivery[1]}]
            )
        shape, miles = extract_shape_and_miles(resp)
        if shape:
            mode = "valhalla"
        else:
            incr("unit_dashboard_valhalla_route_fail")

    if not shape:
        # No Valhalla (or it failed): synthesize a STRAIGHT-LINE route so the screen still renders.
        # Corridor split + display work the same; distances are crow-flies × a road factor and the
        # payload is flagged `approximate` so the UI says so. Set VALHALLA_URL to upgrade — no code
        # change. (Truck-legality and real road miles require Valhalla.)
        incr("unit_dashboard_route_approximate")
        shape = encode_polyline([pickup, delivery], 6)
        miles = round(haversine_miles(pickup[0], pickup[1], delivery[0], delivery[1])
                      * _APPROX_ROAD_FACTOR, 1)
        mode = "approximate"

    entry = {"key": key, "shape": shape, "miles": miles, "mode": mode,
             "polyline": decode_polyline(shape, 6), "ts": time.monotonic()}
    _route_cache[unit] = entry
    return entry


async def _load_stations(pool, price_mode: str) -> list[dict[str, Any]]:
    with Timer("unit_dashboard_stations_query"):
        rows = await pool.fetch(
            """
        SELECT f.pilot_site_id AS site_id, f.station_name, f.city, f.state,
               f.latitude, f.longitude, f.truck_accessible,
               c.your_price, c.retail_price
        FROM fuel_stops f
        JOIN contracted_prices c ON c.site_id = f.pilot_site_id
        WHERE c.effective_date = (SELECT MAX(effective_date) FROM contracted_prices)
          AND f.latitude IS NOT NULL AND f.longitude IS NOT NULL
        """
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        src = r["retail_price"] if price_mode == "retail" else r["your_price"]
        out.append({
            "site_id": r["site_id"], "name": r["station_name"],
            "city": r["city"], "state": r["state"],
            "lat": float(r["latitude"]), "lon": float(r["longitude"]),
            "truck_accessible": bool(r["truck_accessible"]),
            "your_price": float(r["your_price"]) if r["your_price"] is not None else None,
            "retail_price": float(r["retail_price"]) if r["retail_price"] is not None else None,
            "ifta_adjusted": _ifta_adjusted(src, r["state"]),
        })
    return out


async def build_unit_dashboard(unit: str, price_mode: str = "discount") -> dict[str, Any]:
    price_mode = "retail" if str(price_mode).lower() == "retail" else "discount"
    pool = await get_pool()

    vehicle_id, driver = await _resolve_vehicle(pool, unit)
    try:
        with Timer("unit_dashboard_samsara"):
            async with SamsaraClient() as sams:
                stats = await sams.get_vehicle_stats(vehicle_id)
    except SamsaraError as exc:
        incr("unit_dashboard_samsara_fail")
        raise UnitDashboardError(f"Samsara has no live data for unit {unit}: {exc}")

    tank = float(settings.TANK_CAPACITY_GALLONS)
    fuel_at_send_pct = max(0.0, min(100.0, stats.fuel_gallons / tank * 100.0)) if tank else 0.0
    mpg = float(stats.mpg_rolling or settings.FLEET_DEFAULT_MPG)
    truck_pos = (stats.lat, stats.lng)

    with Timer("unit_dashboard_quickmanage"):
        load = await _active_load(unit)
    route = await _route_for(unit, load["pickup_coords"], load["delivery_coords"])

    stations = await _load_stations(pool, price_mode)
    in_corr, off_corr = split_corridor(stations, route["polyline"], settings.CORRIDOR_MILES)
    gauge("unit_dashboard_in_corridor", len(in_corr))
    gauge("unit_dashboard_off_corridor", len(off_corr))

    # ONE matrix call: live truck GPS → in-corridor stops (road miles). Skipped entirely when
    # Valhalla isn't configured (approximate mode) → haversine miles-to-stop per stop below.
    val = ValhallaHTTP()
    dists: list = []
    if val.available and in_corr:
        with Timer("unit_dashboard_valhalla_matrix"):
            dists = await val.matrix(truck_pos, [(s["lat"], s["lon"]) for s in in_corr])
        if not dists:
            incr("unit_dashboard_valhalla_matrix_fail")
    for i, s in enumerate(in_corr):
        road = dists[i] if (dists and i < len(dists)) else None
        if road is None:
            incr("unit_dashboard_matrix_fallback_haversine")
        s["miles_to_stop"] = road if road is not None else round(
            haversine_miles(truck_pos[0], truck_pos[1], s["lat"], s["lon"]), 1)

    current_station = None
    if stations:
        nearest = min(stations, key=lambda s: haversine_miles(truck_pos[0], truck_pos[1], s["lat"], s["lon"]))
        current_station = nearest["name"]

    gallons_to_buy = max(0.0, tank * (1.0 - fuel_at_send_pct / 100.0))

    return build_unit_view(
        unit=unit,
        date=datetime.now(timezone.utc).date().isoformat(),
        current_station=current_station,
        price_mode=price_mode,
        truck={"driver": driver, "tank_size": tank, "avg_mpg": mpg,
               "gps_age_minutes": stats.gps_age_minutes},
        load={"load_id": load["load_id"], "pickup": load["pickup"], "delivery": load["delivery"]},
        route={"shape": route["shape"], "miles": route["miles"], "mode": route.get("mode")},
        in_stations=in_corr,
        off_stations=off_corr,
        fuel_at_send_pct=fuel_at_send_pct,
        gallons_to_buy=gallons_to_buy,
        reject_over=settings.REJECT_OVER,
        truck_pos=truck_pos,
    )
