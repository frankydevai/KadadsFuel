"""
Fuel brain — fleet-wide telemetry memory + silent fueling detection.

`capture_fleet_state(bot)` runs every 5 minutes (APScheduler, wired in
main.py). Unlike compliance — which only watches trucks that have a PENDING
fuel plan — the brain watches EVERY Samsara vehicle, every cycle:

  1. ROUTE MEMORY — writes a truck_snapshots row (position, fuel, speed,
     heading) for every successful GPS poll. This is the fleet's driving
     history: where every truck went, at what fuel level, forever (pruned
     after SNAPSHOT_RETENTION_DAYS).

  2. FUELING DETECTION — compares each truck's fuel percent against its
     previous snapshot. A jump >= FUEL_JUMP_PCT means the truck is fueling
     RIGHT NOW. The event is recorded in fuel_events and classified:

       'recommended'      — at the pending plan's recommended stop
       'contracted_other' — at a contracted Pilot/FJ stop that was NOT the
                            assigned one
       'off_network'      — not near any contracted stop (Love's, TA, cash
                            pump)
       no active plan     — recorded either way

     A fueling in progress spans several cycles; the open event row keeps
     being updated while fuel rises and is finalized when it stops.

The second brain is intentionally silent: it records observations for maps,
compliance, analytics, and future AI/routing models. Customer-facing Telegram
alerts remain the compliance/briefing layer's job.

Sensor caveat: fuel-percent telemetry is noisy (sloshing, slope, sensor
recalibration). FUEL_JUMP_GALLONS = 30 gal is the compliance-grade floor:
smaller rises are treated as noise/top-offs until RTS card data lands.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

import asyncpg
from telegram import Bot

from dieselup import metrics
from dieselup.bot.sender import safe_send
from dieselup.clients.samsara import SamsaraClient, SamsaraError, extract_unit_digits
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.core.compliance import (
    _claim_alert_fingerprint,
    _nearby_priced_stop,
    _parse_candidates,
    haversine_km,
)
from dieselup.db import execute, fetch_all, fetch_one

log = logging.getLogger(__name__)

# Minimum fuel RISE between snapshots that opens a NEW fueling event. Spec
# floor is 30 gal: anything smaller is sensor slosh/recalibration or a trivial
# top-up and must NOT be flagged as a fueled-elsewhere violation or scored as a
# loss. The smallest plan-worthy fill is 50 gal, so a 30-gal floor never misses
# a real fueling while killing the false positives an 8% (~16 gal) floor let in.
FUEL_JUMP_GALLONS = 30.0

# Once a fueling event is OPEN, any continued rise above this small threshold
# keeps it open (extends gallons, never re-alerts). A rise below it means the
# pump has stopped, so the event is finalized. Kept well under FUEL_JUMP_GALLONS
# so a multi-cycle fill is tracked to completion rather than closed mid-fill.
FUEL_RISE_CONTINUE_GALLONS = 4.0

# The second brain is a learning layer: write one snapshot for every successful
# Samsara GPS poll, including parked/dwell time. Retention keeps storage bounded.

# An open fuel_event older than this without a further rise is finalized.
FUEL_EVENT_MERGE_WINDOW_MINUTES = 90.0

# Route-history retention.
SNAPSHOT_RETENTION_DAYS = 90

@dataclass(frozen=True)
class _FuelLocationMatch:
    classification: str
    nearby: dict[str, Any] | None
    lat: float
    lng: float
    source: str


async def capture_fleet_state(bot: Bot) -> None:
    """APScheduler entrypoint — IntervalTrigger(minutes=5)."""
    log.info("fuel_brain: starting fleet capture")
    metrics.incr("fuel_brain_cycles_total")

    snapshots_written = fuel_events_detected = errored = 0

    try:
        async with SamsaraClient() as samsara:
            vehicles = await samsara.list_vehicles()
            prev_by_vid = await _last_snapshots()
            open_events = await _open_fuel_events()
            trucks_by_vid = await _trucks_by_vehicle_id()
            pending_by_unit = await _pending_events_by_unit()

            for vehicle in vehicles:
                roster = trucks_by_vid.get(vehicle.id)
                if allowed_units() is not None:
                    # A restricted test requires an existing saved identity;
                    # names alone must never bring another vehicle into scope.
                    from dieselup.clients.samsara import extract_samsara_index_keys
                    if not roster or not allows(roster['truck_unit']):
                        continue
                    names = {unit_key(k) for k in extract_samsara_index_keys(vehicle.name)}
                    if unit_key(roster['truck_unit']) not in names:
                        log.warning('fuel_brain: saved vehicle identity mismatch for truck %s', roster['truck_unit'])
                        continue
                try:
                    wrote, detected = await _process_vehicle(
                        vehicle=vehicle,
                        samsara=samsara,
                        bot=bot,
                        prev=prev_by_vid.get(vehicle.id),
                        open_event=open_events.get(vehicle.id),
                        truck_row=trucks_by_vid.get(vehicle.id),
                        pending_by_unit=pending_by_unit,
                    )
                    snapshots_written += int(wrote)
                    fuel_events_detected += int(detected)
                except Exception:  # noqa: BLE001 — one vehicle can't kill the sweep
                    errored += 1
                    log.exception("fuel_brain: error processing vehicle %s", vehicle.id)
    except SamsaraError as exc:
        log.warning("fuel_brain: Samsara unavailable — skipping cycle: %s", exc)
        metrics.incr("fuel_brain_aborted_samsara")
        return
    except (asyncpg.exceptions.UndefinedColumnError,
            asyncpg.exceptions.UndefinedTableError) as exc:
        # Schema mismatch — almost always a pre-existing fuel_events /
        # truck_snapshots table with a different shape that CREATE TABLE IF NOT
        # EXISTS skipped. Without this handler the job crash-looped every 5 min
        # with a raw traceback and nobody was told.
        metrics.incr("fuel_brain_schema_mismatch")
        log.error("fuel_brain: schema mismatch — cycle skipped: %s", exc)
        if await _claim_alert_fingerprint(
            alert_type="fuel_brain_schema_mismatch",
            truck_unit="-",
            load_id="-",
            dedupe_key=str(exc)[:120],
        ):
            await safe_send(
                bot=bot,
                chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
                text=(
                    f"fuel_brain is DISABLED by a database schema mismatch: {exc}. "
                    "A pre-existing fuel_events/truck_snapshots table has the "
                    "wrong shape. Fix: run in Postgres —\n"
                    "ALTER TABLE fuel_events RENAME TO fuel_events_old;\n"
                    "DROP TABLE IF EXISTS truck_snapshots;\n"
                    "then restart the service. (This alert fires once.)"
                ),
                alert_type="fuel_brain_schema_mismatch",
                parse_mode=None,
            )
        return

    # Route-history retention.
    try:
        await execute(
            "DELETE FROM truck_snapshots WHERE taken_at < NOW() - ($1 || ' days')::INTERVAL AND ($2::text[] IS NULL OR ltrim(upper(truck_unit),'0')=ANY($2))",
            str(SNAPSHOT_RETENTION_DAYS), allowed_units(),
        )
    except Exception:  # noqa: BLE001
        log.exception("fuel_brain: snapshot prune failed")

    import time as _t
    metrics.gauge("fuel_brain_last_heartbeat_mono", _t.monotonic())
    metrics.gauge("fuel_brain_last_cycle_snapshots", snapshots_written)
    metrics.gauge("fuel_brain_last_cycle_fuel_events", fuel_events_detected)
    log.info(
        "fuel_brain: done — snapshots=%d fuel_events=%d errored=%d",
        snapshots_written, fuel_events_detected, errored,
    )


async def _process_vehicle(
    *,
    vehicle: Any,
    samsara: SamsaraClient,
    bot: Bot,
    prev: dict[str, Any] | None,
    open_event: dict[str, Any] | None,
    truck_row: dict[str, Any] | None,
    pending_by_unit: dict[str, dict[str, Any]],
) -> tuple[bool, bool]:
    """Snapshot one vehicle and detect/extend/finalize its fuel events.

    Returns (snapshot_written, fuel_event_detected).
    """
    if allowed_units() is not None and (not truck_row or not allows(truck_row.get('truck_unit'))):
        return False, False
    try:
        location = await samsara.get_vehicle_location(vehicle.id)
    except SamsaraError:
        return False, False  # no GPS — nothing to record this cycle

    fuel_pct: float | None = None
    fuel_time = None
    try:
        fuel_gallons, fuel_time = await samsara.get_vehicle_fuel_reading(vehicle.id)
        if fuel_time is not None and 0 <= (datetime.now(timezone.utc)-fuel_time).total_seconds() <= settings.MAX_ADVICE_FUEL_AGE_MINUTES*60:
            fuel_pct = fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100.0
    except SamsaraError:
        pass  # no fuel sensor data — still snapshot position

    truck_unit = (
        (truck_row or {}).get("truck_unit")
        or extract_unit_digits(vehicle.name)
        or vehicle.id
    )

    wrote = await _maybe_write_snapshot(
        vehicle_id=vehicle.id,
        truck_unit=truck_unit,
        location=location,
        fuel_pct=fuel_pct,
        prev=prev,
        fuel_observed_at=fuel_time,
    )

    detected = False
    prev_pct = _to_float((prev or {}).get("fuel_pct"))
    previous_time = (prev or {}).get('fuel_observed_at')
    gps_age = getattr(location,'gps_age_minutes',None)
    if (fuel_pct is not None and prev_pct is not None and previous_time is not None and fuel_time is not None
            and 0 < (fuel_time-previous_time).total_seconds() <= 2400
            and gps_age is not None and 0 <= gps_age <= settings.MAX_ADVICE_GPS_AGE_MINUTES):
        jump_gallons = (fuel_pct - prev_pct) / 100.0 * settings.TANK_CAPACITY_GALLONS
        if open_event is not None:
            # A fueling is already in progress for this truck.
            if jump_gallons >= FUEL_RISE_CONTINUE_GALLONS:
                # Still rising — extend the open event (never re-alerts).
                detected = await _handle_fuel_rise(
                    bot=bot,
                    vehicle_id=vehicle.id,
                    truck_unit=str(truck_unit),
                    location=location,
                    prev=prev,
                    fuel_observed_at=fuel_time,
                    fuel_pct_start=prev_pct,
                    fuel_pct_now=fuel_pct,
                    open_event=open_event,
                    truck_row=truck_row,
                    pending=pending_by_unit.get(str(truck_unit)),
                )
            else:
                # Fuel stopped rising — the pump is done; close the event.
                await execute(
                    "UPDATE fuel_events SET finalized_at = NOW() WHERE id = $1 AND finalized_at IS NULL",
                    open_event["id"],
                )
        elif jump_gallons >= FUEL_JUMP_GALLONS:
            # New fueling: a real fill of >= 30 gal (not noise, not a top-up).
            detected = await _handle_fuel_rise(
                bot=bot,
                vehicle_id=vehicle.id,
                truck_unit=str(truck_unit),
                location=location,
                prev=prev,
                fuel_observed_at=fuel_time,
                fuel_pct_start=prev_pct,
                fuel_pct_now=fuel_pct,
                open_event=open_event,
                truck_row=truck_row,
                pending=pending_by_unit.get(str(truck_unit)),
            )

    from dieselup.core.stationary_context import record_stationary_context
    await record_stationary_context(vehicle.id,str(truck_unit),pending_by_unit.get(str(truck_unit)))
    return wrote, detected


async def _handle_fuel_rise(
    *,
    bot: Bot,
    vehicle_id: str,
    truck_unit: str,
    location: Any,
    prev: dict[str, Any] | None,
    fuel_pct_start: float,
    fuel_pct_now: float,
    open_event: dict[str, Any] | None,
    truck_row: dict[str, Any] | None,
    pending: dict[str, Any] | None,
    fuel_observed_at: datetime | None = None,
) -> bool:
    """Create or extend a fuel_event for a detected fuel rise.

    Returns True when a NEW event was created, False when an
    existing in-progress event was extended.
    """
    if open_event is not None:
        # Same fueling still in progress — extend, never re-alert.
        start_pct = _to_float(open_event.get("fuel_pct_start")) or fuel_pct_start
        gallons = round(
            (fuel_pct_now - start_pct) / 100.0 * settings.TANK_CAPACITY_GALLONS, 1
        )
        await execute(
            """
            UPDATE fuel_events
            SET fuel_pct_end = $2, gallons = $3
            WHERE id = $1
            """,
            open_event["id"],
            round(fuel_pct_now, 2),
            gallons,
        )
        return False

    gallons = round(
        (fuel_pct_now - fuel_pct_start) / 100.0 * settings.TANK_CAPACITY_GALLONS, 1
    )

    # WHERE did the truck fuel? Nearest contracted stop within the geofence,
    # or off-network. Samsara fuel-percent updates can lag behind GPS by one
    # poll, so check the previous snapshot too before blaming the driver.
    match = await _classify_fuel_location(location=location, prev=prev, pending=pending,fuel_observed_at=fuel_observed_at)
    nearby = match.nearby
    classification = match.classification

    row = await fetch_one(
        """
        INSERT INTO fuel_events
            (samsara_vehicle_id, truck_unit, load_id, stop_event_id, site_id,
             station_name, latitude, longitude, fuel_pct_start, fuel_pct_end,
             gallons, classification,detected_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,$13)
        RETURNING id
        """,
        vehicle_id,
        truck_unit,
        pending["load_id"] if pending else None,
        pending["id"] if pending else None,
        int(nearby["site_id"]) if nearby else None,
        nearby["station_name"] if nearby else None,
        match.lat,
        match.lng,
        round(fuel_pct_start, 2),
        round(fuel_pct_now, 2),
        gallons,
        classification,
        fuel_observed_at or datetime.now(timezone.utc),
    )
    event_id = int(row["id"]) if row else None
    metrics.incr(f"fuel_brain_event_{classification}")
    log.info(
        "fuel_brain: truck %s fueling detected — +%.1f gal, %s%s (%s GPS)",
        truck_unit, gallons, classification,
        f" at {nearby['station_name']}" if nearby else "",
        match.source,
    )

    await _alert_for_fuel_event(
        bot=bot,
        event_id=event_id,
        classification=classification,
        truck_unit=truck_unit,
        gallons=gallons,
        nearby=nearby,
        pending=pending,
        truck_row=truck_row,
    )
    return True


async def _classify_fuel_location(
    *,
    location: Any,
    prev: dict[str, Any] | None,
    pending: dict[str, Any] | None,
    fuel_observed_at: datetime | None = None,
) -> _FuelLocationMatch:
    """Classify where the fuel was bought, tolerating delayed Samsara fuel data."""
    current_lat = float(location.lat)
    current_lng = float(location.lng)
    points: list[tuple[float, float, str]] = []
    current_speed=_to_float(getattr(location,'speed_mph',None))
    current_time=getattr(location,'gps_time',None)
    aligned = fuel_observed_at is None or (current_time is not None and abs((current_time-fuel_observed_at).total_seconds())<=600)
    if current_speed is not None and 0<=current_speed<=5 and aligned:
        points.append((current_lat,current_lng,'current'))

    prev_coords = _snapshot_coords(prev)
    previous_speed=_to_float((prev or {}).get('speed_mph'))
    aligned_previous=fuel_observed_at is None or ((prev or {}).get('gps_observed_at') is not None and abs((prev['gps_observed_at']-fuel_observed_at).total_seconds())<=600)
    if prev_coords is not None and previous_speed is not None and 0<=previous_speed<=5 and aligned_previous:
        points.append((prev_coords[0], prev_coords[1], "previous"))

    rec_site_id = _pending_site_id(pending)
    advised_stop = _advised_stop_from_pending(pending)
    advised_lat = _to_float((advised_stop or {}).get("latitude"))
    advised_lng = _to_float((advised_stop or {}).get("longitude"))

    ambiguous=set()
    if rec_site_id is not None and advised_lat is not None and advised_lng is not None:
        for lat, lng, source in points:
            distance_to_advised = haversine_km(lat, lng, advised_lat, advised_lng)
            if distance_to_advised <= .25:
                other=await _nearby_priced_stop(lat,lng,exclude_site_id=rec_site_id)
                if other and haversine_km(lat,lng,float(other['latitude']),float(other['longitude']))<=.25:
                    ambiguous.add(source)
                    continue  # Overlapping station geofences cannot prove the pump used.
                return _FuelLocationMatch(
                    classification="recommended",
                    nearby=_stop_dict_from_advised(advised_stop, rec_site_id),
                    lat=lat,
                    lng=lng,
                    source=source,
                )

    other_match: _FuelLocationMatch | None = None
    for lat, lng, source in points:
        if source in ambiguous:
            continue
        nearby = await _nearby_priced_stop(lat, lng, exclude_site_id=-1)
        if nearby is None or haversine_km(lat,lng,float(nearby["latitude"]),float(nearby["longitude"]))>.25:
            continue
        if rec_site_id is not None and int(nearby["site_id"]) == rec_site_id:
            return _FuelLocationMatch(
                classification="recommended",
                nearby=nearby,
                lat=lat,
                lng=lng,
                source=source,
            )
        if other_match is None:
            other_match = _FuelLocationMatch(
                classification="contracted_other",
                nearby=nearby,
                lat=lat,
                lng=lng,
                source=source,
            )

    if other_match is not None:
        return other_match

    return _FuelLocationMatch(
        classification="off_network",
        nearby=None,
        lat=current_lat,
        lng=current_lng,
        source="current",
    )


def _snapshot_coords(prev: dict[str, Any] | None) -> tuple[float, float] | None:
    if prev is None:
        return None
    age = prev.get('age_minutes')
    if age is None or not 0 <= float(age) <= 10:
        return None
    gps_time = prev.get('gps_observed_at')
    if gps_time is None or not 0 <= (datetime.now(timezone.utc)-gps_time).total_seconds() <= 900:
        return None
    lat = _to_float(prev.get("latitude"))
    lng = _to_float(prev.get("longitude"))
    if lat is None or lng is None:
        return None
    return lat, lng


def _pending_site_id(pending: dict[str, Any] | None) -> int | None:
    if pending is None:
        return None
    try:
        return int(pending["recommended_site_id"])
    except (KeyError, TypeError, ValueError):
        return None


def _stop_dict_from_advised(
    advised_stop: dict[str, Any] | None,
    rec_site_id: int,
) -> dict[str, Any]:
    advised_stop = advised_stop or {}
    return {
        "site_id": rec_site_id,
        "your_price": _to_float(advised_stop.get("your_price")) or 0.0,
        "state": str(advised_stop.get("state") or ""),
        "station_name": advised_stop.get("station_name") or "recommended stop",
        "address": advised_stop.get("address"),
        "city": advised_stop.get("city"),
        "latitude": _to_float(advised_stop.get("latitude")),
        "longitude": _to_float(advised_stop.get("longitude")),
    }


async def _alert_for_fuel_event(
    *,
    bot: Bot,
    event_id: int | None,
    classification: str,
    truck_unit: str,
    gallons: float,
    nearby: dict[str, Any] | None,
    pending: dict[str, Any] | None,
    truck_row: dict[str, Any] | None,
) -> None:
    """Record-only hook: the second brain never sends Telegram alerts."""
    log.info(
        "fuel_brain: truck %s fuel event %s recorded silently — %s%s",
        truck_unit,
        event_id,
        classification,
        f" at {nearby['station_name']}" if nearby else "",
    )


# ── Snapshot plumbing ────────────────────────────────────────────────────────

async def _maybe_write_snapshot(
    *,
    vehicle_id: str,
    truck_unit: str,
    location: Any,
    fuel_pct: float | None,
    prev: dict[str, Any] | None,
    fuel_observed_at: datetime | None = None,
) -> bool:
    """Write one truck_snapshots row for every successful GPS poll."""
    await execute(
        """
        INSERT INTO truck_snapshots
            (samsara_vehicle_id, truck_unit, latitude, longitude,
             fuel_pct, speed_mph, heading,gps_observed_at,fuel_observed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7,$8,$9)
        """,
        vehicle_id,
        truck_unit,
        float(location.lat),
        float(location.lng),
        round(fuel_pct, 2) if fuel_pct is not None else None,
        getattr(location, "speed_mph", None),
        None,  # VehicleLocation has no heading; kept for schema compat
        getattr(location,'gps_time',None),
        fuel_observed_at,
    )
    return True


async def _last_snapshots() -> dict[str, dict[str, Any]]:
    """Latest snapshot per vehicle, with age in minutes."""
    rows = await fetch_all(
        """
        SELECT DISTINCT ON (samsara_vehicle_id)
               samsara_vehicle_id, latitude, longitude, fuel_pct,speed_mph,gps_observed_at,fuel_observed_at,
               EXTRACT(EPOCH FROM (NOW() - taken_at)) / 60.0 AS age_minutes
        FROM truck_snapshots
        ORDER BY samsara_vehicle_id, taken_at DESC
        """
    )
    return {r["samsara_vehicle_id"]: dict(r) for r in rows}


async def _open_fuel_events() -> dict[str, dict[str, Any]]:
    """In-progress (unfinalized) fuel events per vehicle. Stale ones are closed."""
    rows = await fetch_all(
        """
        SELECT id, samsara_vehicle_id, fuel_pct_start,
               EXTRACT(EPOCH FROM (NOW() - detected_at)) / 60.0 AS age_minutes
        FROM fuel_events
        WHERE finalized_at IS NULL
          AND ($1::text[] IS NULL OR ltrim(upper(truck_unit),'0')=ANY($1))
        """, allowed_units()
    )
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        if (_to_float(r["age_minutes"]) or 0.0) >= FUEL_EVENT_MERGE_WINDOW_MINUTES:
            await execute(
                "UPDATE fuel_events SET finalized_at = NOW() WHERE id = $1 AND finalized_at IS NULL",
                r["id"],
            )
            continue
        out[r["samsara_vehicle_id"]] = dict(r)
    return out


async def _trucks_by_vehicle_id() -> dict[str, dict[str, Any]]:
    rows = await fetch_all(
        "SELECT truck_unit, driver_telegram_id, samsara_vehicle_id "
        "FROM trucks_drivers WHERE samsara_vehicle_id IS NOT NULL"
    )
    return {r["samsara_vehicle_id"]: dict(r) for r in rows}


async def _pending_events_by_unit() -> dict[str, dict[str, Any]]:
    """Newest pending stop_event per truck_unit (and per vehicle id alias)."""
    rows = await fetch_all(
        """
        SELECT DISTINCT ON (truck_unit)
               id, truck_unit, load_id, recommended_site_id,
               recommended_true_cost, worst_candidate_true_cost,
               candidates, samsara_vehicle_id
        FROM stop_events
        WHERE status = 'pending'
        ORDER BY truck_unit, recommended_at DESC
        """
    )
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not allows(r['truck_unit']):
            continue
        d = dict(r)
        out[str(r["truck_unit"])] = d
        if r["samsara_vehicle_id"]:
            out.setdefault(str(r["samsara_vehicle_id"]), d)
    return out


def _advised_stop_from_pending(pending: dict[str, Any] | None) -> dict[str, Any] | None:
    if pending is None:
        return None
    candidates = _parse_candidates(pending.get("candidates"))
    return candidates[0] if candidates else None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
