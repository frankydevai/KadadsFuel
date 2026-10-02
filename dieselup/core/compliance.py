"""
Compliance resolver — every 5 min, decide if a pending stop_event became
saved, lost, or skipped.

APScheduler fires `resolve_pending_events(bot)` on an IntervalTrigger(minutes=5).
For every row in stop_events with status='pending' the resolver:

  1. Looks up the truck's Samsara vehicle ID via trucks_drivers.
  2. Pulls the truck's current GPS from Samsara.
  3. Runs the 1-km haversine geofence check against:
       a. The recommended stop's coordinates → status='saved'
       b. Any other priced Pilot/FJ stop's coordinates → status='lost'
  4. For verified advice, checks fresh GPS/fuel and progress along the
     recorded road route, with a two-mile buffer and persisted fueling
     observations. Ambiguous loops/off-route positions cannot prove a miss.
     Legacy advice without route evidence expires for a fresh calculation.
  5. On any successful resolution writes status, actual_site_id,
     actual_true_cost, dollar_impact, resolved_at — atomically per event.

Dollar math:

    saved   : (worst_candidate_true_cost - recommended_true_cost) * gallons
    lost    : (recommended_true_cost     - actual_true_cost     ) * gallons
    skipped : zero; a bypass alone does not prove a financial loss.

`gallons` is the recommendation size, sourced from the
stop_events row itself so re-tuning FULL_FUEL_TARGET_GALLONS later can't
retroactively change historical math.

Per-event exceptions are logged and forwarded to admin via Telegram; the
sweep keeps going so one bad row can't stall the whole fleet. Samsara or
DataTruck transport failures leave the event pending for the next cycle —
never a silent skip.
"""
from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import time
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any

from telegram import Bot

from dieselup import metrics
from dieselup.bot.messages import (
    approach_reminder_message,
    approach_reminder_keyboard,
    missed_fuel_stop_message,
    off_network_fueling_message,
    wrong_fuel_stop_message,
)
from dieselup.core.load_sync import _is_driver_resting
from dieselup.bot.sender import safe_send
from dieselup.clients.tms import TMSClient, TMS_ERRORS, make_tms_client
from dieselup.clients.samsara import SamsaraClient, SamsaraError
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.core import advice_audit
from dieselup.core.route_progress import passed_stop_evidence
from dieselup.db import execute, fetch_all, fetch_one
from dieselup.core.price_sources import pilot_price_rows

log = logging.getLogger(__name__)

# Haversine in kilometers — the geofence radius is specified in km in CLAUDE.md.
EARTH_RADIUS_KM = 6371.0088
GEOFENCE_RADIUS_KM = 1.0

# 30-mile approach-reminder threshold, converted to km for the haversine helper.
MILES_TO_KM = 1.609344
APPROACH_REMINDER_MILES = 30.0
APPROACH_REMINDER_KM = APPROACH_REMINDER_MILES * MILES_TO_KM

# Pre-filter bbox half-width for the "nearby priced stop" query. 0.02° latitude
# is ~2.2 km, comfortably wider than the 1 km geofence so haversine never
# misses a stop the SQL bbox excluded.
NEARBY_BBOX_DEGREES = 0.02

# Longitude jitter buffer — GPS at the pump can drift a few hundred meters.
# 0.01° (~1.1 km at mid-latitudes) keeps the "past the stop" check from
# firing while the truck is still parked next to the pump.
PAST_LONGITUDE_BUFFER_DEGREES = 0.01

# Minimum distance from the recommended stop before a "lost" (wrong stop)
# alert fires. Many Pilot + Flying J locations share a campus — a truck within
# this buffer of the recommended stop is treated as still on-site even if a
# neighbouring stop is also within the 1-km geofence.
LOST_BUFFER_KM = GEOFENCE_RADIUS_KM * 1.5  # 1.5 km

# A real bypass should be detected shortly after the truck passes the advised
# stop. When the truck is hundreds/thousands of miles away, the pending event is
# stale or attached to the wrong/old route; scoring it as a missed stop creates
# fake losses like "1972 mi past the stop." Expire and replan instead.
MISSED_STOP_MAX_ALERT_DISTANCE_MILES = 250.0

# Stale-event safety valve. A pending event older than STALE_HOURS whose truck
# has clearly MOVED ON (>= EXPIRE_MOVED_MILES from where the plan was made), or
# whose truck is unreachable in Samsara, is force-resolved as 'expired' (zero
# dollar impact) so the next sequential fuel leg can fire. A truck that is
# merely detained (long shipper dwell, 10-hr HOS reset — hasn't moved) gets
# until HARD_STALE_HOURS before the valve fires regardless.
STALE_HOURS = 8.0
HARD_STALE_HOURS = 24.0
EXPIRE_MOVED_MILES = 25.0

# Fueling resolution threshold. A stop is not marked saved/lost just because a
# truck entered a geofence; Samsara fuel must rise by at least this many gallons.
# This matches fuel_brain's event floor and avoids scoring sensor slosh, parking
# visits, or tiny top-ups as compliance outcomes.
FUELED_EVENT_GALLONS = 30.0

PER_EVENT_TIMEOUT_SECONDS = 30.0
# Finish or fail before the next five-minute scheduler slot. Per-event limits
# alone do not cover the initial database read, replans, or cleanup passes.
COMPLIANCE_CYCLE_TIMEOUT_SECONDS = 240.0
ERROR_ALERT_COOLDOWN_SECONDS = 6 * 60 * 60
_recent_error_alerts: dict[tuple[int, str, str], float] = {}


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance between two WGS-84 points in kilometers."""
    lat1_r, lat2_r = radians(lat1), radians(lat2)
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a))


def compute_dollar_impact(
    *,
    status: str,
    recommended_true_cost: float,
    worst_candidate_true_cost: float,
    actual_true_cost: float | None,
    gallons: float,
) -> float:
    """Return the signed dollar impact for a resolved event.

    `lost` requires `actual_true_cost`; a missed stop alone has no financial loss.
    Raises ValueError for any other status or for `lost` with no actual cost.
    """
    if status == "saved":
        return (worst_candidate_true_cost - recommended_true_cost) * gallons
    if status == "lost":
        if actual_true_cost is None:
            raise ValueError("status='lost' requires actual_true_cost")
        return (recommended_true_cost - actual_true_cost) * gallons
    if status == "skipped":
        return 0.0
    if status == "expired":
        # Operational timeout, not a driver decision — must not pollute loss stats.
        return 0.0
    raise ValueError(f"unknown status: {status!r}")


async def resolve_pending_events(bot: Bot) -> None:
    """APScheduler entrypoint — wire to IntervalTrigger(minutes=5)."""
    log.info("compliance: starting resolution sweep")
    metrics.incr("compliance_cycles_total")
    deadline = asyncio.timeout(COMPLIANCE_CYCLE_TIMEOUT_SECONDS)
    try:
        async with deadline:
            await _resolve_pending_events(bot)
    except TimeoutError:
        if not deadline.expired():
            raise  # Preserve an upstream/database timeout's own diagnosis.
        metrics.incr("compliance_cycle_timeouts_total")
        log.exception(
            "compliance: resolution sweep timed out (cycle limit %.0fs); "
            "pending evidence is retained for the next sweep",
            COMPLIANCE_CYCLE_TIMEOUT_SECONDS,
        )
        raise
    # Only completed work counts as healthy, including post-fueling checks
    # and requested replans. Exceptions and cancellation leave this unchanged.
    metrics.gauge("compliance_last_heartbeat_mono", time.monotonic())


async def _resolve_pending_events(bot: Bot) -> None:
    pending = await fetch_all(
        """
        SELECT id, truck_unit, driver_id, load_id, datatruck_order_id, tms_order_id,
               recommended_site_id, recommended_true_cost,
               worst_candidate_true_cost, gallons, candidates,
               approach_ping_sent_at, briefing_driver_msg_id,
               delivery_driver_msg_id, fuel_pct_before, recommended_at,
               samsara_vehicle_id
        FROM stop_events
        WHERE status = 'pending'
          AND ($1::text[] IS NULL OR ltrim(upper(truck_unit),'0')=ANY($1))
        """, allowed_units()
    )
    if not pending:
        log.info("compliance: no pending events")
        from dieselup.core.fuel_replan import run_requested_replans
        await run_requested_replans(bot)
        return

    saved = lost = skipped = expired = errored = still_pending = 0

    async with SamsaraClient() as samsara, make_tms_client() as datatruck:
        for event in pending:
            if not allows(event['truck_unit']):
                continue
            try:
                outcome = await asyncio.wait_for(
                    _resolve_one(event, samsara=samsara, datatruck=datatruck, bot=bot),
                    timeout=PER_EVENT_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001 — one bad row can't stall the sweep
                errored += 1
                log.exception("compliance: error resolving event %s", event["id"])
                await _alert_admin_error(bot, event, exc)
                continue

            if outcome == "saved":
                saved += 1
                metrics.incr("compliance_resolved_saved")
            elif outcome == "lost":
                lost += 1
                metrics.incr("compliance_resolved_lost")
            elif outcome == "skipped":
                skipped += 1
                metrics.incr("compliance_resolved_skipped")
            elif outcome == "expired":
                expired += 1
                metrics.incr("compliance_resolved_expired")
            else:
                still_pending += 1

    metrics.gauge("compliance_last_cycle_pending", still_pending)
    metrics.gauge("compliance_last_cycle_errored", errored)

    log.info(
        "compliance: done — saved=%d lost=%d skipped=%d expired=%d "
        "still_pending=%d errored=%d",
        saved, lost, skipped, expired, still_pending, errored,
    )

    # Post-fueling verification: for saved events from the last 2 hours that
    # still lack a fuel_pct_after reading, ask Samsara for the current fuel
    # level and compute actual_gallons = delta × TANK_CAPACITY. Runs after
    # the main sweep so a single SamsaraClient session covers both passes.
    await _verify_fueling_deltas(bot)
    from dieselup.core.fuel_replan import run_requested_replans
    await run_requested_replans(bot)


async def _resolve_one(
    event: Any,
    *,
    samsara: SamsaraClient,
    datatruck: TMSClient,
    bot: Bot,
) -> str | None:
    """Resolve one pending event. Returns the new status, or None if still pending.

    Stale-event safety valve: the age check runs BEFORE every early return.
    Previously it sat at the bottom of this function, so the exact failure
    modes it was built for (missing samsara_vehicle_id, Samsara unavailable,
    unit mismatch) returned None first and the event stayed pending forever,
    blocking all subsequent fuel legs for the load.
    """
    if not allows(event['truck_unit']):
        return None
    age_hours = _event_age_hours(event)
    stale = age_hours is not None and age_hours >= STALE_HOURS

    truck = await fetch_one(
        "SELECT samsara_vehicle_id, driver_telegram_id, driver_full_name FROM trucks_drivers WHERE truck_unit = $1",
        event["truck_unit"],
    )
    # Canonical identity: prefer the vehicle id stamped on the event itself;
    # fall back to trucks_drivers for legacy rows.
    vehicle_id = _event_get(event, "samsara_vehicle_id") or (
        truck["samsara_vehicle_id"] if truck else None
    )
    driver_telegram_id = truck["driver_telegram_id"] if truck else None
    if not vehicle_id:
        if stale:
            return await _expire_event(event, age_hours, reason="no samsara_vehicle_id")
        log.warning(
            "compliance: event %d truck %s missing samsara_vehicle_id",
            event["id"], event["truck_unit"],
        )
        return None

    # Prefer coordinates from the stored candidates JSON — they were written at
    # recommendation time and are exact. The DB fallback (_site_coords) joins on
    # city+state, which is ambiguous when multiple stops share the same city/state
    # and can return the wrong row's lat/lng, causing false wrong-stop alerts.
    rec_stop = _selected_stop_from_event(event)
    _rec_lat = rec_stop.get("latitude") if rec_stop else None
    _rec_lng = rec_stop.get("longitude") if rec_stop else None

    if _rec_lat is not None and _rec_lng is not None:
        rec_coords: dict[str, Any] = {
            "latitude": float(_rec_lat),
            "longitude": float(_rec_lng),
        }
    else:
        rec_coords = await _site_coords(event["recommended_site_id"])  # type: ignore[assignment]
        if rec_coords is None:
            if stale:
                return await _expire_event(event, age_hours, reason="unresolvable stop coords")
            log.warning(
                "compliance: event %d recommended_site_id %d not joinable to fuel_stops",
                event["id"], event["recommended_site_id"],
            )
            return None

    try:
        location = await samsara.get_vehicle_location(vehicle_id)
    except SamsaraError as exc:
        if stale:
            return await _expire_event(event, age_hours, reason=f"Samsara unavailable ({exc})")
        log.warning(
            "compliance: Samsara unavailable for truck %s (event %d): %s",
            event["truck_unit"], event["id"], exc,
        )
        return None

    proof = (rec_stop or {}).get("plan", {}).get("route_evidence", {})
    verified_route = (proof.get("model") == "remaining_route_v1" and proof.get("trip_verified") is True
                      and proof.get("complete_candidate_coverage") is True and proof.get("samsara_vehicle_id")==vehicle_id)
    if not verified_route:
        return await _expire_event(event, age_hours, reason="legacy_plan_requires_route_refresh")
    if verified_route:
        if truck and truck["samsara_vehicle_id"] != vehicle_id:
            return await _expire_event(event, age_hours, reason="truck_assignment_changed")
        frozen_driver = (rec_stop or {}).get("plan", {}).get("driver_at_advice") or {}
        if (truck and _event_get(event, "driver_id") is not None
                and _event_get(event, "driver_id") != driver_telegram_id):
            return await _expire_event(event, age_hours, reason="driver_assignment_changed")
        if (truck and frozen_driver.get("name") and truck.get("driver_full_name")
                and " ".join(frozen_driver["name"].casefold().split())
                    != " ".join(truck["driver_full_name"].casefold().split())):
            return await _expire_event(event, age_hours, reason="driver_assignment_changed")
        if location.gps_age_minutes is None or not 0 <= location.gps_age_minutes <= settings.MAX_ADVICE_GPS_AGE_MINUTES:
            await advice_audit.record("monitor_held", truck_unit=event["truck_unit"], event_id=event["id"],
                                      details={"reason": "Fresh GPS is required to detect a missed stop"})
            return None
        from dieselup.core.stop_visits import observe_visit
        await observe_visit(event["id"], location, rec_coords)
        stats = await samsara.get_vehicle_stats(vehicle_id)
        if stats.fuel_age_minutes is None or not 0 <= stats.fuel_age_minutes <= settings.MAX_ADVICE_FUEL_AGE_MINUTES:
            await advice_audit.record("monitor_held", truck_unit=event["truck_unit"], event_id=event["id"],
                                      details={"reason": "Fresh fuel data is required to monitor fueling"})
            return None
        if _event_get(event,'tms_order_id') or _event_get(event,'datatruck_order_id'):
            order=await datatruck.get_order(_event_get(event,'tms_order_id') or _event_get(event,'datatruck_order_id'))
            from dieselup.core.load_sync import _extract_truck_unit
            from dieselup.clients.samsara import extract_unit_digits
            if extract_unit_digits(_extract_truck_unit(order) or '') != extract_unit_digits(event['truck_unit']):
                return await _expire_event(event,age_hours,reason='trip_assignment_changed')
        # A truck may have fueled and left between compliance polls. Use the
        # brain's persisted observation before declaring a route bypass.
        observed = await fetch_one("""SELECT classification, site_id, gallons, fuel_pct_end,detected_at,finalized_at
            FROM fuel_events WHERE stop_event_id=$1 AND samsara_vehicle_id=$2
              AND gallons >= $3 AND detected_at >= $4 ORDER BY detected_at DESC LIMIT 1""",
            event["id"], vehicle_id, FUELED_EVENT_GALLONS,event["recommended_at"])
        if observed and observed.get('finalized_at') is None:
            await advice_audit.record('monitor_held',event_id=event['id'],details={'reason':'Fueling is in progress; final quantity awaits a stable reading'})
            return None
        if observed and observed.get("classification") == "recommended":
            actual = float(observed["gallons"])
            await _mark_resolved(event_id=event["id"], status="saved", actual_site_id=event["recommended_site_id"],
                                 actual_true_cost=float(event["recommended_true_cost"]),
                                 dollar_impact=compute_dollar_impact(status="saved", recommended_true_cost=float(event["recommended_true_cost"]),
                                     worst_candidate_true_cost=float(event["worst_candidate_true_cost"]), actual_true_cost=None, gallons=actual))
            await _stamp_fuel_delta(event_id=event["id"], fuel_pct_after=float(observed["fuel_pct_end"]), actual_gallons=actual)
            return "saved"
        if observed and observed.get("classification") in {"contracted_other", "off_network"}:
            return await _resolve_observed_other_fueling(event,observed,bot,driver_telegram_id,location=location)

    gallons = int(event["gallons"])

    distance_to_rec_km = haversine_km(
        location.lat, location.lng, rec_coords["latitude"], rec_coords["longitude"]
    )

    # 0. 30-mile approach reminder — fires once per event when the truck gets
    # within APPROACH_REMINDER_MILES of the recommended stop and we haven't
    # pinged yet. Outside the 1km geofence (we don't want to fire it while
    # the truck is parked at the pump). Failure to send is logged but never
    # blocks the saved/lost/skipped resolution that follows.
    # During quiet hours the driver ping is suppressed — dispatch still gets it.
    if (
        event["approach_ping_sent_at"] is None
        and GEOFENCE_RADIUS_KM < distance_to_rec_km <= APPROACH_REMINDER_KM
    ):
        driver_resting = _is_driver_resting(location)
        if driver_resting:
            log.info(
                "compliance: truck %s is resting (speed=%.1f mph, GPS age=%.0f min) — "
                "approach reminder suppressed for driver, dispatch only",
                event["truck_unit"],
                location.speed_mph or 0.0,
                location.gps_age_minutes or 0.0,
            )
        await _send_approach_reminder(
            bot=bot,
            event_id=event["id"],
            truck_unit=event["truck_unit"],
            # Resting is the ONLY driver suppression for approach pings. The old
            # extra gate (driver must have received the auto briefing) silenced
            # approach reminders whenever the briefing had gone dispatch-only —
            # exactly when the driver most needs the ping.
            driver_telegram_id=(
                driver_telegram_id if not driver_resting else None
            ),
            distance_miles=distance_to_rec_km / MILES_TO_KM,
            candidates_raw=event["candidates"],
            gallons=gallons,
            truck_lat=location.lat,
            truck_lng=location.lng,
        )

    # Presence alone is a physical visit. Fueling is resolved only from the
    # attributed, finalized sensor event above, never from a trip-total delta.
    if distance_to_rec_km <= GEOFENCE_RADIUS_KM:
        return None

    # 3. Verify passage on the stored road route; never infer a miss from longitude.
    missed_evidence = None
    if verified_route:
        order_id = _event_get(event, "tms_order_id") or _event_get(event, "datatruck_order_id")
        order = await datatruck.get_order(order_id)
        if _delivery_completed(order):
            return await _expire_event(event, age_hours, reason="trip_completed_without_verified_fueling")
        from dieselup.core.load_sync import _extract_truck_unit
        from dieselup.clients.samsara import extract_unit_digits
        if extract_unit_digits(_extract_truck_unit(order) or "") != extract_unit_digits(event["truck_unit"]):
            return await _expire_event(event, age_hours, reason="trip_assignment_changed")
        stats = await samsara.get_vehicle_stats(vehicle_id)
        if stats.fuel_age_minutes is None or not 0 <= stats.fuel_age_minutes <= settings.MAX_ADVICE_FUEL_AGE_MINUTES:
            await advice_audit.record("monitor_held", truck_unit=event["truck_unit"], event_id=event["id"],
                                      details={"reason": "Fresh fuel data is required to detect a missed stop"})
            return None
        missed_evidence = passed_stop_evidence(proof, location)
        if missed_evidence is not None and await _fuel_delta_since_recommendation(event=event,samsara=samsara,vehicle_id=vehicle_id):
            await advice_audit.record('monitor_held',event_id=event['id'],details={'reason':'Fuel rise observed; station attribution awaits fuel capture'})
            return None
        triggered = missed_evidence is not None
    else:
        # Legacy advice has no trustworthy ordered route. It must be refreshed;
        # longitude alone is no longer sufficient to score a driver miss.
        return await _expire_event(event, age_hours, reason="legacy_plan_requires_route_refresh")
    if triggered:
        distance_miles = distance_to_rec_km / MILES_TO_KM
        if not _missed_stop_distance_reliable(distance_miles):
            log.warning(
                "compliance: event %d truck %s load %s is %.0f mi from recommended "
                "stop — treating missed-stop evidence as stale/unreliable",
                event["id"],
                event["truck_unit"],
                event["load_id"],
                distance_miles,
            )
            return await _expire_event(
                event,
                age_hours,
                reason=f"unreliable_missed_stop_distance_{round(distance_miles)}mi",
            )
        visited = await advice_audit.fetch_one("SELECT id FROM fuel_advice_audit WHERE stop_event_id=$1 AND kind='stop_visited' LIMIT 1", event["id"])
        if visited:
            missed_evidence = {**missed_evidence, "visited": True, "reason": "visited_without_confirmed_fueling"}
        impact = 0.0  # Passing a station is not proof of a financial loss.
        await _mark_resolved(
            event_id=event["id"],
            status="skipped",
            actual_site_id=None,
            actual_true_cost=None,
            dollar_impact=impact,
            evidence=missed_evidence,
        )
        # CLAUDE.md: missed-stop alerts are NEVER suppressed.
        if visited:
            from dieselup.core.fuel_replan import replan_truck
            await replan_truck(bot, event["truck_unit"], event["id"])
            return "skipped"
        await _send_missed_stop_alert(
            bot=bot,
            samsara=samsara,
            vehicle_id=vehicle_id,
            event=event,
            driver_telegram_id=driver_telegram_id,
            location=location,
            distance_miles=distance_miles,
            dollar_impact=impact,
        )
        return "skipped"

    # Stale-event safety valve (movement-aware). Only force-expire when the
    # truck has clearly moved on from where the plan was generated — a truck
    # detained at a shipper or on a 10-hr HOS reset hasn't moved and may still
    # fuel at the recommended stop, so it gets until HARD_STALE_HOURS.
    if stale:
        moved_miles = _miles_from_plan_position(event, location)
        if (
            moved_miles is None
            or moved_miles >= EXPIRE_MOVED_MILES
            or (age_hours or 0.0) >= HARD_STALE_HOURS
        ):
            return await _expire_event(
                event,
                age_hours,
                reason=(
                    f"moved {moved_miles:.0f} mi since plan"
                    if moved_miles is not None
                    else "no plan position stored"
                ),
            )
        log.info(
            "compliance: event %d is %.1fh old but truck only moved %.1f mi — "
            "likely detained; deferring expiry until %dh",
            event["id"], age_hours or 0.0, moved_miles, int(HARD_STALE_HOURS),
        )

    return None


def _event_get(event: Any, key: str) -> Any:
    """Tolerant field access for asyncpg Records and plain dicts alike."""
    try:
        return event[key]
    except (KeyError, IndexError, TypeError):
        return None


def _event_age_hours(event: Any) -> float | None:
    rec_at = _event_get(event, "recommended_at")
    if rec_at is None:
        return None
    if rec_at.tzinfo is None:
        rec_at = rec_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - rec_at).total_seconds() / 3600


def _miles_from_plan_position(event: Any, location: Any) -> float | None:
    """Miles between the truck now and where it was when the plan was made."""
    stop = _selected_stop_from_event(event)
    if stop is None:
        return None
    plan_lat = stop.get("truck_latitude")
    plan_lng = stop.get("truck_longitude")
    if not isinstance(plan_lat, (int, float)) or not isinstance(plan_lng, (int, float)):
        return None
    return haversine_km(location.lat, location.lng, float(plan_lat), float(plan_lng)) / MILES_TO_KM


async def _expire_event(event: Any, age_hours: float | None, *, reason: str) -> str:
    """Force-resolve a stale pending event as 'expired' (zero dollar impact).

    Also releases the driver-briefing fingerprint for this truck/load/site so
    the replacement plan can re-brief the driver even if it picks the same
    stop — without this, the replan went dispatch-only and the driver was left
    with no live plan at all.
    """
    log.warning(
        "compliance: event %d truck %s load %s is %.1fh old — force-resolving "
        "as EXPIRED (%s) to unblock sequential briefing",
        event["id"], event["truck_unit"], event["load_id"], age_hours or -1.0, reason,
    )
    await _mark_resolved(
        event_id=event["id"],
        status="expired",
        actual_site_id=None,
        actual_true_cost=None,
        dollar_impact=0.0,
    )
    try:
        from dieselup.core.load_sync import _driver_briefing_fingerprint
        fingerprint = _driver_briefing_fingerprint(
            alert_kind="briefing",
            truck_unit=str(event["truck_unit"]),
            load_id=str(event["load_id"]),
            recommended_site_id=int(event["recommended_site_id"]),
        )
        await execute(
            "DELETE FROM alert_send_fingerprints WHERE fingerprint = $1",
            fingerprint,
        )
    except Exception:  # noqa: BLE001 — fingerprint release is best-effort
        log.exception(
            "compliance: failed to release briefing fingerprint for event %d",
            event["id"],
        )
    return "expired"


async def _fuel_delta_since_recommendation(
    *,
    event: Any,
    samsara: SamsaraClient,
    vehicle_id: str,
) -> tuple[float, float] | None:
    """Return (fuel_pct_after, gallons_delta) only for a real 30+ gal fueling."""
    pct_before_raw = _event_get(event, "fuel_pct_before")
    if pct_before_raw is None:
        return None
    try:
        fuel_gallons_now = await samsara.get_vehicle_fuel(vehicle_id)
    except SamsaraError:
        return None

    pct_before = float(pct_before_raw)
    pct_now = fuel_gallons_now / settings.TANK_CAPACITY_GALLONS * 100.0
    delta_gallons = round(
        (pct_now - pct_before) / 100.0 * settings.TANK_CAPACITY_GALLONS, 1
    )
    if delta_gallons < FUELED_EVENT_GALLONS:
        return None
    return round(pct_now, 2), delta_gallons


async def _stamp_fuel_delta(
    *,
    event_id: int,
    fuel_pct_after: float,
    actual_gallons: float,
) -> None:
    await execute(
        """
        UPDATE stop_events
        SET fuel_pct_after = $2, actual_gallons = $3
        WHERE id = $1 AND fuel_pct_after IS NULL
        """,
        event_id,
        round(fuel_pct_after, 2),
        actual_gallons,
    )


async def _resolve_observed_other_fueling(event,observed,bot,driver_telegram_id,*,location=None):
    from dieselup.core.observed_fueling import analysis
    advised = _selected_stop_from_event(event) or {}
    facts, actual_stop = await analysis(event,observed,advised)
    proof=advised.get('plan',{}).get('route_evidence',{})
    passage=passed_stop_evidence(proof,location) if location else None
    visited=await advice_audit.fetch_one("SELECT id FROM fuel_advice_audit WHERE stop_event_id=$1 AND kind='stop_visited' LIMIT 1",event['id'])
    if passage and passage["miles_beyond_stop"]<=MISSED_STOP_MAX_ALERT_DISTANCE_MILES and not visited:
        facts['bypass_evidence']=passage
    impact = (facts['saving'] or 0)-(facts['extra_cost'] or 0)
    await _mark_resolved(event_id=event['id'],status='lost',actual_site_id=observed.get('site_id'),
        actual_true_cost=facts.get('actual_price'),dollar_impact=impact,evidence=facts)
    await _stamp_fuel_delta(event_id=event['id'],fuel_pct_after=float(observed['fuel_pct_end']),
                           actual_gallons=float(observed['gallons']))
    if facts['price_status']=='contracted_estimate' and facts['extra_cost']>0 and actual_stop:
        await _send_wrong_stop_alert(bot=bot,event=event,driver_telegram_id=driver_telegram_id,
            advised_stop=advised,actual_stop=actual_stop,actual_true_cost=facts['actual_price'],dollar_impact=impact)
    return 'lost'


async def _detect_off_network_fueling(
    *,
    event: Any,
    samsara: SamsaraClient,
    vehicle_id: str,
    bot: Bot,
    driver_telegram_id: int | None,
    rec_true_cost: float,
    worst_true_cost: float,
) -> str | None:
    """Resolve as 'lost' when the fuel level jumped away from any contracted stop.

    Returns 'lost' on detection, None otherwise. Uses the measured Samsara fuel
    delta as gallons and the worst contracted true cost as a conservative proxy
    for the unknown off-network price.
    """
    fuel_delta = await _fuel_delta_since_recommendation(
        event=event,
        samsara=samsara,
        vehicle_id=vehicle_id,
    )
    if fuel_delta is None:
        return None

    pct_now, delta_gallons = fuel_delta
    actual_tc = None
    impact = 0.0  # Unknown pump price cannot establish a financial loss.

    log.warning(
        "compliance: event %d truck %s — fuel jumped +%.1f gal away from any "
        "contracted stop; resolving as lost (off-network fueling)",
        event["id"], event["truck_unit"], delta_gallons,
    )
    await _mark_resolved(
        event_id=event["id"],
        status="lost",
        actual_site_id=None,
        actual_true_cost=actual_tc,
        dollar_impact=impact,
        evidence={'fueling_confirmed':True,'price_status':'pending','gallons_estimated':delta_gallons},
    )
    await _stamp_fuel_delta(
        event_id=event["id"],
        fuel_pct_after=pct_now,
        actual_gallons=delta_gallons,
    )

    advised_stop = _selected_stop_from_event(event)
    text = off_network_fueling_message(
        truck_unit=str(event["truck_unit"]),
        advised_stop=advised_stop,
        gallons_estimate=delta_gallons,
        estimated_loss_dollars=None,
    )
    await _send_red_flag_alert(
        bot=bot,
        driver_telegram_id=driver_telegram_id,
        text=text,
        alert_type="off_network_fueling",
        truck_unit=str(event["truck_unit"]),
        load_id=str(event["load_id"]),
        event_id=int(event["id"]),
        dedupe_key=f"site:{event['recommended_site_id']}",
    )
    metrics.incr("compliance_off_network_fueling")
    return "lost"


async def _site_coords(site_id: int) -> dict[str, Any] | None:
    """Latitude/longitude for a contracted Pilot/FJ site ID."""
    row = await fetch_one(
        """
        SELECT COALESCE(fs.snapped_lat, fs.latitude) AS latitude,
               COALESCE(fs.snapped_lon, fs.longitude) AS longitude
        FROM fuel_stops fs
        WHERE fs.pilot_site_id = $1
        ORDER BY CASE WHEN fs.snapped_lat IS NULL OR fs.snapped_lon IS NULL
                      THEN 1 ELSE 0 END,
                 fs.id
        LIMIT 1
        """,
        site_id,
    )
    if row is None:
        return None
    return {
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
    }


async def _nearby_priced_stop(
    lat: float,
    lng: float,
    *,
    exclude_site_id: int,
) -> dict[str, Any] | None:
    """Closest priced Pilot/FJ stop within the 1-km geofence, excluding `exclude_site_id`."""
    query = f"""
        WITH prices AS ({pilot_price_rows('$6','$7')}), nearby_stops AS MATERIALIZED (
            SELECT DISTINCT ON(site_id) site_id,your_price,station_name,address,city,state,
                   latitude,longitude,effective_date
            FROM prices
            WHERE ((NOW() AT TIME ZONE 'America/New_York')::date)-effective_date BETWEEN 0 AND $8
              AND latitude BETWEEN $2 AND $3 AND longitude BETWEEN $4 AND $5
              AND truck_accessible IS DISTINCT FROM FALSE
            ORDER BY site_id,effective_date DESC,uploaded_at DESC
        )
        SELECT * FROM nearby_stops WHERE site_id<>$1
        """
    args = (
        exclude_site_id,
        lat - NEARBY_BBOX_DEGREES, lat + NEARBY_BBOX_DEGREES,
        lng - NEARBY_BBOX_DEGREES, lng + NEARBY_BBOX_DEGREES,
        settings.FTS_PRICE_CUSTOMER,settings.PILOT_ACCOUNT_NUMBER,settings.MAX_FUEL_PRICE_AGE_DAYS,
    )
    try:
        rows = await fetch_all(query, *args)
    except TimeoutError:
        # Compliance and load sync start on the same scheduler boundary. A
        # momentarily busy database must not turn a pending event into an admin
        # error; retry once after yielding, then let the normal per-event guard
        # preserve it as pending if the database is genuinely unavailable.
        log.warning(
            "compliance: nearby-stop lookup timed out for excluded site %s; retrying once",
            exclude_site_id,
        )
        await asyncio.sleep(0.25)
        rows = await fetch_all(query, *args)

    best: dict[str, Any] | None = None
    best_distance = GEOFENCE_RADIUS_KM
    for row in rows:
        distance = haversine_km(lat, lng, float(row["latitude"]), float(row["longitude"]))
        if distance <= best_distance:
            best = {
                "site_id": int(row["site_id"]),
                "your_price": float(row["your_price"]),
                "state": str(row["state"]),
                "station_name": row["station_name"],
                "address": row["address"],
                "city": row["city"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "price_date": str(row.get("effective_date")) if row.get("effective_date") else None,
            }
            best_distance = distance
    return best


async def _resolution_triggered(
    event: Any,
    *,
    datatruck: TMSClient,
    rec_lng: float,
    truck_lng: float,
) -> bool:
    """True if the load is delivered OR the truck has clearly bypassed the stop."""
    order_id = _event_get(event, "tms_order_id") or _event_get(event, "datatruck_order_id")
    if order_id is None:
        return False

    try:
        order = await datatruck.get_order(order_id)
    except TMS_ERRORS as exc:
        # Transport failure — try again next cycle, do not skip the event.
        log.warning(
            "compliance: TMS unavailable for order %s (event %d): %s",
            order_id, event["id"], exc,
        )
        return False

    if _delivery_completed(order):
        return True

    dest_lng = _destination_longitude(order)
    if dest_lng is None:
        return False
    return _truck_past_stop(truck_lng=truck_lng, rec_lng=rec_lng, dest_lng=dest_lng)


_DELIVERED_STATUSES = {"delivered", "invoiced", "completed", "complete"}


def _delivery_completed(order: dict[str, Any]) -> bool:
    """True if the order is actually delivered.

    The DataTruck v1 shape stores the appointment time in `delivery_time`
    on every order (set the moment the load is created), so it cannot be
    used as a "delivered" signal. The authoritative flag is the top-level
    `status` field. We also accept `completed_at` on a delivery stop as a
    secondary signal in case the API exposes one.
    """
    status = str(order.get("status") or "").strip().lower()
    if status in _DELIVERED_STATUSES:
        return True
    delivery = order.get("delivery")
    if isinstance(delivery, dict) and delivery.get("completed_at"):
        return True
    stops = order.get("stops")
    if isinstance(stops, list):
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            if str(stop.get("type") or stop.get("stop_type") or "").lower() in {
                "delivery", "dropoff", "drop", "destination",
            }:
                if stop.get("completed_at"):
                    return True
    return False


def _destination_longitude(order: dict[str, Any]) -> float | None:
    """Best-effort destination longitude — None if the order has no delivery coords."""
    for key in ("delivery", "dropoff", "destination"):
        v = order.get(key)
        if isinstance(v, dict):
            lng = _lng(v)
            if lng is not None:
                return lng

    stops = order.get("stops")
    if isinstance(stops, list):
        for stop in reversed(stops):
            if not isinstance(stop, dict):
                continue
            stop_type = str(stop.get("type") or stop.get("stop_type") or "").lower()
            if stop_type in {"delivery", "dropoff", "drop", "destination"}:
                lng = _lng(stop)
                if lng is not None:
                    return lng
        for stop in reversed(stops):
            if isinstance(stop, dict):
                lng = _lng(stop)
                if lng is not None:
                    return lng
    return None


def _lng(d: dict[str, Any]) -> float | None:
    v = d.get("longitude") if "longitude" in d else d.get("lng") or d.get("lon")
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _truck_past_stop(*, truck_lng: float, rec_lng: float, dest_lng: float) -> bool:
    """Decide if the truck has passed the recommended stop heading toward destination.

    Direction is inferred from the destination's longitude relative to the
    stop. A buffer absorbs GPS jitter so we don't fire 'skipped' while the
    truck is still parked at the pump.
    """
    if dest_lng == rec_lng:
        return False
    if dest_lng > rec_lng:
        return truck_lng > rec_lng + PAST_LONGITUDE_BUFFER_DEGREES
    return truck_lng < rec_lng - PAST_LONGITUDE_BUFFER_DEGREES


def _missed_stop_distance_reliable(distance_miles: float) -> bool:
    return 0 <= float(distance_miles) <= MISSED_STOP_MAX_ALERT_DISTANCE_MILES


async def _mark_resolved(
    *,
    event_id: int,
    status: str,
    actual_site_id: int | None,
    actual_true_cost: float | None,
    dollar_impact: float,
    evidence: dict | None = None,
) -> None:
    """Atomic write of resolution fields — only flips a row from 'pending'."""
    await execute(
        """
        WITH resolved AS (UPDATE stop_events
        SET status = $1,
            actual_site_id = $2,
            actual_true_cost = $3,
            dollar_impact = $4,
            resolved_at = NOW()
        WHERE id = $5 AND status = 'pending'
        RETURNING id,truck_unit,load_id)
        INSERT INTO fuel_advice_audit(event_key,kind,truck_unit,load_id,stop_event_id,details)
        SELECT 'resolved:' || id || ':' || $1,
               CASE WHEN $1='skipped' AND $6::jsonb @> '{"visited":true}'::jsonb THEN 'stop_visited_no_fill'
                    WHEN $1='skipped' THEN 'missed_detected' ELSE 'stop_' || $1 END,
               truck_unit,load_id,id,jsonb_build_object('status',$1::text,'dollar_impact',$4::numeric) || $6::jsonb
        FROM resolved ON CONFLICT(event_key) DO NOTHING
        """,
        status,
        actual_site_id,
        actual_true_cost,
        round(dollar_impact, 2),
        event_id,
        json.dumps(evidence or {}),
    )


async def _verify_fueling_deltas(bot: Bot) -> None:
    """Post-fueling pass: compute actual_gallons for recently saved events.

    Saved events within the last 2 hours that still have no fuel_pct_after are
    queried; for each we ask Samsara for the current fuel level. If the truck's
    fuel is HIGHER than fuel_pct_before, the delta × TANK_CAPACITY is the
    actual gallons pumped and is written back.  If the reading is lower or
    equal (truck hasn't fueled yet or Samsara data is stale) we leave the row
    unchanged — the next sweep will retry for up to 2 hours.
    """
    events = await fetch_all(
        """
        SELECT se.id, se.truck_unit, se.fuel_pct_before, se.samsara_vehicle_id
        FROM stop_events se
        WHERE se.status = 'saved'
          AND se.fuel_pct_after IS NULL
          AND se.resolved_at >= NOW() - INTERVAL '2 hours'
          AND ($1::text[] IS NULL OR ltrim(upper(se.truck_unit),'0')=ANY($1))
        """, allowed_units(),
    )
    if not events:
        return

    log.info("compliance: post-fueling delta check for %d event(s)", len(events))

    async with SamsaraClient() as samsara:
        for event in events:
            if not allows(event['truck_unit']):
                continue
            try:
                vehicle_id = _event_get(event, "samsara_vehicle_id")
                if not vehicle_id:
                    truck = await fetch_one(
                        "SELECT samsara_vehicle_id FROM trucks_drivers WHERE truck_unit = $1",
                        event["truck_unit"],
                    )
                    vehicle_id = truck["samsara_vehicle_id"] if truck else None
                if not vehicle_id:
                    continue

                fuel_pct_before = (
                    float(event["fuel_pct_before"]) if event["fuel_pct_before"] is not None else None
                )

                try:
                    fuel_gallons_now = await samsara.get_vehicle_fuel(vehicle_id)
                except SamsaraError as exc:
                    log.warning(
                        "compliance: Samsara fuel unavailable for truck %s (event %d): %s",
                        event["truck_unit"], event["id"], exc,
                    )
                    continue

                fuel_pct_after = fuel_gallons_now / settings.TANK_CAPACITY_GALLONS * 100.0
                actual_gallons: float | None = None

                if fuel_pct_before is not None and fuel_pct_after > fuel_pct_before + 1.0:
                    # At least 1% increase (~2 gal) to avoid GPS/sensor noise
                    delta_pct = fuel_pct_after - fuel_pct_before
                    actual_gallons = round(delta_pct / 100.0 * settings.TANK_CAPACITY_GALLONS, 1)

                await execute(
                    """
                    UPDATE stop_events
                    SET fuel_pct_after = $2,
                        actual_gallons = $3
                    WHERE id = $1 AND fuel_pct_after IS NULL
                    """,
                    event["id"],
                    round(fuel_pct_after, 2),
                    actual_gallons,
                )
                log.info(
                    "compliance: event %d fuel_pct_before=%.1f%% fuel_pct_after=%.1f%% actual_gallons=%s",
                    event["id"],
                    fuel_pct_before if fuel_pct_before is not None else -1.0,
                    fuel_pct_after,
                    actual_gallons,
                )

            except Exception:  # noqa: BLE001
                log.exception("compliance: error in post-fueling delta for event %d", event["id"])


async def _send_wrong_stop_alert(
    *,
    bot: Bot,
    event: Any,
    driver_telegram_id: int | None,
    advised_stop: dict[str, Any] | None,
    actual_stop: dict[str, Any],
    actual_true_cost: float,
    dollar_impact: float,
) -> None:
    """Tell driver/dispatch the truck fueled at a different priced stop."""
    if advised_stop is None:
        log.warning(
            "compliance: event %d has no selected candidate; skipping wrong-stop alert",
            event["id"],
        )
        return

    estimated_loss = max(-float(dollar_impact), 0.0)
    text = wrong_fuel_stop_message(
        truck_unit=str(event["truck_unit"]),
        advised_stop=advised_stop,
        actual_stop=actual_stop,
        actual_price=float(actual_stop.get("your_price", actual_true_cost)),
        estimated_loss_dollars=estimated_loss,
    )
    await _send_red_flag_alert(
        bot=bot,
        driver_telegram_id=driver_telegram_id,
        text=text,
        alert_type="wrong_fuel_stop",
        truck_unit=str(event["truck_unit"]),
        load_id=str(event["load_id"]),
        event_id=int(event["id"]),
        dedupe_key=f"site:{event['recommended_site_id']}",
    )


async def _send_missed_stop_alert(
    *,
    bot: Bot,
    samsara: SamsaraClient,
    vehicle_id: str,
    event: Any,
    driver_telegram_id: int | None,
    location: Any,
    distance_miles: float,
    dollar_impact: float,
) -> None:
    """Tell driver/dispatch the truck passed the assigned stop."""
    stop = _selected_stop_from_event(event)
    if stop is None:
        log.warning(
            "compliance: event %d has no selected candidate; skipping missed-stop alert",
            event["id"],
        )
        return

    current_fuel_percent = await _fuel_percent_or_none(samsara, vehicle_id)
    text = missed_fuel_stop_message(
        truck_unit=str(event["truck_unit"]),
        stop=stop,
        distance_miles=distance_miles,
        current_fuel_percent=current_fuel_percent,
        gallons_to_pump=int(event["gallons"]),
        estimated_loss_dollars=max(-float(dollar_impact), 0.0),
        truck_lat=float(location.lat),
        truck_lng=float(location.lng),
        past_stop=True,
    )
    await _send_red_flag_alert(
        bot=bot,
        driver_telegram_id=driver_telegram_id,
        text=text,
        alert_type="missed_fuel_stop",
        truck_unit=str(event["truck_unit"]),
        load_id=str(event["load_id"]),
        event_id=int(event["id"]),
        dedupe_key=f"site:{event['recommended_site_id']}",
    )


async def _send_red_flag_alert(
    *,
    bot: Bot,
    driver_telegram_id: int | None,
    text: str,
    alert_type: str,
    truck_unit: str,
    load_id: str,
    event_id: int,
    dedupe_key: str,
) -> None:
    """Send a red-flag alert to the driver chat and dispatch, without raising."""
    try:
        if not await _claim_red_flag_event(event_id, alert_type):
            log.info(
                "compliance: red-flag alert already claimed for event %d; skipping",
                event_id,
            )
            return
        if not await _claim_alert_fingerprint(
            alert_type=alert_type,
            truck_unit=truck_unit,
            load_id=load_id,
            dedupe_key=dedupe_key,
        ):
            log.info(
                "compliance: duplicate red-flag fingerprint for truck %s load %s; skipping",
                truck_unit,
                load_id,
            )
            return

        driver_msg_id: int | None = None
        dispatch_msg_id: int | None = None
        if driver_telegram_id:
            driver_msg_id = await safe_send(
                bot=bot,
                chat_id=int(driver_telegram_id),
                text=text,
                alert_type=alert_type,
                truck_unit=truck_unit,
                load_id=load_id,
                extra={"event_id": event_id},
                queue_on_failure=False,
                replace_previous_driver_alert=True,
            )
        if settings.TELEGRAM_DISPATCH_CHAT_ID is not None:
            dispatch_msg_id = await safe_send(
                bot=bot,
                chat_id=settings.TELEGRAM_DISPATCH_CHAT_ID,
                text=text,
                alert_type=f"dispatch_{alert_type}",
                truck_unit=truck_unit,
                load_id=load_id,
                extra={"event_id": event_id},
                stop_event_id=event_id,
                msg_id_column="red_flag_dispatch_msg_id",
            )
        if driver_msg_id is not None or dispatch_msg_id is not None:
            await execute(
                """
                UPDATE stop_events
                SET red_flag_driver_msg_id = COALESCE($2, red_flag_driver_msg_id),
                    red_flag_dispatch_msg_id = COALESCE($3, red_flag_dispatch_msg_id)
                WHERE id = $1
                """,
                event_id,
                driver_msg_id,
                dispatch_msg_id,
            )
    except Exception:  # noqa: BLE001
        log.exception(
            "compliance: failed to send %s red-flag alert for event %d",
            alert_type,
            event_id,
        )


async def _claim_red_flag_event(event_id: int, alert_type: str) -> bool:
    status = await execute(
        """
        UPDATE stop_events
        SET red_flag_sent_at = NOW(),
            red_flag_alert_type = $2
        WHERE id = $1
          AND red_flag_sent_at IS NULL
        """,
        event_id,
        alert_type,
    )
    return status.endswith(" 1")


async def _claim_alert_fingerprint(
    *,
    alert_type: str,
    truck_unit: str,
    load_id: str,
    dedupe_key: str,
) -> bool:
    fingerprint = _red_flag_fingerprint(
        alert_type=alert_type,
        truck_unit=truck_unit,
        load_id=load_id,
        dedupe_key=dedupe_key,
    )
    row = await fetch_one(
        """
        INSERT INTO alert_send_fingerprints
            (fingerprint, alert_type, truck_unit, load_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING fingerprint
        """,
        fingerprint,
        alert_type,
        truck_unit,
        load_id,
    )
    return row is not None


def _red_flag_fingerprint(
    *,
    alert_type: str,
    truck_unit: str,
    load_id: str,
    dedupe_key: str,
) -> str:
    payload = "\x1f".join([alert_type, truck_unit, load_id, dedupe_key])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def _fuel_percent_or_none(samsara: SamsaraClient, vehicle_id: str) -> int | None:
    try:
        fuel_gallons = await samsara.get_vehicle_fuel(vehicle_id)
    except SamsaraError:
        log.warning("compliance: Samsara fuel lookup failed for vehicle %s", vehicle_id)
        return None
    return round(fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100)


def _selected_stop_from_event(event: Any) -> dict[str, Any] | None:
    candidates = _parse_candidates(event["candidates"])
    return candidates[0] if candidates else None


async def _send_approach_reminder(
    *,
    bot: Bot,
    event_id: int,
    truck_unit: str,
    driver_telegram_id: int | None,
    distance_miles: float,
    candidates_raw: Any,
    gallons: int,
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> None:
    """Fire the 30-mile approach ping and stamp approach_ping_sent_at.

    Best-effort: any Telegram or parse failure is logged but doesn't prevent
    the timestamp from being set — we never want to spam multiple reminders
    for the same event because of a transient send failure on the first try.
    """
    stop_dict: dict[str, Any] | None = None
    try:
        candidates = _parse_candidates(candidates_raw)
        if candidates:
            stop_dict = candidates[0]
    except Exception:  # noqa: BLE001 — JSON parse must never crash compliance
        log.exception("compliance: could not parse candidates for event %d", event_id)

    if stop_dict is None:
        log.warning(
            "compliance: event %d has no usable candidate stop; skipping approach ping",
            event_id,
        )
        return

    text = approach_reminder_message(
        truck_unit=str(truck_unit),
        distance_miles=distance_miles,
        stop=stop_dict,
        gallons_to_pump=gallons,
    )
    keyboard = approach_reminder_keyboard(
        stop=stop_dict,
        truck_lat=truck_lat,
        truck_lng=truck_lng,
    )

    driver_msg_id: int | None = None
    dispatch_msg_id: int | None = None

    if not await _claim_approach_event(event_id):
        log.info("compliance: approach reminder already claimed for event %d", event_id)
        return

    if driver_telegram_id:
        driver_msg_id = await safe_send(
            bot=bot,
            chat_id=int(driver_telegram_id),
            text=text,
            alert_type="approach",
            truck_unit=str(truck_unit),
            extra={"event_id": event_id},
            stop_event_id=event_id,
            msg_id_column="approach_driver_msg_id",
            reply_markup=keyboard,
            queue_on_failure=False,
            replace_previous_driver_alert=True,
        )
    if settings.TELEGRAM_DISPATCH_CHAT_ID is not None:
        dispatch_msg_id = await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_DISPATCH_CHAT_ID,
            text=text,
            alert_type="dispatch_approach",
            truck_unit=str(truck_unit),
            extra={"event_id": event_id},
            stop_event_id=event_id,
            msg_id_column="approach_dispatch_msg_id",
        )

    try:
        await execute(
            """
            UPDATE stop_events
            SET approach_driver_msg_id = COALESCE($2, approach_driver_msg_id),
                approach_dispatch_msg_id = COALESCE($3, approach_dispatch_msg_id)
            WHERE id = $1
            """,
            event_id,
            driver_msg_id,
            dispatch_msg_id,
        )
    except Exception:  # noqa: BLE001
        log.exception("compliance: failed to store approach msg ids for event %d", event_id)


async def _claim_approach_event(event_id: int) -> bool:
    status = await execute(
        """
        UPDATE stop_events
        SET approach_ping_sent_at = NOW()
        WHERE id = $1
          AND approach_ping_sent_at IS NULL
        """,
        event_id,
    )
    return status.endswith(" 1")


def _parse_candidates(raw: Any) -> list[dict[str, Any]]:
    """asyncpg returns JSONB as a str — accept already-parsed lists too."""
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    return []


async def _safe_send_admin(bot: Bot, text: str) -> None:
    await safe_send(
        bot=bot,
        chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
        text=text,
        alert_type="admin_compliance_err",
        parse_mode=None,
    )


async def _alert_admin_error(bot: Bot, event: Any, exc: Exception) -> None:
    """Notify once per event/error signature during a six-hour window."""
    event_id = int(event["id"])
    signature = (event_id, type(exc).__name__, str(exc))
    now = time.monotonic()
    last_sent = _recent_error_alerts.get(signature)
    if last_sent is not None and now - last_sent < ERROR_ALERT_COOLDOWN_SECONDS:
        metrics.incr("compliance_error_alert_suppressed")
        return
    _recent_error_alerts[signature] = now
    if len(_recent_error_alerts) > 2_000:
        cutoff = now - ERROR_ALERT_COOLDOWN_SECONDS
        for key, sent_at in list(_recent_error_alerts.items()):
            if sent_at < cutoff:
                _recent_error_alerts.pop(key, None)
    await _safe_send_admin(
        bot,
        f"compliance error on event {event_id} "
        f"(truck {event['truck_unit']} load {event['load_id']}): "
        f"{type(exc).__name__}: {exc}",
    )
