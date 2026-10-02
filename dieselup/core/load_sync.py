"""Refresh one verified current trip per physical truck and compute fuel
advice from fresh GPS through every remaining pickup and delivery. Incomplete
assignments, progress, locations, prices or routes hold advice for review.
Pending advice is compared with a fresh plan and superseded when it changes.
Telegram delivery remains subject to test mode and the current-advice guard.
"""
from __future__ import annotations

import asyncio
import difflib
import hashlib
import json
import logging
import re
import time
from typing import Any

from telegram import Bot
from dieselup import metrics
from dieselup.bot.messages import (
    delivery_complete_message,
    fuel_plan_keyboard,
    sequential_fuel_plan_message,
)
from dieselup.bot.sender import safe_send
from dieselup.bot.group_link import driver_names_match, refresh_and_verify_linked_groups
from dieselup.clients.tms import TMS_ERRORS, make_tms_client
from dieselup.clients.routing import RoutingError
from dieselup.clients.samsara import (
    SamsaraClient,
    SamsaraError,
    VehicleSummary,
    extract_samsara_index_keys,
    extract_unit_digits,
)
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.core.fuel_plan import NoFeasibleFuelPlan
from dieselup.core.remaining_route import plan_remaining_route
from dieselup.core.trip_context import TripContextError, remaining_stops
from dieselup.core import advice_audit
from dieselup.core.optimizer import (
    NoValidStopError,
    StaleFuelPricesError,
    haversine_miles,
)
from dieselup.db import execute, fetch_one

log = logging.getLogger(__name__)
_truck_plan_locks: dict[str, asyncio.Lock] = {}

ACTIVE_STATUSES = {
    "active", "upcoming", "dispatching", "dispatched", "in_transit",
    "in transit", "enroute", "en route",
}
DELIVERED_STATUSES = {"delivered", "invoiced", "completed", "complete"}

# Per-load hard ceiling — one stuck Samsara/optimizer call can't block the
# rest of the sweep. Generous enough that real work always fits (a load
# normally takes <2s: cached Samsara lookup + bbox SQL + briefing send).
PER_LOAD_TIMEOUT_SECONDS = 30.0

# The active-load sweep runs every 15 minutes. Bad upstream records used to
# send the identical traceback summary on every pass, burying useful alerts.
# Keep one process-local notification per order/error signature for six hours;
# the full exception is still logged on every sweep for diagnosis.
ERROR_ALERT_COOLDOWN_SECONDS = 6 * 60 * 60
_recent_error_alerts: dict[tuple[str, str, str], float] = {}


# Bound per-sweep order paging so it fits the 15-minute schedule. DataTruck
# returns 10 orders per page; QuickManage can return more rows but is slower.
# Active loads sort newest-first by id, so the most-recent window covers every
# currently-active and recently-delivered load while still preventing a stuck
# TMS feed from blocking the scheduler forever.
LOAD_SYNC_MAX_PAGES = 50
ORDER_ENUMERATION_TIMEOUT_SECONDS = settings.LOAD_SYNC_ORDER_ENUMERATION_TIMEOUT_SECONDS
QUICKMANAGE_ACTIVE_FILTER_STATUSES = ["upcoming", "dispatching", "dispatched", "in_transit"]
QUICKMANAGE_DELIVERED_FILTER_STATUSES = ["completed", "delivered"]
QUICKMANAGE_DELIVERED_MAX_PAGES = 5
DELIVERED_ENUMERATION_TIMEOUT_SECONDS = 60.0

# Driver-sleep protection: if the truck's GPS is stale, or the truck is still
# parked near where the last plan was generated, keep the new plan dispatch-only.
PARKED_GPS_STALE_MINUTES = 60.0
PARKED_RECENT_PLAN_HOURS = 6.0
PARKED_RADIUS_MILES = 1.0


class LoadContextError(ValueError):
    """The DataTruck order is missing fields we need (truck, destination, ID)."""


def _assigned_vehicle(order: dict[str, Any], unit_map: dict[str, list[VehicleSummary]]) -> VehicleSummary:
    """Truck identity wins. Driver names can disambiguate within that truck,
    but can never move a trip onto an unrelated vehicle.
    """
    unit = _extract_truck_unit(order)
    if not unit or order.get("assignment_conflict"):
        raise LoadContextError("Missing or conflicting truck assignment")
    if not unit_map:
        raise LoadContextError("Samsara fleet identity cannot be verified")
    matches = {v.id: v for v in _resolve_samsara_matches(unit, unit_map)}
    name = _extract_order_driver(order)
    if len(matches) > 1 and name:
        matches = {k: v for k, v in matches.items() if driver_names_match(_driver_name_from_samsara(v.name), name)}
    if len(matches) != 1:
        raise LoadContextError("Assigned truck has no unique Samsara vehicle")
    vehicle = next(iter(matches.values()))
    label_driver = _driver_name_from_samsara(vehicle.name)
    # A vehicle named only by its truck number contains no driver evidence.
    # Its unique unit identity is still valid; TMS/roster/group driver checks
    # remain mandatory before any driver advice can be delivered.
    if name and label_driver and not driver_names_match(label_driver, name):
        raise LoadContextError("Assigned truck and Samsara driver disagree")
    return vehicle


def _select_current_loads(orders: list[dict], unit_map: dict[str, list[VehicleSummary]]) -> list[dict]:
    """One current trip per physical truck. Equal-priority conflicts hold all
    affected trips; upcoming trips never displace an in-progress assignment.
    """
    grouped: dict[str, list[tuple[int, dict]]] = {}
    blocked: set[str] = set()
    for order in orders:
        status = str(order.get("raw_status") or order.get("status") or "").strip().lower()
        if not _is_active(order) or status == "upcoming": continue
        unit = _extract_truck_unit(order)
        if unit and not allows(unit):
            continue
        if not unit:
            name = _extract_order_driver(order)
            if name:
                blocked.update(v.id for values in unit_map.values() for v in values
                               if driver_names_match(_driver_name_from_samsara(v.name), name))
            continue
        try:
            vehicle = _assigned_vehicle(order, unit_map)
        except LoadContextError as exc:
            blocked.add(_normalize_unit(unit))
            blocked.update(v.id for v in _resolve_samsara_matches(unit, unit_map))
            log.warning("load_sync: assignment held for %s: %s", unit, exc)
            continue
        key = vehicle.id if vehicle else _normalize_unit(unit)
        priority = 2 if status in {"active", "in_transit", "in transit", "enroute", "en route"} else 1
        grouped.setdefault(key, []).append((priority, order))
    result = []
    for key, group in grouped.items():
        if key in blocked: continue
        if any(_normalize_unit(_extract_truck_unit(order) or "") in blocked for _, order in group): continue
        priority = max(p for p, _ in group)
        current = {str(order.get("id")): order for p, order in group if p == priority}
        if len(current) == 1: result.append(next(iter(current.values())))
        else:
            metrics.incr("load_sync_ambiguous_current_trip")
            log.warning("load_sync: multiple current trips; holding truck %s", _extract_truck_unit(group[0][1]))
    return result


async def sync_active_loads(bot: Bot) -> None:
    """Entrypoint for the APScheduler IntervalTrigger(minutes=15) job.

    Three phases per sweep:
      A. Fetch/process active orders first so live fuel alerts are never
         blocked behind delivered-order history.
      B. Scan a capped delivered-order window for completion follow-ups.
      C. Send standalone delivery-complete notices for delivered trucks that
         have no new active load.
    """
    log.info("load_sync: starting active-load sweep")
    metrics.incr("load_sync_cycles_total")

    processed = skipped = briefed = no_stop = errored = delivery_pings = 0
    delivered_now: dict[str, str] = {}  # truck_unit → just-delivered load_id
    notified_units: set[str] = set()    # per-sweep dedupe of admin pings

    sweep_timer = metrics.Timer("load_sync_cycle_seconds")
    sweep_timer.__enter__()
    try:
        # Mandatory first gate: Telegram title -> Supabase roster. Only links
        # verified in this pass may receive driver-facing alerts.
        verified_driver_links = await refresh_and_verify_linked_groups(bot)
        async with make_tms_client() as datatruck, SamsaraClient() as samsara:
            samsara_by_unit = await _build_samsara_unit_map(samsara)

            enumeration_complete = False
            active_orders: list[dict[str, Any]] = []
            delivered_orders: list[dict[str, Any]] = []
            try:
                async with asyncio.timeout(ORDER_ENUMERATION_TIMEOUT_SECONDS):
                    if settings.TMS_PROVIDER == "quickmanage":
                        async for order in datatruck.iter_orders(
                            filters=[{
                                "field": "status",
                                "operator": "in",
                                "value": QUICKMANAGE_ACTIVE_FILTER_STATUSES,
                            }],
                            max_pages=LOAD_SYNC_MAX_PAGES,
                        ):
                            processed += 1
                            status = (order.get("status") or "").strip().lower()
                            if status in ACTIVE_STATUSES:
                                active_orders.append(order)
                    else:
                        async for order in datatruck.iter_orders(max_pages=LOAD_SYNC_MAX_PAGES):
                            processed += 1
                            status = (order.get("status") or "").strip().lower()
                            if status in ACTIVE_STATUSES:
                                active_orders.append(order)
                            elif status in DELIVERED_STATUSES:
                                delivered_orders.append(order)
            except TimeoutError:
                metrics.incr("load_sync_order_enumeration_timed_out")
                log.warning(
                    "load_sync: %s active-order enumeration timed out after %.0fs — "
                    "holding fuel advice for incomplete assignment coverage (processed=%d active=%d delivered=%d)",
                    settings.TMS_PROVIDER,
                    ORDER_ENUMERATION_TIMEOUT_SECONDS,
                    processed,
                    len(active_orders),
                    len(delivered_orders),
                )
            else:
                enumeration_complete = True
                log.info(
                    "load_sync: %s active enumeration complete — processed=%d active=%d delivered=%d",
                    settings.TMS_PROVIDER,
                    processed,
                    len(active_orders),
                    len(delivered_orders),
                )

            # Phase B — process active orders before any delivered-history
            # scan. A large completed-load archive must not delay live fuel
            # alerts to drivers.
            # A partial list cannot prove that another current trip is absent.
            selected_orders = _select_current_loads(active_orders, samsara_by_unit) if enumeration_complete else []
            for order in selected_orders:
                truck_unit = _extract_truck_unit(order) or ""
                followup = truck_unit in delivered_now
                try:
                    if settings.TMS_PROVIDER == "quickmanage":
                        fresh = await asyncio.wait_for(datatruck.get_order(order.get("tms_order_id") or order["id"]), timeout=PER_LOAD_TIMEOUT_SECONDS)
                        if _extract_truck_unit(fresh) != _extract_truck_unit(order) or not _is_active(fresh):
                            raise LoadContextError("Trip assignment/status changed during planning")
                        order = fresh
                    outcome = await asyncio.wait_for(
                        _process_one_load(
                            order,
                            samsara=samsara,
                            bot=bot,
                            samsara_by_unit=samsara_by_unit,
                            notified_units=notified_units,
                            verified_driver_links=verified_driver_links,
                            delivery_complete_followup=followup,
                            current_trip_verified=True,
                        ),
                        timeout=PER_LOAD_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    errored += 1
                    metrics.incr("load_sync_timed_out")
                    log.warning(
                        "load_sync: per-load timeout (%.0fs) on order %s — moving on",
                        PER_LOAD_TIMEOUT_SECONDS, order.get("id"),
                    )
                    continue
                except NoValidStopError as exc:
                    if getattr(exc, "reason", None) == "arrival_fuel_too_high":
                        skipped += 1
                        metrics.incr("load_sync_skipped_arrival_fuel_too_high")
                        log.info(
                            "load_sync: truck %s on load %s has %.1f gal; "
                            "skipping until arrival fuel drops into briefable range",
                            exc.truck_unit,
                            exc.load_id,
                            exc.current_fuel_gallons,
                        )
                    else:
                        no_stop += 1
                        metrics.incr("load_sync_no_valid_stop")
                        await _alert_admin_no_valid_stop(bot, exc)
                except LoadContextError as exc:
                    skipped += 1
                    metrics.incr("load_sync_skipped_context")
                    log.warning("load_sync: skipping order %s — %s", order.get("id"), exc)
                except StaleFuelPricesError as exc:
                    errored += 1
                    metrics.incr("load_sync_stale_prices")
                    # Feed-wide issue: stop new plans and report once, not one
                    # identical failure for every truck in this sweep.
                    await _alert_admin_error(bot, {"id": "fuel-price-feed"}, exc)
                    break
                except Exception as exc:  # noqa: BLE001 — log every failure mode
                    errored += 1
                    metrics.incr("load_sync_errored")
                    log.exception("load_sync: error processing order %s", order.get("id"))
                    await _alert_admin_error(bot, order, exc)
                else:
                    if outcome == "briefed":
                        briefed += 1
                        metrics.incr("load_sync_briefed")
                        if followup:
                            delivery_pings += 1
                            metrics.incr("load_sync_delivery_followup")
                            delivered_now.pop(truck_unit, None)
                    else:
                        skipped += 1
                        metrics.incr("load_sync_skipped_other")

            if settings.TMS_PROVIDER == "quickmanage":
                delivered_processed = 0
                try:
                    async with asyncio.timeout(DELIVERED_ENUMERATION_TIMEOUT_SECONDS):
                        async for order in datatruck.iter_orders(
                            filters=[{
                                "field": "status",
                                "operator": "in",
                                "value": QUICKMANAGE_DELIVERED_FILTER_STATUSES,
                            }],
                            max_pages=QUICKMANAGE_DELIVERED_MAX_PAGES,
                        ):
                            delivered_processed += 1
                            processed += 1
                            status = (order.get("status") or "").strip().lower()
                            if status in DELIVERED_STATUSES:
                                delivered_orders.append(order)
                except TimeoutError:
                    metrics.incr("load_sync_delivered_order_enumeration_timed_out")
                    log.warning(
                        "load_sync: quickmanage delivered-order scan timed out after %.0fs — "
                        "continuing after partial delivery scan (processed=%d delivered=%d)",
                        DELIVERED_ENUMERATION_TIMEOUT_SECONDS,
                        delivered_processed,
                        len(delivered_orders),
                    )
                except TMS_ERRORS as exc:
                    # Historical pagination can stop at its deliberate cap
                    # or fail independently of the completed active sweep.
                    # Hold delivery follow-ups; retain the real active result.
                    delivered_orders.clear()
                    metrics.incr('load_sync_delivered_history_held')
                    log.warning('load_sync: delivered history incomplete (%s); delivery follow-ups held', type(exc).__name__)
                else:
                    log.info(
                        "load_sync: quickmanage delivered scan complete — processed=%d delivered=%d",
                        delivered_processed,
                        len(delivered_orders),
                    )

            # Phase C — detect deliveries we owe a follow-up for. Each
            # delivered order is its own try/except so one malformed row
            # can't abort the rest of the detection pass.
            for order in delivered_orders:
                try:
                    truck_unit = _extract_truck_unit(order)
                    if not allows(truck_unit):
                        continue
                    load_id = _load_id_of(order)
                    if not truck_unit or not load_id:
                        continue
                    # The stop_event may be stored under a different unit alias
                    # (SUBUNIT outer vs paren number, or a driver-name corrected
                    # unit) — match on every alias AND on the canonical Samsara
                    # vehicle id, otherwise delivery-complete pings silently miss.
                    matched_vehicles = _resolve_samsara_matches(truck_unit, samsara_by_unit)
                    unit_aliases = {truck_unit}
                    vehicle_ids: list[str] = []
                    for v in matched_vehicles:
                        vehicle_ids.append(v.id)
                        unit_aliases.update(extract_samsara_index_keys(v.name))
                    row = await fetch_one(
                        """
                        SELECT id FROM stop_events
                        WHERE (truck_unit = ANY($1::text[])
                               OR samsara_vehicle_id = ANY($2::text[]))
                          AND load_id = $3
                          AND notified_complete_at IS NULL
                        LIMIT 1
                        """,
                        list(unit_aliases), vehicle_ids, load_id,
                    )
                    if row is None:
                        continue
                    await execute(
                        "UPDATE stop_events SET notified_complete_at = NOW() WHERE id = $1",
                        row["id"],
                    )
                    delivered_now[truck_unit] = load_id
                except Exception:  # noqa: BLE001 — one bad delivered-order row can't kill Phase B
                    metrics.incr("load_sync_phaseb_errors")
                    log.exception(
                        "load_sync: Phase B failed on delivered order %s",
                        order.get("id"),
                    )

            # Phase D — any remaining just-delivered trucks (no next active
            # load yet, or new load already had a pending briefing) get a
            # standalone delivery-complete notice with no fuel plan.
            for truck_unit in list(delivered_now.keys()):
                try:
                    await _send_standalone_delivery_complete(
                        bot=bot,
                        samsara=samsara,
                        truck_unit=truck_unit,
                    )
                    delivery_pings += 1
                except Exception:  # noqa: BLE001 — never crash the sweep on a follow-up failure
                    log.exception(
                        "load_sync: standalone delivery-complete failed for truck %s",
                        truck_unit,
                    )
    except TMS_ERRORS as exc:
        log.exception("load_sync: %s call failed — aborting sweep", settings.TMS_PROVIDER)
        metrics.incr("load_sync_aborted_datatruck")
        await _safe_send_admin(
            bot,
            f"load_sync: {settings.TMS_PROVIDER} call failed mid-sweep — {exc}. "
            "Next 15-min sweep will retry.",
        )
        return
    finally:
        sweep_timer.__exit__(None, None, None)
        import time as _t
        metrics.gauge("load_sync_last_heartbeat_mono", _t.monotonic())
        metrics.gauge("load_sync_last_cycle_briefed", briefed)
        metrics.gauge("load_sync_last_cycle_no_valid_stop", no_stop)
        metrics.gauge("load_sync_last_cycle_errored", errored)
        metrics.gauge("load_sync_last_cycle_skipped", skipped)
        metrics.gauge("load_sync_last_cycle_processed", processed)

    log.info(
        "load_sync: done — processed=%d briefed=%d skipped=%d no_valid_stop=%d "
        "errored=%d delivery_pings=%d",
        processed, briefed, skipped, no_stop, errored, delivery_pings,
    )


async def _build_samsara_unit_map(
    samsara: SamsaraClient,
) -> dict[str, list[VehicleSummary]]:
    """Fetch every Samsara vehicle once per sweep, keyed by unit digits.

    Returns an empty dict on Samsara failure — auto-onboard then falls through
    and the sweep skips trucks it can't resolve, which is the same end-state
    as before this feature.
    """
    try:
        vehicles = await samsara.list_vehicles()
    except SamsaraError as exc:
        log.warning("load_sync: could not fetch Samsara vehicle list: %s", exc)
        return {}
    out: dict[str, list[VehicleSummary]] = {}
    for v in vehicles:
        for key in extract_samsara_index_keys(v.name):
            for index_key in _unit_key_aliases(key):
                out.setdefault(index_key, []).append(v)
    return out


_PAREN_UNIT_RE = re.compile(r"\((\d+)\)")


def _unit_key_aliases(key: str) -> list[str]:
    """Return lookup aliases for a unit key, preserving order and uniqueness."""
    raw = str(key).strip()
    if not raw:
        return []
    stripped = raw.lstrip("0") or raw
    return [raw] if stripped == raw else [raw, stripped]


def _normalize_unit(unit: str | None) -> str | None:
    """Canonical digit form of a unit string for comparisons (no leading zeros)."""
    if not unit:
        return None
    digits = extract_unit_digits(str(unit)) or str(unit).strip()
    if not digits:
        return None
    return digits.lstrip("0") or digits


def _unit_matches_keys(unit: str | None, keys: list[str]) -> bool:
    """True when `unit` refers to any of a vehicle's Samsara index keys.

    Tolerates leading zeros and embedded formats ('005' matches key '5');
    a plain exact `in` check silently failed those and caused needless
    fallbacks to the Samsara-derived unit.
    """
    target = _normalize_unit(unit)
    if target is None:
        return False
    return any((k.lstrip("0") or k) == target for k in keys)


def _resolve_samsara_matches(
    truck_unit: str,
    samsara_by_unit: dict[str, list[VehicleSummary]],
) -> list[VehicleSummary]:
    """Find Samsara vehicles for a DataTruck truck_unit string.

    Handles three extra formats beyond a plain unit number:

    1. Compound units like '005/1646' — DataTruck sometimes joins a trailer
       code and a truck unit with a slash. We try each slash-separated part
       and return the first one that matches.

    2. Units with leading zeros ('005') — stripped to bare digits ('5') as a
       fallback so '005' can still find a Samsara vehicle named '5 - Driver'.

    3. Parenthetical inner numbers like '898725 (551566)' — DataTruck may send
       the inner (paren) number alone, e.g. just '551566', for vehicles whose
       Samsara name has no SUBUNIT prefix. The extract_unit_digits primary only
       returns the first number, so we also try the paren number as a fallback.
    """
    def _lookup(key: str) -> list[VehicleSummary]:
        normalized = extract_unit_digits(key) or key
        for candidate in _unit_key_aliases(normalized):
            result = samsara_by_unit.get(candidate, [])
            if result:
                return result
        target = (str(normalized).strip().lstrip("0") or str(normalized).strip())
        for indexed_key, result in samsara_by_unit.items():
            if (indexed_key.lstrip("0") or indexed_key) == target:
                return result
        return []

    matches = _lookup(truck_unit)
    if not matches and "/" in truck_unit:
        for part in truck_unit.split("/"):
            part = part.strip()
            if part:
                matches = _lookup(part)
                if matches:
                    break

    # Fallback: try the parenthetical number from compound units like "898725 (551566)".
    # Relevant when DataTruck sends only the inner number for a non-SUBUNIT vehicle.
    if not matches:
        paren_m = _PAREN_UNIT_RE.search(truck_unit)
        if paren_m:
            matches = _lookup(paren_m.group(1))

    return matches


_NAME_STRIP_RE = re.compile(r"[^a-zA-Z\s]")
_SUBUNIT_NAME_RE = re.compile(r"^(SUB(UNIT)?[#\s\-]*)", re.IGNORECASE)
_PAREN_STRIP_RE = re.compile(r"\(\d+\)")


_UNIT_PREFIX_RE = re.compile(r"^(UNIT[#\s\-]*)", re.IGNORECASE)
_LEADING_DIGITS_RE = re.compile(r"^\d+[A-Za-z]?\s*[-–]?\s*")


def _driver_name_from_samsara(vehicle_name: str | None) -> str | None:
    """Extract the driver portion from a Samsara vehicle name."""
    # Samsara occasionally returns a vehicle with a null name. A malformed
    # fleet row must not abort every load that falls back to driver matching.
    name = str(vehicle_name or "").strip()
    # Strip SUBUNIT/SUB prefix
    name = _SUBUNIT_NAME_RE.sub("", name).strip()
    # Strip UNIT prefix (separate regex — doesn't match SUBUNIT)
    name = _UNIT_PREFIX_RE.sub("", name).strip()
    # Strip parenthesized numbers like (551802)
    name = _PAREN_STRIP_RE.sub("", name).strip()
    # Take the part after the last " - " separator
    if " - " in name:
        name = name.split(" - ")[-1].strip()
    # Strip any remaining leading digits (unit number without separator)
    name = _LEADING_DIGITS_RE.sub("", name).strip()
    ignore = {"no driver", "inactive", "new", "past", "not working", "totalled", ""}
    if name.lower() in ignore or len(name) < 3:
        return None
    return name


def _name_similarity(a: str, b: str) -> float:
    def norm(s: str) -> str:
        return " ".join(_NAME_STRIP_RE.sub(" ", s).upper().split())
    na, nb = norm(a), norm(b)
    if na == nb:
        return 1.0
    ta, tb = set(na.split()), set(nb.split())
    overlap = len(ta & tb) / min(len(ta), len(tb)) if ta and tb else 0.0
    seq = difflib.SequenceMatcher(None, na, nb).ratio()
    return max(overlap, seq)


def _match_vehicle_by_driver_name(
    dt_driver: str,
    samsara_by_unit: dict[str, list[VehicleSummary]],
    threshold: float = 0.85,
) -> VehicleSummary | None:
    """Return the single best Samsara vehicle match by driver name, or None.

    Iterates all vehicles (deduped by id). Returns None if no match exceeds
    the threshold or if multiple vehicles tie at the same score.
    """
    seen: set[str] = set()
    best_score = 0.0
    best: VehicleSummary | None = None

    for vehicles in samsara_by_unit.values():
        for v in vehicles:
            if v.id in seen:
                continue
            seen.add(v.id)
            sam_driver = _driver_name_from_samsara(v.name)
            if not sam_driver:
                continue
            score = _name_similarity(dt_driver, sam_driver)
            if score > best_score:
                best_score = score
                best = v
            elif score == best_score and score >= threshold and best and best.id != v.id:
                best = None  # ambiguous tie — don't guess

    if best_score >= threshold and best is not None:
        log.info(
            "load_sync: driver-name match '%s' -> Samsara '%s' (score %.0f%%)",
            dt_driver, best.name, best_score * 100,
        )
        return best
    return None


def _extract_order_driver(order: dict[str, Any]) -> str | None:
    """Extract driver full name from a DataTruck order."""
    trip = order.get("trip") or {}
    if isinstance(trip, dict):
        v = trip.get("driver__full_name") or trip.get("driver__name")
        if isinstance(v, str) and v.strip():
            return v.strip()
    adt = order.get("assigned_driver_n_truck") or {}
    if isinstance(adt, dict):
        v = adt.get("driver_full_name")
        if isinstance(v, str) and v.strip():
            return v.strip()
    for key in ("driver_full_name", "driver__full_name", "driver_name"):
        v = order.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _is_verified_driver_assignment(
    *,
    driver: dict[str, Any],
    truck_unit: str,
    quickmanage_driver: str | None,
    verified_driver_links: dict[str, int],
) -> bool:
    """Require the same group, truck and driver across all three systems."""
    chat_id = driver.get("driver_telegram_id")
    expected_chat = verified_driver_links.get(str(truck_unit))
    if chat_id is None or expected_chat is None or int(chat_id) != int(expected_chat):
        return False
    if not quickmanage_driver:
        return False
    return driver_names_match(driver.get("driver_full_name"), quickmanage_driver)


async def _ensure_truck_onboarded(
    *,
    bot: Bot,
    truck_unit: str,
    samsara_by_unit: dict[str, list[VehicleSummary]],
    notified_units: set[str],
    matched_vehicle: VehicleSummary | None = None,
) -> dict[str, Any] | None:
    """Ensure trucks_drivers has a row with samsara_vehicle_id for this truck.

    Returns the resolved row (dict) or None when no unambiguous Samsara
    vehicle could be matched.

    Dedupe strategy:
      * Persistent: an existing trucks_drivers row with NULL samsara_vehicle_id
        is the marker for "we've already notified admin about this unmatched
        truck". Subsequent sweeps retry the Samsara match silently and only
        notify when something changes (a fix → auto-onboard, or first-ever
        sighting → new placeholder + first notice).
      * Per-sweep: `notified_units` covers the case where the same truck has
        multiple active loads in one sweep — still notify once per sweep, not
        once per load.
      * Existing samsara_vehicle_id is never overwritten — manual /linktruck
        and earlier matches win.
    """
    if not allows(truck_unit):
        return None
    row = await fetch_one(
        """
        SELECT id, driver_full_name, driver_telegram_id, samsara_vehicle_id
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        truck_unit,
    )
    if row is not None and row["samsara_vehicle_id"]:
        return dict(row)

    if not settings.AUTO_LINK_ENABLED:
        log.info('load_sync: automatic onboarding disabled for truck %s', truck_unit)
        return None
    matches = _resolve_samsara_matches(truck_unit, samsara_by_unit)
    was_placeholder_present = row is not None  # null samsara_vehicle_id case

    # Driver-name disambiguation: when the caller already identified the exact
    # vehicle via driver name (100% match), trust it over the ambiguous unit
    # index. Unblocks trucks like '2480' where a junk duplicate ('2480 - Our
    # Gateway…') shares the unit number with the real truck.
    if len(matches) > 1 and matched_vehicle is not None:
        if any(m.id == matched_vehicle.id for m in matches):
            log.info(
                "load_sync: unit %s had %d Samsara matches; resolved by "
                "driver-name match to %r",
                truck_unit, len(matches), matched_vehicle.name,
            )
            matches = [matched_vehicle]

    if len(matches) > 1:
        # Disambiguation: if multiple Samsara vehicles share the same unit index
        # key (e.g. unit "476604" appears as the PRIMARY unit on "UNIT # 476604 -
        # EMMANUEL ISHIMWE" AND as the paren key on "SUB# 527198 (476604) - …"),
        # prefer the vehicle where the unit IS its primary identifier (unit_digits
        # == truck_unit). The SUBUNIT vehicle that merely shares the paren key is a
        # different physical truck and should not win this resolution.
        original_match_count = len(matches)
        target_norm = _normalize_unit(truck_unit)
        primary_matches = [
            m for m in matches
            if m.unit_digits and (m.unit_digits.lstrip("0") or m.unit_digits) == target_norm
        ]
        if len(primary_matches) == 1:
            matches = primary_matches
            log.info(
                "load_sync: unit %s had %d Samsara matches; "
                "disambiguated to primary-unit vehicle %r",
                truck_unit, original_match_count, matches[0].name,
            )

    if len(matches) == 1:
        vehicle = matches[0]
        await execute(
            """
            INSERT INTO trucks_drivers (truck_unit, samsara_vehicle_id)
            VALUES ($1, $2)
            ON CONFLICT (truck_unit) DO UPDATE SET
                samsara_vehicle_id = COALESCE(trucks_drivers.samsara_vehicle_id, EXCLUDED.samsara_vehicle_id),
                updated_at = NOW()
            """,
            truck_unit,
            vehicle.id,
        )
        log.info(
            "load_sync: auto-onboarded truck %s → samsara %s (%s)",
            truck_unit, vehicle.id, vehicle.name,
        )
        if truck_unit not in notified_units:
            notified_units.add(truck_unit)
            await _safe_send_admin(
                bot,
                f"load_sync: auto-onboarded truck {truck_unit} → Samsara {vehicle.name!r}. "
                "Briefings will post to dispatch only until the drivers' group is linked.",
            )
        row = await fetch_one(
            """
            SELECT id, driver_full_name, driver_telegram_id, samsara_vehicle_id
            FROM trucks_drivers
            WHERE truck_unit = $1
            """,
            truck_unit,
        )
        return dict(row) if row else None

    # Zero or multiple matches — un-resolvable until admin fixes Samsara.
    # Insert a placeholder so future sweeps stay quiet.
    if not was_placeholder_present:
        await execute(
            """
            INSERT INTO trucks_drivers (truck_unit)
            VALUES ($1)
            ON CONFLICT (truck_unit) DO NOTHING
            """,
            truck_unit,
        )

    if len(matches) == 0:
        # Zero matches: notify only on the transition from "never seen" to
        # "seen but unmatched" (placeholder row is the persistent marker).
        if not was_placeholder_present and truck_unit not in notified_units:
            notified_units.add(truck_unit)
            await _safe_send_admin(
                bot,
                f"load_sync: no Samsara vehicle matches truck unit {truck_unit} — "
                "rename the vehicle in Samsara so its name starts with the unit number. "
                "(This notice fires once per truck — subsequent sweeps stay quiet until fixed.)",
            )
    elif truck_unit not in notified_units:
        # Genuine ambiguous duplicates: dedupe on the SET of matched vehicle
        # ids, not on the placeholder. A placeholder left over from an earlier
        # zero-match era used to silence duplicate alerts forever; and when the
        # duplicate set changes (a new clone appears), admin is told again.
        notified_units.add(truck_unit)
        dedupe_key = "ids:" + ",".join(sorted(m.id for m in matches))
        if await _claim_admin_alert_fingerprint(
            alert_type="duplicate_samsara_match",
            truck_unit=truck_unit,
            dedupe_key=dedupe_key,
        ):
            listing = "; ".join(f"{m.id}={m.name!r}" for m in matches)
            await _safe_send_admin(
                bot,
                f"load_sync: unit {truck_unit} matches {len(matches)} Samsara vehicles — "
                f"fix duplicates in Samsara. Candidates: {listing}",
            )
    return None


async def _claim_admin_alert_fingerprint(
    *,
    alert_type: str,
    truck_unit: str,
    dedupe_key: str,
) -> bool:
    """One-shot claim for an admin alert keyed on (type, truck, dedupe_key)."""
    payload = "\x1f".join([alert_type, truck_unit, dedupe_key])
    fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    row = await fetch_one(
        """
        INSERT INTO alert_send_fingerprints
            (fingerprint, alert_type, truck_unit, load_id)
        VALUES ($1, $2, $3, NULL)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING fingerprint
        """,
        fingerprint,
        alert_type,
        truck_unit,
    )
    return row is not None


def _load_id_of(order: dict[str, Any]) -> str | None:
    raw = order.get("load_number") or order.get("load_id") or order.get("id")
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


async def _build_routed_leg(
    *,
    ctx: dict[str, Any],
    stats: Any,
    is_first_plan: bool = False,
    delivery_complete_followup: bool = False,
) -> dict[str, Any] | None:
    """Plan from fresh GPS through every verified remaining customer stop."""
    age = getattr(stats, "gps_age_minutes", None)
    if age is None or not 0 <= age <= settings.MAX_ADVICE_GPS_AGE_MINUTES:
        raise LoadContextError("Fresh truck GPS is required before fuel advice")
    fuel_age = getattr(stats, "fuel_age_minutes", None)
    if fuel_age is None or not 0 <= fuel_age <= settings.MAX_ADVICE_FUEL_AGE_MINUTES:
        raise LoadContextError("Fresh truck fuel telemetry is required before fuel advice")
    try:
        waypoints = remaining_stops(ctx["order"])
    except TripContextError as exc:
        raise LoadContextError(str(exc)) from exc
    if not waypoints:
        return None
    mpg_fallback = not (stats.mpg_rolling and stats.mpg_rolling > 0)
    mpg = settings.FLEET_DEFAULT_MPG if mpg_fallback else stats.mpg_rolling
    if not settings.VALHALLA_URL.strip():
        raise RoutingError("VALHALLA_URL is required; routing fallbacks are disabled")
    planning_started = time.monotonic()
    lane = await plan_remaining_route(stats=stats, waypoints=waypoints, mpg=mpg)
    if age + (time.monotonic() - planning_started) / 60 > settings.MAX_ADVICE_GPS_AGE_MINUTES:
        raise LoadContextError("Truck GPS became stale during route planning")
    if fuel_age + (time.monotonic() - planning_started) / 60 > settings.MAX_ADVICE_FUEL_AGE_MINUTES:
        raise LoadContextError("Truck fuel reading became stale during route planning")
    plan_kwargs = {"terminal_reserve_gal": settings.TANK_CAPACITY_GALLONS * settings.DELIVERY_RESERVE_PCT / 100}
    metrics.incr("load_sync_valhalla_planned")

    if lane.degraded:
        raise NoFeasibleFuelPlan("Relaxed fuel plans require dispatcher review; normal reserves must be preserved")
    if not lane.legs:
        return None

    first = lane.legs[0]
    gallons = int(round(first.gallons))
    if gallons <= 0:
        return None

    stop_count = len(lane.legs)
    recommended_true_cost = first.net_price  # net_price == IFTA true cost

    def _leg_dict(leg: Any, index: int) -> dict[str, Any]:
        cand = leg.candidate
        return {
            "site_id": cand.site_id,
            "station_name": cand.station_name,
            "address": cand.address,
            "city": cand.city,
            "state": cand.state,
            "latitude": cand.latitude,
            "longitude": cand.longitude,
            "your_price": cand.your_price,
            "retail_price": cand.retail_price,
            "price_date": cand.price_date,
            "true_cost_per_gallon": leg.net_price,
            "gallons_to_pump": int(round(leg.gallons)),
            "planned_gallons": leg.gallons,
            "fill_to_full": leg.fill_to_full,
            "distance_miles": leg.distance_from_truck_mi,
            "mile_marker": leg.distance_from_truck_mi,
            "detour_miles": leg.detour_miles,
            "stop_number": index + 1,
            "stop_count": stop_count,
            # Context the /briefing replay handler + approach reminder read off [0].
            "origin_label": ctx["origin_label"],
            "destination_label": ctx["destination_label"],
            "current_fuel_gallons": stats.fuel_gallons,
            "truck_latitude": stats.lat,
            "truck_longitude": stats.lng,
        }

    # Persist the ENTIRE plan (all legs + routing + economics), not just the
    # briefed leg — the recompute model would otherwise discard legs 2..n every
    # sweep. candidates[0] stays the briefed stop for back-compat.
    legs_payload = [_leg_dict(leg, i) for i, leg in enumerate(lane.legs)]
    legs_payload[0]["plan"] = {
        "mpg": mpg,
        "mpg_fallback": mpg_fallback,
        "reserve_gal": settings.SAFETY_FLOOR_GALLONS,
        "tank_capacity_gal": settings.TANK_CAPACITY_GALLONS,
        "cost_per_mile": settings.COST_PER_MILE,
        "stop_time_penalty": settings.STOP_TIME_PENALTY,
        "bridge_min_purchase_gal": max(
            settings.MIN_FUEL_PURCHASE_GALLONS,
            settings.BRIDGE_MIN_FUEL_PURCHASE_GALLONS,
        ),
        "max_stop_detour_miles": settings.MAX_STOP_DETOUR_MILES,
        "worst_true_cost": lane.worst_true_cost,
        "leg_count": stop_count,
        "total_plan_gallons": sum(l.gallons for l in lane.legs),
        "start_fuel_gallons": stats.fuel_gallons,
        "degraded": lane.degraded,
        "ranking_strategy": settings.RANK_STRATEGY,
        "distance_model": "remaining_route_directed_truck_road_matrix",
        "route_evidence": lane.route_evidence,
        "terminal_reserve_gal": plan_kwargs["terminal_reserve_gal"],
    }

    # Always send ONE stop at a time — driver gets the next immediate stop only.
    # The full plan is stored in candidates JSON for compliance and audit, but
    # the briefing message is always the next single stop so the driver isn't
    # overwhelmed. Stop 2 brief fires automatically when Stop 1 is resolved.
    if delivery_complete_followup:
        fuel_pct = round(stats.fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100)
        briefing_text = delivery_complete_message(
            truck_unit=ctx["truck_unit"],
            next_load_id=ctx["load_id"],
            current_fuel_percent=fuel_pct,
            distance_to_next_stop_miles=first.distance_from_truck_mi,
            stop=legs_payload[0],
            gallons_to_pump=gallons,
        )
        alert_kind = "delivery"
    else:
        _flags: list[str] = []
        if mpg_fallback:
            _flags.append("mpg_fallback_used")
        if lane.degraded:
            _flags.append(f"plan_degraded_{lane.degraded}")
        briefing_text = sequential_fuel_plan_message(
            load_id=ctx["load_id"],
            truck_unit=ctx["truck_unit"],
            origin_label=ctx["origin_label"],
            destination_label=ctx["destination_label"],
            current_fuel_gallons=stats.fuel_gallons,
            stop=legs_payload[0],
            gallons_to_buy=gallons,
            stop_number=1,
            stop_count=stop_count,
            is_final_leg=(stop_count == 1),
            flags=_flags,
            truck_lat=stats.lat,
            truck_lng=stats.lng,
        )
        alert_kind = "briefing"

    return {
        "recommended_site_id": first.candidate.site_id,
        "recommended_true_cost": recommended_true_cost,
        # Keep compliance's saved-dollar math sane: worst >= chosen.
        "worst_true_cost": max(lane.worst_true_cost, recommended_true_cost),
        "gallons": gallons,
        "candidates_json": json.dumps(legs_payload),
        "briefing_text": briefing_text,
        "alert_kind": alert_kind,
        "degraded": lane.degraded,
    }


async def _process_one_load(order, **kwargs):
    unit = _extract_truck_unit(order) or "unknown"
    if not allows(unit):
        return 'skipped'
    vehicle = _assigned_vehicle(order, kwargs.get("samsara_by_unit") or {})
    key = vehicle.id if vehicle else extract_unit_digits(unit) or unit
    lock = _truck_plan_locks.setdefault(key, asyncio.Lock())
    async with lock:
        try:
            return await _process_one_load_unlocked(order, **kwargs)
        except Exception as exc:
            reason = str(exc) if isinstance(exc, (LoadContextError, TripContextError, StaleFuelPricesError)) else type(exc).__name__
            await advice_audit.record("plan_held", truck_unit=unit, load_id=_load_id_of(order),
                                      details={"reason": reason[:250]})
            raise


async def _process_one_load_unlocked(
    order: dict[str, Any],
    *,
    samsara: SamsaraClient,
    bot: Bot,
    samsara_by_unit: dict[str, list[VehicleSummary]] | None = None,
    notified_units: set[str] | None = None,
    verified_driver_links: dict[str, int] | None = None,
    delivery_complete_followup: bool = False,
    current_trip_verified: bool = False,
    replaces_event_id: int | None = None,
) -> str:
    """Process one active order. Returns 'briefed' or 'skipped'.

    Raises NoValidStopError (caught by caller, surfaces to admin) or
    LoadContextError (caught by caller, logged as skip).

    Auto-onboarding: if the truck has no trucks_drivers row, or no
    samsara_vehicle_id on its row, we resolve via the pre-fetched Samsara
    vehicle map and upsert. driver_telegram_id may be NULL — the briefing
    then goes to the dispatch group only.

    When delivery_complete_followup=True, the briefing renders in the
    Delivery Complete format (📍 header + next-load plan) instead of the
    standard [FUEL PLAN] format. Same stop selection logic either way.
    """
    if not allows(_extract_truck_unit(order)):
        return 'skipped'
    ctx = _load_context(order)
    _samsara_map = samsara_by_unit or {}
    _notified = notified_units if notified_units is not None else set()

    dt_driver = _extract_order_driver(order)

    if not current_trip_verified:
        raise LoadContextError("Current assigned trip has not been verified")
    truck_unit = ctx["truck_unit"]
    if not truck_unit or order.get("assignment_conflict"):
        raise LoadContextError("Missing or conflicting truck assignment")
    matched_vehicle = _assigned_vehicle(order, _samsara_map)
    ctx = {**ctx, "truck_unit": truck_unit}

    driver = await _ensure_truck_onboarded(
        bot=bot,
        truck_unit=truck_unit,
        samsara_by_unit=_samsara_map,
        notified_units=_notified,
        matched_vehicle=matched_vehicle,
    )

    if driver is None or not driver["samsara_vehicle_id"]:
        raise LoadContextError(
            f"truck {truck_unit!r} could not be onboarded from Samsara"
        )

    if str(driver["samsara_vehicle_id"]) != matched_vehicle.id:
        raise LoadContextError("Saved vehicle does not match the truck assigned to this trip")
    if dt_driver and driver.get("driver_full_name") and not driver_names_match(driver["driver_full_name"], dt_driver):
        raise LoadContextError("Trip driver does not match the saved truck assignment")

    # Advice for the former trip is no longer current. Keep its audit record.
    await execute("""WITH changed AS (
        UPDATE stop_events SET status='expired', resolved_at=NOW()
        WHERE truck_unit=$1 AND load_id<>$2 AND status='pending' RETURNING id, truck_unit, load_id
    ) INSERT INTO fuel_advice_audit(event_key,kind,truck_unit,load_id,stop_event_id,details)
      SELECT 'expired:' || id,'plan_expired',truck_unit,load_id,id,'{"reason":"trip_changed"}'::jsonb
      FROM changed ON CONFLICT(event_key) DO NOTHING""", ctx["truck_unit"], ctx["load_id"])
    # A pending recommendation must be compared to a fresh plan, never used
    # as a reason to skip GPS/route validation after the truck has moved.
    existing = await fetch_one(
        "SELECT id, recommended_site_id, gallons, candidates, fuel_pct_before FROM stop_events "
        "WHERE truck_unit = $1 AND load_id = $2 AND status = 'pending' "
        "ORDER BY recommended_at DESC LIMIT 1",
        ctx["truck_unit"],
        ctx["load_id"],
    )
    is_first_plan = existing is None
    if existing is None and replaces_event_id is None:
        replaces_event_id = await advice_audit.missed_predecessor(truck_unit, ctx["load_id"])

    try:
        stats = await samsara.get_vehicle_stats(driver["samsara_vehicle_id"])
    except SamsaraError as exc:
        raise LoadContextError(f"Samsara unavailable for truck {ctx['truck_unit']}: {exc}") from exc

    age = getattr(stats, "gps_age_minutes", None)
    if age is None or not 0 <= age <= settings.MAX_ADVICE_GPS_AGE_MINUTES:
        raise LoadContextError("Fresh truck GPS is required before fuel advice")

    # Remaining-route planner using only the private Valhalla server. The legacy nearby-stop optimizer and external/geometric routing
    # fallbacks are fully retired from this flow.
    try:
        leg = await _build_routed_leg(
            ctx=ctx,
            stats=stats,
            is_first_plan=is_first_plan,
            delivery_complete_followup=delivery_complete_followup,
        )
    except NoFeasibleFuelPlan as exc:
        metrics.incr("load_sync_routed_no_feasible_plan")
        log.warning(
            "load_sync: lane planner found no feasible fuel plan for "
            "truck %s load %s (%s)",
            ctx["truck_unit"],
            ctx["load_id"],
            exc,
        )
        raise NoValidStopError(
            truck_unit=ctx["truck_unit"],
            load_id=ctx["load_id"],
            current_fuel_gallons=stats.fuel_gallons,
            reason="no_feasible_routed_plan",
        ) from exc
    if leg is None:
        if existing is not None:
            await advice_audit.expire_pending(existing["id"], "fuel_no_longer_needed")
        await advice_audit.record("no_fuel_needed", truck_unit=truck_unit, load_id=ctx["load_id"],
                                  related_event_id=replaces_event_id)
        if replaces_event_id:
            await advice_audit.record("replan_completed", truck_unit=truck_unit, event_id=replaces_event_id,
                                      key=f"replan_completed:{replaces_event_id}", details={"outcome":"no_fuel_needed"})
        return "skipped"
    if existing is not None:
        previous = _selected_candidate(existing["candidates"]) or {}
        fresh = _selected_candidate(leg["candidates_json"]) or {}
        model = previous.get("plan", {}).get("route_evidence", {}).get("model")
        same = (model == "remaining_route_v1"
                and str(existing["recommended_site_id"]) == str(leg["recommended_site_id"])
                and int(existing["gallons"]) == int(leg["gallons"])
                and abs(float(previous.get("distance_miles", -100)) - float(fresh.get("distance_miles", 100))) < 1)
        if same:
            return "skipped"
        replaces_event_id = existing["id"]
        from dieselup.core.route_progress import passed_stop_evidence
        evidence = passed_stop_evidence(previous.get("plan", {}).get("route_evidence", {}), stats)
        if evidence:
            observed = await fetch_one("SELECT id FROM fuel_events WHERE stop_event_id=$1 AND gallons>=30 LIMIT 1", existing["id"])
            before = existing.get("fuel_pct_before")
            fuel_rose = before is not None and stats.fuel_gallons - float(before)/100*settings.TANK_CAPACITY_GALLONS >= 30
            if observed is None and not fuel_rose:
                from dieselup.core.compliance import _mark_resolved
                await _mark_resolved(event_id=existing["id"], status="skipped", actual_site_id=None,
                                     actual_true_cost=None, dollar_impact=0, evidence=evidence)
            else:
                await advice_audit.expire_pending(existing["id"], "fueling_detected_route_changed")
        else:
            await advice_audit.expire_pending(existing["id"], "current_route_or_quantity_changed")


    if leg.get("degraded"):
        raise LoadContextError("Relaxed fuel advice requires dispatcher review")

    recommended_site_id = leg["recommended_site_id"]
    recommended_true_cost = leg["recommended_true_cost"]
    worst_true_cost = leg["worst_true_cost"]
    gallons_to_pump = leg["gallons"]
    candidates_json = leg["candidates_json"]
    briefing_text = leg["briefing_text"]
    alert_kind = leg["alert_kind"]
    _parked_suppress = (
        alert_kind == "briefing"
        and await _suppress_driver_briefing_for_parked_truck(
            truck_unit=ctx["truck_unit"],
            stats=stats,
            samsara_vehicle_id=driver["samsara_vehicle_id"],
        )
    )
    _rest_suppress = (
        alert_kind == "briefing"
        and _is_driver_resting(stats)
    )
    if _rest_suppress:
        log.info(
            "load_sync: truck %s is resting (speed=%.1f mph, GPS age=%.0f min) — "
            "driver briefing suppressed, dispatch only",
            ctx["truck_unit"],
            stats.speed_mph or 0.0,
            stats.gps_age_minutes or 0.0,
        )
    suppress_driver_briefing = _parked_suppress or _rest_suppress

    stored = json.loads(candidates_json)
    stored[0].setdefault("plan", {})["driver_at_advice"] = {
        "name": driver.get("driver_full_name"), "group_id": driver.get("driver_telegram_id")}
    stored[0]["plan"]["messaging_mode"] = settings.TELEGRAM_MESSAGING_MODE
    proof = stored[0].get("plan", {}).get("route_evidence")
    if proof is not None:
        proof.update(trip_verified=True, tms_order_id=str(ctx["tms_order_id"]),
                     samsara_vehicle_id=str(driver["samsara_vehicle_id"]))
    candidates_json = json.dumps(stored)

    fuel_pct_before = round(stats.fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100.0, 2)

    inserted = await fetch_one(
        """
        INSERT INTO stop_events
            (truck_unit, driver_id, load_id, datatruck_order_id, tms_order_id,
             recommended_site_id, recommended_true_cost, candidates,
             worst_candidate_true_cost, gallons, status, fuel_pct_before,
             samsara_vehicle_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, 'pending', $11, $12)
        RETURNING id
        """,
        ctx["truck_unit"],
        driver["driver_telegram_id"],
        ctx["load_id"],
        ctx["datatruck_order_id"],
        ctx.get("tms_order_id") or (
            str(ctx["datatruck_order_id"]) if ctx.get("datatruck_order_id") is not None else None
        ),
        recommended_site_id,
        recommended_true_cost,
        candidates_json,
        worst_true_cost,
        gallons_to_pump,
        fuel_pct_before,
        driver["samsara_vehicle_id"],
    )
    event_id = int(inserted["id"]) if inserted else None
    if event_id is not None:
        selected = stored[0]
        await advice_audit.record("plan_created", truck_unit=truck_unit, load_id=ctx["load_id"],
                                  event_id=event_id, related_event_id=replaces_event_id,
                                  key=f"created:{event_id}", details={
                                      "site_id": recommended_site_id, "station_name": selected.get("station_name"),
                                      "planned_gallons": gallons_to_pump, "fill_to_full": bool(selected.get("fill_to_full")),
                                      "distance_miles": selected.get("distance_miles"),
                                      "messaging_mode": settings.TELEGRAM_MESSAGING_MODE})
        if replaces_event_id:
            await advice_audit.record("replan_completed", truck_unit=truck_unit, event_id=replaces_event_id,
                                      related_event_id=event_id, key=f"replan_completed:{replaces_event_id}",
                                      details={"outcome":"replacement_recorded"})

    # Build inline keyboard for the driver's copy only — skip for delivery follow-ups
    # (no stop to confirm) and when event_id is unknown (insert failed).
    _briefing_keyboard = None
    if event_id is not None and alert_kind == "briefing":
        import json as _json
        _first_stop = (_json.loads(candidates_json) if isinstance(candidates_json, str) else candidates_json or [{}])[0]
        _briefing_keyboard = fuel_plan_keyboard(
            stop=_first_stop,
            stop_event_id=event_id,
            origin_label=ctx.get("origin_label", ""),
            truck_lat=stats.lat if stats else None,
            truck_lng=stats.lng if stats else None,
        )

    driver_msg_id: int | None = None
    dispatch_msg_id: int | None = None
    msg_col_driver, msg_col_dispatch = _msg_id_columns_for(alert_kind)
    link_verified = True
    if verified_driver_links is not None:
        link_verified = _is_verified_driver_assignment(
            driver=driver,
            truck_unit=str(ctx["truck_unit"]),
            quickmanage_driver=dt_driver,
            verified_driver_links=verified_driver_links,
        )
        if not link_verified and driver["driver_telegram_id"]:
            metrics.incr("load_sync_driver_assignment_mismatch")
            log.warning(
                "load_sync: driver alert blocked by Telegram/Supabase/QuickManage "
                "mismatch truck=%s load=%s quickmanage_driver=%r",
                ctx["truck_unit"], ctx["load_id"], dt_driver,
            )

    can_send_driver_briefing = (
        bool(driver["driver_telegram_id"])
        and link_verified
        and alert_kind != "delivery"
        and not suppress_driver_briefing
    )
    if can_send_driver_briefing:
        can_send_driver_briefing = await _claim_driver_briefing_fingerprint(
            alert_kind=alert_kind,
            truck_unit=ctx["truck_unit"],
            load_id=ctx["load_id"],
            recommended_site_id=recommended_site_id,
        )
        if not can_send_driver_briefing:
            log.info(
                "load_sync: duplicate driver briefing suppressed for truck %s load %s site %s",
                ctx["truck_unit"],
                ctx["load_id"],
                recommended_site_id,
            )

    if can_send_driver_briefing:
        # queue_on_failure=True: a failed driver send (Telegram blip, circuit
        # open) lands in alert_dlq and the dlq_retry job delivers it within
        # ~10 min. Previously failures were dropped, and because the briefing
        # fingerprint had already been claimed, the driver was PERMANENTLY
        # silenced for this truck+load+stop — briefings vanished with no retry.
        driver_msg_id = await safe_send(
            bot=bot,
            chat_id=int(driver["driver_telegram_id"]),
            text=briefing_text,
            alert_type=alert_kind,
            reply_markup=_briefing_keyboard,
            truck_unit=ctx["truck_unit"],
            load_id=ctx["load_id"],
            stop_event_id=event_id,
            msg_id_column=msg_col_driver,
            queue_on_failure=True,
            replace_previous_driver_alert=True,
        )
    elif driver["driver_telegram_id"] and alert_kind == "delivery":
        log.info(
            "load_sync: suppressing delivery-complete follow-up to driver chat "
            "for truck %s load %s",
            ctx["truck_unit"],
            ctx["load_id"],
        )
    elif driver["driver_telegram_id"] and suppress_driver_briefing:
        reason = "resting (speed=0, stopped >1hr)" if _rest_suppress else "parked near last plan"
        log.info(
            "load_sync: suppressing driver briefing (%s) for truck %s load %s",
            reason,
            ctx["truck_unit"],
            ctx["load_id"],
        )
    if settings.TELEGRAM_DISPATCH_CHAT_ID is not None:
        dispatch_msg_id = await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_DISPATCH_CHAT_ID,
            text=briefing_text,
            alert_type=f"dispatch_{alert_kind}",
            truck_unit=ctx["truck_unit"],
            load_id=ctx["load_id"],
            stop_event_id=event_id,
            msg_id_column=msg_col_dispatch,
        )

    if driver_msg_id is None and dispatch_msg_id is None:
        log.warning(
            "load_sync: event %s briefing landed nowhere — no driver chat and dispatch send failed/circuit-open",
            event_id,
        )

    # Persist whichever sides actually went out so we can edit/audit later.
    if event_id is not None and (driver_msg_id or dispatch_msg_id):
        await execute(
            f"""
            UPDATE stop_events
            SET {msg_col_driver} = COALESCE($2, {msg_col_driver}),
                {msg_col_dispatch} = COALESCE($3, {msg_col_dispatch})
            WHERE id = $1
            """,
            event_id,
            driver_msg_id,
            dispatch_msg_id,
        )

    return "briefed"


def _msg_id_columns_for(alert_kind: str) -> tuple[str, str]:
    """Map an alert_kind ('briefing'/'approach'/'delivery') to its msg_id columns."""
    if alert_kind == "approach":
        return "approach_driver_msg_id", "approach_dispatch_msg_id"
    if alert_kind == "delivery":
        return "delivery_driver_msg_id", "delivery_dispatch_msg_id"
    return "briefing_driver_msg_id", "briefing_dispatch_msg_id"


async def _claim_driver_briefing_fingerprint(
    *,
    alert_kind: str,
    truck_unit: str,
    load_id: str,
    recommended_site_id: int,
) -> bool:
    """Remember an auto driver briefing so the same truck/load/stop is one-shot."""
    fingerprint = _driver_briefing_fingerprint(
        alert_kind=alert_kind,
        truck_unit=truck_unit,
        load_id=load_id,
        recommended_site_id=recommended_site_id,
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
        f"driver_{alert_kind}",
        truck_unit,
        load_id,
    )
    return row is not None


def _driver_briefing_fingerprint(
    *,
    alert_kind: str,
    truck_unit: str,
    load_id: str,
    recommended_site_id: int,
) -> str:
    payload = "\x1f".join([
        "driver_briefing",
        alert_kind,
        truck_unit,
        load_id,
        str(recommended_site_id),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def _truck_moved_since_last_plan(
    *,
    truck_unit: str,
    current_lat: float,
    current_lng: float,
    samsara_vehicle_id: str | None = None,
    threshold_miles: float = 5.0,
) -> bool:
    """True if the truck moved at least threshold_miles since the last stop_event.

    Used for movement gating: skip replanning sweeps when the truck hasn't
    moved — saves Samsara calls, DB queries, and prevents briefing spam for
    trucks waiting at shippers, rest stops, or delivery docks.
    Returns True (allow planning) when no prior position is available.
    Matches on canonical samsara_vehicle_id as well as truck_unit so SUBUNIT
    alias flips between loads can't hide the prior plan.
    """
    row = await fetch_one(
        """
        SELECT candidates
        FROM stop_events
        WHERE (truck_unit = $1 OR samsara_vehicle_id = COALESCE($2, ''))
          AND recommended_at >= NOW() - INTERVAL '12 hours'
        ORDER BY recommended_at DESC
        LIMIT 1
        """,
        truck_unit,
        samsara_vehicle_id,
    )
    if row is None:
        return True  # no prior plan — always allow first plan

    prior = _selected_candidate(row["candidates"])
    if prior is None:
        return True

    prior_lat = _float_or_none(prior.get("truck_latitude"))
    prior_lng = _float_or_none(prior.get("truck_longitude"))
    if prior_lat is None or prior_lng is None:
        return True

    moved = haversine_miles(current_lat, current_lng, prior_lat, prior_lng)
    return moved >= threshold_miles


def _is_driver_resting(stats: Any) -> bool:
    """True when the truck is stationary and has been stopped long enough that
    the driver is likely resting (sleeping or on a long break).

    Detection uses two signals from Samsara:
      1. speed_mph < DRIVER_REST_SPEED_MPH (≤ 2 mph) — truck is not rolling.
      2. gps_age_minutes >= DRIVER_REST_MINUTES (≥ 60 min) — Samsara stops
         sending frequent GPS updates when ignition is off, so a stale-GPS +
         speed=0 combination reliably indicates the truck has been parked for
         at least one hour.

    If either signal is unavailable (None), we default to False — prefer
    sending the alert over silently dropping it when data is missing.

    Dispatch always receives messages regardless of driver rest state.
    Wrong-stop and missed-stop alerts are never suppressed (truck is moving).
    """
    speed = getattr(stats, "speed_mph", None)
    gps_age = getattr(stats, "gps_age_minutes", None)
    if speed is None or gps_age is None:
        return False
    return speed < settings.DRIVER_REST_SPEED_MPH and gps_age >= settings.DRIVER_REST_MINUTES


async def _suppress_driver_briefing_for_parked_truck(
    *,
    truck_unit: str,
    stats: Any,
    samsara_vehicle_id: str | None = None,
) -> bool:
    """True when a new auto-briefing should be dispatch-only to avoid waking drivers."""
    gps_age = getattr(stats, "gps_age_minutes", None)
    if gps_age is not None and gps_age >= PARKED_GPS_STALE_MINUTES:
        log.info(
            "load_sync: truck %s GPS is stale %.0f min; treating as parked",
            truck_unit,
            gps_age,
        )
        return True

    row = await fetch_one(
        """
        SELECT candidates
        FROM stop_events
        WHERE (truck_unit = $1 OR samsara_vehicle_id = COALESCE($3, ''))
          AND recommended_at >= NOW() - ($2 || ' hours')::INTERVAL
        ORDER BY recommended_at DESC
        LIMIT 1
        """,
        truck_unit,
        str(PARKED_RECENT_PLAN_HOURS),
        samsara_vehicle_id,
    )
    if row is None:
        return False

    prior = _selected_candidate(row["candidates"])
    if prior is None:
        return False

    prior_lat = _float_or_none(prior.get("truck_latitude"))
    prior_lng = _float_or_none(prior.get("truck_longitude"))
    if prior_lat is None or prior_lng is None:
        return False

    moved_miles = haversine_miles(float(stats.lat), float(stats.lng), prior_lat, prior_lng)
    if moved_miles <= PARKED_RADIUS_MILES:
        log.info(
            "load_sync: truck %s moved %.2f mi since last plan; suppressing driver briefing",
            truck_unit,
            moved_miles,
        )
        return True
    return False


def _selected_candidate(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, list):
        candidates = raw
    elif isinstance(raw, (bytes, bytearray)):
        try:
            candidates = json.loads(raw.decode("utf-8"))
        except ValueError:
            return None
    elif isinstance(raw, str):
        try:
            candidates = json.loads(raw)
        except ValueError:
            return None
    else:
        return None
    if not isinstance(candidates, list) or not candidates:
        return None
    first = candidates[0]
    return first if isinstance(first, dict) else None


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _is_active(order: dict[str, Any]) -> bool:
    status = order.get("status")
    if not isinstance(status, str):
        return False
    return status.strip().lower() in ACTIVE_STATUSES


def _load_context(order: dict[str, Any]) -> dict[str, Any]:
    """Extract truck_unit, load_id, datatruck_order_id, origin/destination lat/lng + labels."""
    order_id = order.get("id")
    if not isinstance(order_id, (int, str)) or not str(order_id).strip():
        raise LoadContextError("order missing 'id'")
    tms_order_id = str(order.get("tms_order_id") or order_id).strip()
    datatruck_order_id = order_id if isinstance(order_id, int) else None

    load_id_raw = order.get("load_number") or order.get("load_id") or order_id
    load_id = str(load_id_raw).strip()

    truck_unit = _extract_truck_unit(order)
    # Don't raise here — caller will attempt driver-name fallback if None

    dest = _extract_endpoint(order, kind="delivery")
    if dest is None or dest.get("lat") is None or dest.get("lng") is None:
        raise LoadContextError(f"order {order_id} has no delivery coordinates")

    origin = _extract_endpoint(order, kind="pickup")
    origin_label = _label_from_endpoint(origin) or "Origin"
    destination_label = _label_from_endpoint(dest) or "Destination"

    # Shipper coordinates — used as the planning origin for full-route DP at
    # load assignment so the optimizer sees the complete shipper→delivery price
    # map rather than just the truck's current position → delivery slice.
    origin_lat = float(origin["lat"]) if origin and origin.get("lat") is not None else None
    origin_lng = float(origin["lng"]) if origin and origin.get("lng") is not None else None

    return {
        "datatruck_order_id": datatruck_order_id,
        "tms_order_id": tms_order_id,
        "load_id": load_id,
        "truck_unit": truck_unit,
        "destination_lat": float(dest["lat"]),
        "destination_lng": float(dest["lng"]),
        "destination_label": destination_label,
        "origin_lat": origin_lat,
        "origin_lng": origin_lng,
        "origin_label": origin_label,
        "order": order,
    }


def _extract_truck_unit(order: dict[str, Any]) -> str | None:
    """Extract truck unit number from a DataTruck order.

    Confirmed top-level field from the real API: truck_unit_number.
    Keeps all prior fallbacks for shape drift.
    """
    # Primary path: trip.truck__unit_number (confirmed from real API)
    trip = order.get("trip")
    if isinstance(trip, dict):
        v = trip.get("truck__unit_number") or trip.get("truck_unit_number")
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
        truck = trip.get("truck")
        if isinstance(truck, dict):
            for sub in ("unit_number", "unit", "number", "id"):
                inner = truck.get(sub)
                if isinstance(inner, (str, int)) and str(inner).strip():
                    return str(inner).strip()

    # assigned_driver_n_truck.truck_unit_number (confirmed from real API)
    adt = order.get("assigned_driver_n_truck")
    if isinstance(adt, dict):
        v = adt.get("truck_unit_number")
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()

    # Further fallbacks for shape drift
    for key in ("truck_unit_number", "tractor_unit", "truck_unit", "unit"):
        v = order.get(key)
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
    for key in ("tractor", "truck"):
        v = order.get(key)
        if isinstance(v, dict):
            for sub in ("unit", "number", "unit_number", "id"):
                inner = v.get(sub)
                if isinstance(inner, (str, int)) and str(inner).strip():
                    return str(inner).strip()
        elif isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
    return None


def _extract_endpoint(order: dict[str, Any], *, kind: str) -> dict[str, Any] | None:
    """Best-available endpoint dict ({lat, lng, city, state}) for pickup or delivery.

    `kind` is "pickup" or "delivery". Falls back across nested keys and the
    stops[] array. Either coordinate may be None — caller decides if that's
    OK (deliveries need both; the origin can render with just city/state).
    """
    if kind == "delivery":
        primary_keys = ("delivery", "dropoff", "destination")
        stop_types = {"delivery", "dropoff", "drop", "destination"}
        reverse = True
    else:
        primary_keys = ("pickup", "origin", "shipper")
        stop_types = {"pickup", "shipper", "origin"}
        reverse = False

    for key in primary_keys:
        v = order.get(key)
        if isinstance(v, dict):
            ep = _endpoint_from_dict(v)
            if ep:
                return ep

    stops = order.get("stops")
    if isinstance(stops, list):
        seq = list(reversed(stops)) if reverse else list(stops)
        for stop in seq:
            if not isinstance(stop, dict):
                continue
            stop_type = str(stop.get("type") or stop.get("stop_type") or "").lower()
            if stop_type in stop_types:
                ep = _endpoint_from_dict(stop)
                if ep:
                    return ep
        for stop in seq:
            if isinstance(stop, dict):
                ep = _endpoint_from_dict(stop)
                if ep:
                    return ep
    return None


def _endpoint_from_dict(d: dict[str, Any]) -> dict[str, Any] | None:
    """Pull {lat, lng, city, state} from a DataTruck stop/location-shaped dict.

    Descends into a nested `location` sub-dict (the DataTruck stops[] shape
    used in production puts coords inside `location.latitude`, `location.state`,
    etc., not at the top level).
    """
    src = d
    inner = d.get("location")
    if isinstance(inner, dict):
        src = inner
    lat = src.get("latitude") if "latitude" in src else src.get("lat")
    lng = src.get("longitude") if "longitude" in src else (src.get("lng") or src.get("lon"))
    if isinstance(lat, str):
        try:
            lat = float(lat)
        except ValueError:
            lat = None
    if isinstance(lng, str):
        try:
            lng = float(lng)
        except ValueError:
            lng = None
    city = src.get("city")
    state = src.get("state")
    if (lat is None or lng is None) and not city and not state:
        return None
    return {
        "lat": float(lat) if isinstance(lat, (int, float)) else None,
        "lng": float(lng) if isinstance(lng, (int, float)) else None,
        "city": str(city).strip() if city else None,
        "state": _normalize_state(state),
    }


# DataTruck returns full state names ("Arizona"); we use 2-letter codes everywhere
# (IFTA, fuel_stops.state) — normalize at the boundary.
_STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}


def _normalize_state(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 2:
        return s.upper()
    return _STATE_NAME_TO_CODE.get(s.lower(), s.upper()[:2])


def _label_from_endpoint(ep: dict[str, Any] | None) -> str | None:
    if ep is None:
        return None
    city = ep.get("city")
    state = ep.get("state")
    if city and state:
        return f"{city}, {state}"
    return city or state or None


async def _send_standalone_delivery_complete(
    *,
    bot: Bot,
    samsara: SamsaraClient,
    truck_unit: str,
) -> None:
    """Post a dispatch-only delivery-complete message with no fuel plan.

    Fires when a truck delivered a load but there is no new active order in
    the same sweep, without spamming the driver's group.
    """
    driver = await fetch_one(
        """
        SELECT samsara_vehicle_id
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        truck_unit,
    )
    samsara_vehicle_id = driver["samsara_vehicle_id"] if driver else None

    current_fuel_percent: int | None = None
    if samsara_vehicle_id:
        try:
            fuel_gallons = await samsara.get_vehicle_fuel(samsara_vehicle_id)
            current_fuel_percent = round(
                fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100
            )
        except SamsaraError:
            log.warning("delivery-complete: Samsara fuel lookup failed for %s", truck_unit)

    text = delivery_complete_message(
        truck_unit=truck_unit,
        next_load_id=None,
        current_fuel_percent=current_fuel_percent,
        distance_to_next_stop_miles=None,
        stop=None,
        gallons_to_pump=settings.TANK_CAPACITY_GALLONS,
    )

    if settings.TELEGRAM_DISPATCH_CHAT_ID is not None:
        await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_DISPATCH_CHAT_ID,
            text=text,
            alert_type="dispatch_standalone_delivery",
            truck_unit=truck_unit,
        )


async def _alert_admin_no_valid_stop(bot: Bot, exc: NoValidStopError) -> None:
    """Persist + notify once per (truck, load).

    no_valid_stop_alerts itself is the dedupe marker: if a prior row exists
    for this (truck_unit, load_id), we've already told admin about this
    over-full / under-floor combo and stay silent on subsequent sweeps.
    First-time sightings get one DM and one DB row; everything else is a
    no-op until the load resolves or the truck transitions out of the
    rejection range (at which point a real briefing fires instead).
    """
    existing = await fetch_one(
        """
        SELECT 1 FROM no_valid_stop_alerts
        WHERE truck_unit = $1 AND load_id = $2
        LIMIT 1
        """,
        exc.truck_unit,
        exc.load_id,
    )
    if existing is not None:
        return

    try:
        await execute(
            """
            INSERT INTO no_valid_stop_alerts
                (truck_unit, load_id, current_fuel_gallons)
            VALUES ($1, $2, $3)
            """,
            exc.truck_unit,
            exc.load_id,
            exc.current_fuel_gallons,
        )
    except Exception:  # noqa: BLE001 — alert persistence must never block the Telegram ping
        log.exception("load_sync: failed to persist no_valid_stop alert")

    reason = getattr(exc, "reason", "no_valid_stop")
    if reason == "no_feasible_routed_plan":
        # Lane DP (shipper -> delivery) — there is no arrival window here.
        # This means delivery is unreachable under the reserve with the
        # contracted stops available on the lane (or the corridor has no
        # priced stops at all — check today's price upload).
        detail = (
            "Lane planner: no feasible shipper→delivery buy plan — delivery "
            f"unreachable while keeping >= {settings.SAFETY_FLOOR_GALLONS} gal "
            "reserve with the contracted stops on this lane. Check that "
            "today's price upload exists and the lane has Pilot/FJ coverage."
        )
    else:
        detail = (
            "Legacy single-stop optimizer: no stop on the route satisfies "
            f"{settings.SAFETY_FLOOR_GALLONS} <= arrival fuel "
            f"<= {settings.MAX_ARRIVAL_FUEL_GALLONS}."
        )
    await _safe_send_admin(
        bot,
        f"no_valid_stop ({reason}): truck {exc.truck_unit} on load {exc.load_id} — "
        f"current fuel {exc.current_fuel_gallons:.1f} gal. {detail} "
        f"(This notice fires once per truck+load combo — subsequent sweeps stay quiet "
        f"until the load resolves or the truck moves into briefable range.)",
    )


async def _alert_admin_error(bot: Bot, order: dict[str, Any], exc: Exception) -> None:
    order_id = str(order.get("id") or "?")
    signature = (order_id, type(exc).__name__, str(exc))
    now = time.monotonic()
    last_sent = _recent_error_alerts.get(signature)
    if last_sent is not None and now - last_sent < ERROR_ALERT_COOLDOWN_SECONDS:
        metrics.incr("load_sync_error_alert_suppressed")
        return
    _recent_error_alerts[signature] = now
    # Bound memory even if the TMS produces a long stream of unique bad rows.
    if len(_recent_error_alerts) > 2_000:
        cutoff = now - ERROR_ALERT_COOLDOWN_SECONDS
        for key, sent_at in list(_recent_error_alerts.items()):
            if sent_at < cutoff:
                _recent_error_alerts.pop(key, None)
    await _safe_send_admin(
        bot,
        f"load_sync error on order {order_id}: {type(exc).__name__}: {exc}",
        alert_type="admin_load_sync_error",
    )


async def _safe_send_admin(bot: Bot, text: str, *, alert_type: str = "admin_misc") -> None:
    await safe_send(
        bot=bot,
        chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
        text=text,
        alert_type=alert_type,
        parse_mode=None,
    )
