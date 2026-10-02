"""Prevent stored/queued fuel advice from bypassing current-route validation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import time

from dieselup.clients.samsara import SamsaraClient
from dieselup.clients.tms import make_tms_client
from dieselup.config import settings
from dieselup.core.operating_scope import allows
from dieselup.core.remaining_route import plan_remaining_route
from dieselup.core.trip_context import remaining_stops
from dieselup.db import fetch_one

log = logging.getLogger(__name__)
FUEL_ADVICE_TYPES = {
    "briefing",
    "approach",
    "delivery",
    "delivery_complete",
    "dispatch_briefing",
    "dispatch_approach",
    "dispatch_delivery",
}


def is_fuel_advice(alert_type: str) -> bool:
    while alert_type.startswith("retry_"):
        alert_type = alert_type[6:]
    return alert_type in FUEL_ADVICE_TYPES


async def validate_fuel_event(
    event_id: int | None, truck_unit: str | None, chat_id: int | None = None
) -> bool:
    if event_id is None or not truck_unit or not allows(truck_unit):
        return False
    try:
        async with asyncio.timeout(30):
            return await _validate(event_id, truck_unit, chat_id)
    except Exception as exc:
        log.warning(
            "fuel advice held event=%s truck=%s reason=%s",
            event_id,
            truck_unit,
            type(exc).__name__,
        )
        return False


async def _validate(event_id: int, truck_unit: str, chat_id: int | None) -> bool:
    from dieselup.core.load_sync import (
        _assigned_vehicle,
        _build_samsara_unit_map,
        _extract_order_driver,
        _select_current_loads,
        _is_active,
        _load_id_of,
    )
    from dieselup.bot.group_link import driver_names_match

    row = await fetch_one(
        """
        SELECT se.id, se.truck_unit, se.load_id, se.tms_order_id, se.status,
               se.recommended_site_id, se.gallons, se.candidates, se.samsara_vehicle_id,
               td.driver_telegram_id, td.driver_full_name,
               td.samsara_vehicle_id AS roster_vehicle_id
        FROM stop_events se JOIN trucks_drivers td ON td.truck_unit = se.truck_unit
        WHERE se.id = $1 AND se.truck_unit = $2
    """,
        event_id,
        truck_unit,
    )
    if (
        row is None
        or row["status"] != "pending"
        or str(row["roster_vehicle_id"]) != str(row["samsara_vehicle_id"])
    ):
        return False
    if chat_id is not None and chat_id not in {
        row["driver_telegram_id"],
        settings.TELEGRAM_DISPATCH_CHAT_ID,
    }:
        return False
    data = row["candidates"]
    if isinstance(data, (str, bytes)):
        data = json.loads(data)
    if not isinstance(data, list) or not data:
        return False
    proof = data[0].get("plan", {}).get("route_evidence", {})
    if (
        proof.get("model") != "remaining_route_v1"
        or not proof.get("complete_candidate_coverage")
        or not proof.get("trip_verified")
    ):
        return False  # old advice must be replanned, never replayed blindly
    checked = datetime.fromisoformat(proof["checked_at"])
    age = (datetime.now(timezone.utc) - checked).total_seconds()
    if (
        0 <= age <= 10
        and proof.get("tms_order_id") == str(row["tms_order_id"])
        and proof.get("samsara_vehicle_id") == str(row["samsara_vehicle_id"])
    ):
        return True  # immediate send of the just-computed route, same snapshot
    async with make_tms_client() as tms, SamsaraClient() as samsara:
        unit_map = await _build_samsara_unit_map(samsara)
        # Exhaust the iterator: a partial fleet list cannot prove assignment.
        orders = [order async for order in tms.iter_orders()]
        current = [
            order
            for order in _select_current_loads(orders, unit_map)
            if str(order.get("tms_order_id") or order.get("id"))
            == str(row["tms_order_id"])
        ]
        if len(current) != 1:
            return False
        order = await tms.get_order(row["tms_order_id"])
        if not _is_active(order) or _load_id_of(order) != str(row["load_id"]):
            return False
        vehicle = _assigned_vehicle(order, unit_map)
        if vehicle is None or vehicle.id != str(row["samsara_vehicle_id"]):
            return False
        if not driver_names_match(
            row["driver_full_name"], _extract_order_driver(order)
        ):
            return False
        stats = await samsara.get_vehicle_stats(vehicle.id)
        if (
            stats.gps_age_minutes is None
            or not 0 <= stats.gps_age_minutes <= settings.MAX_ADVICE_GPS_AGE_MINUTES
        ):
            return False
        if (
            stats.fuel_age_minutes is None
            or not 0 <= stats.fuel_age_minutes <= settings.MAX_ADVICE_FUEL_AGE_MINUTES
        ):
            return False
        waypoints = remaining_stops(order)
        if not waypoints:
            return False
        started = time.monotonic()
        lane = await plan_remaining_route(
            stats=stats,
            waypoints=waypoints,
            mpg=stats.mpg_rolling or settings.FLEET_DEFAULT_MPG,
        )
        elapsed = (time.monotonic() - started) / 60
        if stats.gps_age_minutes + elapsed > settings.MAX_ADVICE_GPS_AGE_MINUTES or stats.fuel_age_minutes + elapsed > settings.MAX_ADVICE_FUEL_AGE_MINUTES:
            return False
        if not lane.legs:
            return False
        first = lane.legs[0]
        return first.candidate.site_id == int(row["recommended_site_id"]) and (
            first.fill_to_full
            and data[0].get("fill_to_full")
            or not first.fill_to_full
            and abs(first.gallons - float(row["gallons"])) < 0.51
        )
