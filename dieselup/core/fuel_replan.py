"""Durable missed-stop requests, rechecked immediately and on five-minute polls."""
import asyncio
import logging

from dieselup.core import advice_audit
from dieselup.db import fetch_all
from dieselup.core.operating_scope import allows, allowed_units

log = logging.getLogger(__name__)


async def run_requested_replans(bot):
    requests = await fetch_all("""
        SELECT DISTINCT ON (a.truck_unit) a.truck_unit, a.load_id, a.stop_event_id
        FROM fuel_advice_audit a
        WHERE (a.kind IN ('missed_detected','stop_visited_no_fill')
          OR (a.kind='stop_lost' AND a.details->'bypass_evidence'->>'method'='ordered_road_route')
          OR (a.kind='stop_expired' AND a.details->>'reason' IN
              ('advice_not_delivered_before_passage','advice_not_delivered_before_fueling')))
          AND ($1::text[] IS NULL OR ltrim(upper(a.truck_unit),'0')=ANY($1))
          AND NOT EXISTS (SELECT 1 FROM fuel_advice_audit done
              WHERE done.kind='replan_completed' AND done.truck_unit=a.truck_unit AND done.created_at>=a.created_at)
          AND NOT EXISTS (SELECT 1 FROM fuel_advice_audit attempt
              WHERE attempt.stop_event_id=a.stop_event_id AND attempt.kind='replan_attempted'
                AND attempt.created_at > NOW()-INTERVAL '5 minutes')
        ORDER BY a.truck_unit, a.created_at DESC LIMIT 20
    """, allowed_units())
    for row in requests:
        await replan_truck(bot, str(row["truck_unit"]), row["stop_event_id"])


async def replan_truck(bot, unit, previous_event_id):
    if not allows(unit):
        return False
    from dieselup.clients.samsara import SamsaraClient, extract_unit_digits
    from dieselup.clients.tms import make_tms_client
    from dieselup.bot.group_link import refresh_and_verify_linked_groups
    from dieselup.core.load_sync import (
        _build_samsara_unit_map, _select_current_loads, _extract_truck_unit,
        _is_active, _process_one_load, LOAD_SYNC_MAX_PAGES,
    )

    await advice_audit.record("replan_attempted", truck_unit=unit, event_id=previous_event_id)
    try:
        async with asyncio.timeout(90):
            async with make_tms_client() as tms, SamsaraClient() as samsara:
                unit_map = await _build_samsara_unit_map(samsara)
                orders = [order async for order in tms.iter_orders(max_pages=LOAD_SYNC_MAX_PAGES)]
                current = [order for order in _select_current_loads(orders, unit_map)
                           if extract_unit_digits(_extract_truck_unit(order) or "") == extract_unit_digits(unit)]
                if len(current) != 1:
                    raise ValueError("Current truck assignment is absent or conflicting")
                selected = current[0]
                order = await tms.get_order(selected.get("tms_order_id") or selected["id"])
                if not _is_active(order) or _extract_truck_unit(order) != _extract_truck_unit(selected):
                    raise ValueError("Trip assignment changed during the recheck")
                verified = await refresh_and_verify_linked_groups(bot)
                async with asyncio.timeout(30):
                    result = await _process_one_load(order, samsara=samsara, bot=bot,
                        samsara_by_unit=unit_map, verified_driver_links=verified,
                        current_trip_verified=True, replaces_event_id=previous_event_id)
        await advice_audit.record("replan_completed", truck_unit=unit, event_id=previous_event_id,
                                  key=f"replan_completed:{previous_event_id}", details={"outcome": result})
        return True
    except Exception as exc:
        reason = {"TripContextError":"Exact customer locations and verified stop progress are required",
                  "StaleFuelPricesError":"Upload current contracted fuel prices",
                  "RoutingError":"Truck routing is unavailable or incomplete",
                  "TimeoutError":"The replacement check timed out; a retry is queued"}.get(type(exc).__name__,
                    str(exc) if isinstance(exc, ValueError) else "Could not verify a safe replacement fuel stop")
        await advice_audit.record("replan_held", truck_unit=unit, event_id=previous_event_id,
                                  details={"reason": reason, "retry": "next compliance poll"})
        log.warning("fuel replan held truck=%s event=%s reason=%s", unit, previous_event_id, type(exc).__name__)
        return False
