"""Remaining navigation from explicit stop progress or the owner's TMS phase rule."""

from __future__ import annotations

import math
import hashlib
import json
from typing import Any

from dieselup.core.operating_scope import unit_key


class TripContextError(ValueError):
    pass


def completion_state(stop: dict[str, Any]) -> bool | None:
    for key in ("completed", "is_completed"):
        if isinstance(stop.get(key), bool):
            return stop[key]
    status = str(stop.get("status") or "").strip().lower()
    if status in {"completed", "complete", "delivered", "departed"}:
        return True
    if status in {"pending", "scheduled", "not_started", "arrived", "in_progress"}:
        return False
    if stop.get("actual_departure_at") or stop.get("completed_at"):
        return True
    return None


def quickmanage_route_phase(status: str) -> str:
    """Owner-defined navigation phase; never a statement of stop completion."""
    status = str(status or "").strip().lower()
    if status == "in_transit":
        return "delivery_only"
    if status in {"dispatched", "dispatching"}:
        return "pickup_then_delivery"
    if status in {"reserved", "upcoming"}:
        return "reserved"
    return "unknown"


def _quickmanage_navigation_stops(order, raw, states):
    """Apply the status rule locally without changing any historical evidence."""
    status = str(order.get("raw_status") or order.get("status") or "").strip().lower()
    phase = quickmanage_route_phase(status)
    if (order.get("route_context_source") != "quickmanage_status"
            or order.get("route_phase") != phase
            or order.get("route_phase_status") != status):
        raise TripContextError("QuickManage navigation phase is missing or inconsistent")
    if phase == "reserved":
        raise TripContextError("Reserved next load cannot provide the current truck route")
    if phase == "unknown":
        raise TripContextError("QuickManage status does not authorize a current route")
    if any(stop.get("type") not in {"pickup", "delivery"} for stop in raw):
        raise TripContextError("QuickManage stop type is unknown")
    if not any(stop.get("type") == "delivery" for stop in raw):
        raise TripContextError("QuickManage trip has no delivery endpoint")
    if phase == "pickup_then_delivery":
        if any(state is True for state in states):
            raise TripContextError("Completed stop contradicts the dispatched trip status")
        # The dispatched phase authorizes visiting these stops in order. It
        # does not establish that the corresponding stop records are pending.
        return raw, [False] * len(raw)

    if any(state is False for stop, state in zip(raw, states)
           if stop.get("type") == "pickup"):
        raise TripContextError("Uncompleted pickup contradicts the in_transit trip status")
    deliveries = [(stop, state) for stop, state in zip(raw, states)
                  if stop.get("type") == "delivery"]
    seen_remaining = False
    for _, state in deliveries:
        if state is True and seen_remaining:
            raise TripContextError("Stop completion is out of sequence")
        if state is not True:
            seen_remaining = True
    remaining = [(stop, state) for stop, state in deliveries if state is not True]
    if len(remaining) > 1 and any(state is None for _, state in remaining):
        raise TripContextError("Multiple delivery stops need explicit remaining-stop progress")
    return [stop for stop, _ in remaining], [False] * len(remaining)


def remaining_stops(order: dict[str, Any]) -> list[dict[str, Any]]:
    raw = order.get("stops")
    if not isinstance(raw, list) or not raw:
        # A provider may explicitly supply the remaining delivery endpoint.
        delivery = order.get("delivery") or order.get("destination")
        if not isinstance(delivery, dict):
            raise TripContextError("Trip has no ordered remaining stops")
        raw = [{**delivery, "type": "delivery", "completed": False}]
    if any(not isinstance(s, dict) for s in raw):
        raise TripContextError("Trip contains an invalid stop")
    states = [completion_state(s) for s in raw]
    if order.get("tms_provider") == "quickmanage":
        raw, states = _quickmanage_navigation_stops(order, raw, states)
    # A single delivery is already a remaining-route contract; a full trip
    # needs explicit progress. GPS proximity and appointments are not proof.
    if len(raw) == 1 and states[0] is None and str(raw[0].get("type")) == "delivery":
        states[0] = False
    if any(s is None for s in states):
        raise TripContextError(
            "Stop completion/progress is missing; dispatcher review required"
        )
    seen_pending = False
    result = []
    for index, (stop, complete) in enumerate(zip(raw, states)):
        if complete:
            if seen_pending:
                raise TripContextError("Stop completion is out of sequence")
            continue
        seen_pending = True
        if stop.get("coordinate_source") == "zip_centroid":
            raise TripContextError(
                "Exact stop coordinates required; ZIP centers cannot guide trucks"
            )
        if stop.get("coordinate_source") == "census_address_range" and (
            stop.get("coordinate_verified_for") != "fuel_route"
            or stop.get("coordinate_accuracy") != "address_range_interpolation"
            or not stop.get("address_fingerprint")
        ):
            raise TripContextError("Street coordinates have no verified full-address route match")
        try:
            lat = float(stop.get("latitude", stop.get("lat")))
            lng = float(stop.get("longitude", stop.get("lng")))
        except (TypeError, ValueError) as exc:
            raise TripContextError("Remaining stop has no exact coordinates") from exc
        if (
            not math.isfinite(lat)
            or not math.isfinite(lng)
            or not -90 <= lat <= 90
            or not -180 <= lng <= 180
        ):
            raise TripContextError("Remaining stop coordinates are invalid")
        result.append(
            {
                "id": str(stop.get("id") or index),
                "latitude": lat,
                "longitude": lng,
                "type": stop.get("type", "delivery"),
                "label": ", ".join(
                    str(stop[k]) for k in ("city", "state") if stop.get(k)
                ),
                **{key: stop[key] for key in (
                    "coordinate_source", "coordinate_accuracy", "coordinate_verified_for",
                    "address_id", "address_fingerprint", "coordinate_provider",
                    "coordinate_benchmark",
                ) if stop.get(key) is not None},
            }
        )
    return result


def route_context_signature(order: dict[str, Any]) -> str:
    """Stable phase/endpoints fingerprint for planning and delivery rechecks."""
    waypoints = remaining_stops(order)
    context = {
        "provider": order.get("tms_provider"),
        "assigned_truck": unit_key(order.get("truck_unit_number")),
        "assigned_driver": " ".join(str(order.get("driver_full_name") or "").upper().split()),
        "route_phase": order.get("route_phase"),
        "route_phase_status": str(order.get("route_phase_status")
                                  or order.get("raw_status") or order.get("status") or "").strip().lower(),
        "route_context_source": order.get("route_context_source", "explicit_stop_progress"),
        "remaining_stops": [
            {key: stop[key] for key in (
                "id", "type", "latitude", "longitude", "coordinate_source",
                "coordinate_accuracy", "coordinate_verified_for", "address_fingerprint",
            ) if key in stop}
            for stop in waypoints
        ],
    }
    return hashlib.sha256(json.dumps(context, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()
