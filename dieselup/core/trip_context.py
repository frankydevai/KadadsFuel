"""Verified remaining trip stops. Missing progress or exact coordinates is a hold."""

from __future__ import annotations

import math
from typing import Any


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
            }
        )
    return result
