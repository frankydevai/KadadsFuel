"""HTTP transport for a self-hosted Valhalla server (env VALHALLA_URL).

Mirrors the method names of the in-process dieselup.clients.valhalla.ValhallaClient (route / matrix)
so dashboard code is transport-agnostic. Reuses the same TRUCK_OPTS so HTTP routes are legal for
the worst-case loaded rig, identical to the in-process client. Fails soft → None (never crashes
the dashboard); the caller falls back to geometry/haversine.

Used at US scale where full-US tiles live on a separate Valhalla service rather than in-process.
"""

from __future__ import annotations

import logging
import os

import httpx

from dieselup.clients.valhalla import TRUCK_OPTS  # single source of truth for the truck profile

log = logging.getLogger(__name__)


class ValhallaHTTP:
    def __init__(self, base_url: str | None = None, timeout: float = 20.0) -> None:
        self._base = (base_url or os.environ.get("VALHALLA_URL") or "").rstrip("/")
        self._timeout = timeout

    @property
    def available(self) -> bool:
        return bool(self._base)

    async def _post(self, path: str, payload: dict) -> dict | None:
        if not self._base:
            log.warning("VALHALLA_URL not set — routing unavailable")
            return None
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(f"{self._base}{path}", json=payload)
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:  # transport / 4xx / 5xx / bad JSON — degrade gracefully
            log.warning("Valhalla %s failed: %s", path, exc)
            return None

    async def route(self, locations: list[dict], alternates: int = 0) -> dict | None:
        """Truck-legal route. `locations` = [{"lat":..,"lon":..}, …] in order."""
        payload = {
            "locations": locations,
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
            "directions_options": {"units": "miles"},
            "alternates": max(0, int(alternates or 0)),
        }
        return await self._post("/route", payload)

    async def matrix(
        self, origin: tuple[float, float], targets: list[tuple[float, float]]
    ) -> list[float | None] | None:
        """One-source → many-targets road miles. Returns a list aligned with `targets`."""
        if not targets:
            return []
        payload = {
            "sources": [{"lat": float(origin[0]), "lon": float(origin[1])}],
            "targets": [{"lat": float(la), "lon": float(lo)} for la, lo in targets],
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
            "units": "miles",
            "directions_options": {"units": "miles"},
        }
        data = await self._post("/sources_to_targets", payload)
        if not isinstance(data, dict):
            return None
        rows = data.get("sources_to_targets")
        if not rows:
            return None
        cells = rows[0] if isinstance(rows[0], list) else rows
        out: list[float | None] = []
        for cell in cells:
            if isinstance(cell, dict) and cell.get("distance") is not None:
                out.append(round(float(cell["distance"]), 2))
            else:
                out.append(None)
        return out


def extract_shape_and_miles(route_resp: dict | None) -> tuple[str | None, float | None]:
    """Pull the concatenated leg shape (encoded polyline, precision 6) and total miles from a
    Valhalla /route response. Returns (None, None) when the response has no usable trip."""
    if not isinstance(route_resp, dict):
        return None, None
    trip = route_resp.get("trip") if isinstance(route_resp.get("trip"), dict) else route_resp
    legs = trip.get("legs") if isinstance(trip, dict) else None
    if not isinstance(legs, list) or not legs:
        return None, None
    # One leg per location pair; a single shape string per leg. For a 2-point route there is one
    # leg — return its shape directly (concatenating multi-leg shapes would duplicate the seam).
    shape = legs[0].get("shape") if isinstance(legs[0], dict) else None
    summary = trip.get("summary") if isinstance(trip.get("summary"), dict) else {}
    miles = summary.get("length")
    return shape, (round(float(miles), 1) if miles is not None else None)
