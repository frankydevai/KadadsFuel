"""
OpenStreetMap road routing via OpenRouteService (ORS).

Samsara has no directions engine, so the min-cost-path fuel planner gets its
road geometry here. ORS is hosted, runs on OSM data, and authenticates with a
free API key in settings.ORS_API_KEY. One Matrix call per lane returns the
pairwise road distances; core.lane_plan turns those into each stop's
mile_marker (progress along the lane) and detour_miles (one-way off-route
deadhead) via fuel_plan.route_position.

Profile is driving-hgv (heavy goods vehicle) so distances reflect truck-legal
roads, not car shortcuts.

On 429 we back off and retry up to _MAX_RETRIES; every other non-2xx, transport
error, or non-JSON body raises RoutingError. Never returns partial data.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from dieselup.config import settings

log = logging.getLogger(__name__)


class RoutingError(RuntimeError):
    """Any ORS failure: missing token, non-2xx, transport error, or bad JSON."""


class OpenRouteServiceClient:
    """Thin async wrapper around the ORS Matrix API (distances only)."""

    _BASE_URL = "https://api.openrouteservice.org"
    _PROFILE = "driving-hgv"
    _MAX_RETRIES = 3
    _INITIAL_BACKOFF_SECONDS = 1.0

    def __init__(self, *, timeout: float = 30.0) -> None:
        token = settings.ORS_API_KEY
        if not token:
            raise RoutingError(
                "ORS_API_KEY is not configured — routed fuel planner is disabled"
            )
        # ORS expects the raw key in Authorization (no 'Bearer ' prefix).
        self._client = httpx.AsyncClient(
            base_url=self._BASE_URL,
            headers={
                "Authorization": token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=timeout,
        )

    async def __aenter__(self) -> "OpenRouteServiceClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def distance_matrix_miles(
        self, points: list[tuple[float, float]]
    ) -> list[list[float | None]]:
        """Pairwise road distances in miles for (lat, lng) points.

        ORS takes coordinates as [lng, lat]. Unreachable pairs come back as
        null and are surfaced as None for the caller to drop.
        """
        if len(points) < 2:
            raise RoutingError("distance matrix needs at least two points")
        body = {
            "locations": [[lng, lat] for (lat, lng) in points],
            "metrics": ["distance"],
            "units": "mi",
        }
        payload = await self._post(f"/v2/matrix/{self._PROFILE}", body)
        distances = payload.get("distances")
        if not isinstance(distances, list):
            raise RoutingError("ORS matrix response missing 'distances'")
        return distances

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        backoff = self._INITIAL_BACKOFF_SECONDS
        for attempt in range(self._MAX_RETRIES + 1):
            try:
                resp = await self._client.post(path, json=body)
            except httpx.HTTPError as exc:
                raise RoutingError(f"ORS request to {path} failed: {exc}") from exc

            if resp.status_code == 429:
                if attempt >= self._MAX_RETRIES:
                    raise RoutingError(
                        f"ORS rate limit (429) for {path} after {self._MAX_RETRIES} retries"
                    )
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            if resp.status_code in (401, 403):
                raise RoutingError(
                    f"ORS auth failed ({resp.status_code}) for {path} — check ORS_API_KEY"
                )
            if resp.status_code >= 400:
                body_text = resp.text[:200].replace("\n", " ")
                raise RoutingError(f"ORS returned {resp.status_code} for {path}: {body_text}")

            try:
                return resp.json()
            except ValueError as exc:
                raise RoutingError(f"ORS response for {path} was not JSON") from exc

        raise RoutingError(f"ORS request to {path} failed after retries")
