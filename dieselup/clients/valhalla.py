"""Thin async wrapper around pyvalhalla's Actor.

Valhalla is a routing dependency, not a bot-critical dependency: every public method logs and
returns None on failure so a bad tile/config/route never crashes the poll loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from dieselup.clients.routing import RoutingError
from dieselup.config import settings
from dieselup import metrics

log = logging.getLogger(__name__)


TRUCK_OPTS: dict[str, Any] = {
    "height": 4.1148,
    "width": 2.5908,
    "length": 22.86,
    "weight": 36.2874,
    "axle_load": 9.07185,
    "hazmat": False,
}


@dataclass(frozen=True)
class SnappedStop:
    lat: float
    lon: float
    road_name: str | None = None


@dataclass(frozen=True)
class MatrixCell:
    distance_miles: float
    duration_seconds: int | None


class ValhallaClient:
    def __init__(self, config_path: str | None = None, actor=None, http_client=None) -> None:
        self._actor = actor
        self._config_path = config_path or os.environ.get("VALHALLA_CONFIG")
        self._base_url = settings.VALHALLA_URL.strip().rstrip("/")
        self._owns_http = http_client is None
        self._http = http_client
        if self._base_url and not settings.VALHALLA_API_SECRET:
            raise RoutingError("VALHALLA_API_SECRET is required with VALHALLA_URL")

    @property
    def available(self) -> bool:
        return self._actor is not None or bool(self._config_path) or bool(self._base_url)

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()

    def _ensure_http(self):
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                headers={
                    "X-Valhalla-Key": settings.VALHALLA_API_SECRET,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(settings.VALHALLA_TIMEOUT_SECONDS),
            )
        return self._http

    def _ensure_actor(self):
        if self._actor is not None:
            return self._actor
        if not self._config_path:
            raise RuntimeError("VALHALLA_CONFIG is not set")
        try:
            from valhalla import Actor  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on optional system package
            raise RuntimeError("pyvalhalla is not installed") from exc
        config = json.loads(Path(self._config_path).read_text())
        self._actor = Actor(config)
        return self._actor

    async def snap_stop(self, lat: float, lon: float) -> SnappedStop | None:
        payload = {
            "locations": [{"lat": float(lat), "lon": float(lon), "radius": 200}],
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
        }
        data = await self._actor_call("locate", payload)
        if data is None:
            return None
        snapped = _find_correlated_location(data)
        if snapped is None:
            log.warning("Valhalla could not snap stop at %.6f, %.6f", lat, lon)
            return None
        return snapped

    async def matrix(self, origin: tuple[float, float], targets: list[tuple[float, float]]) -> list[MatrixCell | None] | None:
        if not targets:
            return []
        payload = {
            "sources": [{"lat": float(origin[0]), "lon": float(origin[1])}],
            "targets": [{"lat": float(lat), "lon": float(lon)} for lat, lon in targets],
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
            "units": "miles",
            "directions_options": {"units": "miles"},
        }
        data = await self._actor_call("matrix", payload)
        if data is None:
            return None
        rows = data.get("sources_to_targets") if isinstance(data, dict) else None
        if not rows:
            return None
        cells = rows[0] if isinstance(rows[0], list) else rows
        if len(cells) != len(targets):
            metrics.gauge("valhalla_last_failure_mono", time.monotonic())
            raise RoutingError("Valhalla matrix response was incomplete")
        out: list[MatrixCell | None] = []
        for cell in cells:
            if not isinstance(cell, dict) or cell.get("distance") is None:
                out.append(None)
                continue
            out.append(MatrixCell(
                distance_miles=_nonnegative_number(cell["distance"], "distance"),
                duration_seconds=(int(_nonnegative_number(cell["time"], "time")) if cell.get("time") is not None else None),
            ))
        metrics.gauge("valhalla_last_success_mono", time.monotonic())
        return out

    async def distance_matrix_miles(
        self, points: list[tuple[float, float]]
    ) -> list[list[float | None]] | None:
        """Pairwise truck-legal road distances in miles for (lat, lon) points."""
        if len(points) < 2:
            return []
        if len(points) > settings.VALHALLA_MAX_MATRIX_LOCATIONS:
            raise RoutingError(
                f"Valhalla matrix has {len(points)} locations; maximum is "
                f"{settings.VALHALLA_MAX_MATRIX_LOCATIONS}"
            )
        locations = [{"lat": float(lat), "lon": float(lon)} for lat, lon in points]
        payload = {
            "sources": locations,
            "targets": locations,
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
            "units": "miles",
            "directions_options": {"units": "miles"},
        }
        data = await self._actor_call("matrix", payload)
        if data is None:
            return None
        rows = data.get("sources_to_targets") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            metrics.gauge("valhalla_last_failure_mono", time.monotonic())
            return None

        expected = len(points)
        if len(rows) != expected or any(
            not isinstance(row, list) or len(row) != expected for row in rows
        ):
            metrics.gauge("valhalla_last_failure_mono", time.monotonic())
            raise RoutingError(
                "Valhalla matrix response was incomplete: "
                f"expected {expected}x{expected} cells"
            )

        matrix: list[list[float | None]] = []
        for row in rows:
            if not isinstance(row, list):
                return None
            out_row: list[float | None] = []
            for cell in row:
                if not isinstance(cell, dict) or cell.get("distance") is None:
                    out_row.append(None)
                else:
                    out_row.append(_nonnegative_number(cell["distance"], "distance"))
            matrix.append(out_row)
        metrics.gauge("valhalla_last_success_mono", time.monotonic())
        return matrix

    async def route(self, locations: list[dict], alternates: int = 0) -> dict | None:
        payload = {
            "locations": locations,
            "costing": "truck",
            "costing_options": {"truck": TRUCK_OPTS},
            "directions_options": {"units": "miles"},
            "alternates": max(0, int(alternates or 0)),
        }
        data = await self._actor_call("route", payload)
        if isinstance(data, dict) and isinstance(data.get("trip"), dict):
            metrics.gauge("valhalla_last_success_mono", time.monotonic())
        return data

    async def distance_matrix_miles_batched(self, points: list[tuple[float, float]]) -> list[list[float | None]]:
        """Evaluate every point without dropping stations to fit a request cap.

        Small source blocks bound each request. Any failed/incomplete block
        rejects the entire calculation; partial coverage never becomes advice.
        """
        limit = settings.VALHALLA_MAX_MATRIX_LOCATIONS
        if limit < 2:
            raise RoutingError("Matrix limit must allow at least two locations")
        source_size = min(8, max(1, limit // 2))
        target_size = max(1, limit - source_size)
        n = len(points)
        result: list[list[float | None]] = [[None] * n for _ in points]
        for start in range(0, n, source_size):
            sources = points[start:start + source_size]
            for end in range(0, n, target_size):
                targets = points[end:end + target_size]
                payload = {"sources": [{"lat": p[0], "lon": p[1]} for p in sources],
                           "targets": [{"lat": p[0], "lon": p[1]} for p in targets],
                           "costing": "truck", "costing_options": {"truck": TRUCK_OPTS},
                           "units": "miles", "directions_options": {"units": "miles"}}
                data = await self._actor_call("matrix", payload)
                rows = data.get("sources_to_targets") if isinstance(data, dict) else None
                if not isinstance(rows, list) or len(rows) != len(sources) or any(
                    not isinstance(row, list) or len(row) != len(targets) for row in rows
                ):
                    raise RoutingError("Valhalla batched matrix was incomplete")
                for i, row in enumerate(rows):
                    for j, cell in enumerate(row):
                        if not isinstance(cell, dict):
                            raise RoutingError("Valhalla batched matrix contains an invalid cell")
                        result[start + i][end + j] = (
                            _nonnegative_number(cell["distance"], "distance")
                            if cell.get("distance") is not None else None
                        )
        metrics.gauge("valhalla_last_success_mono", time.monotonic())
        return result

    async def _actor_call(self, method_name: str, payload: dict) -> dict | list | None:
        if self._base_url:
            path = {
                "matrix": "/sources_to_targets",
                "route": "/route",
                "status": "/status",
            }.get(method_name)
            if path is None:
                log.warning("Valhalla endpoint %s is intentionally not exposed", method_name)
                return None
            started = time.monotonic()
            for attempt in range(3):
                try:
                    response = await self._ensure_http().post(
                        path,
                        json=payload,
                        headers={"X-Valhalla-Key": settings.VALHALLA_API_SECRET},
                    )
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    if attempt < 2:
                        await asyncio.sleep(0.25 * (2**attempt))
                        continue
                    metrics.gauge("valhalla_last_failure_mono", time.monotonic())
                    raise RoutingError(
                        f"Valhalla transport failure: {type(exc).__name__}"
                    ) from exc
                if response.status_code in {429, 502, 503, 504} and attempt < 2:
                    await asyncio.sleep(0.25 * (2**attempt))
                    continue
                if response.status_code >= 400:
                    metrics.gauge("valhalla_last_failure_mono", time.monotonic())
                    raise RoutingError(f"Valhalla returned HTTP {response.status_code}")
                try:
                    value = response.json()
                except ValueError as exc:
                    metrics.gauge("valhalla_last_failure_mono", time.monotonic())
                    raise RoutingError("Valhalla returned invalid JSON") from exc
                log.info(
                    "routing_request provider=valhalla path=%s response_ms=%d matrix_size=%s",
                    path,
                    int((time.monotonic() - started) * 1000),
                    len(payload.get("sources", [])) or None,
                )
                return value
            raise RoutingError("Valhalla temporary failure after retries")
        try:
            actor = self._ensure_actor()
            method = getattr(actor, method_name)
            raw = await asyncio.to_thread(_call_actor_method, method, payload)
            if isinstance(raw, (dict, list)):
                return raw
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            return json.loads(raw)
        except Exception as exc:
            metrics.gauge("valhalla_last_failure_mono", time.monotonic())
            log.warning("Valhalla %s failed: %s", method_name, exc)
            return None


def _nonnegative_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        metrics.gauge("valhalla_last_failure_mono", time.monotonic())
        raise RoutingError(f"Valhalla returned invalid {field}") from exc
    if not math.isfinite(number) or number < 0:
        metrics.gauge("valhalla_last_failure_mono", time.monotonic())
        raise RoutingError(f"Valhalla returned invalid {field}")
    return number


def _call_actor_method(method, payload: dict):
    try:
        return method(payload)
    except TypeError:
        return method(json.dumps(payload))


def _find_correlated_location(data: Any) -> SnappedStop | None:
    if isinstance(data, list):
        for item in data:
            found = _find_correlated_location(item)
            if found:
                return found
        return None
    if not isinstance(data, dict):
        return None

    if data.get("correlated_lat") is not None and data.get("correlated_lon") is not None:
        return SnappedStop(
            lat=float(data["correlated_lat"]),
            lon=float(data["correlated_lon"]),
            road_name=_road_name(data),
        )

    for key in ("edges", "nodes", "locations", "input"):
        value = data.get(key)
        found = _find_correlated_location(value)
        if found:
            return found
    return None


def _road_name(edge: dict) -> str | None:
    names = edge.get("names")
    if isinstance(names, list) and names:
        return str(names[0])
    info = edge.get("edge_info")
    if isinstance(info, dict):
        names = info.get("names")
        if isinstance(names, list) and names:
            return str(names[0])
    return None
