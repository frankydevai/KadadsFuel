"""TomTom truck routing client for lane fuel planning.

The bot only needs road miles here, not browser map tiles. This client uses the
server-side TOMTOM_API_KEY and returns the same pairwise miles matrix contract
as the Valhalla/ORS clients so core.lane_plan can keep one planner.

For lane planning we only need:
  * shipper -> delivery
  * shipper -> each candidate
  * each candidate -> delivery

The client intentionally avoids a full all-pairs square matrix because long
lanes with dozens of stops can exceed TomTom's synchronous Matrix v2 item
limits. The returned matrix is sparse but satisfies the lane planner contract.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from dieselup.clients.routing import RoutingError
from dieselup.clients.valhalla import TRUCK_OPTS
from dieselup.config import settings

log = logging.getLogger(__name__)

_BASE_URL = "https://api.tomtom.com"
_METERS_PER_MILE = 1609.344
_MAX_RETRIES = 2
_MAX_STANDARD_ITEMS = 100


class TomTomClient:
    """Thin async wrapper around TomTom's truck routing matrix API."""

    def __init__(self, *, timeout: float = 20.0, http_client: httpx.AsyncClient | None = None) -> None:
        self._key = settings.TOMTOM_API_KEY.strip()
        if not self._key:
            raise RoutingError("TOMTOM_API_KEY is not configured")
        self._own_client = http_client is None
        self._client = http_client or httpx.AsyncClient(base_url=_BASE_URL, timeout=timeout)

    async def __aenter__(self) -> "TomTomClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def distance_matrix_miles(self, points: list[tuple[float, float]]) -> list[list[float | None]]:
        """Pairwise truck road distances in miles for (lat, lng) points."""
        if len(points) < 2:
            raise RoutingError("TomTom matrix needs at least two points")

        shipper = points[0]
        delivery = points[1]
        candidates = points[2:]

        # Matrix v2 limits are per origin*destination item. Two skinny matrix
        # calls give the lane planner every distance it needs with O(n) items
        # instead of O(n^2), which matters on 700+ stop networks.
        from_shipper = await self._one_to_many(shipper, [delivery, *candidates])
        to_delivery = (
            await self._many_to_one(candidates, delivery)
            if candidates
            else []
        )

        n = len(points)
        matrix: list[list[float | None]] = [[None for _ in range(n)] for _ in range(n)]
        for i in range(n):
            matrix[i][i] = 0.0

        if not from_shipper or not from_shipper[0]:
            raise RoutingError("TomTom matrix response missing shipper distances")
        matrix[0][1] = from_shipper[0][0]
        for candidate_idx, _point_value in enumerate(candidates, start=2):
            shipper_dest_idx = candidate_idx - 1
            if shipper_dest_idx < len(from_shipper[0]):
                matrix[0][candidate_idx] = from_shipper[0][shipper_dest_idx]
            delivery_row_idx = candidate_idx - 2
            if delivery_row_idx < len(to_delivery) and to_delivery[delivery_row_idx]:
                matrix[candidate_idx][1] = to_delivery[delivery_row_idx][0]
        return matrix

    async def _one_to_many(
        self,
        origin: tuple[float, float],
        destinations: list[tuple[float, float]],
    ) -> list[list[float | None]]:
        row: list[float | None] = []
        for chunk in _chunks(destinations, _MAX_STANDARD_ITEMS):
            part = await self._matrix(origins=[origin], destinations=chunk)
            row.extend(part[0] if part else [None] * len(chunk))
        return [row]

    async def _many_to_one(
        self,
        origins: list[tuple[float, float]],
        destination: tuple[float, float],
    ) -> list[list[float | None]]:
        rows: list[list[float | None]] = []
        for chunk in _chunks(origins, _MAX_STANDARD_ITEMS):
            rows.extend(await self._matrix(origins=chunk, destinations=[destination]))
        return rows

    async def _matrix(
        self,
        *,
        origins: list[tuple[float, float]],
        destinations: list[tuple[float, float]],
    ) -> list[list[float | None]]:
        body = {
            "origins": [_point(lat, lng) for lat, lng in origins],
            "destinations": [_point(lat, lng) for lat, lng in destinations],
            "options": {
                "departAt": "any",
                "traffic": "historical",
                "routeType": "fastest",
                "travelMode": "truck",
                "vehicleCommercial": True,
                "vehicleMaxSpeed": 75,
                "vehicleWeight": int(TRUCK_OPTS["weight"] * 1000),
                "vehicleAxleWeight": int(TRUCK_OPTS["axle_load"] * 1000),
                "vehicleLength": TRUCK_OPTS["length"],
                "vehicleWidth": TRUCK_OPTS["width"],
                "vehicleHeight": TRUCK_OPTS["height"],
            },
        }
        payload = await self._post("/routing/matrix/2", body)
        return _parse_matrix(payload, len(origins), len(destinations))

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        params = {"key": self._key}
        backoff = 1.0
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(path, params=params, json=body)
            except httpx.HTTPError as exc:
                raise RoutingError(f"TomTom request to {path} failed: {exc}") from exc

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt >= _MAX_RETRIES:
                    raise RoutingError(f"TomTom returned {resp.status_code} for {path}")
                retry_after = _retry_after_seconds(resp.headers.get("Retry-After"))
                await asyncio.sleep(retry_after if retry_after is not None else backoff)
                backoff *= 2
                continue
            if resp.status_code in (401, 403):
                raise RoutingError(f"TomTom auth failed ({resp.status_code}) for {path}")
            if resp.status_code >= 400:
                body_text = resp.text[:200].replace("\n", " ")
                raise RoutingError(f"TomTom returned {resp.status_code} for {path}: {body_text}")

            try:
                return resp.json()
            except ValueError as exc:
                raise RoutingError(f"TomTom response for {path} was not JSON") from exc

        raise RoutingError(f"TomTom request to {path} failed after retries")


def _point(lat: float, lng: float) -> dict[str, dict[str, float]]:
    return {"point": {"latitude": float(lat), "longitude": float(lng)}}


def _chunks(points: list[tuple[float, float]], size: int) -> list[list[tuple[float, float]]]:
    return [points[i:i + size] for i in range(0, len(points), size)]


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, min(float(value), 10.0))
    except ValueError:
        return None


def _parse_matrix(
    data: dict[str, Any],
    origin_count: int,
    destination_count: int,
) -> list[list[float | None]]:
    """Parse TomTom matrix responses.

    Supports the flattened `data[]` shape used by Matrix Routing v2 and the
    older nested `matrix[][]` style. Unknown/unreachable cells remain None so
    lane_plan can drop only the impossible stops and keep planning.
    """
    matrix: list[list[float | None]] = [
        [None for _ in range(destination_count)] for _ in range(origin_count)
    ]

    rows = data.get("matrix")
    if isinstance(rows, list):
        for i, row in enumerate(rows[:origin_count]):
            if not isinstance(row, list):
                continue
            for j, cell in enumerate(row[:destination_count]):
                matrix[i][j] = _cell_miles(cell)
        return matrix

    flat = data.get("data") or data.get("results")
    if isinstance(flat, list):
        for index, cell in enumerate(flat):
            if not isinstance(cell, dict):
                continue
            i = int(cell.get("originIndex", index // destination_count))
            j = int(cell.get("destinationIndex", index % destination_count))
            if 0 <= i < origin_count and 0 <= j < destination_count:
                matrix[i][j] = _cell_miles(cell)
        return matrix

    raise RoutingError("TomTom matrix response missing data")


def _cell_miles(cell: Any) -> float | None:
    if not isinstance(cell, dict):
        return None
    status = cell.get("statusCode") or cell.get("status")
    if status not in (None, 200, "OK", "ok"):
        return None
    summary = cell.get("routeSummary") or cell.get("summary") or cell
    meters = summary.get("lengthInMeters")
    if meters is None:
        meters = summary.get("distanceInMeters")
    if meters is None:
        return None
    try:
        return round(float(meters) / _METERS_PER_MILE, 2)
    except (TypeError, ValueError):
        return None
