"""Read-only QuickManage TMS client normalized to DieselUp's order contract.

Only documented read endpoints are used: token acquisition and `/x/*/search`.
The rest of the bot consumes the same normalized order shape regardless of TMS,
so the shipper-to-delivery planner and compliance engine remain provider-neutral.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import time
from typing import Any, AsyncIterator
from uuid import UUID

import httpx

from dieselup.circuit_breaker import CircuitOpenError, quickmanage_breaker
from dieselup.config import settings
from dieselup.core.trip_context import completion_state, quickmanage_route_phase


class QuickManageError(RuntimeError):
    """Raised when QuickManage auth, transport, or payload validation fails."""


class _RetryableResponse(RuntimeError):
    """Internal wrapper that lets 429/5xx responses count toward the breaker."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        super().__init__(f"QuickManage returned {response.status_code}")


class QuickManageClient:
    _MAX_RETRIES = 3
    _RATE_LIMIT_SECONDS = 3.0
    _shared_next_allowed_at = 0.0
    _shared_gate_lock: asyncio.Lock | None = None

    def __init__(self, *, timeout: float = 30.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.QUICKMANAGE_BASE_URL,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=timeout,
        )
        self._token: str | None = None
        self._token_expiry: datetime | None = None
        self._truck_units: dict[str, str | None] = {}
        self._location_samsara = None
        self._stop_locations = None
        self._street_addresses = None

    async def __aenter__(self) -> "QuickManageClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()
        if self._street_addresses is not None:
            await self._street_addresses.aclose()
        if self._location_samsara is not None:
            await self._location_samsara.close()

    async def iter_orders(
        self,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        *,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        page = 0
        seen_trip_ids: set[str] = set()
        while True:
            payload = await self._post(
                "/x/trips/search",
                {
                    "query": "",
                    "filters": filters if isinstance(filters, list) else [],
                    "page": page,
                    "page_size": 100,
                },
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            page_meta = data if isinstance(data, dict) else payload
            items = _extract_items(payload)
            if not isinstance(items, list):
                raise QuickManageError("QuickManage trips search returned invalid items")
            if not items:
                return
            new_items: list[dict[str, Any]] = []
            for item in items:
                trip_id = _clean(item.get("id"))
                if trip_id and trip_id in seen_trip_ids:
                    continue
                if trip_id:
                    seen_trip_ids.add(trip_id)
                new_items.append(item)
            # Observed QuickManage deployments can ignore the page number and
            # return the first page forever without count metadata. Stop as
            # soon as a page contributes no unseen trip IDs.
            if not new_items:
                raise QuickManageError("Trip search repeated a page; complete assignment coverage is unverified")
            for item in new_items:
                yield await self._normalize_trip(item)
            count = _as_int(page_meta.get("count")) if isinstance(page_meta, dict) else None
            page_size = (
                _as_int(page_meta.get("page_size")) if isinstance(page_meta, dict) else None
            ) or len(items)
            if count is not None and (page + 1) * page_size >= count:
                return
            if max_pages is not None and page + 1 >= max_pages:
                raise QuickManageError("Trip search reached the page limit before proving completion")
            page += 1

    async def get_order(self, order_id: int | str) -> dict[str, Any]:
        payload = await self._post(
            "/x/trips/search",
            {
                "query": "",
                "filters": [{"field": "id", "operator": "eq", "value": str(order_id)}],
                "page": 0,
                "page_size": 1,
            },
        )
        items = _extract_items(payload)
        matching = [item for item in items if isinstance(item, dict) and str(item.get("id")) == str(order_id)]
        if len(matching) != 1:
            raise QuickManageError(f"QuickManage trip {order_id!r} was not found")
        order = await self._normalize_trip(matching[0])
        return await self._resolve_stop_locations(order)

    async def _resolve_stop_locations(self, order: dict[str, Any]) -> dict[str, Any]:
        """Enrich selected current-trip addresses; fleet enumeration stays cheap."""
        from dieselup.core.operating_scope import allows, unit_key
        from dieselup.clients.samsara import SamsaraClient
        from dieselup.core.stop_locations import StopLocationResolver, CensusStreetAddressResolver

        if order.get("assignment_conflict") or not allows(order.get("truck_unit_number")) or order.get("route_phase") not in {
            "pickup_then_delivery", "delivery_only"
        }:
            return order
        unresolved = any(stop.get("coordinate_source") in {"zip_centroid", "missing"}
                         and stop.get("address_line_1") for stop in order.get("stops", [])
                         if order["route_phase"] != "delivery_only" or stop.get("type") == "delivery")
        if not unresolved:
            return order
        if self._stop_locations is None:
            self._location_samsara = SamsaraClient()
            self._stop_locations = StopLocationResolver(self._location_samsara)
        permitted_units = {unit_key(unit) for unit in settings.CENSUS_GEOCODING_TRUCK_UNITS.split(",") if unit.strip()}
        census_permitted = settings.CENSUS_GEOCODING_ENABLED and unit_key(order.get("truck_unit_number")) in permitted_units
        if census_permitted and self._street_addresses is None:
            self._street_addresses = CensusStreetAddressResolver()
        return await self._stop_locations.enrich_order(
            order, street_address_resolver=self._street_addresses if census_permitted else None)

    async def _normalize_trip(self, trip: dict[str, Any]) -> dict[str, Any]:
        raw_stops = trip.get("stops") or []
        stops: list[dict[str, Any]] = []
        trip_truck = trip.get("truck") if isinstance(trip.get("truck"), dict) else {}
        trip_units = _truck_units(trip_truck)
        trip_units = [unit for unit in (_clean(trip.get("truck_number")),
                                       _clean(trip.get("tractor_unit")), *trip_units) if unit]
        truck_unit = next(iter(trip_units), None)
        trip_ids = _truck_ids(trip.get("truck_id"), trip_truck.get("id"))
        assigned_ids = set(trip_ids)
        assigned_units = {_assignment_unit_key(unit) for unit in trip_units}
        id_units: dict[str, set[str]] = {}
        # An ID and unit supplied together identify one assignment. A bare ID
        # must be resolved even when another part of the trip has a unit number.
        for truck_id in trip_ids:
            id_units.setdefault(truck_id, set()).update(trip_units)
        for raw in raw_stops:
            if not isinstance(raw, dict):
                continue
            assigned_truck = raw.get("assigned_truck")
            assigned_truck = assigned_truck if isinstance(assigned_truck, dict) else {}
            ids = _truck_ids(raw.get("assigned_truck_id"), assigned_truck.get("id"))
            units = _truck_units(assigned_truck)
            assigned_ids.update(ids)
            assigned_units.update(_assignment_unit_key(unit) for unit in units)
            if len(ids) == 1:
                id_units.setdefault(ids[0], set()).update(units)
        unresolved_id = False
        # Two different TMS truck records cannot prove a single physical truck,
        # even if both happen to report the same display unit. Hold immediately.
        if len(assigned_ids) == 1:
            truck_id = next(iter(assigned_ids))
            if not id_units.get(truck_id):
                try:
                    resolved_unit = await self._truck_unit(truck_id)
                except QuickManageError:
                    resolved_unit = None
                if resolved_unit:
                    id_units[truck_id] = {resolved_unit}
                    assigned_units.add(_assignment_unit_key(resolved_unit))
                else:
                    unresolved_id = True
            known_units = id_units.get(truck_id, set())
            truck_unit = truck_unit or (sorted(known_units)[0] if known_units else None)
        trip_driver = trip.get("driver") if isinstance(trip.get("driver"), dict) else {}
        driver_name: str | None = _person_name(trip_driver) if trip_driver else None

        for index, raw in enumerate(raw_stops):
            if not isinstance(raw, dict):
                continue
            assigned_truck = raw.get("assigned_truck")
            assigned_truck = assigned_truck if isinstance(assigned_truck, dict) else {}
            stop_ids = _truck_ids(raw.get("assigned_truck_id"), assigned_truck.get("id"))
            truck_id = stop_ids[0] if len(stop_ids) == 1 else None
            stop_unit = next(iter(_truck_units(assigned_truck)), None)
            if not stop_unit and truck_id:
                known_units = id_units.get(truck_id, set())
                if len({_assignment_unit_key(unit) for unit in known_units}) == 1:
                    stop_unit = sorted(known_units)[0]
            truck_unit = truck_unit or stop_unit

            assigned_driver = _first_dict(raw.get("assigned_driver"), raw.get("assigned_drivers"))
            if isinstance(assigned_driver, dict):
                driver_name = driver_name or _person_name(assigned_driver)

            pickup = bool(raw.get("pickup", raw.get("is_pickup", index == 0)))
            address = raw.get("address") if isinstance(raw.get("address"), dict) else {}
            lat = _as_float(raw.get("lat", raw.get("latitude")))
            lng = _as_float(raw.get("lng", raw.get("longitude")))
            coordinate_source = "exact"
            if lat is None or lng is None:
                coordinate_source = "missing"
                # ZIP centers are rejected by the routing guard. Keep the
                # full address for approved facility/Census matching instead
                # of reading or downloading ZIP data on the event loop.
            stops.append(
                {
                    "id": _clean(raw.get("id")) or str(index),
                    "sequence": index,
                    "type": "pickup" if pickup else "delivery",
                    "pickup": pickup,
                    "completed": completion_state(raw),
                    "coordinate_source": coordinate_source,
                    "address_line_1": _clean(address.get("address_line_1")),
                    "assigned_truck_id": truck_id,
                    "assigned_truck_ids": stop_ids,
                    "assigned_truck_unit": stop_unit,
                    "latitude": lat,
                    "longitude": lng,
                    "city": _clean(address.get("city") or raw.get("city")),
                    "state": _clean(address.get("state") or raw.get("state")),
                    "zip_code": _clean(address.get("zip_code") or raw.get("zip_code")),
                    "company_name": _clean(raw.get("company_name")),
                }
            )

        raw_status = str(trip.get("status") or "").strip().lower()
        route_context = {
            "route_phase": quickmanage_route_phase(raw_status),
            "route_context_source": "quickmanage_status",
            "route_phase_status": raw_status,
        }
        status = raw_status
        status = {
            "upcoming": "dispatched",
            "dispatching": "dispatched",
            "completed": "completed",
            "cancelled": "cancelled",
            "canceled": "cancelled",
        }.get(status, status)
        trip_id = _clean(trip.get("id"))
        if not trip_id:
            raise QuickManageError("QuickManage trip is missing id")

        return {
            "id": trip_id,
            "tms_order_id": trip_id,
            "load_number": _clean(trip.get("ref_number") or trip.get("trip_num") or trip_id),
            "load_id": _clean(trip.get("ref_number") or trip_id),
            "status": status,
            "raw_status": raw_status,
            "assignment_conflict": len(assigned_ids) > 1 or len(assigned_units) > 1 or unresolved_id,
            "assignment_conflict_reason": (
                "multiple_truck_ids" if len(assigned_ids) > 1 else
                "multiple_truck_units" if len(assigned_units) > 1 else
                "unresolved_truck_id" if unresolved_id else None),
            "truck_id": next(iter(assigned_ids)) if len(assigned_ids) == 1 else None,
            "assigned_truck_ids": sorted(assigned_ids),
            "truck_unit_number": truck_unit,
            "driver_full_name": driver_name,
            "stops": stops,
            "tms_provider": "quickmanage",
            **route_context,
            "trip_metadata": route_context.copy(),
        }

    async def _truck_unit(self, truck_id: str) -> str | None:
        if truck_id in self._truck_units:
            return self._truck_units[truck_id]
        payload = await self._post(
            "/x/trucks/search",
            {
                "query": "",
                "filters": [{"field": "id", "operator": "eq", "value": truck_id}],
                "page": 0,
                "page_size": 1,
            },
        )
        items = _extract_items(payload)
        items = [item for item in items if isinstance(item, dict) and str(item.get("id")) == truck_id]
        unit = None
        if len(items) == 1:
            units = _truck_units(items[0])
            if len({_assignment_unit_key(value) for value in units}) == 1:
                unit = units[0]
        self._truck_units[truck_id] = unit
        return unit

    async def _ensure_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and self._token_expiry and now + timedelta(seconds=60) < self._token_expiry:
            return self._token
        try:
            response = await self._request(
                "/auth/token",
                json={
                    "client_id": settings.QUICKMANAGE_CLIENT_ID,
                    "client_secret": settings.QUICKMANAGE_CLIENT_SECRET,
                },
            )
            if not response.is_success:
                response = await self._request(
                    "/auth/token",
                    data={
                        "client_id": settings.QUICKMANAGE_CLIENT_ID,
                        "client_secret": settings.QUICKMANAGE_CLIENT_SECRET,
                    },
                )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
            token = data.get("access_token") or data.get("token")
            if not token:
                raise KeyError("access_token")
            self._token = str(token)
            raw_expiry = str(data.get("expire") or "")
            self._token_expiry = (
                datetime.fromisoformat(raw_expiry.replace("Z", "+00:00"))
                if raw_expiry
                else now + timedelta(seconds=_as_int(data.get("expires_in")) or 3600)
            )
            return self._token
        except CircuitOpenError as exc:
            raise QuickManageError("QuickManage circuit is open; retry after cooldown") from exc
        except Exception as exc:  # http and malformed auth payloads share one safe error
            raise QuickManageError(f"QuickManage authentication failed: {exc}") from exc

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        backoff = 1.0
        for attempt in range(self._MAX_RETRIES + 1):
            token = await self._ensure_token()
            try:
                await self._rate_limit()
                response = await self._request(
                    path,
                    json=body,
                    headers={"Authorization": f"Bearer {token}"},
                )
            except _RetryableResponse as exc:
                response = exc.response
                if attempt >= self._MAX_RETRIES:
                    raise QuickManageError(
                        f"QuickManage returned {response.status_code} for {path} after retries"
                    ) from exc
                retry_after = response.headers.get("Retry-After")
                await asyncio.sleep(float(retry_after) if retry_after else backoff)
                backoff *= 2
                continue
            except CircuitOpenError as exc:
                raise QuickManageError("QuickManage circuit is open; retry after cooldown") from exc
            except httpx.HTTPError as exc:
                if attempt >= self._MAX_RETRIES:
                    raise QuickManageError(f"QuickManage request failed: {exc}") from exc
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            if response.status_code == 401 and attempt == 0:
                self._token = None
                self._token_expiry = None
                continue
            if response.status_code >= 400:
                raise QuickManageError(
                    f"QuickManage returned {response.status_code} for {path}: "
                    f"{response.text[:200]}"
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise QuickManageError(f"QuickManage response for {path} was not JSON") from exc
            if not isinstance(payload, dict):
                raise QuickManageError(f"QuickManage response for {path} had invalid shape")
            return payload
        raise QuickManageError(f"QuickManage request failed for {path}")

    async def _request(self, path: str, **kwargs: Any) -> httpx.Response:
        async def _send() -> httpx.Response:
            response = await self._client.post(path, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                raise _RetryableResponse(response)
            return response

        return await quickmanage_breaker.call(_send)

    async def _rate_limit(self) -> None:
        cls = type(self)
        if cls._shared_gate_lock is None:
            cls._shared_gate_lock = asyncio.Lock()
        async with cls._shared_gate_lock:
            wait = cls._shared_next_allowed_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            cls._shared_next_allowed_at = time.monotonic() + cls._RATE_LIMIT_SECONDS


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _truck_ids(*values: Any) -> list[str]:
    ids = set()
    for value in values:
        text = _clean(value)
        if text is None:
            continue
        try:
            if UUID(text).int == 0:
                # QuickManage uses the nil UUID for an unassigned stop. It is
                # absence of an assignment, not a second physical truck.
                continue
        except ValueError:
            pass
        ids.add(text)
    return sorted(ids)


def _truck_units(truck: dict[str, Any]) -> list[str]:
    return [unit for key in ("unit_number", "truck_unit_number", "unit", "number")
            if (unit := _clean(truck.get(key)))]


def _assignment_unit_key(unit: str) -> str:
    return unit.strip().upper().lstrip("0") or "0"


def _as_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _person_name(value: dict[str, Any]) -> str | None:
    full = _clean(value.get("full_name") or value.get("name"))
    if full:
        return full
    return _clean(" ".join(
        str(value.get(key) or "").strip() for key in ("first_name", "last_name")
    ))


def _first_dict(*values: Any) -> dict[str, Any]:
    """Return a dict from either a single object or the first dict in a list."""
    for value in values:
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    return item
    return {}


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    """Normalize every documented/observed QuickManage search envelope."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("items", "trips", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    for key in ("items", "trips", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _geocode_zip(value: Any) -> tuple[float, float] | None:
    """Best-effort offline ZIP centroid for QuickManage stops missing lat/lng."""
    text = _clean(value)
    if not text:
        return None
    code = text.split("-", 1)[0][:5]
    if not code.isdigit():
        return None
    try:
        import pgeocode

        result = pgeocode.Nominatim("us").query_postal_code(code)
        lat, lng = float(result.latitude), float(result.longitude)
        if lat != lat or lng != lng:  # NaN without importing math
            return None
        return lat, lng
    except Exception:
        return None
