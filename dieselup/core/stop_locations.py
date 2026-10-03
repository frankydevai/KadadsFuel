"""Match customer addresses without treating a route point as visit evidence.

Saved Samsara facilities take priority. Public street-address interpolation is
available only when the caller explicitly supplies a Census resolver.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
import re
import time
from typing import Any
import unicodedata
from uuid import UUID

from dieselup.core.trip_context import (
    TripContextError, _quickmanage_navigation_stops, completion_state,
)


class StopLocationError(RuntimeError):
    """A complete, current provider result could not be verified."""


def _truck_assignment_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if UUID(text).int == 0:
            return None
    except ValueError:
        pass
    return text


_STATES = dict(zip(
    ("ALABAMA|ALASKA|ARIZONA|ARKANSAS|CALIFORNIA|COLORADO|CONNECTICUT|"
     "DELAWARE|DISTRICT OF COLUMBIA|FLORIDA|GEORGIA|HAWAII|IDAHO|ILLINOIS|"
     "INDIANA|IOWA|KANSAS|KENTUCKY|LOUISIANA|MAINE|MARYLAND|MASSACHUSETTS|"
     "MICHIGAN|MINNESOTA|MISSISSIPPI|MISSOURI|MONTANA|NEBRASKA|NEVADA|"
     "NEW HAMPSHIRE|NEW JERSEY|NEW MEXICO|NEW YORK|NORTH CAROLINA|NORTH DAKOTA|"
     "OHIO|OKLAHOMA|OREGON|PENNSYLVANIA|RHODE ISLAND|SOUTH CAROLINA|SOUTH DAKOTA|"
     "TENNESSEE|TEXAS|UTAH|VERMONT|VIRGINIA|WASHINGTON|WEST VIRGINIA|WISCONSIN|"
     "WYOMING").split("|"),
    ("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN "
     "MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA "
     "WV WI WY").split(),
))
_STATE_CODES = frozenset(_STATES.values())
_SUFFIXES = {
    "STREET": "ST", "ROAD": "RD", "AVENUE": "AVE", "BOULEVARD": "BLVD",
    "DRIVE": "DR", "LANE": "LN", "COURT": "CT", "CIRCLE": "CIR",
    "HIGHWAY": "HWY", "PARKWAY": "PKWY", "PLACE": "PL", "TERRACE": "TER",
    "TRAIL": "TRL", "TURNPIKE": "TPKE", "EXPRESSWAY": "EXPY",
}
_DIRECTIONS = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
}
_UNITS = {"SUITE": "STE", "APARTMENT": "APT"}


def _tokens(value: Any) -> tuple[str, ...]:
    value = unicodedata.normalize("NFKC", str(value or "")).upper()
    return tuple(re.findall(r"[A-Z0-9]+(?:[-/][A-Z0-9]+)*", value))


def _street(value: Any) -> str | None:
    tokens = list(_tokens(value))
    if len(tokens) < 2 or not re.fullmatch(r"\d+[A-Z]?(?:[-/]\d+[A-Z]?)?", tokens[0]):
        return None
    # Keep the house number, unit and every street word. Only known postal
    # spelling variants are interchangeable; no fuzzy city/ZIP-only matches.
    return " ".join(_UNITS.get(t, _DIRECTIONS.get(t, _SUFFIXES.get(t, t)))
                    for t in tokens)


def _state(value: Any) -> str | None:
    value = " ".join(_tokens(value))
    return value if value in _STATE_CODES else _STATES.get(value)


def _zip(value: Any) -> str | None:
    value = str(value or "").strip()
    return value[:5] if re.fullmatch(r"\d{5}(?:-\d{4})?", value) else None


@dataclass(frozen=True)
class _AddressKey:
    street: str
    city: str
    state: str
    postal: str

    @property
    def fingerprint(self) -> str:
        payload = (self.street, self.city, self.state, self.postal)
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def _stop_address(stop: dict[str, Any]) -> _AddressKey | None:
    street = _street(stop.get("address_line_1"))
    city = " ".join(_tokens(stop.get("city")))
    state, postal = _state(stop.get("state")), _zip(stop.get("zip_code"))
    if street and city and state and postal:
        return _AddressKey(street, city, state, postal)
    return None


def _formatted_address(value: Any) -> _AddressKey | None:
    if not isinstance(value, str):
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if parts and " ".join(_tokens(parts[-1])) in {"USA", "US", "U S A", "UNITED STATES"}:
        parts.pop()
    if len(parts) == 4 and _zip(parts[-1]):
        parts[-2:] = [parts[-2] + " " + parts[-1]]
    if len(parts) != 3:
        return None
    match = re.fullmatch(r"(.+?)\s+(\d{5}(?:-\d{4})?)", parts[2])
    if not match:
        return None
    return _stop_address({"address_line_1": parts[0], "city": parts[1],
                          "state": match[1], "zip_code": match[2]})


def _coordinates(lat: Any, lng: Any) -> tuple[float, float] | None:
    if isinstance(lat, bool) or isinstance(lng, bool):
        return None
    try:
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError, OverflowError):
        return None
    if (math.isfinite(lat) and math.isfinite(lng) and -90 <= lat <= 90
            and -180 <= lng <= 180 and (lat, lng) != (0, 0)):
        return lat, lng
    return None


def _facility_coordinates(address: dict[str, Any]) -> tuple[float, float] | None:
    lat, lng = address.get("latitude"), address.get("longitude")
    if lat is not None or lng is not None:
        # An invalid or incomplete explicit override must not be hidden by a
        # different circle point. Samsara top-level overrides take precedence.
        return _coordinates(lat, lng)
    geofence = address.get("geofence")
    circle = geofence.get("circle") if isinstance(geofence, dict) else None
    if isinstance(circle, dict):
        return _coordinates(circle.get("latitude"), circle.get("longitude"))
    return None


def match_samsara_address(stop: dict[str, Any], addresses: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return a unique full-address route point; never manufacture a gate."""
    key = _stop_address(stop)
    if key is None:
        return None
    matches = [a for a in addresses if isinstance(a, dict)
               and _formatted_address(a.get("formattedAddress")) == key]
    if len(matches) != 1:
        return None
    address = matches[0]
    coords = _facility_coordinates(address)
    address_id = address.get("id")
    if coords is None or not isinstance(address_id, str) or not address_id.strip():
        return None
    return {"latitude": coords[0], "longitude": coords[1],
            "coordinate_source": "samsara_address", "address_id": address_id,
            "address_fingerprint": key.fingerprint,
            "coordinate_verified_for": "fuel_route",
            "coordinate_accuracy": "saved_facility_point"}


def parse_census_address(stop: dict[str, Any], payload: Any) -> dict[str, Any] | None:
    """Accept one complete US street match as an address-range estimate only."""
    key = _stop_address(stop)
    result = payload.get("result") if isinstance(payload, dict) else None
    matches = result.get("addressMatches") if isinstance(result, dict) else None
    if key is None or not isinstance(matches, list) or len(matches) != 1:
        return None
    match = matches[0]
    if not isinstance(match, dict) or _formatted_address(match.get("matchedAddress")) != key:
        return None
    components = match.get("addressComponents")
    coordinates = match.get("coordinates")
    if not isinstance(components, dict) or not isinstance(coordinates, dict):
        return None
    # Check the structured response too; a partial match or address echoed in
    # matchedAddress alone must not upgrade a ZIP centroid to a street point.
    house = key.street.split()[0]
    street = " ".join(str(p) for p in (
        house, components.get("preDirection"), components.get("preQualifier"),
        components.get("preType"), components.get("streetName"),
        components.get("suffixType"), components.get("suffixDirection"),
        components.get("suffixQualifier"),
    ) if p)
    if (not components.get("streetName") or _stop_address({
            "address_line_1": street, "city": components.get("city"),
            "state": components.get("state"), "zip_code": components.get("zip"),
        }) != key):
        return None
    # Census coordinates are x=longitude/y=latitude. These coarse national
    # envelopes reject impossible US points, without claiming rooftop accuracy.
    coords = _coordinates(coordinates.get("y"), coordinates.get("x"))
    if coords is None:
        return None
    lat, lng = coords
    if key.state == "AK":
        in_us = 51 <= lat <= 72 and (-180 <= lng <= -129 or 172 <= lng <= 180)
    elif key.state == "HI":
        in_us = 18 <= lat <= 23 and -161 <= lng <= -154
    else:
        in_us = 24 <= lat <= 50 and -125 <= lng <= -66
    if not in_us:
        return None
    return {"latitude": lat, "longitude": lng,
            "coordinate_source": "census_address_range",
            "coordinate_verified_for": "fuel_route",
            "coordinate_accuracy": "address_range_interpolation",
            "address_fingerprint": key.fingerprint,
            "matched_address": match["matchedAddress"],
            "coordinate_provider": "US Census Bureau",
            "coordinate_benchmark": "Public_AR_Current",
            "coordinate_provider_endpoint": CensusStreetAddressResolver.ENDPOINT}


class CensusStreetAddressResolver:
    """Optional, bounded, no-key street lookup; never enabled implicitly."""

    ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/address"

    def __init__(self, client: Any | None = None, *, cache_seconds: float = 3600,
                 max_cache_entries: int = 256):
        if cache_seconds <= 0 or max_cache_entries < 1:
            raise ValueError("Street address cache limits must be positive")
        self._client = client
        self._owns_client = client is None
        self._cache_seconds = cache_seconds
        self._max_cache_entries = max_cache_entries
        self._cache: OrderedDict[_AddressKey, tuple[float, dict[str, Any] | None]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def aclose(self):
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def resolve_stop(self, stop: dict[str, Any]) -> dict[str, Any] | None:
        key = _stop_address(stop)
        if key is None:
            return None
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None and time.monotonic() - cached[0] < self._cache_seconds:
                self._cache.move_to_end(key)
                return deepcopy(cached[1])
            if self._client is None:
                import httpx
                self._client = httpx.AsyncClient(timeout=8.0)
            params = {"street": str(stop["address_line_1"]).strip(), "city": str(stop["city"]).strip(),
                      "state": key.state, "zip": key.postal,
                      "benchmark": "Public_AR_Current", "format": "json"}
            try:
                response = await self._client.get(self.ENDPOINT, params=params, timeout=8.0)
                response.raise_for_status()
                resolved = parse_census_address(stop, response.json())
            except Exception:
                raise StopLocationError("Street address lookup is unavailable") from None
            self._cache[key] = (time.monotonic(), deepcopy(resolved))
            self._cache.move_to_end(key)
            while len(self._cache) > self._max_cache_entries:
                self._cache.popitem(last=False)
            return resolved


class StopLocationResolver:
    """Use one complete cached address index for an authorized planning sweep."""

    def __init__(self, samsara: Any, *, cache_seconds: float = 300, max_pages: int = 100):
        if cache_seconds <= 0 or max_pages < 1:
            raise ValueError("Saved facility cache limits must be positive")
        self._samsara = samsara
        self._cache_seconds = cache_seconds
        self._max_pages = max_pages
        self._addresses: list[dict[str, Any]] | None = None
        self._cached_at = 0.0
        self._lock = asyncio.Lock()

    async def addresses(self) -> list[dict[str, Any]]:
        async with self._lock:
            if self._addresses is not None and time.monotonic() - self._cached_at < self._cache_seconds:
                return deepcopy(self._addresses)
            addresses: list[dict[str, Any]] = []
            cursor = None
            cursors: set[str] = set()
            try:
                for _ in range(self._max_pages):
                    params = {"limit": 512}
                    if cursor:
                        params["after"] = cursor
                    payload = await self._samsara._get_json("/addresses", params=params)
                    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                        raise StopLocationError("Saved facility address page is invalid")
                    if any(not isinstance(item, dict) for item in payload["data"]):
                        raise StopLocationError("Saved facility address page is invalid")
                    addresses.extend(payload["data"])
                    pagination = payload.get("pagination")
                    if not isinstance(pagination, dict):
                        raise StopLocationError("Saved facility address pagination is missing")
                    if pagination.get("hasNextPage") is False:
                        self._addresses = deepcopy(addresses)
                        self._cached_at = time.monotonic()
                        return deepcopy(addresses)
                    cursor = pagination.get("endCursor")
                    if (pagination.get("hasNextPage") is not True or not isinstance(cursor, str)
                            or not cursor or cursor in cursors):
                        raise StopLocationError("Saved facility address pagination is incomplete")
                    cursors.add(cursor)
                raise StopLocationError("Saved facility address page limit was reached")
            except StopLocationError:
                raise
            except Exception:
                # No upstream URLs, address values or raw HTTP errors in logs.
                raise StopLocationError("Saved facility addresses are unavailable") from None

    async def enrich_order(self, order: dict[str, Any], *,
                           street_address_resolver: Any | None = None) -> dict[str, Any]:
        """Copy and enrich only the current QuickManage phase's route targets.

        Supplying a street_address_resolver is an explicit opt-in; it is never
        constructed or enabled here. Operational status and completion evidence
        stay unchanged, including when a location cannot be resolved.
        """
        from dieselup.core.operating_scope import allows, unit_key

        result = deepcopy(order)
        unit = order.get("truck_unit_number")
        if (order.get("tms_provider") != "quickmanage" or unit_key(unit) is None
                or not allows(unit) or order.get("assignment_conflict")):
            return result
        stops = result.get("stops")
        if not isinstance(stops, list) or any(not isinstance(s, dict) for s in stops):
            return result
        try:
            required, _ = _quickmanage_navigation_stops(
                result, stops, [completion_state(s) for s in stops])
        except TripContextError:
            return result
        if any(s.get("assigned_truck_unit") not in (None, "")
               and unit_key(s["assigned_truck_unit"]) != unit_key(unit) for s in required):
            return result
        trip_id = _truck_assignment_id(order.get("truck_id"))
        ids = {trip_id} if trip_id else set()
        order_ids = order.get("assigned_truck_ids") or []
        if not isinstance(order_ids, list):
            return result
        ids.update(truck_id for value in order_ids if (truck_id := _truck_assignment_id(value)))
        for stop in required:
            values = stop.get("assigned_truck_ids") or []
            if not isinstance(values, list):
                return result
            stop_ids = {truck_id for value in values if (truck_id := _truck_assignment_id(value))}
            if truck_id := _truck_assignment_id(stop.get("assigned_truck_id")):
                stop_ids.add(truck_id)
            ids.update(stop_ids)
            # A bare stop ID has no proven link to the selected physical unit.
            # Normalization resolves it first; direct callers must provide the
            # same trip ID or an explicit matching stop unit before any lookup.
            if stop_ids and stop.get("assigned_truck_unit") in (None, "") and stop_ids != {trip_id}:
                return result
        if len(ids) > 1:
            return result
        unresolved = [s for s in required if not (
            s.get("coordinate_source") in {None, "exact"}
            and _coordinates(s.get("latitude", s.get("lat")),
                             s.get("longitude", s.get("lng"))) is not None)]
        if not unresolved:
            return result
        addresses = await self.addresses()
        for stop in unresolved:
            if stop.get("coordinate_source") in {"samsara_address", "census_address_range"}:
                # Rechecks must not retain a derived point that no longer has a
                # unique match to the currently required address.
                stop.update(latitude=None, longitude=None, coordinate_source="missing")
                for key in ("address_id", "address_fingerprint", "coordinate_verified_for",
                            "coordinate_accuracy", "matched_address", "coordinate_provider",
                            "coordinate_benchmark", "coordinate_provider_endpoint"):
                    stop.pop(key, None)
            match = match_samsara_address(stop, addresses)
            known_key = _stop_address(stop)
            known_facility = known_key is not None and any(
                _formatted_address(a.get("formattedAddress")) == known_key for a in addresses)
            if match is None and not known_facility and street_address_resolver is not None:
                match = await street_address_resolver.resolve_stop(stop)
            if match is not None:
                stop.update(match)
        return result
