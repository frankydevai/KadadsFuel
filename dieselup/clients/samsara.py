"""
Async client for the Samsara Fleet API — bulk-fetch + per-instance cache.

Three bulk endpoints power the optimizer:
  * GET /fleet/vehicles/locations            — every vehicle's latest GPS + timestamp
  * GET /fleet/vehicles/stats/feed?types=fuelPercents
                                              — every vehicle's latest fuel %
  * GET /fleet/reports/vehicles/fuel-energy  — 7-day rolling MPG report
    (requires "Fuel & Energy read" token scope; failures fall back to
    FLEET_DEFAULT_MPG via mpg_rolling=None)

Bulk telemetry is cached for 30 seconds to share calls across trucks while
refreshing during a long sweep. Rolling MPG is cached for the client lifetime.

Public surface unchanged so callers don't move:
  * get_vehicle_location(vehicle_id) → VehicleLocation
  * get_vehicle_fuel(vehicle_id)    → float (gallons)
  * get_vehicle_stats(vehicle_id)   → VehicleStats (gps + fuel + mpg)
  * list_vehicles()                  → list[VehicleSummary]
  * find_vehicles_by_unit(unit)      → list[VehicleSummary]  — matches on all
       index keys (first digits + SUBUNIT parenthetical), like load_sync
  * extract_unit_digits(text)        → str | None  — module-level helper

GPS and fuel timestamps remain separate. The advice planner rejects stale
or missing telemetry instead of using old readings as current measurements.

On 429 we back off exponentially (honoring Retry-After) up to _MAX_RETRIES
attempts before raising.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from dieselup import metrics
from dieselup.circuit_breaker import CircuitOpenError, samsara_breaker
from dieselup.config import settings

log = logging.getLogger(__name__)

# Matches a unit number token NOT immediately preceded by a letter. A few
# BigRig units carry a trailing letter suffix in Samsara ("777N - HECTOR
# MARTIN") while QuickManage may send the numeric part ("777"). We accept one
# trailing letter after the digits and return the numeric key, but still reject
# embedded model/code digits like "GHP2" or "Ford F550".
_UNIT_DIGITS_RE = re.compile(r"(?<![a-zA-Z0-9])(\d+)[a-zA-Z]?(?![a-zA-Z0-9])")
_SUBUNIT_PREFIX_RE = re.compile(r"^SUB(UNIT)?[#\s\-]*", re.IGNORECASE)
_PAREN_DIGITS_RE = re.compile(r"\((\d+)\)")


def extract_unit_digits(text: str | None) -> str | None:
    """First standalone digit sequence in a Samsara vehicle name.

    Skips digits that are embedded inside a word (e.g. 'GHP2', 'F550').
    Handles all naming formats used in this fleet:
      '702658 - MILTON MEDINA'        → '702658'
      'UNIT# 3044 - Jimmy Brown'      → '3044'
      'UNIT - 567667 - KAMALI'        → '567667'
      'SUBUNIT# 727424 (551802) - …'  → '727424'
      'GHP2-GED-P5C'                  → None  (embedded digit, not a unit)
      'Ford F550'                     → None  (embedded digit)
    """
    if not text:
        return None
    m = _UNIT_DIGITS_RE.search(text)
    return m.group(1) if m else None


def extract_samsara_index_keys(name: str | None) -> list[str]:
    """All unit-number keys to index this Samsara vehicle under.

    For SUBUNIT vehicles DataTruck uses the parenthetical number as the truck
    unit, not the first number:
      'SUBUNIT# 247714 (567668) - DEMO DRIVER'
          → DataTruck unit '567668' (paren), also index under '247714'
      'SUBUNIT - 898725(551566) WALNES DORESTAL'
          → DataTruck unit '551566' (paren), also index under '898725'

    For regular vehicles: [first_standalone_digit_sequence].
    Returns an empty list for vehicles with no digit sequence (Inactive, NEW…).
    """
    if not name:
        return []
    keys: list[str] = []

    if _SUBUNIT_PREFIX_RE.match(name):
        paren = _PAREN_DIGITS_RE.search(name)
        if paren:
            keys.append(paren.group(1))  # parenthetical = what DataTruck uses

    primary = extract_unit_digits(name)
    if primary and primary not in keys:
        keys.append(primary)

    return keys


class SamsaraError(RuntimeError):
    """Raised on any non-2xx response, transport error, or invalid JSON payload."""


@dataclass(frozen=True)
class VehicleLocation:
    lat: float
    lng: float
    speed_mph: float | None = None       # current road speed, None if unavailable
    gps_age_minutes: float | None = None # minutes since last GPS update from Samsara
    gps_time: datetime | None = None     # original observation time; repeated polls are not new evidence


@dataclass(frozen=True)
class VehicleStats:
    lat: float
    lng: float
    fuel_gallons: float
    mpg_rolling: float | None
    gps_age_minutes: float | None = None
    heading: float | None = None   # live compass bearing from Samsara (0-360), None when unavailable
    speed_mph: float | None = None # current road speed, None if unavailable
    fuel_age_minutes: float | None = None


@dataclass(frozen=True)
class VehicleSummary:
    id: str
    name: str
    unit_digits: str | None


@dataclass(frozen=True)
class _LocationEntry:
    lat: float
    lng: float
    timestamp: datetime | None
    heading: float | None = None   # compass degrees 0-360, None when unknown
    speed_mph: float | None = None # road speed in mph from Samsara, None when unavailable


class SamsaraClient:
    """Thin async wrapper around the Samsara Fleet API."""

    _BASE_URL = "https://api.samsara.com"
    _MAX_RETRIES = 4
    _INITIAL_BACKOFF_SECONDS = 1.0
    _MPG_LOOKBACK_DAYS = 7

    def __init__(self, *, timeout: float = 30.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._BASE_URL,
            headers={
                "Authorization": f"Bearer {settings.SAMSARA_API_TOKEN}",
                "Accept": "application/json",
            },
            timeout=timeout,
        )
        # Per-instance caches. Created lazily on first lookup and reused
        # across every get_vehicle_* call for the lifetime of this client.
        self._locations_cache: dict[str, _LocationEntry] | None = None
        self._fuel_pct_cache: dict[str, float] | None = None
        self._fuel_times: dict[str, datetime | None] = {}
        self._locations_cached_at = self._fuel_cached_at = 0.0
        self._mpg_cache: dict[str, float] | None = None
        self._mpg_cache_attempted = False
        self._cache_lock = asyncio.Lock()

    async def __aenter__(self) -> "SamsaraClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # -- Cache priming --------------------------------------------------------

    async def _ensure_locations(self) -> dict[str, _LocationEntry]:
        if self._locations_cache is not None and time.monotonic() - self._locations_cached_at < 30:
            return self._locations_cache
        async with self._cache_lock:
            if self._locations_cache is None or time.monotonic() - self._locations_cached_at >= 30:
                self._locations_cache = await self._fetch_all_locations()
                self._locations_cached_at = time.monotonic()
        return self._locations_cache

    async def _ensure_fuel(self) -> dict[str, float]:
        if self._fuel_pct_cache is not None and time.monotonic() - self._fuel_cached_at < 30:
            return self._fuel_pct_cache
        async with self._cache_lock:
            if self._fuel_pct_cache is None or time.monotonic() - self._fuel_cached_at >= 30:
                self._fuel_pct_cache = await self._fetch_all_fuel()
                self._fuel_cached_at = time.monotonic()
        return self._fuel_pct_cache

    async def _ensure_mpg(self) -> dict[str, float]:
        # MPG endpoint requires a token scope we may not have. After the first
        # failed attempt we cache an empty dict and stop retrying.
        if self._mpg_cache is not None:
            return self._mpg_cache
        async with self._cache_lock:
            if self._mpg_cache is None and not self._mpg_cache_attempted:
                self._mpg_cache_attempted = True
                try:
                    self._mpg_cache = await self._fetch_all_mpg()
                except SamsaraError as exc:
                    log.warning(
                        "Samsara fuel-energy report unavailable (%s) — falling back to FLEET_DEFAULT_MPG",
                        exc,
                    )
                    self._mpg_cache = {}
        return self._mpg_cache or {}

    # -- Bulk fetchers --------------------------------------------------------

    async def _fetch_all_locations(self) -> dict[str, _LocationEntry]:
        payload = await self._get_json("/fleet/vehicles/locations")
        items = payload.get("data") or []
        out: dict[str, _LocationEntry] = {}
        for v in items:
            if not isinstance(v, dict):
                continue
            vid = v.get("id")
            loc = v.get("location") or {}
            lat = loc.get("latitude")
            lng = loc.get("longitude")
            if not isinstance(vid, str) or lat is None or lng is None:
                continue
            ts_raw = loc.get("time")
            ts = _parse_iso(ts_raw)
            heading_raw = loc.get("heading")
            heading = float(heading_raw) if heading_raw is not None else None
            speed_raw = loc.get("speed")  # Samsara returns speed in mph
            speed_mph = float(speed_raw) if speed_raw is not None else None
            out[vid] = _LocationEntry(
                lat=float(lat), lng=float(lng), timestamp=ts,
                heading=heading, speed_mph=speed_mph,
            )
        return out

    async def _fetch_all_fuel(self) -> dict[str, float]:
        payload = await self._get_json(
            "/fleet/vehicles/stats/feed",
            params={"types": "fuelPercents"},
        )
        items = payload.get("data") or []
        out: dict[str, float] = {}
        self._fuel_times = {}
        for v in items:
            if not isinstance(v, dict):
                continue
            vid = v.get("id")
            if not isinstance(vid, str):
                continue
            events = v.get("fuelPercents") or []
            if not isinstance(events, list) or not events:
                continue
            latest = max(events, key=lambda e: e.get("time", "") if isinstance(e, dict) else "")
            if not isinstance(latest, dict):
                continue
            raw = latest.get("value")
            if raw is None:
                continue
            try:
                val = float(raw)
            except (TypeError, ValueError):
                continue
            # Samsara fuelPercents is measured in percentage points, including
            # values below 1%. Inferring a fractional scale turns 1% into a
            # full tank and can suppress a critically needed fuel stop.
            pct = val
            if 0.0 <= pct <= 100.0:
                out[vid] = pct
                self._fuel_times[vid] = _parse_iso(latest.get("time"))
        return out

    async def _fetch_all_mpg(self) -> dict[str, float]:
        # The report uses inclusive dates and its latest 72 hours may still
        # be processing. Use a complete seven-day window before that lag.
        end = datetime.now(timezone.utc) - timedelta(days=3)
        start = end - timedelta(days=self._MPG_LOOKBACK_DAYS - 1)
        params = {"startDate": _iso_z(start), "endDate": _iso_z(end)}
        out: dict[str, float] = {}
        cursors: set[str] = set()
        while True:
            payload = await self._get_json("/fleet/reports/vehicles/fuel-energy", params=params)
            data = payload.get("data")
            if not isinstance(data, dict) or not isinstance(data.get("vehicleReports"), list):
                raise SamsaraError("Samsara fuel-energy report has an invalid response shape")
            for report in data["vehicleReports"]:
                if not isinstance(report, dict):
                    continue
                vehicle = report.get("vehicle")
                vid = vehicle.get("id") if isinstance(vehicle, dict) else None
                if not isinstance(vid, str):
                    continue
                try:
                    meters = float(report["distanceTraveledMeters"])
                    milliliters = float(report["fuelConsumedMl"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not all(math.isfinite(v) and v > 0 for v in (meters, milliliters)):
                    continue
                # MPGe is energy-equivalent efficiency; fuel planning needs
                # actual US gallons consumed over the reported road distance.
                mpg = (meters / 1609.344) / (milliliters / 3785.411784)
                if math.isfinite(mpg) and mpg > 0:
                    out[vid] = mpg
            pagination = payload.get("pagination") or {}
            if not pagination.get("hasNextPage"):
                return out
            cursor = pagination.get("endCursor")
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise SamsaraError("Samsara fuel-energy pagination did not advance")
            cursors.add(cursor)
            params = {**params, "after": cursor}

    # -- Public read API ------------------------------------------------------

    async def get_vehicle_location(self, vehicle_id: str) -> VehicleLocation:
        """Return the latest known GPS fix for the vehicle."""
        locations = await self._ensure_locations()
        entry = locations.get(vehicle_id)
        if entry is None:
            raise SamsaraError(f"Samsara has no location entry for vehicle {vehicle_id}")
        age_minutes: float | None = None
        if entry.timestamp is not None:
            age_minutes = (datetime.now(timezone.utc) - entry.timestamp).total_seconds() / 60.0
        return VehicleLocation(
            lat=entry.lat,
            lng=entry.lng,
            speed_mph=entry.speed_mph,
            gps_age_minutes=age_minutes,
            gps_time=entry.timestamp,
        )

    async def get_vehicle_fuel(self, vehicle_id: str) -> float:
        """Return the latest known fuel level in gallons (percent × tank capacity)."""
        fuel = await self._ensure_fuel()
        pct = fuel.get(vehicle_id)
        if pct is None:
            raise SamsaraError(
                f"Samsara has no fuel-percent entry for vehicle {vehicle_id} "
                "(no sensor data in latest feed)"
            )
        return pct / 100.0 * settings.TANK_CAPACITY_GALLONS

    async def get_vehicle_fuel_reading(self, vehicle_id: str) -> tuple[float, datetime | None]:
        gallons = await self.get_vehicle_fuel(vehicle_id)
        return gallons, self._fuel_times.get(vehicle_id)

    async def get_vehicle_stats(self, vehicle_id: str) -> VehicleStats:
        """Return GPS, fuel level (gallons), rolling MPG, and GPS staleness."""
        locations = await self._ensure_locations()
        fuel = await self._ensure_fuel()
        mpg = await self._ensure_mpg()

        loc = locations.get(vehicle_id)
        if loc is None:
            raise SamsaraError(f"Samsara has no location entry for vehicle {vehicle_id}")

        pct = fuel.get(vehicle_id)
        if pct is None:
            raise SamsaraError(
                f"Samsara has no fuel-percent entry for vehicle {vehicle_id} "
                "(no sensor data in latest feed)"
            )
        fuel_gallons = pct / 100.0 * settings.TANK_CAPACITY_GALLONS

        age_minutes: float | None = None
        if loc.timestamp is not None:
            age_minutes = (datetime.now(timezone.utc) - loc.timestamp).total_seconds() / 60.0

        return VehicleStats(
            lat=loc.lat,
            lng=loc.lng,
            fuel_gallons=fuel_gallons,
            mpg_rolling=mpg.get(vehicle_id),
            gps_age_minutes=age_minutes,
            heading=loc.heading,
            speed_mph=loc.speed_mph,
            fuel_age_minutes=((datetime.now(timezone.utc) - self._fuel_times[vehicle_id]).total_seconds() / 60
                              if self._fuel_times.get(vehicle_id) is not None else None),
        )

    # -- Vehicle listing (unchanged from prior version) -----------------------

    async def list_vehicles(self) -> list[VehicleSummary]:
        """Paginated fetch of every vehicle. Returns id + name + first-digit unit."""
        out: list[VehicleSummary] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 200}
            if cursor:
                params["after"] = cursor
            payload = await self._get_json("/fleet/vehicles", params=params)
            for item in payload.get("data", []) or []:
                if not isinstance(item, dict):
                    continue
                vid = item.get("id")
                name = item.get("name") or ""
                if not isinstance(vid, str) or not vid:
                    continue
                out.append(
                    VehicleSummary(
                        id=vid,
                        name=str(name),
                        unit_digits=extract_unit_digits(str(name)),
                    )
                )
            pagination = payload.get("pagination") or {}
            if not pagination.get("hasNextPage"):
                return out
            cursor = pagination.get("endCursor")
            if not cursor:
                return out

    async def find_vehicles_by_unit(self, unit: str) -> list[VehicleSummary]:
        """Return every vehicle this unit resolves to, the way load_sync does.

        A vehicle is matched on *any* of its index keys — the first digit
        sequence AND, for SUBUNIT vehicles, the parenthetical number DataTruck
        actually treats as the unit (see extract_samsara_index_keys). Matching
        only the first digit sequence broke auto-link for SUBUNIT trucks: a
        drivers' group titled with the parenthetical unit (e.g. '567668') never
        matched 'SUBUNIT# 247714 (567668) - …', so auto-link fell through to the
        manual /linktruck fallback. Leading zeros are tolerated both ways so
        '005' links a vehicle named '5 - Driver'.
        """
        target = extract_unit_digits(unit) or (unit.strip() if unit else "")
        if not target:
            return []
        target_norm = target.lstrip("0") or target

        matches: list[VehicleSummary] = []
        for v in await self.list_vehicles():
            for key in extract_samsara_index_keys(v.name):
                if (key.lstrip("0") or key) == target_norm:
                    matches.append(v)
                    break
        return matches

    # -- HTTP plumbing --------------------------------------------------------

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Breaker-guarded entrypoint. Internal 429-retry loop is invisible
        to the breaker — only the final outcome counts."""
        try:
            return await samsara_breaker.call(self._do_request, path, params=params)
        except CircuitOpenError as exc:
            metrics.incr("samsara_circuit_rejected_total")
            raise SamsaraError(
                f"Samsara circuit is OPEN — rejecting call to {path}. "
                "Will probe again after cooldown."
            ) from exc

    async def _do_request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        backoff = self._INITIAL_BACKOFF_SECONDS
        for attempt in range(self._MAX_RETRIES + 1):
            try:
                resp = await self._client.get(path, params=params)
            except httpx.HTTPError as exc:
                detail = str(exc).strip()
                suffix = f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
                raise SamsaraError(f"Samsara request to {path} failed: {suffix}") from exc

            if resp.status_code == 429:
                if attempt >= self._MAX_RETRIES:
                    raise SamsaraError(
                        f"Samsara rate limit exceeded (429) for {path} after "
                        f"{self._MAX_RETRIES} retries"
                    )
                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                wait = retry_after if retry_after is not None else backoff
                await asyncio.sleep(wait)
                backoff *= 2
                continue

            if resp.status_code == 401:
                raise SamsaraError(
                    f"Samsara authentication failed (401) for {path} — "
                    "SAMSARA_API_TOKEN missing scope or expired"
                )
            if resp.status_code == 404:
                raise SamsaraError(f"Samsara resource not found (404) for {path}")
            if resp.status_code >= 400:
                body = resp.text[:200].replace("\n", " ")
                raise SamsaraError(
                    f"Samsara returned {resp.status_code} for {path}: {body}"
                )

            try:
                return resp.json()
            except ValueError as exc:
                raise SamsaraError(
                    f"Samsara response for {path} was not JSON"
                ) from exc

        raise SamsaraError(f"Samsara request to {path} failed after retries")

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None


def _iso_z(dt: datetime) -> str:
    """Samsara expects RFC3339 with a literal Z suffix, not +00:00."""
    return dt.isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    """Tolerant ISO-8601 parse — returns None on anything weird."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None
