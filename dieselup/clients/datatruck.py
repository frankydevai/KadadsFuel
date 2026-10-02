"""
Async client for the DataTruck TMS REST API.

Hits {slug}.datatruck.io/api/v1/openapi with token auth. A gate enforces a
3-second minimum interval between any two requests so we stay under
DataTruck's 20/min rate limit — that gate also covers the iter_orders()
pagination helper, so the 3-second spacing between page fetches required by
the rate limit is automatic.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator

import httpx

from dieselup import metrics
from dieselup.circuit_breaker import CircuitOpenError, datatruck_breaker
from dieselup.config import settings


class DataTruckError(RuntimeError):
    """Raised on any non-2xx response, transport error, or invalid JSON payload."""


class DataTruckClient:
    """Thin async wrapper around DataTruck's /orders/ endpoint."""

    _RATE_LIMIT_SECONDS = 3.0
    _MAX_TRANSPORT_RETRIES = 3
    _MAX_429_RETRIES = 4
    _MAX_5XX_RETRIES = 3
    _INITIAL_BACKOFF_SECONDS = 1.0

    # Class-level shared rate-limit state. Multiple DataTruckClient instances
    # running concurrently (sync_active_loads + compliance both fire on
    # APScheduler at the same minute) used to maintain INDEPENDENT 3-second
    # gates — combined throughput peaked at ~40 req/min and tripped DataTruck's
    # 20/min cap. Sharing the gate on the class keeps the global rate honest.
    _shared_next_allowed_at: float = 0.0
    _shared_gate_lock: asyncio.Lock | None = None

    def __init__(self, *, timeout: float = 30.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=f"https://{settings.DATATRUCK_COMPANY_SLUG}.datatruck.io/api/v1/openapi",
            headers={
                "Authorization": f"Token {settings.DATATRUCK_API_TOKEN}",
                "Accept": "application/json",
            },
            timeout=timeout,
        )

    async def __aenter__(self) -> "DataTruckClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def list_orders(
        self,
        page: int = 1,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fetch one page of orders. Callers iterate pages via iter_orders()."""
        if page < 1:
            raise ValueError(f"page must be >= 1, got {page}")
        params: dict[str, Any] = {"page": page}
        if filters:
            params.update(filters)
        return await self._get_json("/orders/", params=params)

    async def get_order(self, order_id: int | str) -> dict[str, Any]:
        """Fetch a single order by DataTruck order ID.

        Note the path has NO trailing slash. DataTruck v1's API is asymmetric:
        the list endpoint requires `/orders/` (trailing slash → 200) while the
        detail endpoint requires `/orders/{id}` (no trailing slash → 200; with
        trailing slash returns 404).
        """
        return await self._get_json(f"/orders/{order_id}")

    async def iter_orders(
        self,
        filters: dict[str, Any] | None = None,
        *,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield orders across pages, optionally bounded.

        DataTruck returns 10 orders per page and ignores `page_size`/`limit`
        query params. With 8000+ orders, an unbounded iteration costs
        ~45 minutes per sweep at the 3-second rate-limit gate — well past
        the 15-minute schedule. Callers that only need recent activity
        (load_sync, delivery detection) should pass max_pages so the sweep
        completes within budget. Orders sort newest-first by ID, so the
        first N pages cover all recent active and recently-delivered loads.

        max_pages=None means unbounded (legacy behavior — used by /forcebriefing
        which needs to find a specific truck no matter how old its order).
        """
        page = 1
        while True:
            payload = await self.list_orders(page=page, filters=filters)

            if isinstance(payload, list):
                for item in payload:
                    yield item
                return

            if not isinstance(payload, dict):
                raise DataTruckError(
                    f"Unexpected orders payload type: {type(payload).__name__}"
                )

            results = payload.get("results", [])
            if not results:
                return
            for item in results:
                yield item

            if not payload.get("next"):
                return
            if max_pages is not None and page >= max_pages:
                return
            page += 1

    async def _gate(self) -> None:
        """Block until the shared 3-second rate-limit window allows the next request.

        Uses class-level state so that all DataTruckClient instances in the
        process share one global gate — necessary because APScheduler can run
        sync_active_loads and compliance concurrently with separate clients.
        """
        cls = type(self)
        if cls._shared_gate_lock is None:
            cls._shared_gate_lock = asyncio.Lock()
        async with cls._shared_gate_lock:
            wait = cls._shared_next_allowed_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            cls._shared_next_allowed_at = time.monotonic() + cls._RATE_LIMIT_SECONDS

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Breaker-guarded entrypoint. Internal retries are invisible to the
        breaker — only the FINAL outcome of `_do_request` counts."""
        try:
            return await datatruck_breaker.call(self._do_request, path, params=params)
        except CircuitOpenError as exc:
            metrics.incr("datatruck_circuit_rejected_total")
            raise DataTruckError(
                f"DataTruck circuit is OPEN — rejecting call to {path}. "
                "Will probe again after cooldown."
            ) from exc

    async def _do_request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send one rate-limited GET. Retries on transport errors AND on 429.

        429 retry honors the Retry-After header when present, otherwise uses
        exponential backoff starting from _INITIAL_BACKOFF_SECONDS but never
        less than _RATE_LIMIT_SECONDS × 2 (so we don't immediately re-hit the
        same window we just exceeded).
        """
        transport_attempts = 0
        rate_attempts = 0
        server_error_attempts = 0
        backoff = self._INITIAL_BACKOFF_SECONDS

        while True:
            await self._gate()
            try:
                resp = await self._client.get(path, params=params)
            except (httpx.ReadError, httpx.ReadTimeout, httpx.ConnectError, httpx.ConnectTimeout) as exc:
                if transport_attempts >= self._MAX_TRANSPORT_RETRIES:
                    raise DataTruckError(
                        f"DataTruck request to {path} failed after "
                        f"{self._MAX_TRANSPORT_RETRIES} retries: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                transport_attempts += 1
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            except httpx.HTTPError as exc:
                raise DataTruckError(
                    f"DataTruck request to {path} failed: {type(exc).__name__}: {exc}"
                ) from exc

            if resp.status_code == 429:
                if rate_attempts >= self._MAX_429_RETRIES:
                    raise DataTruckError(
                        f"DataTruck rate limit (429) for {path} after "
                        f"{self._MAX_429_RETRIES} retries — backoff exhausted"
                    )
                rate_attempts += 1
                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                wait = retry_after if retry_after is not None else max(
                    backoff, self._RATE_LIMIT_SECONDS * 2
                )
                await asyncio.sleep(wait)
                backoff *= 2
                continue

            if resp.status_code in (502, 503, 504):
                # Transient DataTruck server errors — retry with backoff.
                # These account for the 174 mid-sweep aborts seen in production.
                if server_error_attempts >= self._MAX_5XX_RETRIES:
                    body = resp.text[:200].replace("\n", " ")
                    raise DataTruckError(
                        f"DataTruck server error ({resp.status_code}) for {path} after "
                        f"{self._MAX_5XX_RETRIES} retries: {body}"
                    )
                server_error_attempts += 1
                wait = backoff
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 30.0)
                continue

            if resp.status_code == 401:
                raise DataTruckError(
                    f"DataTruck authentication failed (401) for {path} — "
                    "DATATRUCK_API_TOKEN is missing, expired, or wrong"
                )
            if resp.status_code == 404:
                raise DataTruckError(
                    f"DataTruck resource not found (404) for {path}"
                )
            if resp.status_code >= 400:
                body = resp.text[:200].replace("\n", " ")
                raise DataTruckError(
                    f"DataTruck returned {resp.status_code} for {path}: {body}"
                )

            try:
                return resp.json()
            except ValueError as exc:
                raise DataTruckError(f"DataTruck response for {path} was not JSON") from exc

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None
