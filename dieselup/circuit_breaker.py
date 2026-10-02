"""
circuit_breaker.py — Protect against cascading failures from external APIs.

When Samsara / DataTruck / Telegram have an outage, retrying every sweep
wastes API quota and floods admin chat. The circuit breaker fails fast
during outages, then probes the API after a cooldown.

STATES
──────
  CLOSED     normal — requests pass through
  OPEN       failing — reject calls instantly until cooldown elapses
  HALF_OPEN  cooldown elapsed — allow one probe; success → CLOSED, failure → OPEN

USAGE — async-first since the whole codebase is asyncio:
─────
    from dieselup.circuit_breaker import samsara_breaker, CircuitOpenError

    try:
        result = await samsara_breaker.call(samsara.get_vehicle_stats, vid)
    except CircuitOpenError:
        log.warning("samsara is open — skipping this truck")
        continue

Synchronous `call_sync` is available for the few sync code paths, but most
call sites should use `await call(...)`.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

from dieselup import metrics

log = logging.getLogger(__name__)

T = TypeVar("T")


class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is rejected because the breaker is open."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        fail_threshold: int = 5,
        cooldown: float = 60.0,
    ) -> None:
        self.name = name
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown

        self._state: State = State.CLOSED
        self._failures: int = 0
        self._opened_at: float = 0.0
        self._half_open_probe_in_flight: bool = False
        # Lock is plain threading.Lock — held only across very small critical
        # sections (state machine transitions), never across an await.
        self._lock: threading.Lock = threading.Lock()

    # -- internal --------------------------------------------------------------

    def _can_attempt(self) -> bool:
        with self._lock:
            if self._state is State.CLOSED:
                return True
            if self._state is State.OPEN:
                if time.monotonic() - self._opened_at >= self.cooldown:
                    self._state = State.HALF_OPEN
                    self._half_open_probe_in_flight = True
                    log.info("CircuitBreaker[%s]: → HALF_OPEN (probing)", self.name)
                    return True
                return False
            # HALF_OPEN: exactly one recovery probe may be in flight. Without
            # this latch, every concurrent caller passed while the first probe
            # was still awaiting the upstream service.
            if self._half_open_probe_in_flight:
                return False
            self._half_open_probe_in_flight = True
            return True

    def _record_success(self) -> None:
        with self._lock:
            recovered = self._state is not State.CLOSED
            self._state = State.CLOSED
            self._failures = 0
            self._half_open_probe_in_flight = False
        if recovered:
            log.info("CircuitBreaker[%s]: → CLOSED (recovered)", self.name)
        metrics.incr(f"circuit_{self.name}_success_total")

    def _record_failure(self) -> None:
        opened_now = False
        with self._lock:
            self._failures += 1
            self._half_open_probe_in_flight = False
            if self._state is State.HALF_OPEN:
                self._state = State.OPEN
                self._opened_at = time.monotonic()
                opened_now = True
            elif self._failures >= self.fail_threshold and self._state is not State.OPEN:
                self._state = State.OPEN
                self._opened_at = time.monotonic()
                opened_now = True
        if opened_now:
            log.warning(
                "CircuitBreaker[%s]: → OPEN (%d failures)",
                self.name, self._failures,
            )
            metrics.incr(f"circuit_{self.name}_opened_total")
        metrics.incr(f"circuit_{self.name}_failure_total")

    # -- public ----------------------------------------------------------------

    async def call(
        self,
        fn: Callable[..., Awaitable[T] | T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run `fn(*args, **kwargs)` through the breaker.

        `fn` may be sync or async; both forms are handled. Sync calls run
        directly without thread-pool offloading — keep them fast or use an
        explicit `await asyncio.to_thread(...)` upstream.
        """
        if not self._can_attempt():
            metrics.incr(f"circuit_{self.name}_rejected_total")
            raise CircuitOpenError(f"circuit {self.name!r} is OPEN")
        try:
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            self._record_failure()
            raise
        except asyncio.CancelledError:
            # A cancelled recovery probe must release the half-open latch;
            # otherwise the breaker rejects every future call forever.
            self._record_failure()
            raise
        else:
            self._record_success()
            return result  # type: ignore[return-value]

    def call_sync(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Synchronous variant for sync-only call sites."""
        if not self._can_attempt():
            metrics.incr(f"circuit_{self.name}_rejected_total")
            raise CircuitOpenError(f"circuit {self.name!r} is OPEN")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    def state(self) -> str:
        with self._lock:
            return self._state.value


# -- Shared breakers ----------------------------------------------------------
# Use these everywhere instead of constructing new ones, so multiple call-sites
# for the same API share state.
# Thresholds tuned per service: Telegram tolerates more flakes than the TMSes
# because each user-visible alert is more sensitive to silent loss.

samsara_breaker = CircuitBreaker("samsara", fail_threshold=5, cooldown=60.0)
datatruck_breaker = CircuitBreaker("datatruck", fail_threshold=5, cooldown=60.0)
quickmanage_breaker = CircuitBreaker("quickmanage", fail_threshold=5, cooldown=60.0)
telegram_breaker = CircuitBreaker("telegram", fail_threshold=8, cooldown=30.0)


def tms_breaker(provider: str) -> CircuitBreaker:
    """Return the shared breaker for the configured read-only TMS provider."""
    return quickmanage_breaker if provider.strip().lower() == "quickmanage" else datatruck_breaker
