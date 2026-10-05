"""
Lightweight HTTP server for Railway / external uptime monitors.

Three endpoints:
  GET /          → 200 while scheduled jobs progress on their expected cadence, else 503
  GET /health    → same as /
  GET /metrics   → plain-text dump of every counter/gauge/timer
  GET /circuits  → CLOSED/OPEN/HALF_OPEN state per breaker

Runs in a daemon thread so it never blocks the asyncio loop. Reads metrics
state via the thread-safe API in dieselup.metrics. The "last sweep" heartbeat
comes from gauges that load_sync and compliance update on each cycle.
"""
from __future__ import annotations

import http.server
import json
import logging
import os
import socketserver
import threading
import time
from typing import Any

from dieselup import metrics
from dieselup.config import settings
from dieselup.core.operating_scope import status as operating_scope_status
from dieselup.circuit_breaker import (
    samsara_breaker,
    telegram_breaker,
    tms_breaker,
)

log = logging.getLogger(__name__)

# Sweep heartbeats have paired monotonic and Unix timestamps in metrics gauges.
# If the most recent recorded heartbeat is older than STALE_AFTER_SECONDS
# we report unhealthy (503).
STALE_AFTER_SECONDS = 300.0  # 5 min
_STARTED_MONO = time.monotonic()
_STARTED_UNIX = time.time()
# Each scheduled job must progress independently. Allow its interval plus
# bounded execution time, rather than using the most recent job to mask stalls.
_JOB_MAX_AGE = {
    "load_sync": 1800.0,
    "compliance": 600.0,
    "dlq_retry": 1200.0,
    "fuel_brain": 600.0,
}


def _timestamp_age(gauges: dict, key: str, now_mono: float, now_unix: float) -> float:
    """Retain elapsed active time and count suspension, without negative ages."""
    ages = [0.0, now_mono - float(gauges[key])]
    wall = gauges.get(key[:-5] + '_unix')
    if wall is not None:
        ages.append(now_unix - float(wall))
    return max(ages)


def _job_statuses() -> dict[str, Any]:
    gauges = metrics.snapshot().get("gauges", {})
    now, wall_now = time.monotonic(), time.time()
    startup_age = max(0.0, now - _STARTED_MONO, wall_now - _STARTED_UNIX)
    return {
        job: {
            "age_seconds": round(_timestamp_age(gauges, job + "_last_heartbeat_mono", now, wall_now), 1)
            if job + "_last_heartbeat_mono" in gauges else None,
            "stale": (
                _timestamp_age(gauges, job + "_last_heartbeat_mono", now, wall_now)
                if job + "_last_heartbeat_mono" in gauges else startup_age
            ) > max_age,
            "max_age_seconds": max_age,
        }
        for job, max_age in _JOB_MAX_AGE.items()
    }


def _seconds_since_last_sweep() -> float:
    """Smallest age (in seconds) of the load_sync / compliance / dlq heartbeats.

    Returns +inf if no heartbeat has ever been recorded — implies the bot
    just booted and no scheduled job has fired yet.
    """
    snap = metrics.snapshot()
    g = snap.get("gauges", {})
    now, wall_now = time.monotonic(), time.time()
    ages = []
    for key in (
        "load_sync_last_heartbeat_mono",
        "compliance_last_heartbeat_mono",
        "dlq_retry_last_heartbeat_mono",
    ):
        ts = g.get(key)
        if ts is not None:
            ages.append(_timestamp_age(g, key, now, wall_now))
    if not ages:
        return float("inf")
    return min(ages)


def _ok_status() -> tuple[int, str]:
    if settings.BOT_MODE != "active":
        return 200, f"INACTIVE — {settings.BOT_MODE} mode; automated jobs are disabled"
    age = _seconds_since_last_sweep()
    snap = metrics.snapshot()
    counters = snap.get("counters", {})
    if snap.get("gauges", {}).get("singleton_leader_active") == 0:
        return 503, "BLOCKED — database bot leadership is not verified"
    if snap.get("gauges", {}).get("bot_paused", 0):
        return 503, "PAUSED — automated fuel planning is paused by an administrator"
    if snap.get("gauges", {}).get("fuel_prices_stale", 0):
        return 503, "BLOCKED — contracted fuel prices are absent or stale"
    stale = [job for job, status in _job_statuses().items() if status["stale"]]
    if stale:
        return 503, "STALE — overdue jobs: " + ", ".join(stale)
    if age == float("inf"):
        body = "BOOTING — no sweep heartbeat yet"
        return 200, body  # not stale yet; just starting up
    body = (
        f"OK last_sweep={age:.0f}s_ago "
        f"alerts_ok={counters.get('alerts_ok_total', 0)} "
        f"alerts_failed={counters.get('alerts_telegram_err_total', 0) + counters.get('alerts_transport_err_total', 0)} "
        f"load_sync_cycles={counters.get('load_sync_cycles_total', 0)}"
    )
    return 200, body


def _routing_status() -> dict[str, Any]:
    valhalla_configured = bool(
        settings.VALHALLA_URL.strip() and settings.VALHALLA_API_SECRET.strip()
    )
    gauges = metrics.snapshot().get("gauges", {})
    success = gauges.get("valhalla_last_success_mono")
    failure = gauges.get("valhalla_last_failure_mono")
    age = None if success is None else _timestamp_age(
        gauges, 'valhalla_last_success_mono', time.monotonic(), time.time())
    status = "unverified"
    if not valhalla_configured or (failure is not None and (success is None or failure >= success)):
        status = "red"
    elif success is not None:
        status = "green" if age <= 1800 else "stale"
    return {
        "primary": "valhalla" if valhalla_configured else "unavailable",
        "valhalla_configured": valhalla_configured,
        "valhalla_status": status,
        "last_success_age_seconds": None if age is None else round(age, 1),
        "fallbacks_enabled": False,
    }


class _Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code: int, body: str, content_type: str = "text/plain") -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 — http.server signature
        path = self.path.split("?", 1)[0].rstrip("/")
        try:
            if path in ("", "/health"):
                code, body = _ok_status()
                self._send(code, body)
            elif path == "/api/health":
                code, message = _ok_status()
                self._send(
                    code,
                    json.dumps({
                        "ok": code == 200,
                        "service": "dieselup-carrier-bot",
                        "tms_provider": settings.TMS_PROVIDER,
                        "status": message,
                        "routing": _routing_status(),
                        "operating_scope": operating_scope_status(),
                        "jobs": _job_statuses(),
                        "metrics": metrics.snapshot(),
                    }, default=str),
                    "application/json",
                )
            elif path == "/metrics":
                self._send(200, metrics.render_text())
            elif path == "/circuits":
                provider_breaker = tms_breaker(settings.TMS_PROVIDER)
                body = (
                    f"tms({settings.TMS_PROVIDER}) {provider_breaker.state()}\n"
                    f"samsara   {samsara_breaker.state()}\n"
                    f"telegram  {telegram_breaker.state()}\n"
                )
                self._send(200, body)
            else:
                self._send(404, "not found\n")
        except Exception as exc:  # noqa: BLE001 — health handler must never crash the bot
            log.exception("health handler error: %s", exc)
            try:
                self._send(500, f"handler error: {type(exc).__name__}\n")
            except Exception:  # noqa: BLE001
                pass

    def log_message(self, *_args: Any) -> None:
        # Suppress the default access log — too noisy for Railway's health check polls.
        pass


class _ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_health_server() -> None:
    """Spawn the health server in a background thread. Idempotent — safe to
    call multiple times; later calls are no-ops if a server is already
    listening on PORT.

    PORT is read from env (Railway sets this). If unset, defaults to 8080.
    Set HEALTH_HOST=127.0.0.1 to bind local runs to the Mac only.
    If port can't be bound, the server is skipped (do not crash the bot).
    """
    port = int(os.getenv("PORT", "8080"))
    host = os.getenv("HEALTH_HOST", "0.0.0.0")

    def _run() -> None:
        try:
            server = _ThreadedServer((host, port), _Handler)
        except OSError as exc:
            log.warning("health_server: could not bind PORT=%d: %s", port, exc)
            return
        log.info("health_server: listening on :%d (/, /health, /metrics, /circuits)", port)
        try:
            server.serve_forever(poll_interval=1.0)
        except Exception:  # noqa: BLE001 — log + exit thread without crashing the bot
            log.exception("health_server: serve_forever raised")

    t = threading.Thread(target=_run, daemon=True, name="health_server")
    t.start()
