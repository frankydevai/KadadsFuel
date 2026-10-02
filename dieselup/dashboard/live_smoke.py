"""Live smoke test for the unit corridor dashboard.

Hits the REAL deployed endpoint for ONE unit and checks that every live integration actually
responded — Samsara (fresh GPS), Valhalla (a decodable route + matrix miles), Postgres (priced
stations), and the IFTA tiering. Unlike the e2e harness (which fakes everything for speed), this
proves the deployment and credentials work. It is intentionally shallow: "does the real thing
work at all?", not exhaustive coverage.

Gated by RUN_LIVE_SMOKE=1 so normal CI never runs it. Run after a deploy, after rotating the
Samsara token, or after rebuilding Valhalla tiles.

    RUN_LIVE_SMOKE=1 SMOKE_BASE_URL=https://your-dashboard \
        SMOKE_UNIT=6079 DASHBOARD_SECRET=... python -m dieselup.dashboard.live_smoke

Exit code 0 = all checks pass, 1 = a check failed, 2 = misconfigured/skipped-but-required.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass

from .corridor_stops import decode_polyline


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def evaluate_payload(payload: dict, max_gps_age_min: float = 30.0) -> list[Check]:
    """Run the integration checks on a dashboard payload (pure — no network).

    Verifies the live systems behind the endpoint actually answered:
      Samsara → fresh truck_pos · Valhalla → decodable route + matrix miles ·
      Postgres/IFTA → priced stations with a best stop.
    """
    checks: list[Check] = []

    # Samsara: a current GPS fix.
    tp = payload.get("truck_pos") or {}
    age = (payload.get("truck") or {}).get("gps_age_minutes")
    if tp.get("lat") is None or tp.get("lng") is None:
        checks.append(Check("samsara_gps", False, "no truck_pos in payload (Samsara silent)"))
    elif age is None:
        checks.append(Check("samsara_gps", False, "truck_pos present but GPS age unknown"))
    elif age > max_gps_age_min:
        checks.append(Check("samsara_gps", False, f"GPS fix is {age:.0f} min old (> {max_gps_age_min:.0f})"))
    else:
        checks.append(Check("samsara_gps", True, f"GPS {age:.0f} min old at {tp['lat']},{tp['lng']}"))

    # Valhalla: a route shape that decodes to a real line (tiles cover this lane).
    shape = (payload.get("route") or {}).get("shape")
    pts = decode_polyline(shape, 6) if shape else []
    checks.append(Check("valhalla_route", len(pts) > 1,
                        f"route decodes to {len(pts)} points, {payload.get('route',{}).get('miles')} mi"))

    # Postgres + IFTA: priced stations with a best stop.
    stations = payload.get("stations") or []
    has_best = any(s.get("tier") == "best" for s in stations)
    checks.append(Check("stations_priced", bool(stations) and has_best,
                        f"{len(stations)} stations, best={'yes' if has_best else 'NO'}"))

    # Valhalla matrix: a real road distance to the recommended stop.
    miles = (payload.get("stats") or {}).get("miles_to_stop")
    checks.append(Check("matrix_miles", isinstance(miles, (int, float)),
                        f"miles_to_stop={miles}"))

    return checks


def run_smoke(base_url: str, unit: str, token: str | None = None,
              price: str = "discount", timeout: float = 30.0,
              max_gps_age_min: float = 30.0) -> tuple[bool, list[Check], float]:
    """GET the live endpoint and evaluate it. Returns (ok, checks, latency_seconds)."""
    import httpx

    url = f"{base_url.rstrip('/')}/api/unit/{unit}/dashboard"
    params = {"price": price}
    if token:
        params["token"] = token

    t0 = time.monotonic()
    try:
        resp = httpx.get(url, params=params, timeout=timeout)
    except Exception as exc:  # transport/DNS/timeout
        return False, [Check("http_200", False, f"request failed: {exc}")], time.monotonic() - t0
    latency = time.monotonic() - t0

    http_ok = resp.status_code == 200
    checks = [Check("http_200", http_ok, f"HTTP {resp.status_code} in {latency:.1f}s")]
    if not http_ok:
        checks.append(Check("payload", False, resp.text[:200]))
        return False, checks, latency

    checks += evaluate_payload(resp.json(), max_gps_age_min=max_gps_age_min)
    return all(c.ok for c in checks), checks, latency


def main() -> int:
    if os.environ.get("RUN_LIVE_SMOKE") != "1":
        print("live smoke skipped (set RUN_LIVE_SMOKE=1 to run)")
        return 0

    base_url = os.environ.get("SMOKE_BASE_URL")
    if not base_url:
        print("ERROR: SMOKE_BASE_URL is required when RUN_LIVE_SMOKE=1", file=sys.stderr)
        return 2

    unit = os.environ.get("SMOKE_UNIT", "6079")
    token = os.environ.get("DASHBOARD_SECRET") or None
    max_gps = float(os.environ.get("SMOKE_MAX_GPS_AGE_MIN", "30"))
    timeout = float(os.environ.get("SMOKE_TIMEOUT", "30"))

    print(f"live smoke → {base_url} unit {unit}")
    ok, checks, latency = run_smoke(base_url, unit, token=token, timeout=timeout, max_gps_age_min=max_gps)
    for c in checks:
        print(f"  [{'PASS' if c.ok else 'FAIL'}] {c.name}: {c.detail}")
    print(f"{'OK' if ok else 'FAILED'} ({latency:.1f}s)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
