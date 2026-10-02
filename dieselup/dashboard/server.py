"""
Kadads Fuel — FastAPI operations dashboard.

Endpoints
---------
GET /                   → HTML fleet overview page
GET /health             → {"status": "ok"} (Railway health check)
GET /metrics            → Prometheus-style text metrics
GET /circuits           → Circuit breaker states (JSON)
GET /api/fleet          → Live fleet data (JSON)
GET /api/events         → Recent stop events (JSON)
GET /api/fuel_events    → Recent fueling events (JSON)

Authentication
--------------
Protected endpoints require DASHBOARD_SECRET via a login cookie,
  ?token=<secret>  or  Authorization: Bearer <secret>.
Missing authentication configuration fails closed. Health and the login page
remain accessible; login requires explicitly configured admin credentials.
"""
from __future__ import annotations

import hmac
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

_UNIT_VIEW_HTML = Path(__file__).parent / "unit_view.html"
_STATIC_DIR = Path(__file__).parent / "static"

from dieselup import metrics as met
from dieselup.circuit_breaker import quickmanage_breaker, samsara_breaker, telegram_breaker
from dieselup.config import settings
from dieselup.core.price_sources import pilot_price_rows
from dieselup.core.operating_scope import status as operating_scope_status
from dieselup.db import get_pool
from dieselup.health_server import _ok_status

log = logging.getLogger(__name__)


def _period_start(period: str) -> datetime | None:
    now = datetime.now(timezone.utc)
    key = (period or "month").lower()
    if key in {"today", "day", "daily"}:
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if key in {"week", "weekly"}:
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    if key in {"month", "monthly"}:
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if key in {"year", "yearly"}:
        return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return None


class LoginBody(BaseModel):
    email: str
    password: str


class DriverConnectionBody(BaseModel):
    telegram_group_id: int


# ── Auth dependency ────────────────────────────────────────────────────────────

async def _check_auth(request: Request) -> None:
    secret = settings.DASHBOARD_SECRET
    if not secret:
        raise HTTPException(status_code=503, detail="Dashboard authentication is not configured")
    token = request.query_params.get("token") or ""
    token = token or request.cookies.get("kadads_dashboard_token", "")
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        token = token or header[7:]
    if not hmac.compare_digest(token.encode("utf-8"), secret.encode("utf-8")):
        if not request.url.path.startswith("/api/"):
            raise HTTPException(status_code=303, headers={"Location": "/login"})
        raise HTTPException(status_code=401, detail="Unauthorized")


def create_app() -> FastAPI:
    app = FastAPI(title="Kadads Fuel Dashboard", docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def fresh_dashboard_assets(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith('/api/'):
            response.headers['Cache-Control'] = 'private, no-store'
        elif response.headers.get('content-type','').split(';')[0] in ('text/html','text/css','application/javascript','text/javascript'):
            response.headers['Cache-Control'] = 'no-cache'
        return response

    @app.get("/login", response_class=HTMLResponse)
    async def login_page():
        return """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Kadads Dashboard Login</title>
<style>
body{margin:0;background:#0b0f0d;color:#f4f7f2;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif;display:grid;place-items:center;min-height:100vh}
.card{width:min(420px,calc(100vw - 32px));background:#121815;border:1px solid #273127;border-radius:22px;padding:28px;box-shadow:0 24px 80px #0008}
h1{margin:0 0 8px;font-size:28px}.muted{color:#9aa59b;margin:0 0 24px}
label{display:block;color:#b8c2b8;font-size:13px;margin-top:14px}input{box-sizing:border-box;width:100%;margin-top:7px;padding:13px 14px;border-radius:12px;border:1px solid #303b31;background:#090d0b;color:white;font-size:16px}
button{width:100%;margin-top:22px;padding:14px;border:0;border-radius:14px;background:#7cff2b;color:#071007;font-weight:900;font-size:16px;cursor:pointer}
.err{color:#ff6b61;min-height:22px;margin:14px 0 0}
</style></head><body><form class="card" id="f">
<h1>Kadads Fuel Dashboard</h1><p class="muted">Sign in to view live trucks, fuel stops, and fueling proof.</p>
<label>Email<input name="email" type="email" autocomplete="username" required autofocus></label>
<label>Password<input name="password" type="password" autocomplete="current-password" required></label>
<button>Sign in</button><p class="err" id="err"></p>
</form><script>
f.onsubmit=async e=>{e.preventDefault();err.textContent='';const body=Object.fromEntries(new FormData(f));
const r=await fetch('/api/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
if(!r.ok){err.textContent='Wrong email or password';return} location.href='/'}
</script></body></html>"""

    @app.post("/api/login")
    async def login(body: LoginBody):
        secret = settings.DASHBOARD_SECRET
        email = settings.DASHBOARD_ADMIN_EMAIL.strip().lower()
        password = settings.DASHBOARD_ADMIN_PASSWORD
        if not secret or not email or not password:
            raise HTTPException(status_code=503, detail="Dashboard login is not configured")
        password_matches = hmac.compare_digest(body.password.encode("utf-8"), password.encode("utf-8"))
        if body.email.strip().lower() != email or not password_matches:
            raise HTTPException(status_code=401, detail="Unauthorized")
        response = JSONResponse({"ok": True})
        response.set_cookie(
            "kadads_dashboard_token",
            secret,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,
        )
        return response

    @app.get("/health")
    @app.get("/api/health")
    async def health():
        code, detail = _ok_status()
        return JSONResponse(
            {"status": "ok" if code == 200 else "stale", "service": "kadads-fuel-dashboard", "detail": detail,
             "operating_scope": operating_scope_status()},
            status_code=code,
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics_endpoint(_: None = Depends(_check_auth)):
        return met.render_text()

    @app.get("/circuits")
    async def circuits(_: None = Depends(_check_auth)):
        return {
            "samsara": samsara_breaker.state(),
            "quickmanage": quickmanage_breaker.state(),
            "telegram": telegram_breaker.state(),
        }

    @app.get("/api/fleet")
    async def api_fleet(_: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _fleet_data()))

    @app.get("/api/fleet/overview")
    async def api_fleet_overview(_: None = Depends(_check_auth)):
        fleet = await _fleet_data()
        events = await _recent_events(250)
        fuel_events = await _recent_fuel_events(250)
        total = len(fleet)
        critical = sum(1 for t in fleet if float(t.get("fuel_pct") or 0) < 15)
        low = sum(1 for t in fleet if 15 <= float(t.get("fuel_pct") or 0) < 30)
        pending = [e for e in events if e.get("status") == "pending"]
        saved_month = sum(float(e.get("dollar_impact") or 0) for e in events if e.get("status") == "saved")
        lost_month = abs(sum(float(e.get("dollar_impact") or 0) for e in events if e.get("status") == "lost"))
        return {
            "total": total,
            "critical": critical,
            "low": low,
            "paused": 0,
            "saved_month": round(saved_month, 2),
            "lost_month": round(lost_month, 2),
            "recoverable": round(sum(max(float(e.get("dollar_impact") or 0), 0) for e in pending), 2),
            "verified_wins": sum(1 for e in events if e.get("status") == "saved"),
            "avg_discount_cpg": 0,
            "fuel_proofs": len(fuel_events),
        }

    @app.get("/api/drivers")
    async def api_drivers(_: None = Depends(_check_auth)):
        fleet = await _fleet_data()
        drivers = []
        for t in fleet:
            unit = str(t.get("truck_unit") or "")
            drivers.append({
                "truck_id": unit,
                "unit_number": unit,
                "driver_name": t.get("driver_full_name"),
                "status": "Rolling" if float(t.get("speed_mph") or 0) > 2 else "Idle",
                "fuel_pct": t.get("fuel_pct"),
                "speed_mph": t.get("speed_mph"),
                "latitude": t.get("latitude"),
                "longitude": t.get("longitude"),
                "stop_name": t.get("stop_name"),
                "last_seen_at": t.get("taken_at"),
                "telegram_group_id": t.get("driver_telegram_id"),
                "telegram_group_name": None,
                "samsara_vehicle_id": None,
                "alerts_paused": False,
            })
        return {"drivers": jsonable_encoder(drivers)}

    @app.put("/api/drivers/{truck_unit}/connection")
    async def connect_driver(truck_unit: str, body: DriverConnectionBody, _: None = Depends(_check_auth)):
        pool = await get_pool()
        result = await pool.execute(
            "UPDATE trucks_drivers SET driver_telegram_id=$2, updated_at=NOW() WHERE truck_unit=$1",
            truck_unit, body.telegram_group_id,
        )
        if result != "UPDATE 1":
            raise HTTPException(status_code=404, detail="Truck not found")
        return {"ok": True, "connected": True}

    @app.delete("/api/drivers/{truck_unit}/connection")
    async def disconnect_driver(truck_unit: str, _: None = Depends(_check_auth)):
        pool = await get_pool()
        result = await pool.execute(
            "UPDATE trucks_drivers SET driver_telegram_id=NULL, updated_at=NOW() WHERE truck_unit=$1",
            truck_unit,
        )
        if result != "UPDATE 1":
            raise HTTPException(status_code=404, detail="Truck not found")
        return {"ok": True, "connected": False}

    @app.get("/api/savings")
    async def api_savings(period: str = "month", _: None = Depends(_check_auth)):
        events = await _recent_events(500, period)
        fuel_events = await _recent_fuel_events(500, period)
        saved = sum(float(e.get("dollar_impact") or 0) for e in events if e.get("status") == "saved")
        lost = abs(sum(float(e.get("dollar_impact") or 0) for e in events if e.get("status") == "lost"))
        capturable = sum(max(float(e.get("dollar_impact") or 0), 0) for e in events if e.get("status") == "pending")
        return {
            "period": period,
            "verified_saved": round(saved, 2),
            "capturable": round(capturable, 2),
            "lost": round(lost, 2),
            "fills": len(fuel_events),
            "trend": [],
        }

    @app.get("/api/loads/pnl")
    async def api_loads_pnl(period: str = "month", limit: int = 100, _: None = Depends(_check_auth)):
        events = await _recent_events(max(limit * 5, 250), period)
        fuel_events = await _recent_fuel_events(max(limit * 5, 250), period)
        gallons_by_load: dict[str, float] = {}
        spend_by_load: dict[str, float] = {}
        for fe in fuel_events:
            if fe.get("load_id"):
                load_key = str(fe["load_id"])
                gallons = float(fe.get("gallons") or 0)
                price = float(fe.get("your_price") or fe.get("actual_true_cost") or 0)
                gallons_by_load[load_key] = gallons_by_load.get(load_key, 0.0) + gallons
                spend_by_load[load_key] = spend_by_load.get(load_key, 0.0) + (gallons * price)
        by_load: dict[str, dict[str, Any]] = {}
        for e in events:
            load_id = str(e.get("load_id") or "unknown")
            row = by_load.setdefault(load_id, {
                "load_id": load_id,
                "truck_unit": e.get("truck_unit"),
                "outcome": e.get("status"),
                "saved": 0.0,
                "loss": 0.0,
                "recoverable": 0.0,
                "planned_gallons": 0,
                "actual_gallons": 0,
                "actual_spend": 0.0,
                "event_count": 0,
                "station_name": e.get("stop_name"),
                "station_city": e.get("stop_city"),
                "station_state": "",
                "origin_label": "",
                "destination_label": "",
                "price_source": "live",
                "price_date": e.get("created_at"),
                "your_price": float(e.get("your_price") or 0),
                "retail_price": float(e.get("retail_price") or 0),
            })
            row["event_count"] += 1
            row["planned_gallons"] += int(float(e.get("planned_gallons") or 0))
            impact = float(e.get("dollar_impact") or 0)
            status = e.get("status")
            if status == "saved":
                row["saved"] += impact
            elif status == "lost":
                row["loss"] += max(-impact,0)
            elif status == "pending":
                row["recoverable"] += max(impact, 0)
            row["outcome"] = status or row["outcome"]
        for load_id, gallons in gallons_by_load.items():
            if load_id in by_load:
                by_load[load_id]["actual_gallons"] = round(gallons, 1)
                by_load[load_id]["actual_spend"] = round(spend_by_load.get(load_id, 0.0), 2)
        loads = list(by_load.values())[:limit]
        summary = {
            "saved": round(sum(x["saved"] for x in loads), 2),
            "loss": round(sum(x["loss"] for x in loads), 2),
            "recoverable": round(sum(x["recoverable"] for x in loads), 2),
            "loads": len(loads),
        }
        summary["net"] = round(summary["saved"] - summary["loss"], 2)
        return {"period": period, "summary": summary, "loads": jsonable_encoder(loads)}

    @app.get("/api/events")
    async def api_events(limit: int = 50, period: str = "year", _: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _recent_events(limit, period)))

    @app.get("/api/fuel_events")
    async def api_fuel_events(limit: int = 50, period: str = "year", _: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _recent_fuel_events(limit, period)))

    @app.get("/api/fuel-stops")
    async def api_fuel_stops(limit: int = 2000, _: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _fuel_stops(limit)))

    @app.get("/api/fuel-prices/status")
    async def api_fuel_price_status(_: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _fuel_price_status()))

    @app.get("/api/truck/{unit}/route")
    async def api_truck_route(unit: str, _: None = Depends(_check_auth)):
        return JSONResponse(jsonable_encoder(await _truck_route(unit)))

    @app.post("/api/receipts/upload")
    async def api_receipts_upload(request: Request, _: None = Depends(_check_auth)):
        # The current bot database does not yet have a persisted receipt table.
        # Return a real review queue response so the dashboard can be wired now;
        # persistence can be added without changing the UI contract.
        content_type = request.headers.get("content-type", "")
        body = await request.body()
        return {
            "ok": True,
            "bytes_received": len(body),
            "content_type": content_type,
            "status": "queued_for_manual_reaudit",
            "message": "Receipt received by dashboard; persistent receipt audit table is the next backend step.",
        }

    @app.get("/api/unit/{unit}/dashboard")
    async def unit_dashboard(unit: str, price: str = "discount", _: None = Depends(_check_auth)):
        # Lazy import so a missing optional dep (e.g. Valhalla HTTP) can't break the whole app.
        from dieselup.dashboard.unit_data import UnitDashboardError, build_unit_dashboard
        met.incr("unit_dashboard_requests_total")
        try:
            with met.Timer("unit_dashboard_request"):
                payload = await build_unit_dashboard(unit, price_mode=price)
        except UnitDashboardError as exc:
            met.incr("unit_dashboard_unavailable_total")  # no live data / no load / routing down
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:  # noqa: BLE001
            met.incr("unit_dashboard_error_total")
            log.warning("unit_dashboard(%s) failed: %s", unit, exc)
            raise HTTPException(status_code=502, detail=f"dashboard build failed: {exc}")
        met.incr("unit_dashboard_ok_total")
        return JSONResponse(jsonable_encoder(payload))

    @app.get("/unit/{unit}", response_class=HTMLResponse)
    async def unit_page(unit: str, _: None = Depends(_check_auth)):
        try:
            return _UNIT_VIEW_HTML.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"unit_view.html missing: {exc}")

    @app.get("/legacy", response_class=HTMLResponse)
    async def dashboard(_: None = Depends(_check_auth)):
        fleet = await _fleet_data()
        events = await _recent_events(20)
        fuel_events = await _recent_fuel_events(10)
        return _render_html(fleet, events, fuel_events)

    @app.get("/", response_class=HTMLResponse)
    async def kadads_dashboard(_: None = Depends(_check_auth)):
        return (_STATIC_DIR / "index.html").read_text(encoding="utf-8")

    def _clean_page_handler(page_path: Path):
        async def handler(_: None = Depends(_check_auth)):
            return FileResponse(page_path)
        return handler

    def _legacy_redirect_handler(slug: str):
        async def handler():
            return RedirectResponse(url=f"/{slug}", status_code=308)
        return handler

    for page_path in sorted(_STATIC_DIR.glob("*.html")):
        slug = page_path.stem
        if slug in {"index", "login"}:
            continue
        app.add_api_route(f"/{slug}", _clean_page_handler(page_path), methods=["GET"], include_in_schema=False)
        app.add_api_route(f"/{slug}.html", _legacy_redirect_handler(slug), methods=["GET"], include_in_schema=False)

    from dieselup.dashboard.driver_connections import install_driver_connections
    install_driver_connections(app, _check_auth, get_pool)
    from dieselup.dashboard.fuel_history import install_fuel_history
    install_fuel_history(app, _check_auth, get_pool)
    from dieselup.dashboard.advice_outcomes import install_advice_outcomes
    install_advice_outcomes(app, _check_auth, get_pool)
    app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="kadads-static")

    return app


# ── Data queries ───────────────────────────────────────────────────────────────

async def _fleet_data() -> list[dict[str, Any]]:
    pool = await get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT
                td.truck_unit,
                td.driver_full_name,
                td.driver_telegram_id,
                ts.fuel_pct,
                ts.latitude,
                ts.longitude,
                ts.speed_mph,
                ts.taken_at,
                -- latest open stop event
                se.id          AS event_id,
                se.load_id,
                -- first candidate's stop name from JSONB
                (se.candidates::jsonb -> 0 ->> 'station_name') AS stop_name,
                (se.candidates::jsonb -> 0 ->> 'city')         AS stop_city,
                se.status      AS event_status,
                se.recommended_at AS event_created
            FROM trucks_drivers td
            LEFT JOIN LATERAL (
                SELECT *
                FROM truck_snapshots
                WHERE truck_unit = td.truck_unit
                ORDER BY taken_at DESC
                LIMIT 1
            ) ts ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM stop_events
                WHERE truck_unit = td.truck_unit
                  AND status = 'pending'
                ORDER BY recommended_at DESC
                LIMIT 1
            ) se ON TRUE
            ORDER BY td.truck_unit
            """
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        log.warning("fleet_data query failed: %s", exc)
        raise RuntimeError(f"fleet_data query failed: {exc}") from exc


async def _recent_events(limit: int, period: str | None = None) -> list[dict[str, Any]]:
    pool = await get_pool()
    start = _period_start(period or "")
    try:
        rows = await pool.fetch(
            """
            SELECT
                se.id, se.truck_unit, se.load_id,
                (se.candidates::jsonb -> 0 ->> 'station_name') AS stop_name,
                (se.candidates::jsonb -> 0 ->> 'city')         AS stop_city,
                (se.candidates::jsonb -> 0 ->> 'state')        AS stop_state,
                (se.candidates::jsonb -> 0 ->> 'address')      AS stop_address,
                (se.candidates::jsonb -> 0 ->> 'your_price')   AS your_price,
                (se.candidates::jsonb -> 0 ->> 'retail_price') AS retail_price,
                se.status, se.dollar_impact,
                se.gallons AS planned_gallons,
                se.recommended_true_cost,
                se.actual_true_cost,
                se.recommended_at AS created_at,
                se.resolved_at
            FROM stop_events se
            WHERE ($2::timestamptz IS NULL OR se.recommended_at >= $2)
            ORDER BY se.recommended_at DESC
            LIMIT $1
            """,
            limit,
            start,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        log.warning("recent_events query failed: %s", exc)
        raise RuntimeError(f"recent_events query failed: {exc}") from exc


async def _recent_fuel_events(limit: int, period: str | None = None) -> list[dict[str, Any]]:
    pool = await get_pool()
    start = _period_start(period or "")
    try:
        rows = await pool.fetch(
            f"""WITH prices AS ({pilot_price_rows('$3','$5')})
            SELECT
                fe.id, fe.truck_unit, fe.gallons, fe.stop_event_id,
                fe.fuel_pct_start, fe.fuel_pct_end,
                fe.classification, fe.station_name,
                COALESCE(fs.address, '') AS station_address,
                COALESCE(fs.city, '') AS station_city,
                COALESCE(fs.state, '') AS station_state,
                cp.your_price,
                cp.retail_price,
                cp.effective_date AS price_date,
                cp.price_date_source,
                CASE WHEN cp.your_price IS NULL THEN 'pending'
                     ELSE 'contracted_quote_estimate' END AS price_status,
                se.recommended_true_cost,
                se.actual_true_cost,
                fe.load_id, fe.detected_at, fe.finalized_at
            FROM fuel_events fe
            LEFT JOIN fuel_stops fs
              ON fs.pilot_site_id = fe.site_id
            LEFT JOIN LATERAL (
              SELECT your_price, retail_price, effective_date, price_date_source
              FROM prices
              WHERE site_id = fe.site_id
                AND (fe.detected_at AT TIME ZONE 'America/New_York')::date - effective_date BETWEEN 0 AND $4
              ORDER BY effective_date DESC, uploaded_at DESC
              LIMIT 1
            ) cp ON TRUE
            LEFT JOIN stop_events se
              ON se.id = fe.stop_event_id
            WHERE ($2::timestamptz IS NULL OR fe.detected_at >= $2)
            ORDER BY fe.detected_at DESC
            LIMIT $1
            """,
            limit,
            start,
            settings.FTS_PRICE_CUSTOMER,
            settings.MAX_FUEL_PRICE_AGE_DAYS,
            settings.PILOT_ACCOUNT_NUMBER,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        log.warning("fuel_events query failed: %s", exc)
        raise RuntimeError(f"fuel_events query failed: {exc}") from exc


async def _fuel_stops(limit: int) -> list[dict[str, Any]]:
    pool = await get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT
                fs.id,
                fs.station_name,
                fs.address,
                fs.city,
                fs.state,
                fs.latitude,
                fs.longitude,
                fs.truck_accessible,
                cp.your_price,
                cp.retail_price,
                cp.effective_date,
                cp.uploaded_at,
                COALESCE(cp.price_status, 'missing') AS price_status,
                CASE
                  WHEN fs.station_name ILIKE '%love%' THEN 'loves'
                  WHEN fs.station_name ILIKE '%pilot%' OR fs.station_name ILIKE '%flying j%' THEN 'pilot_fj'
                  ELSE 'other'
                END AS network
            FROM fuel_stops fs
            LEFT JOIN LATERAL (
              SELECT CASE WHEN ((NOW() AT TIME ZONE 'America/New_York')::date) - effective_date BETWEEN 0 AND $3
                          THEN your_price END AS your_price,
                     CASE WHEN ((NOW() AT TIME ZONE 'America/New_York')::date) - effective_date BETWEEN 0 AND $3
                          THEN retail_price END AS retail_price,
                     effective_date, uploaded_at,
                     CASE WHEN effective_date > ((NOW() AT TIME ZONE 'America/New_York')::date) THEN 'future'
                          WHEN ((NOW() AT TIME ZONE 'America/New_York')::date) - effective_date > $3 THEN 'stale'
                          ELSE 'current' END AS price_status
              FROM contracted_prices
              WHERE site_id = fs.pilot_site_id AND account_number = $2 AND $4=''
              ORDER BY effective_date DESC, uploaded_at DESC
              LIMIT 1
            ) cp ON TRUE
            WHERE fs.truck_accessible IS DISTINCT FROM FALSE
            ORDER BY fs.state, fs.city, fs.station_name
            LIMIT $1
            """,
            min(max(int(limit), 1), 10000),
            settings.PILOT_ACCOUNT_NUMBER,
            settings.MAX_FUEL_PRICE_AGE_DAYS,
            settings.FTS_PRICE_CUSTOMER,
        )
        stops = [dict(r) for r in rows]
        quotes = await pool.fetch("""
            SELECT DISTINCT ON(q.fuel_stop_id) q.fuel_stop_id,q.provider,q.your_price,q.retail_price,
                q.effective_date,i.uploaded_at,i.date_source,((NOW() AT TIME ZONE 'America/New_York')::date)-q.effective_date AS age_days
            FROM price_feed_quotes q JOIN price_file_imports i ON i.id=q.import_id
            JOIN fuel_stops verified ON verified.id=q.fuel_stop_id
            WHERE q.fuel_stop_id IS NOT NULL AND i.status IN ('completed','held')
              AND ((q.provider='loves' AND q.account_number=$1 AND $1<>'' AND verified.station_name ILIKE '%love%')
                OR (q.provider='fts' AND q.account_number=$2 AND $2<>'' AND verified.pilot_site_id IS NOT NULL
                    AND (verified.station_name ILIKE '%pilot%' OR verified.station_name ILIKE '%flying j%')))
            ORDER BY q.fuel_stop_id,
                CASE WHEN ((NOW() AT TIME ZONE 'America/New_York')::date)-q.effective_date BETWEEN 0 AND $3 THEN 0 ELSE 1 END,
                q.effective_date DESC NULLS LAST,i.uploaded_at DESC,q.id DESC
        """,settings.LOVES_PRICE_CUSTOMER,settings.FTS_PRICE_CUSTOMER,settings.MAX_FUEL_PRICE_AGE_DAYS)
        by_stop = {q['fuel_stop_id']:dict(q) for q in quotes}
        for stop in stops:
            quote = by_stop.get(stop['id'])
            if quote is None:
                continue
            age = quote['age_days']
            current = age is not None and 0 <= age <= settings.MAX_FUEL_PRICE_AGE_DAYS
            if stop['price_status']=='current' and (not current or stop['your_price']<=quote['your_price']):
                continue
            stop.update(your_price=quote['your_price'] if current else None,
                retail_price=quote['retail_price'] if current else None,
                effective_date=quote['effective_date'],uploaded_at=quote['uploaded_at'],
                price_provider=quote['provider'],price_date_source=quote['date_source'],price_status=('missing_date' if age is None else
                'future' if age<0 else 'current' if current else 'stale'))
        return stops
    except Exception as exc:
        log.warning("fuel_stops query failed: %s", exc)
        raise RuntimeError(f"fuel_stops query failed: {exc}") from exc


async def _fuel_price_status() -> dict[str, Any]:
    pool = await get_pool()
    row = await pool.fetchrow(f"""WITH prices AS ({pilot_price_rows('$1','$2')})
        SELECT MAX(effective_date) AS latest_price_date,
               MAX(uploaded_at) AS last_successful_import,
               ((NOW() AT TIME ZONE 'America/New_York')::date) - MAX(effective_date) AS age_days
        FROM prices WHERE truck_accessible IS DISTINCT FROM FALSE
    """,settings.FTS_PRICE_CUSTOMER,settings.PILOT_ACCOUNT_NUMBER)
    age = row['age_days']
    status = ('missing' if age is None else 'future' if age < 0 else
              'stale' if age > settings.MAX_FUEL_PRICE_AGE_DAYS else 'current')
    imports = await pool.fetch('''SELECT id,filename,provider,account_number,effective_date,date_source,
        status,reason,row_count,matched_rows,excluded_rows,row_issues,uploaded_at FROM price_file_imports
        ORDER BY uploaded_at DESC,id DESC LIMIT 10''')
    feeds = await pool.fetch('''SELECT q.provider,q.account_number,max(q.effective_date) AS latest_price_date,
        max(i.uploaded_at) AS last_import,count(*) AS quote_rows,
        count(DISTINCT q.fuel_stop_id) FILTER(WHERE ((NOW() AT TIME ZONE 'America/New_York')::date)-q.effective_date BETWEEN 0 AND $3)
            AS current_stations
        FROM price_feed_quotes q JOIN price_file_imports i ON i.id=q.import_id
        JOIN fuel_stops verified ON verified.id=q.fuel_stop_id
        WHERE i.status IN ('completed','held') AND
           ((q.provider='loves' AND q.account_number=$1 AND $1<>'' AND verified.station_name ILIKE '%love%') OR
            (q.provider='fts' AND q.account_number=$2 AND $2<>'' AND verified.pilot_site_id IS NOT NULL
              AND (verified.station_name ILIKE '%pilot%' OR verified.station_name ILIKE '%flying j%')))
        AND verified.truck_accessible IS DISTINCT FROM FALSE
        GROUP BY q.provider,q.account_number''',settings.LOVES_PRICE_CUSTOMER,
        settings.FTS_PRICE_CUSTOMER,settings.MAX_FUEL_PRICE_AGE_DAYS)
    any_current = any(f['current_stations']>0 for f in feeds)
    return {**dict(row), 'account_number': settings.PILOT_ACCOUNT_NUMBER,
            'status': 'current' if any_current else status, 'max_age_days': settings.MAX_FUEL_PRICE_AGE_DAYS,
            'source': 'uploaded_contracted_price_sheet', 'automatic_price_feed': False,
            'undated_file_policy':'upload_day',
            'imports':[dict(i) for i in imports],'supplier_feeds':[dict(f) for f in feeds],
            'supported_formats':['Pilot/Flying J','Love’s','FTS'],
            'network_price_sources':{'loves':settings.LOVES_PRICE_CUSTOMER,'pilot_fj':settings.FTS_PRICE_CUSTOMER},
            'routing_price_source':'FTS Plus for Pilot/Flying J; Love’s route support remains held until station identities are integrated'}


async def _truck_route(unit: str) -> dict[str, Any]:
    fleet = await _fleet_data()
    truck = next((t for t in fleet if str(t.get("truck_unit")) == str(unit)), None)
    events = [e for e in await _recent_events(500) if str(e.get("truck_unit")) == str(unit)]
    fuel_events = [e for e in await _recent_fuel_events(200) if str(e.get("truck_unit")) == str(unit)]
    candidates: list[dict[str, Any]] = []
    pool = await get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT
                se.id,
                se.load_id,
                se.status,
                se.recommended_at,
                cand.value AS candidate
            FROM stop_events se
            CROSS JOIN LATERAL jsonb_array_elements(se.candidates::jsonb) cand(value)
            WHERE se.truck_unit = $1
            ORDER BY se.recommended_at DESC
            LIMIT 40
            """,
            unit,
        )
        for r in rows:
            c = dict(r["candidate"])
            c["event_id"] = r["id"]
            c["load_id"] = r["load_id"]
            c["event_status"] = r["status"]
            c["recommended_at"] = r["recommended_at"]
            candidates.append(c)
    except Exception as exc:
        log.warning("truck_route candidates query failed: %s", exc)
    if not candidates:
        for e in events[:12]:
            if e.get("stop_name"):
                candidates.append({
                    "station_name": e.get("stop_name"),
                    "address": e.get("stop_address"),
                    "city": e.get("stop_city"),
                    "state": e.get("stop_state"),
                    "your_price": e.get("your_price"),
                    "retail_price": e.get("retail_price"),
                    "load_id": e.get("load_id"),
                    "event_status": e.get("status"),
                    "recommended_at": e.get("created_at"),
                })
    return {
        "unit": unit,
        "truck": truck,
        "events": events,
        "fuel_events": fuel_events,
        "candidates": candidates,
    }


# ── HTML renderer ──────────────────────────────────────────────────────────────

def _fuel_bar(pct: float | None) -> str:
    if pct is None:
        return "<span style='color:#888'>—</span>"
    pct = float(pct)
    color = "#4ade80" if pct >= 40 else "#facc15" if pct >= 20 else "#f87171"
    return (
        f'<div style="display:inline-block;width:80px;height:12px;'
        f'background:#333;border-radius:6px;vertical-align:middle;margin-right:4px">'
        f'<div style="width:{min(pct,100):.0f}%;height:100%;background:{color};'
        f'border-radius:6px"></div></div>'
        f'<span style="font-size:12px">{pct:.0f}%</span>'
    )


def _fmt_dt(dt) -> str:
    if dt is None:
        return "—"
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%m/%d %H:%M UTC")
    return str(dt)


def _event_badge(status: str | None) -> str:
    color_map = {
        "pending":  "#3b82f6",
        "saved":    "#4ade80",
        "lost":     "#f87171",
        "skipped":  "#94a3b8",
        "expired":  "#f59e0b",
    }
    color = color_map.get(status or "", "#94a3b8")
    return (
        f'<span style="background:{color};color:#000;border-radius:4px;'
        f'padding:1px 6px;font-size:11px;font-weight:600">{status or "?"}</span>'
    )


def _classification_badge(cls: str | None) -> str:
    color_map = {
        "recommended":      "#4ade80",
        "contracted_other": "#facc15",
        "off_network":      "#f87171",
    }
    color = color_map.get(cls or "", "#94a3b8")
    label = (cls or "?").replace("_", " ")
    return (
        f'<span style="background:{color};color:#000;border-radius:4px;'
        f'padding:1px 6px;font-size:11px;font-weight:600">{label}</span>'
    )


def _render_html(
    fleet: list[dict],
    events: list[dict],
    fuel_events: list[dict],
) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── Fleet rows ─────────────────────────────────────────────────────────────
    fleet_rows = ""
    for t in fleet:
        unit     = t.get("truck_unit") or "—"
        driver   = t.get("driver_full_name") or "—"
        fuel_pct = t.get("fuel_pct")
        speed    = t.get("speed_mph")
        snap_at  = _fmt_dt(t.get("taken_at"))
        load_id  = t.get("load_id") or "—"
        stop_n   = t.get("stop_name") or ""
        stop_c   = t.get("stop_city") or ""
        stop_str = f"{stop_n}, {stop_c}".strip(", ") if (stop_n or stop_c) else "—"
        ev_stat  = t.get("event_status")
        spd_str  = f"{float(speed):.0f} mph" if speed is not None else "—"

        fleet_rows += f"""
        <tr>
          <td><b>{unit}</b></td>
          <td>{driver}</td>
          <td>{_fuel_bar(fuel_pct)}</td>
          <td>{spd_str}</td>
          <td>{load_id}</td>
          <td>{stop_str}</td>
          <td>{_event_badge(ev_stat) if ev_stat else '—'}</td>
          <td style="font-size:11px;color:#888">{snap_at}</td>
        </tr>"""

    # ── Stop event rows ────────────────────────────────────────────────────────
    event_rows = ""
    for e in events:
        stop_n = e.get("stop_name") or ""
        stop_c = e.get("stop_city") or ""
        stop_str = f"{stop_n}, {stop_c}".strip(", ") if (stop_n or stop_c) else "—"
        event_rows += f"""
        <tr>
          <td>{e.get('truck_unit') or '—'}</td>
          <td>{e.get('load_id') or '—'}</td>
          <td>{stop_str}</td>
          <td>{_event_badge(e.get('status'))}</td>
          <td style="color:#f87171">{
              f"${float(e['dollar_impact']):.2f}" if e.get('dollar_impact') else '—'
          }</td>
          <td style="font-size:11px;color:#888">{_fmt_dt(e.get('created_at'))}</td>
          <td style="font-size:11px;color:#888">{_fmt_dt(e.get('resolved_at'))}</td>
        </tr>"""

    # ── Fuel event rows ────────────────────────────────────────────────────────
    fuel_rows = ""
    for f in fuel_events:
        gal   = f.get("gallons")
        gal_s = f"{float(gal):.0f} gal" if gal is not None else "—"
        fuel_rows += f"""
        <tr>
          <td>{f.get('truck_unit') or '—'}</td>
          <td>{gal_s}</td>
          <td>{_classification_badge(f.get('classification'))}</td>
          <td>{f.get('station_name') or '—'}</td>
          <td style="font-size:11px;color:#888">{_fmt_dt(f.get('detected_at'))}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="120">
<title>Kadads Fuel — Fleet Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background: #0f172a; color: #e2e8f0; font-size: 14px; padding: 20px; }}
  h1 {{ font-size: 22px; font-weight: 700; color: #f8fafc; margin-bottom: 4px; }}
  .subtitle {{ color: #64748b; font-size: 12px; margin-bottom: 24px; }}
  h2 {{ font-size: 15px; font-weight: 600; color: #94a3b8; text-transform: uppercase;
        letter-spacing: .06em; margin: 28px 0 10px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ text-align: left; padding: 8px 10px; color: #64748b; font-size: 11px;
        text-transform: uppercase; letter-spacing: .05em;
        border-bottom: 1px solid #1e293b; }}
  td {{ padding: 9px 10px; border-bottom: 1px solid #1e293b; vertical-align: middle; }}
  tr:hover td {{ background: #1e293b; }}
  .empty {{ color: #475569; font-style: italic; padding: 16px 10px; }}
  .refresh {{ color: #475569; font-size: 11px; margin-top: 30px; }}
</style>
</head>
<body>
<h1>🚛 Kadads Fuel</h1>
<div class="subtitle">Fleet Operations Dashboard &nbsp;·&nbsp; Auto-refreshes every 2 min &nbsp;·&nbsp; {now_str}</div>

<h2>Live Fleet ({len(fleet)} trucks)</h2>
<table>
  <thead>
    <tr>
      <th>Unit</th><th>Driver</th><th>Fuel</th><th>Speed</th>
      <th>Load</th><th>Next Stop</th><th>Event</th><th>As-of</th>
    </tr>
  </thead>
  <tbody>
    {"".join(fleet_rows) if fleet_rows else '<tr><td colspan="8" class="empty">No trucks registered yet.</td></tr>'}
  </tbody>
</table>

<h2>Recent Stop Events (last 20)</h2>
<table>
  <thead>
    <tr>
      <th>Unit</th><th>Load</th><th>Stop</th><th>Status</th>
      <th>$ Impact</th><th>Created</th><th>Resolved</th>
    </tr>
  </thead>
  <tbody>
    {"".join(event_rows) if event_rows else '<tr><td colspan="7" class="empty">No stop events yet.</td></tr>'}
  </tbody>
</table>

<h2>Recent Fueling Events (last 10)</h2>
<table>
  <thead>
    <tr>
      <th>Unit</th><th>Gallons</th><th>Classification</th><th>Location</th><th>Time</th>
    </tr>
  </thead>
  <tbody>
    {"".join(fuel_rows) if fuel_rows else '<tr><td colspan="5" class="empty">No fueling events yet.</td></tr>'}
  </tbody>
</table>

<p class="refresh">Auto-refreshes every 120 s. Force-reload for latest data.</p>
</body>
</html>"""
