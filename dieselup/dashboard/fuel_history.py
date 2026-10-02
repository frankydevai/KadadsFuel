"""Authenticated read-only fuel advice, replacement and fueling history."""
from fastapi import Depends, Query
from fastapi.encoders import jsonable_encoder


def install_fuel_history(app, check_auth, get_pool):
    @app.get("/api/fuel-advice/history")
    async def history(truck_unit: str | None = Query(None, max_length=40),
                      load_id: str | None = Query(None, max_length=100),
                      before_id: int | None = Query(None, ge=1),
                      limit: int = Query(100, ge=1, le=500), _: None = Depends(check_auth)):
        pool = await get_pool()
        rows = await pool.fetch("""
            SELECT a.id,a.kind,a.truck_unit,a.load_id,a.stop_event_id,a.related_event_id,
                   a.details,a.created_at,se.status,se.gallons AS planned_gallons,se.actual_gallons,
                   se.candidates->0->>'station_name' AS station_name,
                   se.candidates->0->>'fill_to_full' AS fill_to_full,
                   se.briefing_driver_msg_id
            FROM fuel_advice_audit a LEFT JOIN stop_events se ON se.id=a.stop_event_id
            WHERE ($1::text IS NULL OR a.truck_unit=$1)
              AND ($2::text IS NULL OR a.load_id=$2) AND ($3::bigint IS NULL OR a.id<$3)
            ORDER BY a.id DESC LIMIT $4
        """, truck_unit or None, load_id or None, before_id, limit+1)
        fills = await pool.fetch("""
            SELECT id,truck_unit,load_id,stop_event_id,classification,station_name,gallons,
                   fuel_pct_start,fuel_pct_end,detected_at,finalized_at
            FROM fuel_events WHERE ($1::text IS NULL OR truck_unit=$1)
                AND ($2::text IS NULL OR load_id=$2)
            ORDER BY detected_at DESC LIMIT $3
        """, truck_unit or None, load_id or None, limit)
        plans = await pool.fetch("""
            SELECT id,truck_unit,load_id,status,gallons AS planned_gallons,actual_gallons,
                   candidates->0->>'station_name' AS station_name,
                   candidates->0->>'fill_to_full' AS fill_to_full,
                   recommended_at,resolved_at,briefing_driver_msg_id
            FROM stop_events WHERE ($1::text IS NULL OR truck_unit=$1)
                AND ($2::text IS NULL OR load_id=$2)
            ORDER BY recommended_at DESC LIMIT $3
        """, truck_unit or None, load_id or None, limit)
        page = [dict(r) for r in rows[:limit]]
        return jsonable_encoder({"events": page, "fills": [dict(r) for r in fills],
            "plans": [dict(r) for r in plans], "next_cursor": page[-1]["id"] if len(rows)>limit else None,
            "sensor_gallons_estimated": True, "history_is_read_only": True})
