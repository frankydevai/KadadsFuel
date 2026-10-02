"""Bot-owned durable advice history. Dashboard reads; it never writes here."""
from datetime import datetime, timezone
import hashlib
import json

from dieselup.db import execute, fetch_one


async def missed_predecessor(truck_unit, load_id):
    row = await fetch_one("""SELECT a.stop_event_id FROM fuel_advice_audit a
        WHERE a.truck_unit=$1 AND a.load_id=$2 AND (a.kind IN ('missed_detected','stop_visited_no_fill') OR (a.kind='stop_lost' AND a.details->'bypass_evidence'->>'method'='ordered_road_route'))
          AND NOT EXISTS(SELECT 1 FROM fuel_advice_audit done
              WHERE done.kind='replan_completed' AND done.truck_unit=a.truck_unit AND done.created_at>=a.created_at)
        ORDER BY a.created_at DESC LIMIT 1""", truck_unit, load_id)
    return row["stop_event_id"] if row else None


async def record(kind, *, truck_unit=None, load_id=None, event_id=None, related_event_id=None,
                 details=None, key=None):
    payload = json.dumps(details or {}, sort_keys=True, default=str)
    if key is None:
        # Repeated holds/silent attempts are recorded once per five-minute window.
        window = int(datetime.now(timezone.utc).timestamp() // 300)
        key = hashlib.sha256(f"{kind}:{truck_unit}:{load_id}:{event_id}:{window}:{payload}".encode()).hexdigest()
    await execute("""
        INSERT INTO fuel_advice_audit
            (event_key, kind, truck_unit, load_id, stop_event_id, related_event_id, details)
        SELECT $1, $2, COALESCE($3,(SELECT truck_unit FROM stop_events WHERE id=$5)),
               COALESCE($4,(SELECT load_id FROM stop_events WHERE id=$5)), $5, $6, $7::jsonb
        ON CONFLICT (event_key) DO NOTHING
    """, key, kind, truck_unit, load_id, event_id, related_event_id, payload)


async def expire_pending(event_id, reason, *, details=None):
    """Preserve the old plan and write its transition in the same statement."""
    await execute("""
        WITH changed AS (
            UPDATE stop_events SET status='expired', resolved_at=NOW()
            WHERE id=$1 AND status='pending'
            RETURNING id, truck_unit, load_id
        )
        INSERT INTO fuel_advice_audit
            (event_key, kind, truck_unit, load_id, stop_event_id, details)
        SELECT 'expired:' || id, 'plan_expired', truck_unit, load_id, id, $2::jsonb
        FROM changed ON CONFLICT (event_key) DO NOTHING
    """, event_id, json.dumps({"reason": reason, **(details or {})}))
