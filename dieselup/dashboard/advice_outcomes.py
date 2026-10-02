"""One evidence projection for Fuel Advice and Driver Scores; no financial penalties."""
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import Depends, Query
from fastapi.encoders import jsonable_encoder

from dieselup.config import settings

Period = Literal["month", "week", "today", "90_days", "all"]
Outcome = Literal["visited", "missed", "pending", "unconfirmed"]


def period_start(period):
    now = datetime.now(timezone.utc)
    if period == "all":
        return datetime(2000, 1, 1, tzinfo=timezone.utc)
    if period == "90_days":
        return now-timedelta(days=90)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "today":
        return day
    if period == "week":
        return day-timedelta(days=day.weekday())
    return day.replace(day=1)


FACTS = """
WITH facts AS (
 SELECT se.*, se.gallons AS planned_gallons,
   se.candidates->0->>'station_name' AS station_name,
   se.candidates->0->>'city' AS station_city, se.candidates->0->>'state' AS station_state,
   se.candidates->0->>'fill_to_full'='true' AS fill_to_full,
   NULLIF(btrim(se.candidates->0->'plan'->'driver_at_advice'->>'name'),'') AS driver_name,
   se.candidates->0->'plan'->'driver_at_advice'->>'group_id' AS snapshot_group,
   COALESCE(se.candidates->0->'plan'->>'messaging_mode',a.creation_mode,'unknown') AS messaging_mode,
   COALESCE(se.candidates->0->'plan'->'route_evidence'->>'model'='remaining_route_v1'
       AND se.candidates->0->'plan'->'route_evidence'->>'trip_verified'='true'
       AND se.candidates->0->'plan'->'route_evidence'->>'complete_candidate_coverage'='true'
       AND se.candidates->0->'plan'->'route_evidence'->>'samsara_vehicle_id'=se.samsara_vehicle_id,FALSE) AS route_verified,
   a.delivered_at, a.missed_at, a.visit_at AS gps_visit_at,
   LEAST(a.visit_at,f.filled_at) AS visited_at,
   COALESCE(se.actual_gallons,f.detected_gallons) AS detected_gallons,
   f.filled_at IS NOT NULL AS fueling_confirmed,
   COALESCE(a.financial->>'price_status','pending') AS price_status,
   a.financial->>'actual_station_name' AS actual_station_name,
   a.financial->>'classification' AS fill_classification,
   (a.financial->>'extra_cost')::numeric AS extra_cost,
   (a.financial->>'saving')::numeric AS alternative_saving,
   (a.financial->>'planned_price')::numeric AS planned_price,
   (a.financial->>'actual_price')::numeric AS actual_price,
   a.financial->>'planned_price_date' AS planned_price_date,
   a.financial->>'actual_price_date' AS actual_price_date,
   a.financial->>'price_source' AS price_source,
   (a.financial->>'fueling_at')::timestamptz AS fueling_at,
   COALESCE(a.financial->>'fueling_confirmed'='true',FALSE) AS fueled_elsewhere,
   a.replacement_id, a.replaces_id, a.hold_reason
 FROM stop_events se
 LEFT JOIN LATERAL (
   SELECT min(created_at) FILTER (WHERE kind='stop_visited') AS visit_at,
     min(created_at) FILTER (WHERE (kind='missed_detected' AND details->>'method'='ordered_road_route')
       OR (kind='stop_lost' AND details->'bypass_evidence'->>'method'='ordered_road_route')) AS missed_at,
     (array_agg(details ORDER BY id DESC) FILTER (WHERE kind='stop_lost' AND details ? 'price_status'))[1] AS financial,
     min(created_at) FILTER (WHERE kind='message_sent' AND details->>'chat_id'=se.driver_id::text
       AND regexp_replace(details->>'alert_type','^(retry_)+','') IN ('briefing','approach','delivery')) AS delivered_at,
     max(details->>'messaging_mode') FILTER (WHERE kind='plan_created') AS creation_mode,
     max(related_event_id) FILTER (WHERE kind='replan_completed') AS replacement_id,
     max(related_event_id) FILTER (WHERE kind='plan_created') AS replaces_id,
     (array_agg(details->>'reason' ORDER BY id DESC)
       FILTER (WHERE kind IN ('monitor_held','replan_held') AND details ? 'reason'))[1] AS hold_reason
   FROM fuel_advice_audit WHERE stop_event_id=se.id
 ) a ON TRUE
 LEFT JOIN LATERAL (
   SELECT min(detected_at) AS filled_at,max(gallons) AS detected_gallons
   FROM fuel_events WHERE stop_event_id=se.id AND classification='recommended'
      AND site_id=se.recommended_site_id AND samsara_vehicle_id=se.samsara_vehicle_id
      AND gallons>=30 AND detected_at>=se.recommended_at
      AND se.candidates->0->'plan' ? 'driver_at_advice'
 ) f ON TRUE
 WHERE ($1::text IS NULL OR se.truck_unit=$1) AND se.recommended_at >= $2::timestamptz
   AND ($3::text IS NULL OR se.load_id=$3)
), outcomes AS (
 SELECT *, CASE WHEN visited_at IS NOT NULL THEN 'visited'
                 WHEN missed_at IS NOT NULL AND route_verified THEN 'missed'
                 WHEN status='pending' THEN 'pending' ELSE 'unconfirmed' END AS outcome,
    CASE WHEN driver_name IS NOT NULL AND snapshot_group=driver_id::text
         THEN md5(lower(driver_name) || ':' || snapshot_group) END AS driver_key,
    CASE WHEN visited_at IS NOT NULL THEN visited_at ELSE missed_at END AS outcome_at
 FROM facts
), projected AS (
 SELECT *, CASE
   WHEN messaging_mode='silent' THEN 'silent_test_advice'
   WHEN driver_key IS NULL THEN 'driver_identity_not_recorded'
   WHEN NOT route_verified THEN 'route_not_verified'
   WHEN messaging_mode<>'live' THEN 'message_mode_not_recorded'
   WHEN delivered_at IS NULL OR outcome_at IS NULL OR delivered_at>outcome_at THEN 'advice_not_delivered_before_outcome'
   WHEN outcome NOT IN ('visited','missed') THEN 'outcome_not_confirmed'
   ELSE NULL END AS scoring_exclusion,
   COALESCE(messaging_mode='live' AND route_verified AND driver_key IS NOT NULL
      AND delivered_at<=fueling_at AND fueled_elsewhere AND price_status='contracted_estimate',FALSE) AS cost_eligible
 FROM outcomes
)
"""

STOP_COLUMNS = """id,truck_unit,load_id,driver_name,driver_key,station_name,station_city,station_state,
    planned_gallons,fill_to_full,detected_gallons,fueling_confirmed,outcome,status,
    recommended_at,outcome_at,visited_at,missed_at,delivered_at,messaging_mode,
    replacement_id,replaces_id,hold_reason,scoring_exclusion,
    scoring_exclusion IS NULL AS scored,fueled_elsewhere,fill_classification,actual_station_name,
    price_status,extra_cost,alternative_saving,planned_price,actual_price,planned_price_date,
    actual_price_date,price_source,fueling_at,cost_eligible"""


def score(visited, missed):
    total = visited+missed
    return round(100*visited/total, 1) if total else None


def install_advice_outcomes(app, check_auth, get_pool):
    @app.get("/api/fuel-advice/stops")
    async def stops(truck_unit: str | None = Query(None, max_length=40),
                    load_id: str | None = Query(None, max_length=100),
                    driver_key: str | None = Query(None, pattern=r"^[0-9a-f]{32}$"),
                    outcome: Outcome | None = None, period: Period = "month",
                    before_id: int | None = Query(None, ge=1),
                    limit: int = Query(50, ge=1, le=200), _: None = Depends(check_auth)):
        pool = await get_pool()
        args = (truck_unit or None, period_start(period), load_id or None)
        rows = await pool.fetch(FACTS+f"""SELECT {STOP_COLUMNS} FROM projected
            WHERE ($4::text IS NULL OR driver_key=$4) AND ($5::text IS NULL OR outcome=$5)
              AND ($6::bigint IS NULL OR id<$6) ORDER BY id DESC LIMIT $7""",
            *args, driver_key, outcome, before_id, limit+1)
        counts = await pool.fetchrow(FACTS+"""SELECT count(*) AS total,
            count(*) FILTER (WHERE outcome='visited') AS visited,
            count(*) FILTER (WHERE outcome='missed') AS missed,
            count(*) FILTER (WHERE outcome='pending') AS pending,
            count(*) FILTER (WHERE outcome='unconfirmed') AS unconfirmed,
            count(*) FILTER (WHERE scoring_exclusion IS NULL) AS scored,
            count(*) FILTER (WHERE fueled_elsewhere AND price_status='pending') AS price_pending,
            COALESCE(sum(extra_cost) FILTER (WHERE price_status='contracted_estimate'),0) AS estimated_extra_cost
            FROM projected WHERE ($4::text IS NULL OR driver_key=$4)""", *args, driver_key)
        holds = await pool.fetch("""SELECT DISTINCT ON (truck_unit) truck_unit,load_id,kind,
            details->>'reason' AS reason,created_at FROM fuel_advice_audit
            WHERE kind IN ('plan_held','replan_held') AND created_at>=NOW()-INTERVAL '1 hour'
              AND ($1::text IS NULL OR truck_unit=$1) AND ($2::text IS NULL OR load_id=$2)
            ORDER BY truck_unit,created_at DESC LIMIT 20""", truck_unit or None, load_id or None)
        contexts=await pool.fetch("""SELECT a.id,a.truck_unit,a.load_id,a.stop_event_id,a.created_at,a.details
            FROM fuel_advice_audit a WHERE a.kind='stationary_no_fuel' AND a.created_at>=NOW()-INTERVAL '24 hours'
              AND ($1::text IS NULL OR a.truck_unit=$1) AND ($2::text IS NULL OR a.load_id=$2)
            ORDER BY a.id DESC LIMIT 40""",truck_unit or None,load_id or None) if not driver_key else []
        import json
        contexts=[{**dict(r),'details':json.loads(r['details']) if isinstance(r['details'],str) else r['details']} for r in contexts]
        page = [dict(r) for r in rows[:limit]]
        return jsonable_encoder({"stops":page,"summary":dict(counts),"non_fueling_stops":contexts,"holds":[dict(r) for r in holds] if not driver_key else [],
            "next_cursor":page[-1]["id"] if len(rows)>limit else None,"period":period,
            "messaging_mode":settings.TELEGRAM_MESSAGING_MODE,"sensor_gallons_estimated":True})

    @app.get("/api/driver-scores")
    async def scores(truck_unit: str | None = Query(None, max_length=40),
                     driver_key: str | None = Query(None, pattern=r"^[0-9a-f]{32}$"),
                     period: Period = "month", _: None = Depends(check_auth)):
        pool = await get_pool()
        args = (truck_unit or None, period_start(period), None, driver_key)
        rows = await pool.fetch(FACTS+"""SELECT driver_key,driver_name,
            array_agg(DISTINCT truck_unit ORDER BY truck_unit) AS truck_units,
            count(*) AS recommendations,
            count(*) FILTER (WHERE outcome='visited') AS observed_visited,
            count(*) FILTER (WHERE outcome='missed') AS observed_missed,
            count(*) FILTER (WHERE outcome='visited' AND scoring_exclusion IS NULL) AS visited,
            count(*) FILTER (WHERE outcome='missed' AND scoring_exclusion IS NULL) AS missed,
            count(*) FILTER (WHERE fueling_confirmed AND scoring_exclusion IS NULL) AS fueled,
            COALESCE(sum(extra_cost) FILTER (WHERE cost_eligible),0) AS estimated_extra_cost,
            COALESCE(sum(alternative_saving) FILTER (WHERE cost_eligible),0) AS estimated_alternative_saving,
            count(*) FILTER (WHERE fueled_elsewhere AND price_status='pending') AS price_pending,
            count(*) FILTER (WHERE scoring_exclusion IS NOT NULL) AS excluded,
            count(*) OVER() AS matched_groups
            FROM projected WHERE ($4::text IS NULL OR driver_key=$4)
            GROUP BY driver_key,driver_name,CASE WHEN driver_key IS NULL THEN truck_unit END
            ORDER BY count(*) FILTER (WHERE scoring_exclusion IS NULL) DESC,driver_name NULLS LAST
            LIMIT 500""", *args)
        totals = await pool.fetchrow(FACTS+"""SELECT
            count(*) FILTER (WHERE outcome='visited' AND scoring_exclusion IS NULL) AS visited,
            count(*) FILTER (WHERE outcome='missed' AND scoring_exclusion IS NULL) AS missed,
            count(*) FILTER (WHERE scoring_exclusion IS NOT NULL) AS excluded,
            COALESCE(sum(extra_cost) FILTER (WHERE cost_eligible),0) AS estimated_extra_cost,
            count(*) FILTER (WHERE fueled_elsewhere AND price_status='pending') AS price_pending
            FROM projected WHERE ($4::text IS NULL OR driver_key=$4)""", *args)
        drivers = []
        for row in rows:
            item = dict(row)
            item['score'] = score(item['visited'],item['missed'])
            item['scored_stops'] = item['visited']+item['missed']
            drivers.append(item)
        totals = dict(totals)
        totals['score'] = score(totals['visited'],totals['missed'])
        return jsonable_encoder({"drivers":drivers,"summary":totals,"period":period,
            "truncated":bool(rows and rows[0]['matched_groups']>500),
            "messaging_mode":settings.TELEGRAM_MESSAGING_MODE,
            "formula":"Visited / (Visited + Missed) × 100; only delivered, verified advice with a recorded driver identity"})
