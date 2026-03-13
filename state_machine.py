"""
state_machine.py  -  Core truck state logic.

STATES:
  HEALTHY          — fuel > 50%, poll every 30 min
  WATCH            — 35–50%, poll every 15 min
  CRITICAL_MOVING  — ≤35% and moving, poll every 10 min, alert fired
  CRITICAL_PARKED  — ≤35% and parked, poll every 60 min, alert fired once
  IN_YARD          — ignored entirely, poll every 30 min

ALERT RULES (moving):
  - First alert fires immediately when fuel drops below threshold
  - Re-alert every 30 min (ADVISORY/WARNING) or 10 min (CRITICAL/EMERGENCY)
  - Re-alert immediately on tier escalation or 5%+ fuel drop
  - Previous alert message deleted before new one sent

ALERT RULES (parked):
  - One alert fires immediately
  - Re-alert only if fuel drops 5%+ OR truck moves 1+ mile
  - Same spot + same fuel = silent

REFUEL DETECTION:
  - Fuel jumps 5%+ → refueled → close alert, send confirmation
  - Works for both sleeping and moving trucks
"""

import logging
from datetime import datetime, timedelta, timezone

from config import (
    FUEL_ALERT_THRESHOLD_PCT,
    POLL_INTERVAL_HEALTHY,
    POLL_INTERVAL_WATCH,
    POLL_INTERVAL_CRITICAL_MOVING,
    POLL_INTERVAL_CRITICAL_PARKED,
    DEFAULT_TANK_GAL,
    DEFAULT_MPG,
    DISPATCHER_GROUP_ID,
)
from yard_geofence import is_in_yard, get_yard_name
from truck_stop_finder import find_best_stops, calc_savings, get_urgency, find_current_stop, haversine_miles
from california import (
    should_send_ca_reminder,
    should_reset_ca_reminder,
    get_ca_avg_diesel_price,
    _dist_to_ca_border,
)
from telegram_bot import (
    send_low_fuel_alert,
    send_at_stop_alert,
    delete_message,
    send_ca_border_reminder,
    send_refueled_alert,
    send_left_yard_low_fuel,
)
from database import (
    create_fuel_alert,
    resolve_alert,
    get_truck_config,
    get_all_diesel_stops,
)

log = logging.getLogger(__name__)

_MOVING_MPH      = 5     # below this = parked
_REFUEL_PCT      = 5.0   # fuel rise that triggers refuel detection
_PARKED_MOVE_MI  = 1.0   # miles moved to reset parked state
_ALERT_FUEL_DROP = 5.0   # fuel drop to force re-alert


def _utcnow():
    return datetime.now(timezone.utc)

def _next_poll(minutes):
    return _utcnow() + timedelta(minutes=minutes)

def _tz(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# -- State skeleton -----------------------------------------------------------

def _new_state(vid, data):
    return {
        "vehicle_id":             vid,
        "vehicle_name":           data["vehicle_name"],
        "state":                  "UNKNOWN",
        "fuel_pct":               data["fuel_pct"],
        "lat":                    data["lat"],
        "lng":                    data["lng"],
        "speed_mph":              data["speed_mph"],
        "heading":                data["heading"],
        "next_poll":              _utcnow(),
        "parked_since":           None,
        "alert_sent":             False,
        "overnight_alert_sent":   False,
        "open_alert_id":          None,
        "assigned_stop_id":       None,
        "assigned_stop_name":     None,
        "assigned_stop_lat":      None,
        "assigned_stop_lng":      None,
        "assignment_time":        None,
        "in_yard":                False,
        "yard_name":              None,
        "sleeping":               False,
        "fuel_when_parked":       None,
        "ca_reminder_sent":       False,
        "last_alert_time":        None,
        "last_alert_urgency":     None,
        "last_alert_fuel":        None,
        "last_alert_lat":         None,
        "last_alert_lng":         None,
        "last_alerted_fuel":      None,
        "prev_truck_group":       None,
        "prev_truck_msg_id":      None,
        "prev_dispatcher_msg_id": None,
    }


def _clear_alert(state):
    state["open_alert_id"]        = None
    state["assigned_stop_id"]     = None
    state["assigned_stop_name"]   = None
    state["assigned_stop_lat"]    = None
    state["assigned_stop_lng"]    = None
    state["assignment_time"]      = None
    state["alert_sent"]           = False
    state["overnight_alert_sent"] = False
    state["fuel_when_parked"]     = None
    state["sleeping"]             = False
    state["last_alert_time"]      = None
    state["last_alert_urgency"]   = None
    state["last_alert_fuel"]      = None
    state["last_alert_lat"]       = None
    state["last_alert_lng"]       = None
    state["last_alerted_fuel"]    = None


def _get_truck_params(vehicle_name: str) -> tuple[float, float]:
    config = get_truck_config(vehicle_name)
    if config:
        return float(config["tank_capacity_gal"]), float(config["avg_mpg"])
    return DEFAULT_TANK_GAL, DEFAULT_MPG


def _get_state_code(lat: float, lng: float) -> str | None:
    if 35.0 <= lat <= 42.0 and -120.1 <= lng <= -113.9:
        return "NV"
    if 31.3 <= lat <= 37.0 and -115.0 <= lng <= -109.0:
        return "AZ"
    if 41.9 <= lat <= 46.3 and -124.6 <= lng <= -116.3:
        return "OR"
    if 32.5 <= lat <= 42.0 and -124.5 <= lng <= -114.1:
        return "CA"
    return None


# -- Main entry point ---------------------------------------------------------

def process_truck(vid, prev_state, current_data, truck_states):
    fuel    = current_data["fuel_pct"]
    speed   = current_data["speed_mph"]
    lat     = current_data["lat"]
    lng     = current_data["lng"]
    heading = current_data["heading"]
    vname   = current_data["vehicle_name"]

    if vid not in truck_states:
        truck_states[vid] = _new_state(vid, current_data)

    state = truck_states[vid]

    # Update live fields
    state["vehicle_name"] = vname
    state["fuel_pct"]     = fuel
    state["lat"]          = lat
    state["lng"]          = lng
    state["speed_mph"]    = speed
    state["heading"]      = heading

    moving = speed > _MOVING_MPH
    tank_gal, mpg = _get_truck_params(vname)

    log.info(f"  {vname}: fuel={fuel:.1f}%  speed={speed:.0f}mph  "
             f"state={state.get('state','NEW')}  sleeping={state.get('sleeping',False)}")

    # ══════════════════════════════════════════════════════════════════════════
    # 1. YARD CHECK — always first, silences everything
    # ══════════════════════════════════════════════════════════════════════════
    in_yard_now = is_in_yard(lat, lng)
    was_in_yard = state.get("in_yard", False)

    if in_yard_now:
        yard_name = get_yard_name(lat, lng)
        if not was_in_yard:
            log.info(f"  {vname}: entered yard: {yard_name}")
        state.update({
            "in_yard": True, "yard_name": yard_name,
            "state": "IN_YARD", "next_poll": _next_poll(30),
        })
        return

    if was_in_yard and not in_yard_now:
        yard_name = state.get("yard_name", "yard")
        log.info(f"  {vname}: left {yard_name} at {fuel:.1f}% fuel")
        state.update({"in_yard": False, "yard_name": None})
        if fuel <= FUEL_ALERT_THRESHOLD_PCT:
            send_left_yard_low_fuel(vname, fuel, yard_name)
            _fire_alert(vid, state, current_data, tank_gal, mpg, state_code="")
            return

    # ══════════════════════════════════════════════════════════════════════════
    # 2. CALIFORNIA BORDER REMINDER (checked every poll, independent of fuel)
    # ══════════════════════════════════════════════════════════════════════════
    state_code = _get_state_code(lat, lng)

    if should_reset_ca_reminder(state_code or "", fuel, heading,
                                 state.get("ca_reminder_sent", False)):
        log.info(f"  {vname}: CA reminder reset (state={state_code} fuel={fuel:.0f}%)")
        state["ca_reminder_sent"] = False

    if should_send_ca_reminder(state_code or "", lat, lng, heading,
                                fuel, state.get("ca_reminder_sent", False)):
        _fire_ca_reminder(state, current_data, tank_gal, mpg, state_code=state_code or "")

    # ══════════════════════════════════════════════════════════════════════════
    # 3. FUEL IS FINE
    # ══════════════════════════════════════════════════════════════════════════
    if fuel > FUEL_ALERT_THRESHOLD_PCT:
        if state.get("open_alert_id"):
            log.info(f"  {vname}: fuel recovered to {fuel:.1f}% — closing alert")
            resolve_alert(state["open_alert_id"])
            _clear_alert(state)

        if fuel > 50:
            state["state"]     = "HEALTHY"
            state["next_poll"] = _next_poll(POLL_INTERVAL_HEALTHY)
        else:
            state["state"]     = "WATCH"
            state["next_poll"] = _next_poll(
                POLL_INTERVAL_WATCH if moving else POLL_INTERVAL_HEALTHY
            )
        state["parked_since"] = None
        state["sleeping"]     = False
        return

    # ══════════════════════════════════════════════════════════════════════════
    # 4. FUEL IS LOW
    # ══════════════════════════════════════════════════════════════════════════
    was_sleeping = state.get("sleeping", False)

    # ── 4a. REFUEL CHECK (both moving and waking) ─────────────────────────────
    prev_fuel = state.get("fuel_pct", fuel)
    if fuel >= prev_fuel + _REFUEL_PCT:
        stop_name = state.get("assigned_stop_name") or "a fuel stop"
        log.info(f"  {vname}: refueled — {prev_fuel:.0f}%→{fuel:.0f}% at {stop_name}")
        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])
        send_refueled_alert(vname, stop_name, fuel)
        _clear_alert(state)
        state["state"]     = "HEALTHY" if fuel > FUEL_ALERT_THRESHOLD_PCT else "WATCH"
        state["next_poll"] = _next_poll(POLL_INTERVAL_HEALTHY)
        return

    # ── 4b. WOKE UP (was parked, now moving) ──────────────────────────────────
    if was_sleeping and moving:
        fuel_when_parked = state.get("fuel_when_parked") or fuel
        log.info(f"  {vname}: woke up — {fuel_when_parked:.1f}%→{fuel:.1f}%")
        state.update({
            "sleeping": False, "fuel_when_parked": None,
            "parked_since": None, "last_alerted_fuel": None,
        })
        # Fresh alert with current heading
        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])
        _clear_alert(state)
        state["state"]     = "CRITICAL_MOVING"
        state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
        return

    # ── 4c. MOVING + LOW FUEL ─────────────────────────────────────────────────
    if moving:
        state["state"]        = "CRITICAL_MOVING"
        state["next_poll"]    = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        state["parked_since"] = None
        state["sleeping"]     = False

        current_urgency = get_urgency(fuel)
        last_urgency    = state.get("last_alert_urgency")
        urgency_order   = {"ADVISORY": 0, "WARNING": 1, "CRITICAL": 2, "EMERGENCY": 3}

        tier_escalated = (
            last_urgency is not None and
            urgency_order.get(current_urgency, 0) > urgency_order.get(last_urgency, 0)
        )

        # Time since last alert
        last_alert_time = _tz(state.get("last_alert_time"))
        minutes_since   = (
            (_utcnow() - last_alert_time).total_seconds() / 60
            if last_alert_time else 9999
        )
        # 30% - 20%: every 30 min | 20% - 10%: every 10 min
        time_threshold  = 10 if fuel <= 20 else 30
        time_elapsed    = minutes_since >= time_threshold

        # Fuel drop since last alert
        last_alert_fuel = state.get("last_alert_fuel")
        fuel_dropped    = (
            last_alert_fuel is not None and
            fuel <= last_alert_fuel - _ALERT_FUEL_DROP
        )

        # Check if truck passed its assigned stop without stopping
        passed_assigned_stop = False
        assigned_lat = state.get("assigned_stop_lat")
        assigned_lng = state.get("assigned_stop_lng")
        if state.get("alert_sent") and assigned_lat and assigned_lng:
            dist_to_stop = haversine_miles(lat, lng, assigned_lat, assigned_lng)
            assignment_time = _tz(state.get("assignment_time"))
            # If >10 miles past assigned stop AND at least 15 min since assignment
            minutes_since_assign = (
                (_utcnow() - assignment_time).total_seconds() / 60
                if assignment_time else 0
            )
            if dist_to_stop > 10 and minutes_since_assign > 15:
                passed_assigned_stop = True
                log.info(f"  {vname}: passed assigned stop ({dist_to_stop:.1f} mi away) — finding next stop")
                # Clear assignment so next alert picks a fresh stop
                state["assigned_stop_id"]   = None
                state["assigned_stop_name"] = None
                state["assigned_stop_lat"]  = None
                state["assigned_stop_lng"]  = None
                state["assignment_time"]    = None

        should_alert = (
            not state.get("alert_sent")
            or tier_escalated
            or time_elapsed
            or fuel_dropped
            or passed_assigned_stop
        )

        if should_alert:
            if state.get("alert_sent"):
                if passed_assigned_stop:
                    reason = "passed assigned stop"
                elif tier_escalated:
                    reason = f"tier {last_urgency}→{current_urgency}"
                elif fuel_dropped:
                    reason = f"fuel dropped {last_alert_fuel:.0f}%→{fuel:.0f}%"
                else:
                    reason = f"{minutes_since:.0f}min since last alert"
                log.info(f"  {vname}: re-alert — {reason}")
            _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
            state["last_alert_urgency"] = current_urgency
            state["last_alert_time"]    = _utcnow()
            state["last_alert_lat"]     = lat
            state["last_alert_lng"]     = lng
            state["last_alert_fuel"]    = fuel
        else:
            log.info(f"  {vname}: moving, skipping alert — "
                     f"{minutes_since:.0f}min ago, urgency={current_urgency}")
        return

    # ── 4d. PARKED + LOW FUEL ─────────────────────────────────────────────────
    was_parked   = state.get("parked_since") is not None
    last_park_lat = state.get("last_alert_lat")
    last_park_lng = state.get("last_alert_lng")

    # Check if truck moved to a new spot since last alert
    if was_parked and last_park_lat and last_park_lng:
        moved = haversine_miles(last_park_lat, last_park_lng, lat, lng)
        if moved > _PARKED_MOVE_MI:
            log.info(f"  {vname}: re-parked at new spot ({moved:.1f}mi) — reset sleep")
            state["parked_since"]         = None
            state["overnight_alert_sent"] = False
            state["last_alerted_fuel"]    = None
            state["sleeping"]             = False

    if not state.get("parked_since"):
        state["parked_since"]     = _utcnow()
        state["fuel_when_parked"] = fuel
        log.info(f"  {vname}: parked at {fuel:.1f}% — sleep mode")

    state["state"]    = "CRITICAL_PARKED"
    state["sleeping"] = True

    # Poll fast initially to confirm parked, then slow down
    parked_since   = _tz(state.get("parked_since"))
    parked_minutes = (
        (_utcnow() - parked_since).total_seconds() / 60
        if parked_since else 0
    )
    state["next_poll"] = _next_poll(
        POLL_INTERVAL_CRITICAL_MOVING if parked_minutes < 30
        else POLL_INTERVAL_CRITICAL_PARKED
    )

    already_alerted   = state.get("overnight_alert_sent", False)
    last_alerted_fuel = state.get("last_alerted_fuel")

    fuel_dropped = (
        last_alerted_fuel is not None and
        fuel <= last_alerted_fuel - _ALERT_FUEL_DROP
    )

    moved_since_alert = 0.0
    if last_park_lat and last_park_lng:
        moved_since_alert = haversine_miles(last_park_lat, last_park_lng, lat, lng)
    location_changed = moved_since_alert >= _PARKED_MOVE_MI

    if not already_alerted or fuel_dropped or location_changed:
        if already_alerted:
            reason = (
                f"fuel dropped {last_alerted_fuel:.0f}%→{fuel:.0f}%"
                if fuel_dropped else f"moved {moved_since_alert:.1f}mi"
            )
            log.info(f"  {vname}: parked re-alert — {reason}")
        _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
        state["overnight_alert_sent"] = True
        state["last_alerted_fuel"]    = fuel
        state["last_alert_lat"]       = lat
        state["last_alert_lng"]       = lng
    else:
        log.info(f"  {vname}: parked, skipping — fuel={fuel:.1f}% unchanged, same spot")


# -- Alert firing -------------------------------------------------------------

def _fire_alert(vid, state, data, tank_gal, mpg, state_code=""):
    """Find best stops and send Telegram alert. Deletes previous alert first."""
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    speed   = data["speed_mph"]
    heading = data["heading"]

    # Correct heading from movement if GPS heading looks unreliable
    prev_lat = state.get("lat")
    prev_lng = state.get("lng")
    if (prev_lat and prev_lng and speed > 10 and
            (abs(lat - prev_lat) > 0.001 or abs(lng - prev_lng) > 0.001)):
        from truck_stop_finder import bearing as calc_bearing
        real_heading = calc_bearing(prev_lat, prev_lng, lat, lng)
        log.info(f"  {vname}: heading corrected {heading:.0f}°→{real_heading:.0f}°")
        heading = real_heading

    log.info(f"  {vname}: firing alert — fuel={fuel:.1f}% heading={heading:.0f}°")

    # Delete previous messages before sending new ones
    prev_truck_group       = state.get("prev_truck_group")
    prev_truck_msg_id      = state.get("prev_truck_msg_id")
    prev_dispatcher_msg_id = state.get("prev_dispatcher_msg_id")

    if prev_truck_group and prev_truck_msg_id:
        delete_message(prev_truck_group, prev_truck_msg_id)
        log.info(f"  {vname}: deleted prev truck alert {prev_truck_msg_id}")

    if DISPATCHER_GROUP_ID and prev_dispatcher_msg_id:
        delete_message(DISPATCHER_GROUP_ID, prev_dispatcher_msg_id)
        log.info(f"  {vname}: deleted prev dispatcher alert {prev_dispatcher_msg_id}")

    # Check if truck is already parked at a fuel stop
    current_stop = find_current_stop(lat, lng) if speed <= 10 else None
    if current_stop:
        log.info(f"  {vname}: already at {current_stop['store_name']} — sending at-stop alert")
        result = send_at_stop_alert(
            vehicle_name=vname,
            fuel_pct=fuel,
            truck_lat=lat,
            truck_lng=lng,
            current_stop=current_stop,
        )
        if isinstance(result, dict):
            state["prev_truck_group"]       = result.get("truck_group")
            state["prev_truck_msg_id"]      = result.get("truck_msg_id")
            state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
        state["alert_sent"] = True
        return

    # Find best stops and calculate savings
    best, alt = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg, truck_state=state_code or "")
    savings   = calc_savings(best, alt) if best and alt else None

    # Log to DB
    alert_id = create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="low_fuel",
        best_stop=best, alt_stop=alt, savings_usd=savings,
    )
    state["open_alert_id"] = alert_id

    if best:
        state["assigned_stop_id"]   = best["id"]
        state["assigned_stop_name"] = best["store_name"]
        state["assigned_stop_lat"]  = float(best["latitude"])
        state["assigned_stop_lng"]  = float(best["longitude"])
        state["assignment_time"]    = _utcnow()

    # Send alert and track message IDs for future deletion
    result = send_low_fuel_alert(
        vehicle_name=vname,
        fuel_pct=fuel,
        truck_lat=lat,
        truck_lng=lng,
        heading=heading,
        speed_mph=speed,
        best_stop=best,
        alt_stop=alt,
        savings_usd=savings,
    )

    if isinstance(result, dict):
        state["prev_truck_group"]       = result.get("truck_group")
        state["prev_truck_msg_id"]      = result.get("truck_msg_id")
        state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")

    state["alert_sent"] = True


def _fire_ca_reminder(state, data, tank_gal, mpg, state_code=""):
    """Send California border reminder."""
    vid     = state.get("vehicle_id")
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    heading = data["heading"]
    speed   = data["speed_mph"]

    log.info(f"  {vname}: sending CA border reminder")

    best, _     = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg, truck_state=state_code or "")
    all_stops   = get_all_diesel_stops()
    ca_avg      = get_ca_avg_diesel_price(all_stops)
    dist_border = _dist_to_ca_border(lat, lng)

    # Delete previous CA alert before sending new one
    prev_ca_truck      = state.get("prev_ca_truck_msg_id")
    prev_ca_dispatcher = state.get("prev_ca_dispatcher_msg_id")
    truck_group        = state.get("truck_group")
    if prev_ca_truck and truck_group:
        delete_message(truck_group, prev_ca_truck)
    if prev_ca_dispatcher and DISPATCHER_GROUP_ID:
        delete_message(str(DISPATCHER_GROUP_ID), prev_ca_dispatcher)

    result = send_ca_border_reminder(
        vehicle_name=vname,
        fuel_pct=fuel,
        truck_lat=lat,
        truck_lng=lng,
        best_stop=best,
        ca_avg_price=ca_avg,
        dist_to_border=dist_border,
    )

    state["prev_ca_truck_msg_id"]      = result.get("truck_msg_id")
    state["prev_ca_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
    state["truck_group"]               = result.get("truck_group")
    state["ca_reminder_sent"] = True

    create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="ca_border", best_stop=best,
    )