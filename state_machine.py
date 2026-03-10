"""
state_machine.py  -  Core truck state logic.

ALERT LOGIC:
  MOVING + low fuel:
    → Find best 2 stops (true cost scored) → send alert once per trip leg
    → Poll every 10 min

  PARKED + low fuel (sleeping):
    → Send ONE alert so dispatcher knows
    → Do NOT re-alert while truck stays parked
    → Poll every 60 min

  TRUCK WAKES UP (was parked, now moving):
    → Fuel went UP 5%+  → refueled → close alert, send confirmation
    → Fuel still low    → fresh alert with current heading

  YARD trucks:
    → Zero alerts, zero checks, always ignored

  CALIFORNIA BORDER:
    → Checked on every poll for trucks in NV/AZ/OR heading west
    → One reminder per approach, resets when truck fills up or crosses
"""

import logging
from datetime import datetime, timedelta, timezone

from config import (
    FUEL_ALERT_THRESHOLD_PCT,
    POLL_INTERVAL_HEALTHY,
    POLL_INTERVAL_WATCH,
    POLL_INTERVAL_CRITICAL_MOVING,
    POLL_INTERVAL_CRITICAL_PARKED,
    VISIT_RADIUS_MILES,
    DEFAULT_TANK_GAL,
    DEFAULT_MPG,
    MIN_SAVINGS_DISPLAY,
    DISPATCHER_GROUP_ID,
)
from yard_geofence import is_in_yard, get_yard_name
from truck_stop_finder import find_best_stops, calc_savings, is_near_stop, get_urgency, find_current_stop, find_cheaper_nearby
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

_MOVING_MPH = 5


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
        "vehicle_id":           vid,
        "vehicle_name":         data["vehicle_name"],
        "state":                "UNKNOWN",
        "fuel_pct":             data["fuel_pct"],
        "lat":                  data["lat"],
        "lng":                  data["lng"],
        "speed_mph":            data["speed_mph"],
        "heading":              data["heading"],
        "next_poll":            _utcnow(),
        "parked_since":         None,
        "alert_sent":           False,
        "overnight_alert_sent": False,
        "open_alert_id":        None,
        "assigned_stop_id":     None,
        "assigned_stop_name":   None,
        "assigned_stop_lat":    None,
        "assigned_stop_lng":    None,
        "assignment_time":      None,
        "in_yard":              False,
        "yard_name":            None,
        "sleeping":             False,
        "fuel_when_parked":     None,
        "ca_reminder_sent":     False,
        "last_alerted_fuel":    None,
        "last_alert_urgency":   None,
        "last_alert_fuel":      None,
        "last_alert_lat":       None,
        "last_alert_lng":       None,
        "prev_truck_group":     None,
        "prev_truck_msg_id":    None,
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
    state["last_alert_lat"]       = None
    state["last_alert_lng"]       = None
    state["last_alert_fuel"]      = None
    state["last_alert_urgency"]   = None


def _get_truck_params(vehicle_name: str) -> tuple[float, float]:
    """Return (tank_gal, mpg) for this truck. Falls back to defaults."""
    config = get_truck_config(vehicle_name)
    if config:
        return float(config["tank_capacity_gal"]), float(config["avg_mpg"])
    return DEFAULT_TANK_GAL, DEFAULT_MPG


def _get_state_code(lat: float, lng: float) -> str | None:
    """
    Rough state detection from lat/lng bounding boxes.
    Good enough for CA border check — not a full geocoder.
    """
    # Nevada
    if 35.0 <= lat <= 42.0 and -120.0 <= lng <= -114.0:
        return "NV"
    # Arizona
    if 31.3 <= lat <= 37.0 and -114.8 <= lng <= -109.0:
        return "AZ"
    # Oregon
    if 41.9 <= lat <= 46.3 and -124.6 <= lng <= -116.5:
        return "OR"
    # California
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

    log.info(f"  {vname}: fuel={fuel:.1f}%  speed={speed:.0f}mph  "
             f"state={state.get('state','NEW')}  sleeping={state.get('sleeping',False)}")

    tank_gal, mpg = _get_truck_params(vname)

    # ══════════════════════════════════════════════════════════════════════════
    # 1. YARD CHECK — always first, silences everything
    # ══════════════════════════════════════════════════════════════════════════
    in_yard_now = is_in_yard(lat, lng)
    was_in_yard = state.get("in_yard", False)

    if in_yard_now:
        yard_name = get_yard_name(lat, lng)
        if not was_in_yard:
            log.info(f"  {vname} entered yard: {yard_name}")
        state.update({"in_yard": True, "yard_name": yard_name,
                      "state": "IN_YARD", "next_poll": _next_poll(30)})
        return

    if was_in_yard and not in_yard_now:
        yard_name = state.get("yard_name", "yard")
        log.info(f"  {vname} left {yard_name} at {fuel:.1f}% fuel")
        state.update({"in_yard": False, "yard_name": None})
        if fuel <= FUEL_ALERT_THRESHOLD_PCT:
            send_left_yard_low_fuel(vname, fuel, yard_name)
            _fire_alert(vid, state, current_data, tank_gal, mpg)
            return

    # ══════════════════════════════════════════════════════════════════════════
    # 2. CALIFORNIA BORDER REMINDER (check every poll, independent of fuel)
    # ══════════════════════════════════════════════════════════════════════════
    state_code = _get_state_code(lat, lng)

    # Reset reminder if truck filled up, crossed into CA, or turned around
    if should_reset_ca_reminder(state_code or "", fuel, heading,
                                 state.get("ca_reminder_sent", False)):
        log.info(f"  {vname}: CA reminder reset (state={state_code} fuel={fuel:.0f}%)")
        state["ca_reminder_sent"] = False

    # Send CA reminder if conditions met
    if should_send_ca_reminder(state_code or "", lat, lng, heading,
                                fuel, state.get("ca_reminder_sent", False)):
        _fire_ca_reminder(state, current_data, tank_gal, mpg)

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

    # ── 4a. WOKE UP (was parked, now moving) ─────────────────────────────────
    if was_sleeping and moving:
        fuel_when_parked = state.get("fuel_when_parked") or fuel
        log.info(f"  {vname}: woke up — was {fuel_when_parked:.1f}% now {fuel:.1f}%")

        state.update({"sleeping": False, "fuel_when_parked": None, "parked_since": None, "last_alerted_fuel": None})

        if fuel > fuel_when_parked + 5:
            # Fuel went up — refueled during sleep
            stop_name = state.get("assigned_stop_name") or "a fuel stop"
            log.info(f"  {vname}: refueled during sleep (+{fuel - fuel_when_parked:.1f}%)")
            if state.get("open_alert_id"):
                resolve_alert(state["open_alert_id"])
            send_refueled_alert(vname, stop_name, fuel)
            _clear_alert(state)
            state["state"]     = "HEALTHY" if fuel > FUEL_ALERT_THRESHOLD_PCT else "CRITICAL_MOVING"
            state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        else:
            # Still low — fresh alert with current heading
            log.info(f"  {vname}: woke up still low — fresh alert")
            if state.get("open_alert_id"):
                resolve_alert(state["open_alert_id"])
            _clear_alert(state)
            state["state"]     = "CRITICAL_MOVING"
            state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
            _fire_alert(vid, state, current_data, tank_gal, mpg)
        return

    # ── 4b. MOVING + LOW FUEL ────────────────────────────────────────────────
    if moving:
        state["state"]        = "CRITICAL_MOVING"
        state["next_poll"]    = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        state["parked_since"] = None

        # Check if truck refueled at assigned stop
        _check_refueled(state, current_data)

        # Fire alert on first time, OR if urgency tier got worse since last alert
        current_urgency = get_urgency(fuel)
        last_urgency    = state.get("last_alert_urgency")
        urgency_order   = {"ADVISORY": 0, "WARNING": 1, "CRITICAL": 2, "EMERGENCY": 3}
        tier_escalated  = (
            last_urgency is not None and
            urgency_order.get(current_urgency, 0) > urgency_order.get(last_urgency, 0)
        )

        # Re-alert if truck moved 50+ miles since last alert (new best stop likely)
        last_alert_lat = state.get("last_alert_lat")
        last_alert_lng = state.get("last_alert_lng")
        moved_since_alert = 0.0
        if last_alert_lat and last_alert_lng:
            from truck_stop_finder import haversine_miles
            moved_since_alert = haversine_miles(last_alert_lat, last_alert_lng, lat, lng)
        location_changed = moved_since_alert >= 30

        # Re-alert if fuel dropped 5%+ since last alert
        last_alert_fuel = state.get("last_alert_fuel")
        fuel_dropped = (
            last_alert_fuel is not None and
            fuel <= last_alert_fuel - 5
        )

        should_alert = (
            not state.get("alert_sent")
            or tier_escalated
            or location_changed
            or fuel_dropped
        )

        if should_alert:
            if not state.get("alert_sent"):
                reason = "first alert"
            elif tier_escalated:
                reason = f"tier {last_urgency}→{current_urgency}"
            elif fuel_dropped:
                reason = f"fuel dropped {last_alert_fuel:.0f}%→{fuel:.0f}%"
            else:
                reason = f"moved {moved_since_alert:.0f}mi"
            log.info(f"  {vname}: firing alert — {reason}")
            _fire_alert(vid, state, current_data, tank_gal, mpg)
            state["last_alert_urgency"] = current_urgency
            state["last_alert_lat"]     = lat
            state["last_alert_lng"]     = lng
            state["last_alert_fuel"]    = fuel
        return

    # ── 4c. PARKED + LOW FUEL (sleeping) ─────────────────────────────────────
    # Detect if truck re-parked at a new location
    prev_lat = state.get("lat")
    prev_lng = state.get("lng")
    was_parked = state.get("parked_since") is not None

    if was_parked and prev_lat and prev_lng:
        from truck_stop_finder import haversine_miles
        moved_miles = haversine_miles(prev_lat, prev_lng, lat, lng)
        if moved_miles > 1.0:  # increased from 0.5 to reduce GPS drift resets
            log.info(f"  {vname}: re-parked at new location ({moved_miles:.1f}mi) — reset sleep state")
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

    # Poll fast at first to confirm truck is really parked, then slow down
    parked_since   = _tz(state.get("parked_since"))
    parked_minutes = (_utcnow() - parked_since).total_seconds() / 60 if parked_since else 0

    if parked_minutes < 30:
        state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)  # 10 min
    else:
        state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_PARKED)  # 60 min

    already_alerted   = state.get("overnight_alert_sent", False)
    last_alerted_fuel = state.get("last_alerted_fuel")
    last_alert_lat    = state.get("last_alert_lat")
    last_alert_lng    = state.get("last_alert_lng")

    # Only re-alert if fuel dropped 5%+ OR truck moved 1+ mile since last alert
    fuel_dropped = (
        last_alerted_fuel is not None and
        fuel <= last_alerted_fuel - 5
    )

    moved_since_alert = 0.0
    if last_alert_lat and last_alert_lng:
        from truck_stop_finder import haversine_miles
        moved_since_alert = haversine_miles(last_alert_lat, last_alert_lng, lat, lng)
    location_changed = moved_since_alert >= 1.0

    if not already_alerted or fuel_dropped or location_changed:
        if already_alerted:
            reason = f"fuel dropped {last_alerted_fuel:.0f}%→{fuel:.0f}%" if fuel_dropped else f"moved {moved_since_alert:.1f}mi"
            log.info(f"  {vname}: parked re-alert — {reason}")
        _fire_alert(vid, state, current_data, tank_gal, mpg)
        state["overnight_alert_sent"] = True
        state["last_alerted_fuel"]    = fuel
        state["last_alert_lat"]       = lat
        state["last_alert_lng"]       = lng
    else:
        log.info(f"  {vname}: parked, skipping alert — fuel={fuel:.1f}% unchanged, same spot")


# -- Alert firing -------------------------------------------------------------

def _fire_alert(vid, state, data, tank_gal, mpg):
    """Find best 2 stops and send Telegram alert."""
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    speed   = data["speed_mph"]
    heading = data["heading"]

    # Use movement-based heading if GPS heading looks unreliable (slow speed)
    prev_lat = state.get("lat")
    prev_lng = state.get("lng")
    if (prev_lat and prev_lng and speed > 10 and
            (abs(lat - prev_lat) > 0.001 or abs(lng - prev_lng) > 0.001)):
        from truck_stop_finder import bearing as calc_bearing
        real_heading = calc_bearing(prev_lat, prev_lng, lat, lng)
        log.info(f"  {vname}: heading corrected {heading:.0f}°→{real_heading:.0f}° from movement")
        heading = real_heading

    log.info(f"  {vname}: firing alert — fuel={fuel:.1f}% heading={heading:.0f}°")

    # Delete previous alert messages from both groups before sending new ones
    prev_truck_group      = state.get("prev_truck_group")
    prev_truck_msg_id     = state.get("prev_truck_msg_id")
    prev_dispatcher_msg_id = state.get("prev_dispatcher_msg_id")

    if prev_truck_group and prev_truck_msg_id:
        delete_message(prev_truck_group, prev_truck_msg_id)
        log.info(f"  {vname}: deleted previous truck group alert {prev_truck_msg_id}")

    if DISPATCHER_GROUP_ID and prev_dispatcher_msg_id:
        delete_message(DISPATCHER_GROUP_ID, prev_dispatcher_msg_id)
        log.info(f"  {vname}: deleted previous dispatcher alert {prev_dispatcher_msg_id}")

    # Check if truck is already parked at a fuel stop
    current_stop = find_current_stop(lat, lng) if speed <= 10 else None

    if current_stop:
        log.info(f"  {vname}: already at {current_stop['store_name']} — checking nearby prices")
        cheaper = find_cheaper_nearby(lat, lng, current_stop, fuel, tank_gal, mpg)

        # Delete previous alert before sending new one
        if state.get("prev_truck_msg_id") and state.get("prev_truck_group"):
            delete_message(state["prev_truck_group"], state["prev_truck_msg_id"])
        if state.get("prev_dispatcher_msg_id"):
            delete_message(DISPATCHER_GROUP_ID, state["prev_dispatcher_msg_id"])

        result = send_at_stop_alert(
            vehicle_name=vname,
            fuel_pct=fuel,
            truck_lat=lat,
            truck_lng=lng,
            current_stop=current_stop,
            cheaper_stop=cheaper,
        )
        if isinstance(result, dict):
            state["prev_truck_group"]       = result.get("truck_group")
            state["prev_truck_msg_id"]      = result.get("truck_msg_id")
            state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
        state["alert_sent"] = True
        return

    best, alt = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg)

    savings = calc_savings(best, alt) if best and alt else None

    alert_id = create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="low_fuel",
        best_stop=best, alt_stop=alt, savings_usd=savings
    )
    state["open_alert_id"] = alert_id

    if best:
        state["assigned_stop_id"]   = best["id"]
        state["assigned_stop_name"] = best["store_name"]
        state["assigned_stop_lat"]  = float(best["latitude"])
        state["assigned_stop_lng"]  = float(best["longitude"])
        state["assignment_time"]    = _utcnow()

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

    # Track message IDs so we can delete them on next alert
    if isinstance(result, dict):
        state["prev_truck_group"]       = result.get("truck_group")
        state["prev_truck_msg_id"]      = result.get("truck_msg_id")
        state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")

    state["alert_sent"] = True


def _fire_ca_reminder(state, data, tank_gal, mpg):
    """Send California border reminder."""
    vid     = state.get("vehicle_id")
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    heading = data["heading"]
    speed   = data["speed_mph"]

    log.info(f"  {vname}: sending CA border reminder")

    best, _ = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg)

    # Get CA average price for comparison
    all_stops   = get_all_diesel_stops()
    ca_avg      = get_ca_avg_diesel_price(all_stops)
    dist_border = _dist_to_ca_border(lat, lng)

    send_ca_border_reminder(
        vehicle_name=vname,
        fuel_pct=fuel,
        truck_lat=lat,
        truck_lng=lng,
        best_stop=best,
        ca_avg_price=ca_avg,
        dist_to_border=dist_border,
    )

    state["ca_reminder_sent"] = True

    # Log as alert in DB
    create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="ca_border", best_stop=best
    )


def _check_refueled(state, data):
    """Check if truck refueled — at assigned stop OR any fuel stop."""
    fuel  = data["fuel_pct"]
    lat   = data["lat"]
    lng   = data["lng"]
    vname = data["vehicle_name"]

    alert_fuel = state.get("fuel_pct", fuel)

    # Fuel went up 5%+ anywhere — refueled regardless of location
    if fuel >= alert_fuel + 5:
        # Try to find which stop they're near
        current_stop = find_current_stop(lat, lng)
        stop_name = (
            current_stop["store_name"] if current_stop
            else state.get("assigned_stop_name", "a fuel stop")
        )
        log.info(f"  {vname}: refueled at {stop_name} — {alert_fuel:.0f}%→{fuel:.0f}%")
        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])
        send_refueled_alert(vname, stop_name, fuel)
        _clear_alert(state)
        return

    # Also check if near assigned stop but fuel hasn't updated yet (GPS lag)
    if state.get("assigned_stop_lat"):
        near = is_near_stop(lat, lng,
                             state["assigned_stop_lat"],
                             state["assigned_stop_lng"],
                             VISIT_RADIUS_MILES)
        if near:
            log.info(f"  {vname}: at assigned stop {state.get('assigned_stop_name')} — waiting for fuel update")
