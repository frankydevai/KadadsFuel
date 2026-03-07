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
)
from yard_geofence import is_in_yard, get_yard_name
from truck_stop_finder import find_best_stops, calc_savings, is_near_stop
from california import (
    should_send_ca_reminder,
    should_reset_ca_reminder,
    get_ca_avg_diesel_price,
    _dist_to_ca_border,
)
from telegram_bot import (
    send_low_fuel_alert,
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
        _fire_ca_reminder(state, current_data, tank_gal, mpg, state_code)

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

        state.update({"sleeping": False, "fuel_when_parked": None, "parked_since": None})

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

        # Fire alert only once per trip leg
        if not state.get("alert_sent"):
            _fire_alert(vid, state, current_data, tank_gal, mpg)
        return

    # ── 4c. PARKED + LOW FUEL (sleeping) ─────────────────────────────────────
    if not state.get("parked_since"):
        state["parked_since"]     = _utcnow()
        state["fuel_when_parked"] = fuel
        log.info(f"  {vname}: parked at {fuel:.1f}% — sleep mode")

    state["state"]     = "CRITICAL_PARKED"
    state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_PARKED)
    state["sleeping"]  = True

    # ONE alert only — do NOT re-alert while truck stays parked
    if not state.get("overnight_alert_sent"):
        _fire_alert(vid, state, current_data, tank_gal, mpg)
        state["overnight_alert_sent"] = True


# -- Alert firing -------------------------------------------------------------

def _fire_alert(vid, state, data, tank_gal, mpg):
    """Find best 2 stops and send Telegram alert."""
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    speed   = data["speed_mph"]
    heading = data["heading"]

    # Use movement-based heading if Samsara heading looks wrong
    prev_lat = state.get("lat")
    prev_lng = state.get("lng")
    if (prev_lat and prev_lng and speed > 10 and
            (abs(lat - prev_lat) > 0.001 or abs(lng - prev_lng) > 0.001)):
        from truck_stop_finder import bearing as calc_bearing
        real_heading = calc_bearing(prev_lat, prev_lng, lat, lng)
        log.info(f"  {vname}: heading corrected {heading:.0f}°→{real_heading:.0f}° from movement")
        heading = real_heading

    log.info(f"  {vname}: firing alert — fuel={fuel:.1f}%")

    best, alt = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg)

    # Already at a stop — no alert needed
    if best is None and alt is None and not data.get("_no_stop"):
        log.info(f"  {vname}: already at a stop, no alert")
        state["alert_sent"] = True
        return

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

    send_low_fuel_alert(
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

    state["alert_sent"] = True


def _fire_ca_reminder(state, data, tank_gal, mpg):
    """Send California border reminder."""
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
        data["vehicle_id"], vname, fuel, lat, lng, heading, speed,
        alert_type="ca_border", best_stop=best
    )


def _check_refueled(state, data):
    """Check if moving truck refueled at assigned stop."""
    if not state.get("assigned_stop_lat"):
        return

    fuel   = data["fuel_pct"]
    lat    = data["lat"]
    lng    = data["lng"]
    vname  = data["vehicle_name"]

    near = is_near_stop(lat, lng,
                         state["assigned_stop_lat"],
                         state["assigned_stop_lng"],
                         VISIT_RADIUS_MILES)

    # Only mark refueled if fuel actually went up (not just passing by)
    alert_fuel = state.get("fuel_pct", fuel)
    if near and fuel >= alert_fuel + 5:
        stop_name = state.get("assigned_stop_name", "fuel stop")
        log.info(f"  {vname}: refueled at {stop_name} — {alert_fuel:.0f}%→{fuel:.0f}%")
        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])
        send_refueled_alert(vname, stop_name, fuel)
        _clear_alert(state)
