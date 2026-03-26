"""
telegram_bot.py  -  Telegram message sending for FleetFuel bot.
"""

import time
import logging
import requests
from config import TELEGRAM_BOT_TOKEN, DISPATCHER_GROUP_ID, ADMIN_CHAT_ID, MIN_SAVINGS_DISPLAY

log = logging.getLogger(__name__)

force_check_now: bool = False
BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def _post(method: str, payload: dict, retries: int = 4) -> dict | None:
    for attempt in range(retries + 1):
        try:
            resp = requests.post(f"{BASE_URL}/{method}", json=payload, timeout=10)
            if resp.status_code == 429:
                wait = max(resp.json().get("parameters", {}).get("retry_after", 5), 5)
                wait *= (attempt + 1)
                log.warning(f"Telegram 429 — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error(f"Telegram {method} failed (attempt {attempt+1}): {exc}")
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
    return None


def _send_to(chat_id: str, text: str) -> int | None:
    if not chat_id:
        return None
    result = _post("sendMessage", {
        "chat_id": chat_id, "text": text,
        "parse_mode": "Markdown", "disable_web_page_preview": True,
    })
    if result and result.get("ok"):
        return result["result"]["message_id"]
    import re
    plain = re.sub(r"[*_`\[\]]", "", text)
    result2 = _post("sendMessage", {"chat_id": chat_id, "text": plain, "disable_web_page_preview": True})
    if result2 and result2.get("ok"):
        return result2["result"]["message_id"]
    return None


def _send_to_truck(vehicle_name: str, text: str) -> dict:
    from database import get_truck_group
    truck_group = get_truck_group(vehicle_name)
    truck_msg_id = None
    dispatcher_msg_id = None
    if truck_group:
        truck_msg_id = _send_to(truck_group, text)
    else:
        log.info(f"No group set for {vehicle_name} — dispatcher only")
    if DISPATCHER_GROUP_ID and truck_group != str(DISPATCHER_GROUP_ID):
        dispatcher_msg_id = _send_to_dispatcher(text)
    return {"truck_group": truck_group, "truck_msg_id": truck_msg_id, "dispatcher_msg_id": dispatcher_msg_id}


def delete_message(chat_id: str, message_id: int) -> bool:
    result = _post("deleteMessage", {"chat_id": chat_id, "message_id": message_id}, retries=0)
    return bool(result and result.get("ok"))


def _send_to_dispatcher(text: str) -> int | None:
    if not DISPATCHER_GROUP_ID:
        return None
    return _send_to(DISPATCHER_GROUP_ID, text)


def _compass(heading: float) -> str:
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(heading / 22.5) % 16]


def _urgency_emoji(fuel_pct: float) -> str:
    if fuel_pct <= 10: return "🚨"
    if fuel_pct <= 15: return "🔴"
    if fuel_pct <= 25: return "🟠"
    return "🟡"


def send_low_fuel_alert(vehicle_name, fuel_pct, truck_lat, truck_lng,
                        heading, speed_mph, best_stop, alt_stop, savings_usd) -> dict:
    emoji     = _urgency_emoji(fuel_pct)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    compass   = _compass(heading)
    lines = [
        f"{emoji} *Low Fuel Alert — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*   🧭 {speed_mph:.0f} mph {compass}",
        f"📍 [View on Map]({truck_url})",
    ]
    if best_stop:
        name = best_stop.get("store_name", "Unknown")
        addr = ", ".join(filter(None, [best_stop.get("address",""), best_stop.get("city",""),
                                        best_stop.get("state",""), best_stop.get("zip","")]))
        dist  = best_stop.get("distance_miles", 0)
        price = best_stop.get("diesel_price")
        lat   = best_stop.get("latitude")
        lng   = best_stop.get("longitude")
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None
        lines += ["", f"⛽ *{name}*", f"📌 {addr}", f"🛣 {dist:.1f} mi away",
                  f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A"]
        if alt_stop and price and alt_stop.get("diesel_price"):
            np = alt_stop["diesel_price"]
            nd = alt_stop.get("distance_miles", 0)
            nn = alt_stop.get("store_name", "nearest stop")
            diff = np - price
            if diff > 0.01:
                from config import DEFAULT_TANK_GAL, SAFETY_RESERVE
                gal = round(DEFAULT_TANK_GAL * (1 - fuel_pct / 100) * (1 - SAFETY_RESERVE))
                sav = round(diff * gal, 2)
                lines.append(f"💵 Saves *${diff:.2f}/gal × {gal} gal = ${sav:.0f}* vs {nn} ({nd:.1f} mi, ${np:.3f}/gal)")
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        lines += ["", "No diesel stops found within range.", "Dispatcher notified."]
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — {fuel_pct:.0f}% — NO STOP FOUND")
    if fuel_pct <= 15 and best_stop:
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — critically low at {fuel_pct:.0f}%")
    result = _send_to_truck(vehicle_name, "\n".join(lines))
    return result if isinstance(result, dict) else {"truck_group": None, "truck_msg_id": result, "dispatcher_msg_id": None}


def send_ca_border_reminder(vehicle_name, fuel_pct, truck_lat, truck_lng,
                             best_stop, ca_avg_price, dist_to_border):
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    lines = [
        f"🌵 *California Border Ahead — Truck {vehicle_name}*",
        f"🛣 {dist_to_border:.0f} miles to CA border",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
        f"📍 [Truck Location]({truck_url})", "",
        f"💡 *Fill up before crossing — diesel is ~$1/gal more expensive in CA!*",
    ]
    if best_stop:
        addr = ", ".join(filter(None, [best_stop.get("address",""), best_stop.get("city",""),
                                        best_stop.get("state",""), best_stop.get("zip","")]))
        price = best_stop.get("diesel_price")
        lat = best_stop.get("latitude"); lng = best_stop.get("longitude")
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None
        lines += ["", f"⛽ *{best_stop.get('store_name','')}*", f"📌 {addr}",
                  f"🛣 {best_stop.get('distance_miles',0):.1f} mi away",
                  f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A"]
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_at_stop_alert(vehicle_name, fuel_pct, truck_lat, truck_lng, current_stop) -> dict:
    emoji     = _urgency_emoji(fuel_pct)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    name      = current_stop.get("store_name", "Fuel Stop")
    address   = ", ".join(filter(None, [current_stop.get("address",""), current_stop.get("city",""),
                                         current_stop.get("state",""), current_stop.get("zip","")]))
    price     = current_stop.get("diesel_price")
    slat      = current_stop.get("latitude"); slng = current_stop.get("longitude")
    maps_url  = f"https://maps.google.com/?q={slat},{slng}" if slat and slng else None
    lines = [
        f"{emoji} *Low Fuel Alert — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
        f"📍 [View on Map]({truck_url})", "",
        f"🅿️ *Already stopped at:*",
        f"⛽ *{name}*", f"📌 {address}",
        f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A",
    ]
    if maps_url:
        lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_refueled_alert(vehicle_name, stop_name, fuel_pct,
                         truck_lat=None, truck_lng=None, actual_stop=None):
    """Send refuel confirmation showing where truck actually fueled."""
    lines = [
        f"✅ *REFUELED — Truck {vehicle_name}*",
        f"⛽ Fuel now: *{fuel_pct:.0f}%*",
    ]

    # Show actual stop if we detected it
    if actual_stop and actual_stop.get("store_name"):
        name    = actual_stop["store_name"]
        address = ", ".join(filter(None, [
            actual_stop.get("address",""), actual_stop.get("city",""),
            actual_stop.get("state",""), actual_stop.get("zip",""),
        ]))
        price   = actual_stop.get("diesel_price")
        slat    = actual_stop.get("latitude")
        slng    = actual_stop.get("longitude")
        maps_url = f"https://maps.google.com/?q={slat},{slng}" if slat and slng else None
        lines += [
            f"🏪 *Fueled at:* {name}",
            f"📌 {address}",
        ]
        if price:
            lines.append(f"💰 Diesel: ${price:.3f}/gal")
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        # Fallback — show GPS location
        lines.append(f"🏪 *Fueled at:* {stop_name}")
        if truck_lat and truck_lng:
            maps_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
            lines.append(f"🗺 [View location]({maps_url})")

    _send_to_truck(vehicle_name, "\n".join(lines))
    # Also notify dispatcher
    _send_to_dispatcher(f"✅ *{vehicle_name}* refueled at {stop_name} — {fuel_pct:.0f}% fuel")


def send_left_yard_low_fuel(vehicle_name, fuel_pct, yard_name):
    text = f"🏠 *LEFT YARD — LOW FUEL*\n🚛 *Truck:* {vehicle_name}\n⛽ *Fuel:* {fuel_pct:.0f}%\n📍 *Departed:* {yard_name}"
    _send_to_truck(vehicle_name, text)
    _send_to_dispatcher(f"🏠 *{vehicle_name}* left {yard_name} with {fuel_pct:.0f}% fuel.")


def register_commands():
    commands = [
        {"command": "checknow",    "description": "Force immediate fuel check"},
        {"command": "findstop",    "description": "Find cheapest stops — /findstop 0792"},
        {"command": "route",       "description": "Show active load — /route 0792"},
        {"command": "findload",    "description": "Search QM trip — /findload 8656"},
        {"command": "resetpilot",  "description": "Wipe Pilot DB rows"},
        {"command": "dbstats",     "description": "Show DB stats"},
        {"command": "addtruck",    "description": "Add truck — /addtruck 4821 -100123456"},
        {"command": "setgroup",    "description": "Set group — /setgroup 4821 -100123456"},
        {"command": "listtruck",   "description": "List all trucks"},
        {"command": "removetruck", "description": "Deactivate truck"},
    ]
    _post("setMyCommands", {"commands": commands})


def send_startup_message():
    register_commands()
    _send_to(ADMIN_CHAT_ID, "🚛 *FleetFuel Bot online.* Monitoring fuel levels.")


def send_price_update_notification(pilot_count, loves_count):
    log.info(f"Prices updated: Pilot={pilot_count} Love's={loves_count}")


_last_update_id: int = 0


def _get_file_url(file_id):
    result = _post("getFile", {"file_id": file_id})
    if result and result.get("ok"):
        path = result["result"]["file_path"]
        return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{path}"
    return None


def _download_file(file_url):
    try:
        resp = requests.get(file_url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        log.error(f"Download failed: {e}")
        return None


def poll_for_uploads():
    global _last_update_id
    if not ADMIN_CHAT_ID:
        return
    try:
        result = _post("getUpdates", {
            "offset": _last_update_id + 1, "timeout": 0, "limit": 20,
            "allowed_updates": ["message", "my_chat_member"],
        })
        if not result or not result.get("ok"):
            return
        for update in result.get("result", []):
            _last_update_id = update["update_id"]

            # Bot added to group
            chat_member = update.get("my_chat_member", {})
            if chat_member:
                new_status = chat_member.get("new_chat_member", {}).get("status", "")
                if new_status in ("member", "administrator"):
                    chat = chat_member.get("chat", {})
                    g_id = str(chat.get("id", ""))
                    g_title = chat.get("title", "") or ""
                    first_word = g_title.strip().split()[0] if g_title.strip() else ""
                    matched = None
                    if first_word:
                        from database import get_all_registered_trucks, upsert_truck_group
                        for truck in get_all_registered_trucks():
                            if truck["vehicle_name"] == first_word:
                                matched = first_word
                                break
                    if matched:
                        upsert_truck_group(matched, g_id)
                        _send_to(ADMIN_CHAT_ID, f"✅ *Auto-assigned*\nTruck: *{matched}*\nGroup: *{g_title}*\nID: `{g_id}`")
                    else:
                        _send_to(ADMIN_CHAT_ID, f"➕ *Bot added to group*\n*{g_title}*\nID: `{g_id}`\n`/setgroup TRUCKNAME {g_id}`")
                continue

            message  = update.get("message", {})
            chat_id  = str(message.get("chat", {}).get("id", ""))
            document = message.get("document")
            text     = message.get("text", "").strip()

            # QM Notifier — detect by content
            if "NEW TRIP" in text and "HAS BEEN ASSIGNED" in text:
                try:
                    from route_reader import parse_qm_notifier_message
                    from database import save_truck_route, get_truck_by_group
                    route = parse_qm_notifier_message(text, chat_id)
                    if route:
                        truck = get_truck_by_group(chat_id)
                        if truck:
                            save_truck_route(truck["vehicle_name"], chat_id, route)
                            log.info(f"Route saved for truck {truck['vehicle_name']}: trip {route['trip_num']} {route['origin']['city']} → {route['destination']['city']}")
                        else:
                            log.warning(f"QM message in group {chat_id} — no truck matched")
                except Exception as e:
                    log.error(f"QM Notifier parse error: {e}", exc_info=True)

            if text.startswith("/"):
                text = text.split("@")[0]

            # Commands for any group
            if text.startswith("/loadroute"):
                _handle_loadroute(text, chat_id)
            elif text.startswith("/route"):
                _handle_route(text, chat_id)
            elif text.startswith("/newalert"):
                _handle_newalert(text)
            elif text.startswith("/compliance"):
                _handle_compliance(text, chat_id)
            elif text.startswith("/fuelhistory"):
                _handle_fuelhistory(text, chat_id)
            elif text.startswith("/findstop"):
                try:
                    _handle_findstop(text, chat_id)
                except Exception as e:
                    _send_to(chat_id, f"❌ Error: `{e}`")
                continue

            if chat_id != ADMIN_CHAT_ID:
                continue

            if text.startswith("/"):
                try:
                    if text.startswith("/addtruck"):       _handle_addtruck(text)
                    elif text.startswith("/setgroup"):     _handle_setgroup(text)
                    elif text.startswith("/listtruck"):    _handle_listtruck()
                    elif text.startswith("/removetruck"):  _handle_removetruck(text)
                    elif text.startswith("/checkall"):     _handle_checkall()
                    elif text.startswith("/checknow"):     _handle_checknow()
                    elif text.startswith("/dbstats"):      _handle_dbstats()
                    elif text.startswith("/resetpilot"):   _handle_resetpilot()
                    elif text.startswith("/findload"):     _handle_findload(text, chat_id)
                    elif text.startswith("/testroute"):    _handle_testroute(text)
                    elif text.startswith("/routelist"):     _handle_routelist(chat_id)
                    else:
                        _send_to(ADMIN_CHAT_ID,
                            "Available commands:\n"
                            "/addtruck Unit4821 -100123456\n"
                            "/setgroup Unit4821 -100123456\n"
                            "/listtruck\n/removetruck Unit4821\n"
                            "/findstop 0792  ← any group\n"
                            "/route 0792  ← any group\n"
                            "/findload 8656  ← search QM trip"
                        )
                except Exception as e:
                    log.error(f"Command error: {e}", exc_info=True)
                    _send_to(ADMIN_CHAT_ID, f"❌ Command failed: `{e}`")
                continue

            if not document:
                _send_to(ADMIN_CHAT_ID, "📂 Send CSV/XLSX to update prices, or use a command.")
                continue

            filename   = document.get("file_name", "upload")
            file_id    = document.get("file_id")
            ext        = filename.lower().split(".")[-1]
            if ext not in ("csv", "xlsx", "zip"):
                _send_to(ADMIN_CHAT_ID, f"❌ Unsupported file: `{filename}`")
                continue
            _send_to(ADMIN_CHAT_ID, f"📥 Received `{filename}` — processing...")
            file_url = _get_file_url(file_id)
            if not file_url:
                _send_to(ADMIN_CHAT_ID, "❌ Could not retrieve file.")
                continue
            file_bytes = _download_file(file_url)
            if not file_bytes:
                _send_to(ADMIN_CHAT_ID, "❌ Failed to download file.")
                continue
            from price_updater import update_from_file
            count, msg = update_from_file(file_bytes, filename)
            _send_to(ADMIN_CHAT_ID, msg)
            if count > 0:
                log.info(f"Admin uploaded {filename} — {count} stops updated.")
    except Exception as e:
        log.error(f"poll_for_uploads error: {e}", exc_info=True)


# -- Admin handlers -----------------------------------------------------------

def _handle_checkall() -> None:
    """/checkall — immediately check all trucks and report low fuel ones"""
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import get_urgency
    from config import FUEL_ALERT_THRESHOLD_PCT

    _send_to(ADMIN_CHAT_ID, "🔄 Checking all trucks now...")

    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Samsara error: `{e}`")
        return

    low_fuel   = []
    critical   = []
    healthy    = []

    for v in vehicles:
        fuel  = v.get("fuel_pct", 100)
        name  = v.get("vehicle_name", "?")
        speed = v.get("speed_mph", 0)
        if fuel <= 10:
            critical.append((name, fuel, speed))
        elif fuel <= FUEL_ALERT_THRESHOLD_PCT:
            low_fuel.append((name, fuel, speed))
        else:
            healthy.append(name)

    # Sort by fuel level (lowest first)
    critical.sort(key=lambda x: x[1])
    low_fuel.sort(key=lambda x: x[1])

    lines = [
        f"📊 *Fleet Fuel Check — {len(vehicles)} trucks*",
        f"✅ Healthy: {len(healthy)}  |  🟡 Low: {len(low_fuel)}  |  🚨 Critical: {len(critical)}",
        "",
    ]

    if critical:
        lines.append("🚨 *CRITICAL (≤10%):*")
        for name, fuel, speed in critical:
            lines.append(f"   🚨 Truck *{name}* — {fuel:.0f}% | {speed:.0f} mph")
        lines.append("")

    if low_fuel:
        lines.append("🟡 *Low Fuel (≤35%):*")
        for name, fuel, speed in low_fuel:
            urgency = get_urgency(fuel)
            emoji   = {"WARNING": "🟠", "CRITICAL": "🔴"}.get(urgency, "🟡")
            lines.append(f"   {emoji} Truck *{name}* — {fuel:.0f}% | {speed:.0f} mph")
        lines.append("")

    if not critical and not low_fuel:
        lines.append("✅ All trucks have sufficient fuel.")

    _send_to(ADMIN_CHAT_ID, "\n".join(lines))

    # Also trigger force check so alerts fire for low fuel trucks
    global force_check_now
    force_check_now = True
    if low_fuel or critical:
        _send_to(ADMIN_CHAT_ID, f"⚡ Alerts will fire for {len(low_fuel)+len(critical)} trucks in next poll cycle.")


def _handle_newalert(text: str) -> None:
    """/newalert <truck_number> — force immediate new alert for a truck"""
    from database import load_all_truck_states, save_truck_state
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/newalert 3663`")
        return

    truck_num = parts[1].strip()
    states = load_all_truck_states()

    # Find truck by name
    found = None
    for vid, state in states.items():
        if str(state.get("vehicle_name","")) == truck_num:
            found = (vid, state)
            break

    if not found:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck *{truck_num}* not found in active states.")
        return

    vid, state = found
    # Reset alert timer and clear assignment so fresh stop is found
    state["last_alert_time"]    = None
    state["alert_sent"]         = False
    state["assigned_stop_id"]   = None
    state["assigned_stop_name"] = None
    state["assigned_stop_lat"]  = None
    state["assigned_stop_lng"]  = None
    state["assignment_time"]    = None
    save_truck_state(state)

    # Also trigger force check
    global force_check_now
    force_check_now = True

    _send_to(ADMIN_CHAT_ID,
        f"✅ *Truck {truck_num}* — new alert triggered.\n"
        f"Fresh stop recommendation will send in next poll cycle (~30 sec)."
    )


def _handle_checknow():
    global force_check_now
    force_check_now = True
    _send_to(ADMIN_CHAT_ID, "🔄 *Force check triggered.*")


def _handle_addtruck(text):
    from database import auto_register_truck, upsert_truck_group
    parts = text.split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: /addtruck <name> [group_id]")
        return
    name = parts[1]
    gid  = parts[2] if len(parts) >= 3 else None
    try:
        auto_register_truck(name, name)
        if gid:
            upsert_truck_group(name, gid)
        _send_to(ADMIN_CHAT_ID, f"✅ Truck *{name}* added" + (f" → group `{gid}`" if gid else ""))
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Failed: `{e}`")


def _handle_setgroup(text):
    from database import upsert_truck_group
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit4821 -1009876543210`")
        return
    tokens = parts[1].rsplit(maxsplit=1)
    if len(tokens) != 2 or not tokens[1].lstrip("-").isdigit():
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit4821 -1009876543210`")
        return
    name = tokens[0].strip(); gid = tokens[1].strip()
    if upsert_truck_group(name, gid):
        _send_to(ADMIN_CHAT_ID, f"✅ *{name}* → group `{gid}`")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck not found: *{name}*")


def _handle_listtruck():
    from database import get_all_registered_trucks
    trucks = get_all_registered_trucks()
    if not trucks:
        _send_to(ADMIN_CHAT_ID, "No trucks registered.")
        return
    lines = [f"• *{t['vehicle_name']}*  `{t.get('telegram_group_id') or '— no group'}`" for t in trucks]
    chunks = [lines[i:i+50] for i in range(0, len(lines), 50)]
    for i, chunk in enumerate(chunks):
        header = f"🚛 *Trucks ({len(trucks)} total)*" + (f" — page {i+1}/{len(chunks)}" if len(chunks) > 1 else "") + "\n"
        _send_to(ADMIN_CHAT_ID, header + "\n".join(chunk))


def _handle_removetruck(text):
    from database import deactivate_truck
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/removetruck Unit4821`")
        return
    name = parts[1].strip()
    if deactivate_truck(name):
        _send_to(ADMIN_CHAT_ID, f"✅ Deactivated: *{name}*")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Not found: *{name}*")


def _handle_resetpilot():
    from database import db_cursor
    with db_cursor() as cur:
        cur.execute("DELETE FROM fuel_stops WHERE source = 'pilot'")
        deleted = cur.rowcount
    _send_to(ADMIN_CHAT_ID, f"🗑 Deleted *{deleted}* Pilot/Flying J stops.\nNow upload `merged_pilot_data.csv` to reload.")


def _handle_dbstats():
    from database import db_cursor
    with db_cursor() as cur:
        cur.execute("""
            SELECT source, COUNT(*) AS total, COUNT(diesel_price) AS with_price,
                   ROUND(AVG(diesel_price)::numeric,3) AS avg_price,
                   MIN(diesel_price) AS min_price, MAX(diesel_price) AS max_price,
                   MAX(price_updated) AS last_updated
            FROM fuel_stops WHERE has_diesel=TRUE GROUP BY source ORDER BY source
        """)
        rows = cur.fetchall()
    if not rows:
        _send_to(ADMIN_CHAT_ID, "❌ No fuel stops in DB.")
        return
    lines = ["📊 *Fuel Stop DB Stats*\n"]
    for r in rows:
        s = (r["source"] or "unknown").upper()
        upd = r["last_updated"].strftime("%Y-%m-%d %H:%M UTC") if r["last_updated"] else "never"
        lines += [f"*{s}*",
                  f"  Stops: {r['total']}  Priced: {r['with_price']}  Missing: {r['total']-r['with_price']}",
                  f"  Price: ${r['min_price'] or 0:.3f} – ${r['max_price'] or 0:.3f}  (avg ${r['avg_price'] or 0:.3f})",
                  f"  Updated: {upd}\n"]
    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_findload(text: str, chat_id: str) -> None:
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/findload 8656`")
        return
    trip_num = parts[1].strip()
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
            _send_to(chat_id, "❌ QuickManage credentials not configured.")
            return
        from quickmanage_client import _get_token
        token = _get_token()
        if not token:
            _send_to(chat_id, "❌ Could not get QuickManage token.")
            return
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"query": trip_num, "filters": [], "page": 0, "page_size": 10}
        resp = requests.post("https://api.quickmanage.com/x/trips/search", json=payload, headers=headers, timeout=10)
        log.info(f"/findload {trip_num} → {resp.status_code}: {resp.text[:800]}")
        if not resp.ok:
            _send_to(chat_id, f"❌ QM API error {resp.status_code}:\n`{resp.text[:200]}`")
            return
        data  = resp.json()
        items = data.get("data", {}).get("items", [])
        if not items:
            _send_to(chat_id, f"❌ Trip *{trip_num}* not found.\nRaw: `{str(data)[:300]}`")
            return
        trip  = items[0]
        stops = trip.get("stops") or []
        lines = [
            f"✅ *Trip #{trip_num} found*",
            f"📋 Ref: `{trip.get('ref_number','')}` | Status: `{trip.get('status','')}`",
            f"👤 {trip.get('customer_name','')}", "",
        ]
        for i, s in enumerate(stops, 1):
            addr  = s.get("address") or {}
            icon  = "📦" if s.get("pickup") else "🏁"
            stype = "Pickup" if s.get("pickup") else "Delivery"
            truck = s.get("assigned_truck") or {}
            tnum  = truck.get("number", "")
            lines += [f"{icon} *Stop {i} — {stype}*",
                      f"   {s.get('company_name','')}",
                      f"   📍 {addr.get('city','')}, {addr.get('state','')} {addr.get('zip_code','')}"]
            if tnum and tnum != "0":
                lines.append(f"   🚛 Truck: *{tnum}*")
            lines.append("")
        _send_to(chat_id, "\n".join(lines))
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        log.error(f"/findload error: {e}", exc_info=True)


def _handle_route(text: str, chat_id: str) -> None:
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/route 0792`")
        return
    truck_num = parts[1].strip()
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        route = None
        if QM_CLIENT_ID and QM_CLIENT_SECRET:
            from quickmanage_client import get_route_for_truck
            route = get_route_for_truck(truck_num)
        if not route:
            from database import get_truck_route
            route = get_truck_route(truck_num)
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        return
    if not route:
        # Try searching QM by truck number as query string
        try:
            from config import QM_CLIENT_ID, QM_CLIENT_SECRET
            if QM_CLIENT_ID and QM_CLIENT_SECRET:
                from quickmanage_client import _get_token, _build_route, _ACTIVE_STATUSES
                token = _get_token()
                if token:
                    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    resp = requests.post(
                        "https://api.quickmanage.com/x/trips/search",
                        json={"query": truck_num, "filters": [], "page": 0, "page_size": 20},
                        headers=hdrs, timeout=10
                    )
                    if resp.ok:
                        items = resp.json().get("data", {}).get("items", [])
                        for trip in items:
                            if trip.get("status","").lower() in _ACTIVE_STATUSES:
                                route = _build_route(trip, truck_num)
                                if route:
                                    break
        except Exception as e:
            log.warning(f"/route QM query fallback failed: {e}")

    if not route:
        _send_to(chat_id, f"🚛 Truck *{truck_num}*\n❌ No route found.\nRoute is saved when QM Notifier posts a trip in the driver group.")
        return
    status = route.get("status", "").lower()
    status_label = {"dispatched": "🟡 Dispatched → heading to pickup", "in_transit": "🟢 In Transit → heading to delivery"}.get(status, f"📌 {status}")
    dest   = route.get("destination", {})
    lines  = [
        f"🗺 *Truck {truck_num} — Active Load*",
        f"📋 Trip #: `{route.get('trip_num','')}` | Ref: `{route.get('ref_number','')}`",
        f"{status_label}", "",
    ]
    for s in route.get("stops", []):
        icon    = "📦" if s["pickup"] else "🏁"
        stype   = "Pickup" if s["pickup"] else "Delivery"
        loc     = f"{s['city']}, {s['state']} {s['zip']}".strip()
        is_next = (s["city"] == dest.get("city") and s["state"] == dest.get("state"))
        arrow   = "  ← *NEXT*" if is_next else ""
        lines  += [f"{icon} *Stop {s['stop_num']} — {stype}*{arrow}", f"   {s['company']}", f"   📍 {loc}"]
        if s.get("appt"):
            lines.append(f"   🕐 {s['appt'][:16].replace('T',' ')}")
        lines.append("")
    lines.append(f"🏁 *Destination: {dest.get('city')}, {dest.get('state')}*")
    _send_to(chat_id, "\n".join(lines))


def _handle_loadroute(text: str, chat_id: str) -> None:
    parts = text.strip().split(None, 1)
    rest  = parts[1] if len(parts) > 1 else ""
    rest_lines = rest.strip().split("\n", 1)
    if rest_lines[0].strip().replace(" ","").isdigit() or (rest_lines[0].strip() and "NEW TRIP" not in rest_lines[0]):
        truck_num = rest_lines[0].strip()
        msg_text  = rest_lines[1].strip() if len(rest_lines) > 1 else ""
    else:
        truck_num = ""; msg_text = rest.strip()
    if not truck_num:
        _send_to(chat_id, "Usage: `/loadroute 630862\n<paste QM message>`")
        return
    if "NEW TRIP" not in msg_text or "HAS BEEN ASSIGNED" not in msg_text:
        _send_to(chat_id, "❌ Message must contain 'NEW TRIP X HAS BEEN ASSIGNED'")
        return
    try:
        from route_reader import parse_qm_notifier_message
        from database import save_truck_route
        route = parse_qm_notifier_message(msg_text, chat_id)
    except Exception as e:
        _send_to(chat_id, f"❌ Parse error: `{e}`")
        return
    if not route:
        _send_to(chat_id, "❌ Could not parse route.")
        return
    save_truck_route(truck_num, chat_id, route)
    o = route["origin"]; d = route["destination"]
    _send_to(chat_id, f"✅ *Route saved for Truck {truck_num}*\n📋 Trip #{route['trip_num']} | Ref: {route['ref_number']}\n🚀 From: {o['city']}, {o['state']}\n🏁 To: {d['city']}, {d['state']}\n📍 {len(route['stops'])} stops\n\nType `/route {truck_num}` to verify.")


def _handle_testroute(text: str) -> None:
    parts = text.split("\n", 1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/testroute`\n`<paste QM message>`")
        return
    msg_text = parts[1].strip()
    try:
        from route_reader import parse_qm_notifier_message
        route = parse_qm_notifier_message(msg_text, "test")
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Parser error: `{e}`")
        return
    if not route:
        _send_to(ADMIN_CHAT_ID, "❌ Could not parse. Make sure it contains 'NEW TRIP X HAS BEEN ASSIGNED'")
        return
    lines = [f"✅ *Parser Test*\n\n📋 Trip #: `{route['trip_num']}`\n📋 Ref: `{route['ref_number']}`\n"]
    for s in route["stops"]:
        icon   = "📦" if s["pickup"] else "🏁"
        coords = f"{s['lat']:.4f}, {s['lng']:.4f}" if s["lat"] else "❌ no coords"
        lines += [f"{icon} *Stop {s['stop_num']}* {'Pickup' if s['pickup'] else 'Delivery'}", f"   {s['company']}", f"   📍 {s['address']}", f"   🌐 {coords}", ""]
    o = route["origin"]; d = route["destination"]
    lines += [f"🚀 *Origin:* {o['city']}, {o['state']} ({o['lat']:.4f}, {o['lng']:.4f})",
              f"🏁 *Destination:* {d['city']}, {d['state']} ({d['lat']:.4f}, {d['lng']:.4f})"]
    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_findstop(text: str, chat_id: str):
    from database import get_all_diesel_stops
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import haversine_miles

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/findstop 0792`")
        return
    truck_number = parts[1].strip()
    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(chat_id, f"❌ Could not reach Samsara: `{e}`")
        return
    truck = next((v for v in vehicles if truck_number.lower() in v.get("vehicle_name","").lower()), None)
    if not truck:
        _send_to(chat_id, f"❌ Truck *{truck_number}* not found in Samsara.")
        return
    lat = truck.get("lat"); lng = truck.get("lng")
    if not lat or not lng:
        _send_to(chat_id, f"❌ No GPS for truck *{truck.get('vehicle_name',truck_number)}*.")
        return
    fuel  = truck.get("fuel_pct", 0)
    speed = truck.get("speed_mph", 0)
    vname = truck.get("vehicle_name", truck_number)
    all_stops = get_all_diesel_stops()
    nearby = sorted(
        [{ **s, "distance_miles": round(haversine_miles(lat, lng, float(s["latitude"]), float(s["longitude"])), 1)}
         for s in all_stops if haversine_miles(lat, lng, float(s["latitude"]), float(s["longitude"])) <= 50 and s.get("diesel_price")],
        key=lambda s: s["diesel_price"]
    )[:3]
    if not nearby:
        _send_to(chat_id, f"⚠️ No fuel stops within 50 miles of *{vname}*.\n📍 GPS: `{lat:.5f}, {lng:.5f}`")
        return
    lines = [f"⛽ *Fuel Stops — Truck {vname}*", f"📍 ⛽ {fuel:.0f}% fuel | 🧭 {speed:.0f} mph", f"🌐 GPS: `{lat:.5f}, {lng:.5f}`", f"🔍 Top 3 cheapest within 50 miles\n"]
    for i, s in enumerate(nearby, 1):
        addr = ", ".join(filter(None, [s.get("address",""), s.get("city",""), s.get("state","")]))
        lines += [f"*#{i} — {s['store_name']}*", f"📌 {addr}", f"🛣 {s['distance_miles']} mi away",
                  f"💰 Diesel: ${s['diesel_price']:.3f}/gal",
                  f"🗺 [Open in Google Maps](https://maps.google.com/?q={s['latitude']},{s['longitude']})"]
        if i < len(nearby):
            lines.append("")
    _send_to(chat_id, "\n".join(lines))


def _handle_routelist(chat_id: str) -> None:
    """/routelist — show all trucks with active QM routes"""
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        routes = {}
        if QM_CLIENT_ID and QM_CLIENT_SECRET:
            from quickmanage_client import get_all_truck_routes
            routes = get_all_truck_routes()
        if not routes:
            from database import get_all_truck_routes_from_db
            routes = get_all_truck_routes_from_db()
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        return

    if not routes:
        _send_to(chat_id, "❌ No active routes found.")
        return

    status_emoji = {"dispatched": "🟡", "in_transit": "🟢", "upcoming": "🔵"}

    lines = [f"🗺 *Active Routes — {len(routes)} trucks*\n"]
    for truck_num, route in sorted(routes.items()):
        status = route.get("status", "").lower()
        emoji  = status_emoji.get(status, "⚪")
        origin = route.get("origin", {})
        dest   = route.get("destination", {})
        trip   = route.get("trip_num", "")
        o_city = f"{origin.get('city','?')}, {origin.get('state','')}"
        d_city = f"{dest.get('city','?')}, {dest.get('state','')}"
        lines.append(f"{emoji} *Truck {truck_num}* — Trip #{trip}")
        lines.append(f"   {o_city} → {d_city}")
        lines.append("")

    # Split into chunks if too long
    msg = "\n".join(lines)
    if len(msg) > 4000:
        chunks = []
        chunk  = [f"🗺 *Active Routes — {len(routes)} trucks*\n"]
        for truck_num, route in sorted(routes.items()):
            status = route.get("status", "").lower()
            emoji  = status_emoji.get(status, "⚪")
            origin = route.get("origin", {})
            dest   = route.get("destination", {})
            trip   = route.get("trip_num", "")
            line   = f"{emoji} *{truck_num}* #{trip} | {origin.get('city','?')},{origin.get('state','')} → {dest.get('city','?')},{dest.get('state','')}"
            chunk.append(line)
            if len("\n".join(chunk)) > 3800:
                chunks.append("\n".join(chunk))
                chunk = []
        if chunk:
            chunks.append("\n".join(chunk))
        for c in chunks:
            _send_to(chat_id, c)
    else:
        _send_to(chat_id, msg)


def _handle_fuelhistory(text: str, chat_id: str) -> None:
    """/fuelhistory <truck_number> — show recent fuel stop visits"""
    from database import db_cursor
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/fuelhistory 0792`")
        return

    truck_num = parts[1].strip()

    with db_cursor() as cur:
        cur.execute("""
            SELECT alerted_at, best_stop_name, best_stop_price,
                   savings_usd, alert_type, fuel_pct
            FROM fuel_alerts
            WHERE vehicle_name = %s
            ORDER BY alerted_at DESC
            LIMIT 10
        """, (truck_num,))
        rows = cur.fetchall()

        # Also check if truck actually refueled (fuel went up after alert)
        cur.execute("""
            SELECT alerted_at, fuel_pct, best_stop_name
            FROM fuel_alerts
            WHERE vehicle_name = %s AND alert_type = 'refueled'
            ORDER BY alerted_at DESC
            LIMIT 5
        """, (truck_num,))
        refueled = cur.fetchall()

    if not rows:
        _send_to(chat_id, f"❌ No fuel alert history for truck *{truck_num}*.")
        return

    header = f"⛽ *Fuel History — Truck {truck_num}*"
    lines = [header + "\n"]

    if refueled:
        lines.append("✅ *Confirmed Refuels:*")
        for r in refueled:
            dt   = r["alerted_at"].strftime("%b %d %H:%M")
            stop = r["best_stop_name"] or "Unknown stop"
            lines.append(f"   ✅ {dt} — {stop}")
        lines.append("")

    lines.append("📋 *Recent Alerts:*")
    for r in rows:
        dt    = r["alerted_at"].strftime("%b %d %H:%M")
        stop  = r["best_stop_name"] or "No stop found"
        price = f"${r['best_stop_price']:.3f}" if r["best_stop_price"] else "N/A"
        saved = f"saved ${r['savings_usd']:.0f}" if r["savings_usd"] else ""
        fuel  = f"{r['fuel_pct']:.0f}%" if r["fuel_pct"] else ""
        lines.append(f"   🟡 {dt} | ⛽{fuel} | {stop} {price} {saved}")

    _send_to(chat_id, "\n".join(lines))


def _handle_compliance(text: str, chat_id: str) -> None:
    """/compliance [truck_number] — show fuel stop compliance report"""
    from database import db_cursor
    from datetime import datetime, timezone, timedelta

    parts = text.strip().split()
    truck_num = parts[1].strip() if len(parts) > 1 else None
    now       = datetime.now(timezone.utc)
    since     = now - timedelta(days=30)

    with db_cursor() as cur:
        if truck_num:
            # Per-truck detail
            cur.execute("""
                SELECT recommended_stop_name, actual_stop_name, visited,
                       fuel_before, fuel_after, visited_at
                FROM stop_visits
                WHERE vehicle_name = %s AND created_at >= %s
                ORDER BY visited_at DESC LIMIT 15
            """, (truck_num, since))
            rows = cur.fetchall()

            if not rows:
                _send_to(chat_id, f"❌ No compliance data for truck *{truck_num}* in last 30 days.")
                return

            visited = sum(1 for r in rows if r["visited"] is True)
            skipped = sum(1 for r in rows if r["visited"] is False)
            total   = len(rows)
            pct     = round(visited / total * 100) if total else 0

            lines = [
                f"📊 *Compliance — Truck {truck_num}*",
                f"📅 Last 30 days",
                f"",
                f"✅ Visited recommended: *{visited}/{total}* ({pct}%)",
                f"⚠️ Skipped recommended: *{skipped}/{total}*",
                f"",
            ]
            for r in rows:
                dt   = r["visited_at"].strftime("%b %d %H:%M") if r["visited_at"] else "?"
                icon = "✅" if r["visited"] else "⚠️"
                rec  = r["recommended_stop_name"] or "?"
                act  = r["actual_stop_name"] or "?"
                fb   = f"{r['fuel_before']:.0f}%" if r["fuel_before"] else "?"
                fa   = f"{r['fuel_after']:.0f}%" if r["fuel_after"] else "?"
                if r["visited"]:
                    lines.append(f"{icon} {dt} | {rec} | {fb}→{fa}")
                else:
                    lines.append(f"{icon} {dt} | Rec: {rec} | Went to: {act} | {fb}→{fa}")

        else:
            # Fleet-wide summary
            cur.execute("""
                SELECT
                    COUNT(*)                                       AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE)        AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE)       AS skipped,
                    COUNT(*) FILTER (WHERE visited IS NULL)       AS unknown
                FROM stop_visits WHERE created_at >= %s
            """, (since,))
            stats = dict(cur.fetchone())

            cur.execute("""
                SELECT vehicle_name,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE) AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped
                FROM stop_visits WHERE created_at >= %s
                GROUP BY vehicle_name
                ORDER BY (COUNT(*) FILTER (WHERE visited = FALSE)) DESC
                LIMIT 10
            """, (since,))
            trucks = cur.fetchall()

            total   = stats["total"] or 0
            visited = stats["visited"] or 0
            skipped = stats["skipped"] or 0
            pct     = round(visited / total * 100) if total else 0

            lines = [
                f"📊 *Fleet Compliance Report*",
                f"📅 Last 30 days",
                f"",
                f"✅ Visited recommended stop: *{visited}/{total}* ({pct}%)",
                f"⚠️ Skipped recommended stop: *{skipped}/{total}*",
                f"",
            ]

            if trucks:
                lines.append("🚛 *Trucks with most skips:*")
                for t in trucks:
                    if t["skipped"] > 0:
                        lines.append(f"   • Truck *{t['vehicle_name']}* — {t['skipped']} skips / {t['total']} alerts")

            lines += [
                "",
                "Type `/compliance <truck#>` for per-truck detail.",
            ]

    _send_to(chat_id, "\n".join(lines))


def send_weekly_savings_report() -> None:
    from database import db_cursor
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    with db_cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS total_alerts, COUNT(DISTINCT vehicle_id) AS trucks_active,
                   COALESCE(SUM(savings_usd),0) AS total_savings,
                   COUNT(*) FILTER (WHERE savings_usd > 0) AS alerts_with_savings
            FROM fuel_alerts WHERE alerted_at >= %s AND alert_type = 'low_fuel'
        """, (week_ago,))
        stats = dict(cur.fetchone())
        cur.execute("""
            SELECT vehicle_name, COALESCE(SUM(savings_usd),0) AS saved, COUNT(*) AS alerts
            FROM fuel_alerts WHERE alerted_at >= %s AND alert_type = 'low_fuel'
            GROUP BY vehicle_name ORDER BY saved DESC LIMIT 5
        """, (week_ago,))
        top_trucks = cur.fetchall()
        cur.execute("""
            SELECT best_stop_name, best_stop_price FROM fuel_alerts
            WHERE alerted_at >= %s AND best_stop_price IS NOT NULL ORDER BY best_stop_price ASC LIMIT 1
        """, (week_ago,))
        cheapest = cur.fetchone()
    total_savings = float(stats["total_savings"] or 0)
    week_start = (now - timedelta(days=7)).strftime("%b %d")
    week_end   = now.strftime("%b %d, %Y")
    lines = [
        f"📊 *FleetFuel AI — Weekly Savings Report*",
        f"📅 {week_start} – {week_end}", f"─────────────────────────────", "",
        f"🚛 Trucks monitored:     *{stats['trucks_active']}*",
        f"⚡ Alerts fired:          *{stats['total_alerts']}*",
        f"💡 Alerts with savings:  *{stats['alerts_with_savings']}*", "",
        f"💰 *Total Diesel Savings:  ${total_savings:,.2f}*",
    ]
    if cheapest:
        lines += ["", f"🏆 *Cheapest stop:*", f"   {cheapest['best_stop_name']} — ${cheapest['best_stop_price']:.3f}/gal"]
    if top_trucks:
        lines += ["", "🏅 *Top Trucks — Most Saved:*"]
        for i, t in enumerate(top_trucks):
            medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
            lines.append(f"   {medals[i]} Truck {t['vehicle_name']} — *${float(t['saved']):.2f}* ({t['alerts']} alerts)")
    if total_savings == 0:
        lines += ["", "ℹ️ No savings recorded this week."]
    # Compliance summary
    try:
        from datetime import timedelta
        since_week = now - timedelta(days=7)
        with db_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE visited=TRUE) AS visited,
                       COUNT(*) FILTER (WHERE visited=FALSE) AS skipped
                FROM stop_visits WHERE created_at >= %s
            """, (since_week,))
            cv = dict(cur.fetchone())
        if cv["total"]:
            cpct = round((cv["visited"] or 0) / cv["total"] * 100)
            lines += [
                "", "─────────────────────────────",
                f"🎯 *Stop Compliance:* {cv['visited']}/{cv['total']} followed recommendation ({cpct}%)",
                f"⚠️ Skipped: {cv['skipped']} | Type `/compliance` for details",
            ]
    except Exception:
        pass

    lines += ["", "─────────────────────────────", "⚙️ _FleetFuel AI — Automated Report_"]
    msg = "\n".join(lines)
    if DISPATCHER_GROUP_ID:
        _send_to(DISPATCHER_GROUP_ID, msg)
    _send_to(ADMIN_CHAT_ID, msg)
    log.info(f"Weekly report sent — ${total_savings:,.2f} savings")
