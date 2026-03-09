"""
telegram_bot.py  -  Telegram message sending for FleetFuel bot.

All alerts go to the truck's own Telegram group.
Dispatcher group receives: startup, no-stop emergencies, left-yard-low-fuel.
"""

import time
import logging
import requests
from config import TELEGRAM_BOT_TOKEN, DISPATCHER_GROUP_ID, ADMIN_CHAT_ID, MIN_SAVINGS_DISPLAY, TRIP_GROUP_ID

log = logging.getLogger(__name__)
BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# -- Core sender --------------------------------------------------------------

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
    """Send message to a specific chat_id. Returns message_id or None."""
    if not chat_id:
        log.warning("No chat_id — message not sent.")
        return None
    result = _post("sendMessage", {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "Markdown",
        "disable_web_page_preview": True,
    })
    if result and result.get("ok"):
        return result["result"]["message_id"]
    return None


def _send_to_truck(vehicle_name: str, text: str) -> int | None:
    """Send to truck's own group (if set) AND dispatcher group."""
    from database import get_truck_group
    truck_group = get_truck_group(vehicle_name)

    msg_id = None
    # Send to truck's own driver group if configured
    if truck_group:
        msg_id = _send_to(truck_group, text)
    else:
        log.info(f"No group set for {vehicle_name} — dispatcher only")

    # Always also send to dispatcher group (skip if same group)
    if DISPATCHER_GROUP_ID and truck_group != str(DISPATCHER_GROUP_ID):
        _send_to_dispatcher(text)

    return msg_id


def _send_to_dispatcher(text: str) -> int | None:
    """Send message to the dispatcher/admin group."""
    if not DISPATCHER_GROUP_ID:
        log.warning("DISPATCHER_GROUP_ID not set.")
        return None
    return _send_to(DISPATCHER_GROUP_ID, text)


# -- Formatters ---------------------------------------------------------------

def _compass(heading: float) -> str:
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(heading / 22.5) % 16]


def _urgency_emoji(fuel_pct: float) -> str:
    if fuel_pct <= 10: return "🚨"
    if fuel_pct <= 15: return "🔴"
    if fuel_pct <= 25: return "🟠"
    return "🟡"


def _send_location(chat_id: str, lat: float, lng: float, title: str, address: str) -> int | None:
    """Send a Telegram venue (map pin with title + address)."""
    if not chat_id:
        return None
    result = _post("sendVenue", {
        "chat_id":   chat_id,
        "latitude":  lat,
        "longitude": lng,
        "title":     title,
        "address":   address,
    })
    if result and result.get("ok"):
        return result["result"]["message_id"]
    return None


def _format_stop_card(stop: dict) -> str:
    name         = stop.get("store_name", "Unknown")
    address      = stop.get("address", "")
    city         = stop.get("city", "")
    state        = stop.get("state", "")
    zip_         = stop.get("zip", "")
    dist         = stop.get("distance_miles", 0)
    price        = stop.get("diesel_price")
    detour       = stop.get("detour_miles", 0)
    lat          = stop.get("latitude")
    lng          = stop.get("longitude")

    full_address = ", ".join(filter(None, [address, city, state, zip_]))
    price_line   = f"${price:.3f}/gal" if price else "Price N/A"
    dist_line    = f"{dist:.1f} miles ahead"
    if detour and detour > 0.3:
        dist_line += f"  ({detour:.1f} mi detour)"
    maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None

    lines = [
        f"🏪 *{name}*",
        f"📮 {full_address}",
        f"⛽ Diesel #2: *{price_line}*",
        f"📏 {dist_line}",
    ]
    if maps_url:
        lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    return "\n".join(lines)


# -- Public alert functions ---------------------------------------------------

def send_low_fuel_alert(vehicle_name: str, fuel_pct: float,
                         truck_lat: float, truck_lng: float,
                         heading: float, speed_mph: float,
                         best_stop: dict | None,
                         alt_stop: dict | None,
                         savings_usd: float | None) -> int | None:

    emoji     = _urgency_emoji(fuel_pct)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    compass   = _compass(heading)

    lines = [
        f"{emoji} *Low Fuel Alert*",
        "",
        f"🚛 Truck:          *{vehicle_name}*",
        f"⛽ Current fuel:  *{fuel_pct:.0f}%*",
        f"📍 [{truck_lat:.4f}, {truck_lng:.4f}]({truck_url})  ·  {speed_mph:.0f} mph {compass}",
    ]

    if best_stop:
        name    = best_stop.get("store_name", "Unknown")
        address = best_stop.get("address", "")
        city    = best_stop.get("city", "")
        state   = best_stop.get("state", "")
        zip_    = best_stop.get("zip", "")
        dist    = best_stop.get("distance_miles", 0)
        price   = best_stop.get("diesel_price")
        lat     = best_stop.get("latitude")
        lng     = best_stop.get("longitude")

        full_address = ", ".join(filter(None, [address, city, state, zip_]))
        maps_url     = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None

        lines += [
            "",
            "*Recommended Fuel Stop*",
            f"*{name}*",
            f"Address: {full_address}",
            f"{dist:.1f} mi away",
            f"Diesel #2: *${price:.3f}/gal*" if price else "Diesel #2: Price N/A",
        ]
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        lines += [
            "",
            "❌ No diesel stops found within range.",
            "📞 Dispatcher notified.",
        ]
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — {fuel_pct:.0f}% — NO STOP FOUND")

    if fuel_pct <= 15 and best_stop:
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — critically low at {fuel_pct:.0f}%")

    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_ca_border_reminder(vehicle_name: str, fuel_pct: float,
                              truck_lat: float, truck_lng: float,
                              best_stop: dict | None,
                              ca_avg_price: float | None,
                              dist_to_border: float) -> int | None:

    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"

    lines = [
        f"🌵 *CALIFORNIA BORDER AHEAD — {vehicle_name}*",
        f"─────────────────────────────",
        f"🚛 Truck:      *{vehicle_name}*",
        f"⛽ Fuel:       *{fuel_pct:.0f}%*",
        f"📍 Location:  [{truck_lat:.4f}, {truck_lng:.4f}]({truck_url})",
        f"🛣 ~{dist_to_border:.0f} miles to CA border",
        f"─────────────────────────────",
        f"⚠️ *California diesel is significantly more expensive.*",
    ]

    if ca_avg_price and best_stop and best_stop.get("diesel_price"):
        premium = ca_avg_price - best_stop["diesel_price"]
        if premium > 0:
            lines.append(f"   CA avg: ~${ca_avg_price:.3f}/gal  vs here: ${best_stop['diesel_price']:.3f}/gal")

    if best_stop:
        lines.append(f"─────────────────────────────")
        lines.append(f"🏁 *Fill up before the border:*")
        lines.append(_format_stop_card(best_stop))

    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_at_stop_alert(vehicle_name: str, fuel_pct: float,
                        truck_lat: float, truck_lng: float,
                        current_stop: dict,
                        cheaper_stop: dict | None = None) -> None:
    """Alert when truck is already parked at a Pilot/Love's/Flying J."""
    emoji     = _urgency_emoji(fuel_pct)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"

    def _stop_lines(stop, label):
        name    = stop.get("store_name", "Unknown")
        address = stop.get("address", "")
        city    = stop.get("city", "")
        state_  = stop.get("state", "")
        zip_    = stop.get("zip", "")
        price   = stop.get("diesel_price")
        dist    = stop.get("distance_miles", 0)
        lat     = stop.get("latitude")
        lng     = stop.get("longitude")
        full_address = ", ".join(filter(None, [address, city, state_, zip_]))
        maps_url     = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None
        lines = [
            label,
            f"*{name}*",
            f"Address: {full_address}",
        ]
        if dist and dist > 0.1:
            lines.append(f"📏 {dist:.1f} mi away")
        lines.append(f"Diesel #2: *${price:.3f}/gal*" if price else "Diesel #2: Price N/A")
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
        return lines

    lines = [
        f"{emoji} *Low Fuel Alert*",
        "",
        f"🚛 Truck:          *{vehicle_name}*",
        f"⛽ Current fuel:  *{fuel_pct:.0f}%*",
        f"📍 [{truck_lat:.4f}, {truck_lng:.4f}]({truck_url})",
        "",
    ]

    if cheaper_stop:
        net_saving = cheaper_stop.get("net_saving", 0)
        cur_price  = current_stop.get("diesel_price", 0)
        chp_price  = cheaper_stop.get("diesel_price", 0)
        lines += _stop_lines(current_stop, "🅿️ *Currently stopped at:*")
        lines += [
            "",
            f"💡 *Cheaper stop nearby — saves ~${net_saving:.2f}:*",
        ]
        lines += _stop_lines(cheaper_stop, "")
    else:
        lines += _stop_lines(current_stop, "🅿️ *Truck is already stopped at:*")
        lines.append("✅ This is the best available price nearby.")

    _send_to_truck(vehicle_name, "\n".join(lines))


def send_refueled_alert(vehicle_name: str, stop_name: str,
                         fuel_pct: float) -> None:
    text = (
        f"✅ *REFUELED*\n"
        f"─────────────────────────\n"
        f"🚛 *Truck:* {vehicle_name}\n"
        f"🏪 *Refueled at:* {stop_name}\n"
        f"⛽ *Fuel now:* {fuel_pct:.0f}%\n"
        f"✅ Alert closed."
    )
    _send_to_truck(vehicle_name, text)


def send_left_yard_low_fuel(vehicle_name: str, fuel_pct: float,
                              yard_name: str) -> None:
    text = (
        f"🏠 *LEFT YARD — LOW FUEL*\n"
        f"─────────────────────────\n"
        f"🚛 *Truck:* {vehicle_name}\n"
        f"⛽ *Fuel:* {fuel_pct:.0f}%\n"
        f"📍 *Departed:* {yard_name}\n"
        f"Finding nearest stop..."
    )
    _send_to_truck(vehicle_name, text)
    _send_to_dispatcher(
        f"🏠 *{vehicle_name}* left {yard_name} with only {fuel_pct:.0f}% fuel."
    )


def register_commands() -> None:
    """Register bot commands so they appear in the Telegram menu."""
    commands = [
        {"command": "addtruck",    "description": "Add a truck — /addtruck Unit4821 -100123456"},
        {"command": "setgroup",    "description": "Set truck group — /setgroup Unit4821 -100123456"},
        {"command": "listtruck",   "description": "List all trucks and their groups"},
        {"command": "removetruck", "description": "Deactivate a truck — /removetruck Unit4821"},
    ]
    result = _post("setMyCommands", {"commands": commands})
    if result and result.get("ok"):
        log.info("Bot commands registered in Telegram menu")
    else:
        log.warning(f"Failed to register commands: {result}")


def send_startup_message() -> None:
    register_commands()
    _send_to(ADMIN_CHAT_ID, "🚛 *FleetFuel Bot online.* Monitoring fuel levels.")


def send_price_update_notification(pilot_count: int, loves_count: int) -> None:
    """No-op — price update notifications suppressed."""
    log.info(f"Prices updated: Pilot={pilot_count} Love's={loves_count}")


# -- File upload handler (admin only) ----------------------------------------

_last_update_id: int = 0  # tracks last processed Telegram update


def _get_file_url(file_id: str) -> str | None:
    """Get download URL for a Telegram file by file_id."""
    result = _post("getFile", {"file_id": file_id})
    if result and result.get("ok"):
        path = result["result"]["file_path"]
        return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{path}"
    return None


def _download_file(file_url: str) -> bytes | None:
    """Download a file from Telegram servers."""
    try:
        resp = requests.get(file_url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        log.error(f"Failed to download file: {e}")
        return None


def poll_for_uploads() -> None:
    """
    Check Telegram for new messages. If admin sends a CSV/XLSX/ZIP file,
    parse it and update fuel prices in DB.

    Call this from the main loop every 30–60 seconds.
    """
    global _last_update_id

    if not ADMIN_CHAT_ID:
        log.warning("ADMIN_CHAT_ID not set — file upload handler disabled.")
        return

    try:
        result = _post("getUpdates", {
            "offset":  _last_update_id + 1,
            "timeout": 0,
            "limit":   20,
        })

        if not result or not result.get("ok"):
            return

        updates = result.get("result", [])

        for update in updates:
            _last_update_id = update["update_id"]

            message = update.get("message", {})
            chat_id = str(message.get("chat", {}).get("id", ""))

            # Only process messages from admin
            if chat_id != ADMIN_CHAT_ID:
                continue

            document = message.get("document")
            text     = message.get("text", "").strip()

            # Strip bot username from commands e.g. /listtruck@FuelAlertBot → /listtruck
            if text.startswith("/"):
                text = text.split("@")[0]

            # Handle commands first
            if text.startswith("/"):
                try:
                    if text.startswith("/addtruck"):
                        _handle_addtruck(text)
                    elif text.startswith("/setgroup"):
                        _handle_setgroup(text)
                    elif text.startswith("/listtruck"):
                        _handle_listtruck()
                    elif text.startswith("/removetruck"):
                        _handle_removetruck(text)
                    else:
                        _send_to(ADMIN_CHAT_ID,
                            "Available commands:\n"
                            "/addtruck Unit4821 -100123456\n"
                            "/setgroup Unit4821 -100123456\n"
                            "/listtruck\n"
                            "/removetruck Unit4821"
                        )
                except Exception as e:
                    log.error(f"Command error: {e}", exc_info=True)
                    _send_to(ADMIN_CHAT_ID, f"❌ Command failed: `{e}`")
                continue

            if not document:
                # Non-file text from admin — show help
                _send_to(ADMIN_CHAT_ID,
                    "📂 Send a CSV or XLSX file to update fuel prices.\n"
                    "Or use a command:\n"
                    "/addtruck /setgroup /listtruck /removetruck"
                )
                continue

            filename  = document.get("file_name", "upload")
            file_id   = document.get("file_id")
            ext       = filename.lower().split(".")[-1]

            if ext not in ("csv", "xlsx", "zip"):
                _send_to(ADMIN_CHAT_ID,
                    f"❌ Unsupported file type: `{filename}`\n"
                    "Please send a `.csv`, `.xlsx`, or `.zip` file."
                )
                continue

            # Acknowledge receipt
            _send_to(ADMIN_CHAT_ID, f"📥 Received `{filename}` — processing...")

            # Download file
            file_url = _get_file_url(file_id)
            if not file_url:
                _send_to(ADMIN_CHAT_ID, "❌ Could not retrieve file from Telegram.")
                continue

            file_bytes = _download_file(file_url)
            if not file_bytes:
                _send_to(ADMIN_CHAT_ID, "❌ Failed to download file.")
                continue

            # Parse and update DB
            from price_updater import update_from_file
            count, msg = update_from_file(file_bytes, filename)

            _send_to(ADMIN_CHAT_ID, msg)

            if count > 0:
                log.info(f"Admin uploaded {filename} — {count} stops updated.")

    except Exception as e:
        log.error(f"poll_for_uploads error: {e}", exc_info=True)


# -- Admin command handlers ---------------------------------------------------

def _handle_addtruck(text: str):
    """
    /addtruck Unit 4821 -1009876543210
    /addtruck Unit 4821          (no group — uses dispatcher group)
    """
    from database import auto_register_truck, upsert_truck_group
    parts = text.split(maxsplit=2)
    # parts[0] = /addtruck
    # parts[1..] = vehicle name, possibly with group id at end

    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/addtruck Unit 4821 -1009876543210`")
        return

    # Last part might be a group_id (starts with - and is numeric)
    rest  = parts[1] if len(parts) == 2 else parts[1] + " " + parts[2]
    tokens = rest.rsplit(maxsplit=1)

    if len(tokens) == 2 and tokens[1].lstrip("-").isdigit():
        vehicle_name = tokens[0].strip()
        group_id     = tokens[1].strip()
    else:
        vehicle_name = rest.strip()
        group_id     = None

    if not vehicle_name:
        _send_to(ADMIN_CHAT_ID, "❌ No truck name provided.")
        return

    newly = auto_register_truck("", vehicle_name)
    if group_id:
        upsert_truck_group(vehicle_name, group_id)

    if newly:
        msg = f"✅ Truck added: *{vehicle_name}*"
    else:
        msg = f"ℹ️ Truck already exists: *{vehicle_name}*"

    if group_id:
        msg += f"\nGroup ID set: `{group_id}`"
    else:
        msg += "\nNo group ID set — alerts go to dispatcher group."

    _send_to(ADMIN_CHAT_ID, msg)
    log.info(f"Admin added truck: {vehicle_name} group={group_id}")


def _handle_setgroup(text: str):
    """/setgroup Unit 4821 -1009876543210"""
    from database import upsert_truck_group
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit 4821 -1009876543210`")
        return

    tokens = parts[1].rsplit(maxsplit=1)
    if len(tokens) != 2 or not tokens[1].lstrip("-").isdigit():
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit 4821 -1009876543210`")
        return

    vehicle_name = tokens[0].strip()
    group_id     = tokens[1].strip()

    from database import upsert_truck_group
    updated = upsert_truck_group(vehicle_name, group_id)
    if updated:
        _send_to(ADMIN_CHAT_ID, f"✅ *{vehicle_name}* → group `{group_id}`")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck not found: *{vehicle_name}*\nUse `/addtruck` first.")


def _handle_listtruck():
    """/listtruck — show all trucks"""
    from database import get_all_registered_trucks
    trucks = get_all_registered_trucks()
    if not trucks:
        _send_to(ADMIN_CHAT_ID, "No trucks registered yet.")
        return

    lines = []
    for t in trucks:
        name  = t.get("vehicle_name", "?")
        group = t.get("telegram_group_id") or "— no group"
        lines.append(f"• *{name}*  `{group}`")

    # Split into chunks of 50 trucks to stay under Telegram 4096 char limit
    chunk_size = 50
    chunks = [lines[i:i+chunk_size] for i in range(0, len(lines), chunk_size)]
    for i, chunk in enumerate(chunks):
        header = f"🚛 *Trucks ({len(trucks)} total)* — page {i+1}/{len(chunks)}\n" if len(chunks) > 1 else f"🚛 *Trucks ({len(trucks)} total)*\n"
        _send_to(ADMIN_CHAT_ID, header + "\n".join(chunk))


def _handle_removetruck(text: str):
    """/removetruck Unit 4821"""
    from database import deactivate_truck
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/removetruck Unit 4821`")
        return

    vehicle_name = parts[1].strip()
    removed = deactivate_truck(vehicle_name)
    if removed:
        _send_to(ADMIN_CHAT_ID, f"✅ Truck deactivated: *{vehicle_name}*")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck not found: *{vehicle_name}*")


# -- Trip message polling -----------------------------------------------------

_last_trip_update_id: int = 0


def poll_for_trips() -> None:
    """
    Poll Telegram for new messages in the trip/dispatch group.
    Detects trip assignment messages and saves them to DB.
    """
    global _last_trip_update_id

    if not TRIP_GROUP_ID:
        return

    try:
        result = _post("getUpdates", {
            "offset":  _last_trip_update_id + 1,
            "timeout": 2,
            "limit":   20,
            "allowed_updates": ["message"],
        })

        if not result or not result.get("ok"):
            return

        for update in result.get("result", []):
            _last_trip_update_id = update["update_id"]

            message = update.get("message", {})
            chat_id = str(message.get("chat", {}).get("id", ""))

            # Only process messages from the trip group
            if chat_id != str(TRIP_GROUP_ID):
                continue

            text       = message.get("text", "") or ""
            message_id = message.get("message_id")

            if not text:
                continue

            # Try to parse as trip message
            from trip_parser import parse_trip_message, save_trip
            trip = parse_trip_message(text)

            if not trip:
                continue

            log.info(f"Trip message detected: truck={trip['truck_name']} "
                     f"trip={trip.get('trip_number')} stops={len(trip['stops'])}")

            # Save to DB (geocodes stops automatically)
            trip_id = save_trip(trip, group_id=chat_id, message_id=message_id)

            if trip_id:
                stop_count    = len(trip["stops"])
                geocoded      = sum(1 for s in trip["stops"] if s.get("geocoded"))
                truck_name    = trip["truck_name"]
                trip_number   = trip.get("trip_number", "?")

                _send_to(ADMIN_CHAT_ID,
                    f"📋 *Trip {trip_number} parsed for truck {truck_name}*\n"
                    f"✅ {geocoded}/{stop_count} stops geocoded\n"
                    f"🗺 Route heading will use stop destinations"
                )
                log.info(f"Trip {trip_number} saved — {geocoded}/{stop_count} stops geocoded")

    except Exception as e:
        log.error(f"poll_for_trips error: {e}", exc_info=True)
