"""
telegram_bot.py  -  Telegram message sending for FleetFuel bot.

All alerts go to the truck's own Telegram group.
Dispatcher group receives: startup, no-stop emergencies, left-yard-low-fuel.
"""

import time
import logging
import requests
from config import TELEGRAM_BOT_TOKEN, DISPATCHER_GROUP_ID, ADMIN_CHAT_ID, MIN_SAVINGS_DISPLAY

log = logging.getLogger(__name__)

# Shared flag — set True by /checknow, read by main.py
force_check_now: bool = False
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


def _esc(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def _send_to(chat_id: str, text: str) -> int | None:
    """Send message to a specific chat_id. Returns message_id or None.
    Falls back to plain text if MarkdownV2 fails."""
    if not chat_id:
        log.warning("No chat_id — message not sent.")
        return None
    # Try with Markdown first, fall back to plain text on parse error
    result = _post("sendMessage", {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "Markdown",
        "disable_web_page_preview": True,
    })
    if result and result.get("ok"):
        return result["result"]["message_id"]
    # Fallback: strip markdown, send plain
    import re
    plain = re.sub(r"[*_`\[\]]", "", text)
    result2 = _post("sendMessage", {
        "chat_id":                  chat_id,
        "text":                     plain,
        "disable_web_page_preview": True,
    })
    if result2 and result2.get("ok"):
        return result2["result"]["message_id"]
    return None


def _send_to_truck(vehicle_name: str, text: str) -> dict:
    """Send to truck's own group AND dispatcher. Returns {truck_group, truck_msg_id, dispatcher_msg_id}."""
    from database import get_truck_group
    truck_group = get_truck_group(vehicle_name)

    truck_msg_id      = None
    dispatcher_msg_id = None

    if truck_group:
        truck_msg_id = _send_to(truck_group, text)
    else:
        log.info(f"No group set for {vehicle_name} — dispatcher only")

    if DISPATCHER_GROUP_ID and truck_group != str(DISPATCHER_GROUP_ID):
        dispatcher_msg_id = _send_to_dispatcher(text)

    return {
        "truck_group":       truck_group,
        "truck_msg_id":      truck_msg_id,
        "dispatcher_msg_id": dispatcher_msg_id,
    }


def delete_message(chat_id: str, message_id: int) -> bool:
    """Delete a message from a chat. Returns True if successful."""
    result = _post("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    return bool(result and result.get("ok"))


def _send_to_dispatcher(text: str) -> int | None:
    """Send message to the dispatcher group."""
    if not DISPATCHER_GROUP_ID:
        return None
    return _send_to(DISPATCHER_GROUP_ID, text)



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
        f"{emoji} *Low Fuel Alert — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*   🧭 {speed_mph:.0f} mph {compass}",
        f"📍 [View on Map]({truck_url})",
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
            f"⛽ *{name}*",
            f"📌 {full_address}",
            f"🛣 {dist:.1f} mi away",
            f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A",
        ]

        # Savings vs nearest stop
        if alt_stop and price and alt_stop.get("diesel_price"):
            nearest_price   = alt_stop.get("diesel_price")
            nearest_name    = alt_stop.get("store_name", "nearest stop")
            nearest_dist    = alt_stop.get("distance_miles", 0)
            price_diff      = nearest_price - price
            if price_diff > 0.01:
                from config import DEFAULT_TANK_GAL, SAFETY_RESERVE
                gallons_needed  = round(DEFAULT_TANK_GAL * (1 - fuel_pct / 100) * (1 - SAFETY_RESERVE))
                total_savings   = round(price_diff * gallons_needed, 2)
                lines.append(
                    f"💵 Saves *${price_diff:.2f}/gal × {gallons_needed} gal = ${total_savings:.0f}* "
                    f"vs {nearest_name} ({nearest_dist:.1f} mi, ${nearest_price:.3f}/gal)"
                )

        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        lines += [
            "",
            "No diesel stops found within range.",
            "Dispatcher notified.",
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
        f"🌵 *California Border Ahead — Truck {vehicle_name}*",
        f"🛣 {dist_to_border:.0f} miles to CA border",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
        f"📍 [Truck Location]({truck_url})",
        "",
        f"💡 *Fill up before crossing — save on every gallon!*",
    ]

    if best_stop:
        name     = best_stop.get("store_name", "Unknown")
        address  = best_stop.get("address", "")
        city     = best_stop.get("city", "")
        state    = best_stop.get("state", "")
        zip_     = best_stop.get("zip", "")
        dist     = best_stop.get("distance_miles", 0)
        price    = best_stop.get("diesel_price")
        lat      = best_stop.get("latitude")
        lng      = best_stop.get("longitude")
        full_address = ", ".join(filter(None, [address, city, state, zip_]))
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None

        lines += [
            "",
            f"⛽ *{name}*",
            f"📌 {full_address}",
            f"🛣 {dist:.1f} mi away",
            f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A",
        ]
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")

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

    return _send_to_truck(vehicle_name, "\n".join(lines))


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
        {"command": "checknow",    "description": "Force immediate fuel check on all trucks"},
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
            "offset":          _last_update_id + 1,
            "timeout":         0,
            "limit":           20,
            "allowed_updates": ["message", "my_chat_member"],
        })

        if not result or not result.get("ok"):
            return

        updates = result.get("result", [])

        for update in updates:
            _last_update_id = update["update_id"]

            # -- Detect when bot is added to a new group ----------------------
            chat_member = update.get("my_chat_member", {})
            if chat_member:
                new_status = chat_member.get("new_chat_member", {}).get("status", "")
                if new_status in ("member", "administrator"):
                    chat    = chat_member.get("chat", {})
                    g_id    = str(chat.get("id", ""))
                    g_title = chat.get("title", "") or ""

                    # Extract truck number from first word of group title
                    # e.g. "2710 Delima Michel" → "2710"
                    first_word = g_title.strip().split()[0] if g_title.strip() else ""
                    matched = None
                    if first_word:
                        from database import get_all_registered_trucks, upsert_truck_group
                        trucks = get_all_registered_trucks()
                        for truck in trucks:
                            if truck["vehicle_name"] == first_word:
                                matched = first_word
                                break

                    if matched:
                        upsert_truck_group(matched, g_id)
                        _send_to(ADMIN_CHAT_ID,
                            f"✅ *Auto-assigned group*\n"
                            f"Truck: *{matched}*\n"
                            f"Group: *{g_title}*\n"
                            f"ID: `{g_id}`"
                        )
                        log.info(f"Auto-assigned group {g_id} ({g_title}) to truck {matched}")
                    else:
                        # No match — send group info for manual assignment
                        _send_to(ADMIN_CHAT_ID,
                            f"➕ *Bot added to group — no truck matched*\n"
                            f"Group: *{g_title}*\n"
                            f"ID: `{g_id}`\n\n"
                            f"Assign manually:\n`/setgroup TRUCKNAME {g_id}`"
                        )
                        log.info(f"Bot added to group: {g_title} ({g_id}) — no truck matched")
                continue

            message = update.get("message", {})
            chat_id = str(message.get("chat", {}).get("id", ""))

            document = message.get("document")
            text     = message.get("text", "").strip()

            # Strip bot username from commands e.g. /listtruck@FuelAlertBot → /listtruck
            if text.startswith("/"):
                text = text.split("@")[0]

            # /findstop works from ANY group (driver groups)
            if text.startswith("/findstop"):
                try:
                    _handle_findstop(text, chat_id)
                except Exception as e:
                    log.error(f"/findstop error: {e}", exc_info=True)
                    _send_to(chat_id, f"❌ Error finding stop: `{e}`")
                continue

            # All other commands — admin only
            if chat_id != ADMIN_CHAT_ID:
                continue

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
                    elif text.startswith("/checknow"):
                        _handle_checknow()
                    else:
                        _send_to(ADMIN_CHAT_ID,
                            "Available commands:\n"
                            "/addtruck Unit4821 -100123456\n"
                            "/setgroup Unit4821 -100123456\n"
                            "/listtruck\n"
                            "/removetruck Unit4821\n"
                            "/findstop 0792  ← works in any group"
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

def _handle_checknow():
    """/checknow — force immediate check of all trucks"""
    global force_check_now
    force_check_now = True
    _send_to(ADMIN_CHAT_ID, "🔄 *Force check triggered.* Checking all trucks now...")


def _handle_addtruck(text: str):
    """/addtruck <vehicle_name> [group_id]"""
    from database import auto_register_truck, upsert_truck_group
    parts = text.split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: /addtruck <vehicle_name> [telegram_group_id]")
        return
    vehicle_name = parts[1]
    group_id     = parts[2] if len(parts) >= 3 else None
    try:
        auto_register_truck(vehicle_name, vehicle_name)
        if group_id:
            upsert_truck_group(vehicle_name, group_id)
        msg = f"✅ Truck *{vehicle_name}* added"
        if group_id:
            msg += f" → group `{group_id}`"
        _send_to(ADMIN_CHAT_ID, msg)
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Failed to add truck: `{e}`")


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


def _handle_findstop(text: str, chat_id: str):
    """/findstop <truck_number> — find top 3 cheapest stops within 50 miles"""
    from database import get_all_diesel_stops
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import haversine_miles

    FINDSTOP_RADIUS  = 50.0   # miles
    FINDSTOP_TOP_N   = 3

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/findstop <truck number>`\nExample: `/findstop 0792`")
        return

    truck_number = parts[1].strip()

    # Get live location from Samsara
    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(chat_id, f"❌ Could not reach Samsara: `{e}`")
        return

    # Match truck by name (partial match — "0792" matches "Truck 0792")
    truck = None
    for v in vehicles:
        if truck_number.lower() in v.get("vehicle_name", "").lower():
            truck = v
            break

    if not truck:
        _send_to(chat_id, f"❌ Truck *{truck_number}* not found in Samsara.\nCheck the truck number and try again.")
        return

    lat   = truck.get("lat")
    lng   = truck.get("lng")
    speed = truck.get("speed_mph", 0)
    fuel  = truck.get("fuel_pct", 0)
    vname = truck.get("vehicle_name", truck_number)

    if not lat or not lng:
        _send_to(chat_id, f"❌ No GPS location available for truck *{vname}*.")
        return

    # Find all stops within 50 miles, sort by price
    all_stops = get_all_diesel_stops()
    nearby = []
    for stop in all_stops:
        slat = float(stop["latitude"])
        slng = float(stop["longitude"])
        dist = haversine_miles(lat, lng, slat, slng)
        if dist <= FINDSTOP_RADIUS and stop.get("diesel_price"):
            nearby.append({**stop, "distance_miles": round(dist, 1)})

    if not nearby:
        _send_to(chat_id,
            f"⚠️ No fuel stops found within {FINDSTOP_RADIUS:.0f} miles of truck *{vname}*.\n"
            f"📍 Location: `{lat:.4f}, {lng:.4f}`"
        )
        return

    # Sort by price (cheapest first), take top 3
    nearby.sort(key=lambda s: s["diesel_price"])
    top = nearby[:FINDSTOP_TOP_N]

    lines = [
        f"⛽ *Fuel Stops — Truck {vname}*",
        f"📍 Current location | ⛽ {fuel:.0f}% fuel | 🧭 {speed:.0f} mph",
        f"🔍 Top {FINDSTOP_TOP_N} cheapest within {FINDSTOP_RADIUS:.0f} miles\n",
    ]

    for i, stop in enumerate(top, 1):
        name    = stop["store_name"]
        address = f"{stop.get('address','')}, {stop.get('city','')}, {stop.get('state','')}"
        dist    = stop["distance_miles"]
        price   = stop["diesel_price"]
        gmaps   = f"https://maps.google.com/?q={stop['latitude']},{stop['longitude']}"

        lines.append(f"*#{i} — {name}*")
        lines.append(f"📌 {address.strip(', ')}")
        lines.append(f"🛣 {dist} mi away")
        lines.append(f"💰 Diesel: ${price:.3f}/gal")
        lines.append(f"🗺 [Open in Google Maps]({gmaps})")
        if i < len(top):
            lines.append("")

    _send_to(chat_id, "\n".join(lines))




# -- Trip message polling -----------------------------------------------------
