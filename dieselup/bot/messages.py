"""
Telegram message templates for DieselUp NJ.

All bodies are HTML (parse_mode=ParseMode.HTML). Public surface:
  * fuel_plan_message — initial [FUEL PLAN] briefing (one stop, full breakdown)
  * delivery_complete_message — post-delivery follow-up briefing
  * briefing_message — back-compat shim around fuel_plan_message
  * status_message  — /status reply
  * error_message   — uniform failure reply

Stops can be RankedStop dataclasses or dicts replayed from stop_events.candidates.
"""
from __future__ import annotations

from html import escape
from typing import TYPE_CHECKING, Any, Iterable, Sequence
from urllib.parse import quote_plus

from dieselup.config import settings

if TYPE_CHECKING:
    from telegram import InlineKeyboardMarkup


def _pct(gallons: float | int | None) -> int:
    """Render a gallon value as an integer % of TANK_CAPACITY_GALLONS."""
    if gallons is None:
        return 0
    tank = settings.TANK_CAPACITY_GALLONS
    if tank <= 0:
        return 0
    pct = float(gallons) / float(tank) * 100.0
    return max(0, min(100, round(pct)))


def _f(stop: Any, name: str, default: Any = None) -> Any:
    if isinstance(stop, dict):
        return stop.get(name, default)
    return getattr(stop, name, default)


def _address_display(stop: Any) -> str:
    """Best-available complete address: street, city, and state."""
    address = _f(stop, "address")
    city = _f(stop, "city")
    state = _f(stop, "state")
    parts = [str(value).strip() for value in (address, city, state) if value]
    # Some price sources already provide a complete address in `address`.
    # Avoid appending city/state a second time when they are already present.
    result: list[str] = []
    for part in parts:
        if not any(part.casefold() in existing.casefold() for existing in result):
            result.append(part)
    return ", ".join(result) or "(address unknown)"


def _coord_pair(lat: Any, lng: Any) -> str | None:
    if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
        return f"{float(lat):.6f},{float(lng):.6f}"
    return None


def _maps_directions_url(
    *,
    origin_label: str,
    stop: Any,
    origin_lat: float | None = None,
    origin_lng: float | None = None,
) -> str:
    """Google Maps directions URL — origin = pickup city, destination = stop street."""
    origin = _coord_pair(origin_lat, origin_lng) or origin_label
    destination = _coord_pair(_f(stop, "latitude"), _f(stop, "longitude")) or _address_display(stop)
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={quote_plus(origin)}"
        f"&destination={quote_plus(destination)}"
        "&travelmode=driving"
    )


def _maps_location_url(*, lat: float, lng: float) -> str:
    return f"https://maps.google.com/?q={lat:.6f},{lng:.6f}"


def _maps_pin_url(stop: Any) -> str:
    """Google Maps pin URL for the stop's lat/lng (used by delivery-complete msg)."""
    lat = _f(stop, "latitude")
    lng = _f(stop, "longitude")
    if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
        return f"https://maps.google.com/?q={lat},{lng}"
    return _maps_directions_url(origin_label="", stop=stop)


def fuel_plan_message(
    *,
    load_id: str,
    truck_unit: str,
    origin_label: str,
    destination_label: str,
    current_fuel_gallons: float,
    stop: Any,
    gallons_to_pump: int,
    flags: Iterable[str] = (),
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> str:
    """Initial single-stop briefing in the [FUEL PLAN] format."""
    station = str(_f(stop, "station_name") or "Pilot Travel Center")
    address = _address_display(stop)
    distance = float(_f(stop, "distance_miles") or 0.0)
    your_price = float(_f(stop, "your_price") or 0.0)
    retail_price = float(_f(stop, "retail_price") or your_price)
    saving_per_gal = retail_price - your_price
    total_save = saving_per_gal * gallons_to_pump
    projected_tank = float(_f(stop, "projected_tank_at_delivery") or 0.0)
    directions_url = _maps_directions_url(
        origin_label=origin_label,
        stop=stop,
        origin_lat=truck_lat,
        origin_lng=truck_lng,
    )
    truck_coords = _coord_pair(truck_lat, truck_lng)

    save_marker = "✅" if saving_per_gal > 0 else ""

    lines: list[str] = []
    lines.append(
        f"<b>[FUEL PLAN] Load {escape(load_id)} — Truck {escape(truck_unit)}</b>"
    )
    lines.append(escape(f"{origin_label} → {destination_label}"))
    if truck_coords and truck_lat is not None and truck_lng is not None:
        truck_url = _maps_location_url(lat=truck_lat, lng=truck_lng)
        lines.append(f'Truck location: <a href="{escape(truck_url)}">{escape(truck_coords)}</a>')
    lines.append("")
    lines.append("⛽ <b>FUEL STOP AHEAD</b>")
    lines.append("")
    lines.append(f"🏪 {escape(station)}")
    lines.append(f"📌 {escape(address)}")
    lines.append(f"📏 {round(distance)} mi ahead")
    lines.append(f'🗺 <a href="{escape(directions_url)}">Get Directions</a>')
    lines.append("")
    lines.append(
        f"<blockquote>💰 <b>SAVINGS BREAKDOWN</b>\n"
        f"<pre>Pump    ${retail_price:.3f}/gal\n"
        f"Yours   ${your_price:.3f}/gal\n"
        f"Save    ${saving_per_gal:.3f}/gal {save_marker}\n"
        f"Total   ${total_save:,.2f}</pre></blockquote>"
    )
    lines.append(
        f"<blockquote>🛢 <b>FILL PLAN</b>\n"
        f"Pump: <b>{gallons_to_pump} gal</b> (full tank)\n"
        f"Projected at delivery: {_pct(projected_tank)}%"
        f" · Current: {_pct(current_fuel_gallons)}%</blockquote>"
    )
    if flags:
        lines.append("Flags: " + ", ".join(flags))
    return "\n".join(lines).rstrip()


def full_fuel_itinerary_message(
    *,
    load_id: str,
    truck_unit: str,
    origin_label: str,
    destination_label: str,
    current_fuel_gallons: float,
    legs: list[Any],          # all planned stops (dicts or dataclass-like)
    flags: Iterable[str] = (),
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> str:
    """Complete fuel plan sent once at load assignment — shows ALL stops upfront.

    Driver receives the full route itinerary at the start of the load instead
    of a new briefing at each stop. Cheapest stop on the route is highlighted.
    """
    # Find the cheapest stop to highlight it
    prices = [float(_f(leg, "your_price") or 999.0) for leg in legs]
    min_price = min(prices) if prices else 0.0

    total_gal  = sum(int(_f(leg, "gallons_to_pump") or 0) for leg in legs)
    total_cost = sum(
        float(_f(leg, "your_price") or 0) * int(_f(leg, "gallons_to_pump") or 0)
        for leg in legs
    )

    truck_coords = _coord_pair(truck_lat, truck_lng)

    lines: list[str] = []
    lines.append(
        f"<b>🗺 FULL FUEL PLAN — Load {escape(load_id)} · Truck {escape(truck_unit)}</b>"
    )
    lines.append(escape(f"{origin_label} → {destination_label}"))
    lines.append(
        f"Current fuel: {_pct(current_fuel_gallons)}%  ·  "
        f"{len(legs)} fuel stop{'s' if len(legs) != 1 else ''} planned"
    )
    if truck_coords and truck_lat is not None and truck_lng is not None:
        truck_url = _maps_location_url(lat=truck_lat, lng=truck_lng)
        lines.append(
            f'Truck now: <a href="{escape(truck_url)}">{escape(truck_coords)}</a>'
        )
    lines.append("")

    first_stop_block: list[str] = []
    extra_stop_block: list[str] = []

    for i, leg in enumerate(legs, 1):
        station  = str(_f(leg, "station_name") or "Pilot Travel Center")
        address  = _address_display(leg)
        distance = float(_f(leg, "distance_miles") or _f(leg, "mile_marker") or 0.0)
        gallons  = int(_f(leg, "gallons_to_pump") or 0)
        price    = float(_f(leg, "your_price") or 0.0)
        retail   = float(_f(leg, "retail_price") or price)
        saving   = retail - price
        pin_url  = _maps_pin_url(leg)
        is_cheapest = abs(price - min_price) < 0.001 and len(legs) > 1
        cheapest_tag = "  ⭐ cheapest on route" if is_cheapest else ""

        target = first_stop_block if i == 1 else extra_stop_block
        if i > 2:
            target.append("")
        target.append(f"── <b>Stop {i} of {len(legs)}</b> · {round(distance)} mi ahead ──")
        target.append(f"⛽ {escape(station)}")
        target.append(f'📌 <a href="{escape(pin_url)}">{escape(address)}</a>')
        target.append(
            f"🛢 Buy: <b>{gallons} gal</b> @ ${price:.3f}/gal{escape(cheapest_tag)}"
        )
        if saving > 0:
            target.append(
                f"💰 Save ${saving:.3f}/gal vs pump · ${saving * gallons:,.2f} this fill"
            )

    lines.extend(first_stop_block)
    lines.append("")

    if extra_stop_block:
        expandable_inner = "\n".join(extra_stop_block)
        lines.append(f"<blockquote expandable>{expandable_inner}</blockquote>")
        lines.append("")

    lines.append("─" * 28)
    lines.append(f"📦 Total: {total_gal} gal  ·  Est. fuel cost <b>${total_cost:,.2f}</b>")
    if flags:
        lines.append("Flags: " + ", ".join(flags))
    return "\n".join(lines).rstrip()


def sequential_fuel_plan_message(
    *,
    load_id: str,
    truck_unit: str,
    origin_label: str,
    destination_label: str,
    current_fuel_gallons: float,
    stop: Any,
    gallons_to_buy: int,
    stop_number: int,
    stop_count: int,
    is_final_leg: bool,
    flags: Iterable[str] = (),
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> str:
    """One leg of a multi-stop min-cost buy plan: 'STOP n of m' + exact gallons.

    Unlike fuel_plan_message this never says 'full tank' — it shows the exact
    optimal partial buy and why (reach the next stop, or coast to delivery).
    `stop` is a RankedStop/CandidateStop dataclass or a candidates[] dict.
    """
    station = str(_f(stop, "station_name") or "Pilot Travel Center")
    address = _address_display(stop)
    distance = float(_f(stop, "distance_miles") or 0.0)
    your_price = float(_f(stop, "your_price") or 0.0)
    retail_price = float(_f(stop, "retail_price") or your_price)
    saving_per_gal = retail_price - your_price
    total_save = saving_per_gal * gallons_to_buy
    directions_url = _maps_directions_url(
        origin_label=origin_label, stop=stop, origin_lat=truck_lat, origin_lng=truck_lng
    )
    save_marker = "✅" if saving_per_gal > 0 else ""
    reason = "to delivery + reserve" if is_final_leg else f"enough to reach stop {stop_number + 1}"
    flags = list(flags)
    plan_details = _f(stop, "plan") or {}
    partial_lane = (
        isinstance(plan_details, dict) and plan_details.get("degraded") == "partial_lane_best_reachable"
    ) or "plan_degraded_partial_lane_best_reachable" in flags
    if partial_lane:
        reason = "intermediate stop only; delivery plan needs dispatch review"

    lines: list[str] = []
    lines.append(
        f"<b>STOP {stop_number} of {stop_count} — Load {escape(load_id)} "
        f"Truck {escape(truck_unit)}</b>"
    )
    lines.append(escape(f"{origin_label} → {destination_label}"))
    lines.append("")
    lines.append(f"🏪 {escape(station)}")
    lines.append(f"📌 {escape(address)}")
    lines.append(f"📏 {round(distance)} mi ahead")
    lines.append(f'🗺 <a href="{escape(directions_url)}">Get Directions</a>')
    lines.append("")
    purchase_instruction = (
        f"Fill to full (about {gallons_to_buy} gal)"
        if _f(stop, "fill_to_full") else f"Buy: {gallons_to_buy} gal"
    )
    lines.append(
        f"<blockquote>🛢 <b>{purchase_instruction}</b>  ({escape(reason)})\n"
        f"<pre>Yours   ${your_price:.3f}/gal {save_marker}\n"
        f"Pump    ${retail_price:.3f}/gal\n"
        f"Save    ${saving_per_gal:.3f}/gal  →  ${total_save:,.2f}</pre></blockquote>"
    )
    lines.append(f"Current fuel: {_pct(current_fuel_gallons)}%")
    if partial_lane:
        lines.append("⚠️ This stop does not complete the delivery fuel plan. Contact dispatch after fueling.")
    if flags:
        lines.append("Flags: " + ", ".join(flags))
    return "\n".join(lines).rstrip()


def approach_reminder_message(
    *,
    truck_unit: str,
    distance_miles: float,
    stop: Any,
    gallons_to_pump: int,
) -> str:
    """30-mile approach ping — fires once per stop_event, before geofence resolution.

    `stop` is the selected-stop dict (from stop_events.candidates[0]) so we
    have station name, street address, and lat/lng for the directions link.
    """
    station = str(_f(stop, "station_name") or "Pilot Travel Center")
    address = _address_display(stop)
    pin_url = _maps_pin_url(stop)
    fill_instruction = (
        f"Fill to full (about {gallons_to_pump} gal)"
        if _f(stop, "fill_to_full") else f"Fill {gallons_to_pump} gal"
    )
    lines = [
        f"⚠️ <b>FUEL STOP AHEAD — Truck {escape(truck_unit)}</b>",
        f"~{round(distance_miles)} mi to your next stop",
        "",
        f"⛽ <b>{escape(station)}</b>",
        f'📌 <a href="{escape(pin_url)}">{escape(address)}</a>',
        "",
        f"<blockquote>💧 <b>{fill_instruction}</b> when you arrive</blockquote>",
    ]
    return "\n".join(lines)


def approach_reminder_keyboard(
    *,
    stop: Any,
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> "InlineKeyboardMarkup":
    """Directions-only inline button for the 30-mile approach reminder.

    No confirm-fueled button here — the driver hasn't arrived yet.
    Driver can tap directions to open Google Maps straight to the stop.
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # lazy to avoid circular

    directions_url = _maps_directions_url(
        origin_label="",
        stop=stop,
        origin_lat=truck_lat,
        origin_lng=truck_lng,
    )
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🗺 Get Directions", url=directions_url),
    ]])


def missed_fuel_stop_message(
    *,
    truck_unit: str,
    stop: Any,
    distance_miles: float,
    current_fuel_percent: int | None,
    gallons_to_pump: int,
    estimated_loss_dollars: float | None = None,
    truck_lat: float | None = None,
    truck_lng: float | None = None,
    past_stop: bool = True,
) -> str:
    """Alert when compliance decides the advised stop was skipped."""
    station = str(_f(stop, "station_name") or "Pilot/FJ fuel stop")
    price = float(_f(stop, "your_price") or _f(stop, "true_cost_per_gallon") or 0.0)
    estimated_cost = gallons_to_pump * price
    stop_url = _maps_pin_url(stop)
    truck_url = (
        _maps_location_url(lat=truck_lat, lng=truck_lng)
        if truck_lat is not None and truck_lng is not None
        else None
    )
    address = _address_display(stop)
    fuel_suffix = f" · fuel {int(current_fuel_percent)}%" if current_fuel_percent is not None else ""
    relation = "past" if past_stop else "away from"

    lines: list[str] = [
        f"🚩 <b>MISSED FUEL STOP — Truck {escape(truck_unit)}</b>",
        f'Missed: <b><a href="{escape(stop_url)}">{escape(station)}</a></b> · {escape(address)}',
        f"Truck is {round(distance_miles)} mi {relation} the stop{escape(fuel_suffix)}",
        "",
        (
            f"<blockquote>💰 <b>COST IMPACT</b>\n"
            f"<pre>Rec. price   ${price:.3f}/gal\n"
            f"Fill size    {gallons_to_pump} gal\n"
            f"Est. cost    ${estimated_cost:,.0f}</pre></blockquote>"
        ),
    ]
    if estimated_loss_dollars is not None:
        lines.append(
            f"<blockquote>💸 <b>Missed savings: ${estimated_loss_dollars:,.2f}</b></blockquote>"
        )
    lines.append(
        f'📍 <a href="{escape(truck_url)}">Truck location</a>'
        if truck_url
        else "📍 Truck location: n/a"
    )
    lines.append("💧 Next fuel stop plan will follow.")
    return "\n".join(lines).rstrip()


def wrong_fuel_stop_message(
    *,
    truck_unit: str,
    advised_stop: Any,
    actual_stop: Any,
    actual_price: float,
    estimated_loss_dollars: float | None = None,
) -> str:
    def _stop_lines(stop: Any, label: str, emoji: str) -> list[str]:
        """Format one stop block: name, full address, and a map pin link."""
        name    = str(_f(stop, "station_name") or label)
        address = _f(stop, "address")
        city    = _f(stop, "city")
        state   = _f(stop, "state")

        # Build the most complete address string available
        if address and city and state:
            full_addr = f"{address}, {city}, {state}"
        elif city and state:
            full_addr = f"{city}, {state}"
        else:
            full_addr = str(address or city or state or "address unknown")

        pin_url = _maps_pin_url(stop)
        return [
            f"{emoji} <b>{escape(name)}</b>",
            f'   📌 <a href="{escape(pin_url)}">{escape(full_addr)}</a>',
        ]

    advised_lines = _stop_lines(advised_stop, "assigned fuel stop", "✅ Assigned:")
    actual_lines  = _stop_lines(actual_stop,  "another Pilot/FJ stop", "⛽ Fueled at:")

    advised_price = float(_f(advised_stop, "your_price") or 0.0)
    price_diff    = actual_price - advised_price
    diff_str      = f"{'▲ +' if price_diff >= 0 else '▼ '}{abs(price_diff):.3f}/gal"

    lines: list[str] = [
        f"🚩 <b>WRONG FUEL STOP — Truck {escape(truck_unit)}</b>",
        "",
        *advised_lines,
        "",
        *actual_lines,
        "",
        (
            f"<blockquote>💰 <b>PRICE COMPARISON</b>\n"
            f"<pre>Assigned  ${advised_price:.3f}/gal\n"
            f"Actual    ${actual_price:.3f}/gal\n"
            f"Diff      {diff_str}</pre></blockquote>"
        ),
    ]
    if estimated_loss_dollars is not None:
        lines.append(
            f"<blockquote>💸 <b>Estimated loss: ${estimated_loss_dollars:,.2f}</b></blockquote>"
        )
    return "\n".join(lines).rstrip()


def off_network_fueling_message(
    *,
    truck_unit: str,
    advised_stop: Any | None,
    gallons_estimate: float,
    estimated_loss_dollars: float | None = None,
) -> str:
    """Alert when the fuel level jumped away from every contracted Pilot/FJ stop.

    The driver fueled off-network (Love's, TA, cash pump…) so there is no
    contracted price for what was paid — the loss figure is an estimate
    against the lane's worst contracted price.
    """
    lines: list[str] = [
        f"🚩 <b>OFF-NETWORK FUELING — Truck {escape(truck_unit)}</b>",
        f"⛽ Fuel jumped ~{gallons_estimate:,.0f} gal — no contracted Pilot/Flying J stop nearby.",
    ]
    if advised_stop is not None:
        station = str(_f(advised_stop, "station_name") or "assigned fuel stop")
        pin_url = _maps_pin_url(advised_stop)
        address = _address_display(advised_stop)
        price = float(
            _f(advised_stop, "your_price")
            or _f(advised_stop, "true_cost_per_gallon")
            or 0.0
        )
        lines += [
            "",
            f"✅ Assigned: <b>{escape(station)}</b>",
            f'   📌 <a href="{escape(pin_url)}">{escape(address)}</a> · ${price:.3f}/gal',
        ]
    if estimated_loss_dollars is not None:
        lines += [
            "",
            (
                f"<blockquote>💸 <b>Estimated loss: ${estimated_loss_dollars:,.2f}</b>\n"
                f"<i>Based on measured gallons and verified price evidence.</i>"
                f"</blockquote>"
            ),
        ]
    else:
        lines += ["", "Price not confirmed. No dollar loss or driver penalty has been assigned."]
    return "\n".join(lines).rstrip()


def fuel_event_detected_message(
    *,
    truck_unit: str,
    station_name: str | None,
    gallons_estimate: float,
) -> str:
    """Informational note: a truck fueled with no active fuel plan."""
    where = f"at <b>{escape(str(station_name))}</b>" if station_name else "at a contracted stop"
    return "\n".join(
        [
            f"⛽ <b>Fuel event — Truck {escape(truck_unit)}</b>",
            "",
            f"Fueling detected {where} (~{gallons_estimate:,.0f} gal).",
            "No active fuel plan for this truck — recorded for the books.",
        ]
    ).rstrip()


def correct_stop_fueled_message(
    *,
    truck_unit: str,
    stop: Any,
    gallons_to_pump: int,
) -> str:
    station = str(_f(stop, "station_name") or "assigned fuel stop")
    address = _address_display(stop)
    return "\n".join(
        [
            f"✅ <b>Correct stop fueled — Truck {escape(truck_unit)}</b>",
            "",
            (
                f"<blockquote>⛽ <b>{escape(station)}</b>\n"
                f"📌 {escape(address)}\n"
                f"💧 {gallons_to_pump} gal planned fill — on schedule ✅</blockquote>"
            ),
        ]
    ).rstrip()


def delivery_complete_message(
    *,
    truck_unit: str,
    next_load_id: str | None,
    current_fuel_percent: int | None,
    distance_to_next_stop_miles: float | None,
    stop: Any | None,
    gallons_to_pump: int,
) -> str:
    """Post-delivery follow-up briefing — fires once per completed load.

    `stop` may be None (no next active load yet) — message degrades to a
    'next load not yet dispatched' note.
    """
    header = f"📍 <b>Delivery Complete — Truck {escape(truck_unit)}</b>"
    if stop is None:
        return (
            f"{header}\n"
            "No next load dispatched yet — fuel plan will follow once a new load is assigned."
        )

    station = str(_f(stop, "station_name") or "Pilot Travel Center")
    address = _address_display(stop)
    pin_url = _maps_pin_url(stop)
    distance = round(float(distance_to_next_stop_miles or _f(stop, "distance_miles") or 0))
    fuel_pct = (
        f"Current fuel: {int(current_fuel_percent)}%"
        if current_fuel_percent is not None else
        "Current fuel: n/a"
    )
    fill_instruction = (
        f"Fill to full (about {gallons_to_pump} gal)"
        if _f(stop, "fill_to_full") else f"Buy {gallons_to_pump} gal"
    )

    lines: list[str] = [
        header,
    ]
    if next_load_id:
        lines.append(f"New load: {escape(next_load_id)}")
    lines += [
        "",
        f"Next fuel stop · {distance} mi ahead",
        "",
        f"⛽ <b>{escape(station)}</b>",
        f"📌 {escape(address)}",
        f'🗺 <a href="{escape(pin_url)}">Get Directions</a>',
        "",
        (
            f"<blockquote>🛢 <b>FILL PLAN</b>\n"
            f"<b>{fill_instruction}</b>\n"
            f"{fuel_pct}</blockquote>"
        ),
        "",
        "📍 Approach reminder fires at 30 mi.",
    ]
    plan_details = _f(stop, "plan") or {}
    if isinstance(plan_details, dict) and plan_details.get("degraded") == "partial_lane_best_reachable":
        lines.append("⚠️ Delivery is not covered yet. Contact dispatch after this fuel stop.")
    return "\n".join(lines).rstrip()


def briefing_message(truck: str, load: str, candidates: Sequence[Any]) -> str:
    """Back-compat wrapper used by /briefing handler — renders the new format
    against the first candidate stored in stop_events.candidates."""
    if not candidates:
        return (
            f"Fueling briefing — Truck <b>{escape(str(truck))}</b> — Load {escape(str(load))}\n"
            "No candidates stored for this load."
        )
    stop = candidates[0]
    origin_label = str(_f(stop, "origin_label") or "")
    destination_label = str(_f(stop, "destination_label") or "")
    current_fuel = float(_f(stop, "current_fuel_gallons") or 0.0)
    gallons = int(_f(stop, "gallons_to_pump") or settings.TANK_CAPACITY_GALLONS)
    flags = _f(stop, "flags") or []
    truck_lat = _f(stop, "truck_latitude")
    truck_lng = _f(stop, "truck_longitude")
    stop_number = _f(stop, "stop_number")
    stop_count = _f(stop, "stop_count")
    if isinstance(stop_number, int) and isinstance(stop_count, int):
        return sequential_fuel_plan_message(
            load_id=str(load),
            truck_unit=str(truck),
            origin_label=origin_label,
            destination_label=destination_label,
            current_fuel_gallons=current_fuel,
            stop=stop,
            gallons_to_buy=gallons,
            stop_number=stop_number,
            stop_count=stop_count,
            is_final_leg=stop_number >= stop_count,
            flags=flags if isinstance(flags, list) else [],
            truck_lat=float(truck_lat) if isinstance(truck_lat, (int, float)) else None,
            truck_lng=float(truck_lng) if isinstance(truck_lng, (int, float)) else None,
        )
    return fuel_plan_message(
        load_id=str(load),
        truck_unit=str(truck),
        origin_label=origin_label,
        destination_label=destination_label,
        current_fuel_gallons=current_fuel,
        stop=stop,
        gallons_to_pump=gallons,
        flags=flags if isinstance(flags, list) else [],
        truck_lat=float(truck_lat) if isinstance(truck_lat, (int, float)) else None,
        truck_lng=float(truck_lng) if isinstance(truck_lng, (int, float)) else None,
    )


def status_message(truck: str, load: str | None, next_stop: Any | None) -> str:
    """Driver /status reply — current truck, current load, next recommended stop."""
    load_line = f"Load: {escape(str(load))}" if load else "Load: none active"
    lines = [f"<b>Truck {escape(str(truck))}</b>  ·  {load_line}"]
    if next_stop is None:
        lines.append("No active fuel stop recommendation.")
    else:
        station = str(_f(next_stop, "station_name") or "Pilot Travel Center")
        address = _address_display(next_stop)
        your_price = float(_f(next_stop, "your_price") or 0.0)
        lines += [
            "",
            (
                f"<blockquote>⛽ <b>{escape(station)}</b>\n"
                f"📌 {escape(address)}\n"
                f"💲 ${your_price:.3f}/gal</blockquote>"
            ),
        ]
    return "\n".join(lines)


def error_message(exception: BaseException) -> str:
    """Uniform error reply. Escaped so exception text can't break HTML parsing."""
    return escape(f"Error: {type(exception).__name__}: {exception}")


def fuel_plan_keyboard(
    *,
    stop: Any,
    stop_event_id: int,
    origin_label: str = "",
    truck_lat: float | None = None,
    truck_lng: float | None = None,
) -> "InlineKeyboardMarkup":
    """Inline keyboard for driver fuel plan messages.

    Two buttons: a URL button for Google Maps directions and a callback button
    the driver taps to confirm they fueled at the recommended stop. Only attach
    to the driver's copy of the message — dispatch doesn't need it.
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # lazy to avoid circular

    directions_url = _maps_directions_url(
        origin_label=origin_label,
        stop=stop,
        origin_lat=truck_lat,
        origin_lng=truck_lng,
    )
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🗺 Get Directions", url=directions_url),
        InlineKeyboardButton("✅ Confirm Fueled", callback_data=f"fueled:{stop_event_id}"),
    ]])
