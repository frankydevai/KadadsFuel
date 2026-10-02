"""
Auto-link a drivers' Telegram group to a fleet truck.

When the bot is added to a group (or its status changes to member/admin),
`on_my_chat_member` fires:

  1. Pulls the first digit sequence out of the group title — this is the
     truck unit. Missing or ambiguous titles are logged without messaging.
  2. Calls SamsaraClient.find_vehicles_by_unit() — the parsed unit is matched
     against every Samsara vehicle's index keys (first digit sequence plus, for
     SUBUNIT vehicles, the parenthetical number DataTruck uses as the unit).
  3. Exactly one match → upsert trucks_drivers (truck_unit,
     samsara_vehicle_id, driver_telegram_id=group chat_id) silently.
  4. If Samsara listing is unavailable, fall back to an existing DB mapping
     for that unit so known working trucks can still receive Telegram alerts.
  5. Zero or multiple unresolved matches → log the situation, no DB write.
     Admin can fix Samsara naming or use /linktruck manually.

Automatic bot-added events send no group/admin notices and do not replay old
briefings. Explicit linking commands retain their normal feedback.

Also exposes admin command `/linktruck <unit>` (works inside the target
group only) as a manual override when title parsing fails.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from typing import Any

from telegram import Update
from telegram.constants import ChatMemberStatus, ChatType
from telegram.error import RetryAfter
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from dieselup.bot.messages import briefing_message
from dieselup.bot.sender import safe_send
from dieselup.clients.samsara import SamsaraClient, SamsaraError, extract_unit_digits
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.core.driver_assignments import is_inactive_title
from dieselup.db import execute, fetch_one

log = logging.getLogger(__name__)

_GROUP_TYPES = {ChatType.GROUP, ChatType.SUPERGROUP}
_MEMBER_STATUSES = {"member", "administrator"}
_TITLE_PREFIX_RE = re.compile(
    r"^\s*(?:truck|unit|subunit|sub)?\s*#?\s*0*\d+[A-Za-z]?\s*[-–—:|]?\s*",
    re.IGNORECASE,
)
_TITLE_SUFFIX_RE = re.compile(r"\s*\|.*$")


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fired when the bot's own membership status changes in any chat."""
    upd = update.my_chat_member
    if upd is None:
        return
    chat = upd.chat
    if chat.type not in _GROUP_TYPES:
        return

    old_status = upd.old_chat_member.status if upd.old_chat_member else None
    new_status = upd.new_chat_member.status
    if new_status not in _MEMBER_STATUSES:
        return
    if old_status in _MEMBER_STATUSES:
        return

    log.info(
        "group_link: my_chat_member add detected chat_id=%s title=%r old=%s new=%s",
        chat.id,
        chat.title,
        old_status,
        new_status,
    )
    await _attempt_link(
        bot=context.bot,
        chat_id=chat.id,
        chat_title=chat.title or "",
        reason="auto",
    )


async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fallback for Telegram service messages when the bot is added to a group."""
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None or chat.type not in _GROUP_TYPES:
        return

    bot_id = context.bot.id
    if bot_id is None:
        me = await context.bot.get_me()
        bot_id = me.id

    members = message.new_chat_members or []
    if not any(member.id == bot_id for member in members):
        return

    log.info(
        "group_link: new_chat_members add detected chat_id=%s title=%r",
        chat.id,
        chat.title,
    )
    await _attempt_link(
        bot=context.bot,
        chat_id=chat.id,
        chat_title=chat.title or "",
        reason="service_message",
    )


async def on_new_chat_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unlink Home Time/Terminated groups immediately, or reconcile the new title."""
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None or chat.type not in _GROUP_TYPES:
        return
    if not message.new_chat_title:
        return

    log.info(
        "group_link: title change detected chat_id=%s title=%r",
        chat.id,
        message.new_chat_title,
    )
    if is_inactive_title(message.new_chat_title):
        await _unlink_inactive_group(
            bot=context.bot,
            chat_id=chat.id,
            chat_title=message.new_chat_title,
            silent=True,
        )
        return
    await _attempt_link(
        bot=context.bot,
        chat_id=chat.id,
        chat_title=message.new_chat_title,
        reason="title_change",
    )


def _driver_name_from_group_title(title: str | None) -> str | None:
    """Return the driver portion of a conventional Telegram group title."""
    value = _TITLE_SUFFIX_RE.sub("", str(title or "")).strip()
    value = _TITLE_PREFIX_RE.sub("", value).strip(" -–—:|")
    return value or None


def _normalized_name(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(re.sub(r"[^A-Za-z0-9]+", " ", text).upper().split())


def driver_names_match(roster_name: str | None, observed_name: str | None) -> bool:
    """Match one current driver against a roster entry, including team rows."""
    observed = _normalized_name(observed_name)
    if not observed:
        return False
    candidates = re.split(r"\s*/\s*|\s+AND\s+|\s*&\s*", str(roster_name or ""), flags=re.I)
    for candidate in candidates:
        expected = _normalized_name(candidate)
        if not expected:
            continue
        if expected == observed:
            return True
        expected_tokens = set(expected.split())
        observed_tokens = set(observed.split())
        if len(expected_tokens) >= 2 and expected_tokens <= observed_tokens:
            return True
        if len(observed_tokens) >= 2 and observed_tokens <= expected_tokens:
            return True
    return False


async def refresh_and_verify_linked_groups(bot: Any) -> dict[str, int]:
    """Persist live group titles and return only verified active connections."""
    from dieselup.core.driver_assignments import refresh_driver_assignments
    return await refresh_driver_assignments(bot)


async def _read_current_group_title(bot: Any, chat_id: int) -> str:
    """Read a live Telegram title, respecting flood-control retry hints."""
    try:
        chat = await bot.get_chat(chat_id=chat_id)
    except RetryAfter as exc:
        retry_after = exc.retry_after
        seconds = (
            retry_after.total_seconds()
            if hasattr(retry_after, "total_seconds")
            else float(retry_after)
        )
        delay = min(seconds, 15.0)
        log.warning(
            "group_preflight: Telegram rate limit, retrying chat_id=%s after %.1fs",
            chat_id, delay,
        )
        await asyncio.sleep(delay)
        chat = await bot.get_chat(chat_id=chat_id)
    # Pace fleet-wide reads so 50+ linked groups do not trigger Telegram's
    # burst limiter during every 15-minute reconciliation.
    await asyncio.sleep(0.05)
    return str(getattr(chat, "title", "") or "").strip()


async def _unlink_inactive_group(
    *, bot: Any, chat_id: int, chat_title: str, silent: bool = False
) -> None:
    row = await fetch_one(
        """
        UPDATE trucks_drivers
        SET driver_telegram_id = NULL,
            driver_full_name = NULL,
            telegram_group_name = $2,
            assignment_status = 'unlinked',
            updated_at = NOW()
        WHERE driver_telegram_id = $1
          AND ($3::text[] IS NULL OR ltrim(upper(truck_unit),'0')=ANY($3))
        RETURNING truck_unit
        """,
        chat_id,
        chat_title,
        allowed_units(),
    )
    old_unit = str(row["truck_unit"]) if row is not None else None
    if old_unit:
        log.info(
            "group_link: inactive title unlinked chat_id=%s from truck %s",
            chat_id,
            old_unit,
        )
        await _send_group(
            bot,
            chat_id,
            f"Inactive group detected. Fuel alerts are paused for this group; truck {old_unit} was unlinked.",
            silent=silent,
        )
        await _notify_admin(
            bot,
            f"Inactive group unlinked: chat_id={chat_id}, old_unit={old_unit}, title={chat_title!r}",
            silent=silent,
        )
        return
    log.info("group_link: inactive title already unlinked chat_id=%s", chat_id)


async def linktruck(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only manual link: /linktruck <unit> — must be run inside the target group."""
    message = update.effective_message
    if message is None or update.effective_chat is None:
        return
    if update.effective_chat.type not in _GROUP_TYPES:
        await message.reply_text("Run /linktruck inside the drivers' group, not in DM.")
        return
    if not await _is_group_admin(update, context):
        await message.reply_text("Only a group admin can link this truck.")
        return
    args = context.args or []
    if len(args) != 1 or not args[0].strip():
        await message.reply_text("Usage: /linktruck <unit>")
        return

    unit_input = args[0].strip()
    await _attempt_link(
        bot=context.bot,
        chat_id=update.effective_chat.id,
        chat_title=update.effective_chat.title or "",
        reason="manual",
        unit_override=unit_input,
    )


async def _attempt_link(
    *,
    bot: Any,
    chat_id: int,
    chat_title: str,
    reason: str,
    unit_override: str | None = None,
) -> None:
    silent = reason in {"auto", "service_message", "title_change"}
    if silent and not settings.AUTO_LINK_ENABLED and not is_inactive_title(chat_title):
        log.info('group_link: automatic linking disabled')
        return
    if is_inactive_title(chat_title):
        await _unlink_inactive_group(bot=bot, chat_id=chat_id, chat_title=chat_title, silent=silent)
        return
    raw_unit = unit_override if unit_override is not None else chat_title
    unit = extract_unit_digits(raw_unit)
    if not unit:
        await _send_group(
            bot,
            chat_id,
            "Could not detect a truck unit number in this group's title. "
            "Rename the group to include the unit (e.g. \"Truck 3044\") or "
            "have admin run /linktruck <unit> here.",
            silent=silent,
        )
        await _notify_admin(
            bot,
            f"Group link failed (no digits in title) — chat_id={chat_id}, "
            f"title={chat_title!r}",
            silent=silent,
        )
        return

    if not allows(unit):
        log.info('group_link: truck outside test scope')
        return
    existing = await _existing_truck_mapping(unit)
    existing_samsara_id = _existing_samsara_id(existing)

    try:
        async with SamsaraClient() as samsara:
            matches = await samsara.find_vehicles_by_unit(unit)
    except SamsaraError as exc:
        if existing_samsara_id:
            await _link_existing_mapping(
                bot=bot,
                chat_id=chat_id,
                chat_title=chat_title,
                unit=unit,
                samsara_vehicle_id=existing_samsara_id,
                reason=reason,
                note=f"Samsara listing failed: {exc}",
                silent=silent,
            )
            return
        await _send_group(bot, chat_id, f"Samsara error while linking: {exc}", silent=silent)
        await _notify_admin(
            bot,
            f"Group link failed (Samsara) — chat_id={chat_id}, unit={unit}, error={exc}",
            silent=silent,
        )
        return

    if not matches:
        if existing_samsara_id:
            await _link_existing_mapping(
                bot=bot,
                chat_id=chat_id,
                chat_title=chat_title,
                unit=unit,
                samsara_vehicle_id=existing_samsara_id,
                reason=reason,
                note="Samsara returned no unit-name match, using existing DB mapping.",
                silent=silent,
            )
            return
        await _send_group(
            bot,
            chat_id,
            f"No Samsara vehicle found for unit {unit}. "
            "Check the Samsara vehicle name or run /linktruck <unit> after fixing it.",
            silent=silent,
        )
        await _notify_admin(
            bot,
            f"Group link failed (no Samsara match) — chat_id={chat_id}, unit={unit}, "
            f"title={chat_title!r}",
            silent=silent,
        )
        return

    if existing_samsara_id and len(matches) > 1:
        existing_match = [m for m in matches if m.id == existing_samsara_id]
        if len(existing_match) == 1:
            matches = existing_match

    if len(matches) > 1:
        listing = "; ".join(f"{m.id}={m.name!r}" for m in matches)
        await _send_group(
            bot,
            chat_id,
            f"Unit {unit} matches {len(matches)} Samsara vehicles — "
            "fix the duplicates in Samsara, then re-add the bot to this group.",
            silent=silent,
        )
        await _notify_admin(
            bot,
            f"Group link ambiguous — chat_id={chat_id}, unit={unit}, candidates: {listing}",
            silent=silent,
        )
        return

    vehicle = matches[0]
    await _link_resolved_mapping(
        bot=bot,
        chat_id=chat_id,
        chat_title=chat_title,
        unit=unit,
        samsara_vehicle_id=vehicle.id,
        samsara_label=vehicle.name,
        reason=reason,
        admin_note=None,
        group_note=None,
        silent=silent,
    )


async def _existing_truck_mapping(unit: str) -> Any | None:
    return await fetch_one(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        unit,
    )


def _existing_samsara_id(row: Any | None) -> str | None:
    if row is None:
        return None
    value = row["samsara_vehicle_id"]
    return str(value) if value else None


async def _link_existing_mapping(
    *,
    bot: Any,
    chat_id: int,
    chat_title: str,
    unit: str,
    samsara_vehicle_id: str,
    reason: str,
    note: str,
    silent: bool = False,
) -> None:
    await _link_resolved_mapping(
        bot=bot,
        chat_id=chat_id,
        chat_title=chat_title,
        unit=unit,
        samsara_vehicle_id=samsara_vehicle_id,
        samsara_label=f"existing Samsara ID {samsara_vehicle_id}",
        reason=f"{reason}_db_fallback",
        admin_note=note,
        group_note=(
            "Samsara vehicle listing is unavailable or did not match the unit name, "
            "so I used the existing truck mapping already stored in DieselUp."
        ),
        silent=silent,
    )


async def _link_resolved_mapping(
    *,
    bot: Any,
    chat_id: int,
    chat_title: str,
    unit: str,
    samsara_vehicle_id: str,
    samsara_label: str,
    reason: str,
    admin_note: str | None,
    group_note: str | None,
    silent: bool = False,
) -> None:
    from dieselup.core.driver_assignments import connect_driver_group
    try:
        is_active = await connect_driver_group(bot, chat_id, chat_title, unit, samsara_vehicle_id)
    except ValueError as exc:
        await _send_group(bot, chat_id, f"Connection needs review: {exc}", silent=silent)
        return

    log.info(
        "group_link: %s linked truck %s → samsara %s, chat_id %s",
        reason,
        unit,
        samsara_vehicle_id,
        chat_id,
    )
    if silent:
        return
    reply = (
        f"Linked: truck {unit} -> Samsara {samsara_label!r}. "
        + ("Briefings for this truck will be posted here." if is_active else "This connection is paused.")
    )
    if group_note:
        reply += f"\n{group_note}"
    await _send_group(bot, chat_id, reply, silent=silent)
    admin_text = (
        f"Group linked ({reason}): unit {unit}, samsara_vehicle_id={samsara_vehicle_id}, "
        f"chat_id={chat_id}, title={chat_title!r}"
    )
    if admin_note:
        admin_text += f", note={admin_note}"
    await _notify_admin(
        bot,
        admin_text,
        silent=silent,
    )
    if is_active:
        await _replay_latest_pending_alert(bot=bot, chat_id=chat_id, truck_unit=unit)


async def _is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True when the command sender administers the current group."""
    if update.effective_chat is None or update.effective_user is None:
        return False
    try:
        member = await context.bot.get_chat_member(
            chat_id=update.effective_chat.id,
            user_id=update.effective_user.id,
        )
    except Exception:  # noqa: BLE001 - fail closed when Telegram cannot verify
        log.exception(
            "group_link: could not verify admin status chat_id=%s user_id=%s",
            update.effective_chat.id,
            update.effective_user.id,
        )
        return False
    return member.status in {
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    }


def _jsonb_list(raw: Any) -> list[dict[str, Any]]:
    """Parse a stop_events.candidates JSONB value into a list of dicts.

    asyncpg may hand back a str (or bytes) for JSONB, or an already-decoded
    list. Anything unexpected yields an empty list so replay degrades quietly.
    """
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    return []


async def _replay_latest_pending_alert(
    *,
    bot: Any,
    chat_id: int,
    truck_unit: str,
) -> None:
    """Re-send the newest pending briefing to a group that was just linked.

    Rebuilds the text from the stored candidates via briefing_message, which
    renders both the legacy [FUEL PLAN] and the sequential 'STOP n of m'
    formats. Skips if the driver-side briefing already went out for this event.
    """
    event = await fetch_one(
        """
        SELECT id, load_id, candidates, briefing_driver_msg_id
        FROM stop_events
        WHERE truck_unit = $1 AND status = 'pending'
        ORDER BY recommended_at DESC
        LIMIT 1
        """,
        truck_unit,
    )
    if event is None:
        return
    if event["briefing_driver_msg_id"] is not None:
        return

    candidates = _jsonb_list(event["candidates"])
    if not candidates:
        return

    text = briefing_message(str(truck_unit), str(event["load_id"]), candidates)
    msg_id = await safe_send(
        bot=bot,
        chat_id=chat_id,
        text=text,
        alert_type="briefing",
        truck_unit=truck_unit,
        load_id=event["load_id"],
        stop_event_id=int(event["id"]),
        msg_id_column="briefing_driver_msg_id",
        replace_previous_driver_alert=True,
    )
    if msg_id is not None:
        await execute(
            "UPDATE stop_events SET briefing_driver_msg_id = $2 WHERE id = $1",
            int(event["id"]),
            int(msg_id),
        )


async def _send_group(bot: Any, chat_id: int, text: str, *, silent: bool = False) -> None:
    if silent:
        log.info("group_link: %s", text)
        return
    await safe_send(
        bot=bot,
        chat_id=chat_id,
        text=text,
        alert_type="group_link_reply",
        parse_mode=None,
    )


async def _notify_admin(bot: Any, text: str, *, silent: bool = False) -> None:
    if silent:
        log.info("group_link: %s", text)
        return
    await safe_send(
        bot=bot,
        chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
        text=text,
        alert_type="admin_group_link",
        parse_mode=None,
    )


def register_group_link_handlers(application: Application) -> None:
    application.add_handler(
        ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
    )
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_chat_members)
    )
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_TITLE, on_new_chat_title)
    )
    application.add_handler(
        CommandHandler("linktruck", linktruck, filters=filters.ChatType.GROUPS)
    )
