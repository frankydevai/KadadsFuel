"""
Driver-facing Telegram command handlers for DieselUp NJ.

Three commands, all keyed on the current Telegram chat or user's ID matched against
trucks_drivers.driver_telegram_id:

  /status    current truck + current load + next recommended stop
  /briefing  re-fetch the latest briefing for the current load
  /myweek    driver's own savings/losses since Monday 00:00 EST

`register_handlers()` is the only public entrypoint; main.py calls it once
after the Application is built. No driver-specific data is ever surfaced
in any chat except the driver's own — `_resolve_driver()` enforces the
mapping and replies with a friendly "not onboarded" message otherwise.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from dieselup.bot.messages import briefing_message, status_message
from dieselup.core.advice_guard import validate_fuel_event
from dieselup.db import fetch_all, fetch_one

log = logging.getLogger(__name__)

EST = ZoneInfo("America/New_York")


async def _resolve_driver(update: Update) -> dict[str, Any] | None:
    """Return the trucks_drivers row for the Telegram chat/user, or None."""
    user = update.effective_user
    chat = update.effective_chat
    lookup_ids: list[int] = []
    if chat is not None:
        lookup_ids.append(int(chat.id))
    if user is not None and int(user.id) not in lookup_ids:
        lookup_ids.append(int(user.id))
    if not lookup_ids:
        return None
    row = await fetch_one(
        """
        SELECT id, truck_unit, driver_full_name, driver_telegram_id
        FROM trucks_drivers
        WHERE driver_telegram_id = ANY($1::bigint[])
        ORDER BY CASE WHEN driver_telegram_id = $2 THEN 0 ELSE 1 END
        LIMIT 1
        """,
        lookup_ids,
        lookup_ids[0],
    )
    if row is None:
        return None
    return dict(row)


async def _latest_pending_event(truck_unit: str) -> dict[str, Any] | None:
    row = await fetch_one(
        """
        SELECT id, load_id, candidates, recommended_at
        FROM stop_events
        WHERE truck_unit = $1 AND status = 'pending'
        ORDER BY recommended_at DESC
        LIMIT 1
        """,
        truck_unit,
    )
    return dict(row) if row else None


def _candidates_from_json(raw: Any) -> list[dict[str, Any]]:
    """asyncpg returns JSONB as a str — be defensive about already-parsed lists too."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start — liveness check; tells driver if they're onboarded."""
    message = update.effective_message
    if message is None:
        return
    driver = await _resolve_driver(update)
    if driver is None:
        await message.reply_text(
            "DieselUp is running.\n"
            "You are not yet linked — ask your admin to add this chat as your driver group."
        )
        return
    await message.reply_text(
        f"DieselUp running.\n"
        f"Truck: {driver['truck_unit']} — {driver['driver_full_name'] or '(no name)'}\n"
        f"Commands: /status  /briefing  /myweek"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/status — current truck, current load, top recommended stop."""
    message = update.effective_message
    if message is None:
        return
    driver = await _resolve_driver(update)
    if driver is None:
        await message.reply_text(
            "You are not onboarded. Ask admin to run /setdriver <unit> <your name>."
        )
        return

    event = await _latest_pending_event(driver["truck_unit"])
    if event is None:
        await message.reply_text(
            status_message(truck=driver["truck_unit"], load=None, next_stop=None),
            parse_mode=ParseMode.HTML,
        )
        return

    candidates = _candidates_from_json(event["candidates"])
    top = candidates[0] if candidates else None
    if top is not None and not await validate_fuel_event(event["id"], driver["truck_unit"], update.effective_chat.id):
        top = None
    await message.reply_text(
        status_message(
            truck=driver["truck_unit"],
            load=event["load_id"],
            next_stop=top,
        ),
        parse_mode=ParseMode.HTML,
    )


async def briefing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/briefing — re-send the latest top-3 fueling briefing for the current load."""
    message = update.effective_message
    if message is None:
        return
    driver = await _resolve_driver(update)
    if driver is None:
        await message.reply_text(
            "You are not onboarded. Ask admin to run /setdriver <unit> <your name>."
        )
        return

    event = await _latest_pending_event(driver["truck_unit"])
    if event is None:
        await message.reply_text(
            "No active briefing — there is no pending fueling recommendation."
        )
        return

    candidates = _candidates_from_json(event["candidates"])
    if not await validate_fuel_event(event["id"], driver["truck_unit"], update.effective_chat.id):
        await message.reply_text("Saved fuel advice cannot be verified against your current trip and location. Await a fresh route plan.")
        return
    if not candidates:
        await message.reply_text(
            "Briefing is stored but has no candidates. Ask admin to /forcebriefing "
            f"{driver['truck_unit']}."
        )
        return

    await message.reply_text(
        briefing_message(
            truck=driver["truck_unit"],
            load=event["load_id"],
            candidates=candidates,
        ),
        parse_mode=ParseMode.HTML,
    )


async def myweek(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/myweek — driver's saved/lost/skipped + net $ since Monday 00:00 EST."""
    message = update.effective_message
    if message is None:
        return
    driver = await _resolve_driver(update)
    if driver is None:
        await message.reply_text(
            "You are not onboarded. Ask admin to run /setdriver <unit> <your name>."
        )
        return

    now_est = datetime.now(EST)
    week_start_est = (now_est - timedelta(days=now_est.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start_utc = week_start_est.astimezone(timezone.utc)

    totals = await fetch_one(
        """
        SELECT
            COUNT(*) AS recs,
            COUNT(*) FILTER (WHERE status = 'saved')   AS saved,
            COUNT(*) FILTER (WHERE status = 'lost')    AS lost,
            COUNT(*) FILTER (WHERE status = 'skipped') AS skipped,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COALESCE(SUM(dollar_impact) FILTER (WHERE status = 'saved'), 0) AS savings,
            COALESCE(SUM(
                CASE
                    WHEN status = 'lost' AND dollar_impact < 0
                        THEN -dollar_impact
                    WHEN status = 'skipped'
                        THEN GREATEST(worst_candidate_true_cost - recommended_true_cost, 0) * gallons
                    ELSE 0
                END
            ), 0) AS losses
        FROM stop_events
        WHERE truck_unit = $1
          AND recommended_at >= $2
        """,
        driver["truck_unit"],
        week_start_utc,
    )

    recent = await fetch_all(
        """
        SELECT
            load_id,
            status,
            CASE
                WHEN status = 'skipped'
                     AND (dollar_impact IS NULL OR dollar_impact >= 0)
                    THEN -GREATEST(worst_candidate_true_cost - recommended_true_cost, 0) * gallons
                ELSE dollar_impact
            END AS dollar_impact,
            recommended_at
        FROM stop_events
        WHERE truck_unit = $1
          AND recommended_at >= $2
          AND status <> 'pending'
        ORDER BY recommended_at DESC
        LIMIT 5
        """,
        driver["truck_unit"],
        week_start_utc,
    )

    savings = float(totals["savings"] or 0)
    losses = float(totals["losses"] or 0)
    net = savings - losses

    net_emoji = "✅" if net >= 0 else "🔴"
    status_line = (
        f"Saved {int(totals['saved'])}  |  "
        f"Lost {int(totals['lost'])}  |  "
        f"Skipped {int(totals['skipped'])}"
    )
    summary_block = (
        f"<blockquote>{status_line}\n"
        f"Savings: <b>${savings:,.2f}</b>\n"
        f"Losses:  ${losses:,.2f}\n"
        f"Net:     <b>{net_emoji} ${net:,.2f}</b></blockquote>"
    )

    lines = [
        f"<b>Your week — Truck {escape(driver['truck_unit'])}</b>",
        f"Since {week_start_est.strftime('%a %b %d')} EST"
        f"  ·  {int(totals['recs'])} recs (pending {int(totals['pending'])})",
        "",
        summary_block,
    ]

    if recent:
        recent_lines = []
        for row in recent:
            ts = row["recommended_at"].astimezone(EST).strftime("%a %m-%d %H:%M")
            impact = float(row["dollar_impact"] or 0)
            sign = "+" if impact >= 0 else ""
            recent_lines.append(
                f"{ts}  {escape(str(row['load_id']))}  "
                f"{row['status']}  {sign}${impact:,.2f}"
            )
        lines.append(
            "<blockquote expandable><b>Recent stops</b>\n"
            + "\n".join(recent_lines)
            + "</blockquote>"
        )

    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def handle_confirm_fueled(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback for the '✅ Confirm Fueled' inline button on fuel plan messages.

    Marks the stop_event as 'saved' and removes the keyboard from the message
    so the driver can't double-tap. Only the assigned driver can trigger this.
    """
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    driver = await _resolve_driver(update)
    if driver is None:
        await query.answer("Not linked to a truck — contact admin.", show_alert=True)
        return

    try:
        stop_event_id = int((query.data or "").split(":", 1)[1])
    except (ValueError, IndexError):
        return

    # Only update if still pending — idempotent on double-tap
    row = await fetch_one(
        "UPDATE stop_events SET status = 'saved' WHERE id = $1 AND status = 'pending' RETURNING id",
        stop_event_id,
    )

    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass  # message too old or already edited

    if row:
        await query.message.reply_text(
            f"✅ <b>Fill confirmed</b> — logged as saved for truck {escape(driver['truck_unit'])}.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await query.message.reply_text("Already resolved — no change needed.")


def register_handlers(application: Application) -> None:
    """Attach driver command handlers to the Application."""
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("briefing", briefing))
    application.add_handler(CommandHandler("myweek", myweek))
    application.add_handler(
        CallbackQueryHandler(handle_confirm_fueled, pattern=r"^fueled:\d+$")
    )
