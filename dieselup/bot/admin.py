"""
Admin-only Telegram handlers for DieselUp NJ.

Document handler for Pilot/FJ, Love's and FTS price uploads — admin chat only.
Supplier identity, quote dates, station matches and excluded rows are recorded
atomically. Replies obey the test-period messaging policy.

Commands (all restricted to TELEGRAM_ADMIN_CHAT_ID):
  /pricestatus               last upload, account, effective date, summary
  /truckstatus <unit>        driver, load, fuel level, last fueling
  /whodrives <unit>          currently assigned driver
  /setdriver <unit> <name>   manually assign driver to truck
  /movetruckchat <old> <new> move driver Telegram group to another truck
  /forcebriefing <unit>      re-run optimizer + send briefing for the truck
  /refreshgraph              snap Pilot/FJ stops + rebuild Valhalla stop graph
  /fleetstats                current-week fleet stats
  /no_valid_stop_alerts      trucks where optimizer failed in last 24h
  /listtrucks                full fleet link status (/listtruck also works)
  /linkgroup <chat_id> <unit> link a drivers' group from admin chat
  /setgroup <unit> <chat_id> same link, accepting dispatch-friendly order

register_admin_handlers() is the single public entrypoint, wired in main.py.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from telegram import Bot, Update
from telegram.error import NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import traceback

from dieselup import metrics
from dieselup.bot.sender import safe_send
from dieselup.circuit_breaker import samsara_breaker, telegram_breaker, tms_breaker
from dieselup.clients.tms import make_tms_client
from dieselup.clients.samsara import SamsaraClient, SamsaraError
from dieselup.config import settings
from dieselup.core.stop_graph import format_refresh_result, refresh_stop_graph
from dieselup.db import execute, fetch_all, fetch_one, get_pool
from dieselup.ingestion.price_imports import save_import

log = logging.getLogger(__name__)

EST = ZoneInfo("America/New_York")
ALLOWED_EXTENSIONS = (".xls", ".xlsx")
PRICE_DOWNLOAD_ATTEMPTS = 3
PRICE_DOWNLOAD_TIMEOUT_SECONDS = 60.0


async def _download_price_document(bot: Bot, file_id: str, local_path: str) -> None:
    """Download a Telegram attachment, retrying transient transport failures."""
    for attempt in range(1, PRICE_DOWNLOAD_ATTEMPTS + 1):
        try:
            tg_file = await bot.get_file(
                file_id,
                read_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                write_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                connect_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                pool_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
            )
            await tg_file.download_to_drive(
                custom_path=local_path,
                read_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                write_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                connect_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
                pool_timeout=PRICE_DOWNLOAD_TIMEOUT_SECONDS,
            )
            return
        except (TimedOut, NetworkError):
            if attempt == PRICE_DOWNLOAD_ATTEMPTS:
                raise
            log.warning(
                "Pilot price download transport failure; retrying attempt %d/%d",
                attempt + 1,
                PRICE_DOWNLOAD_ATTEMPTS,
                exc_info=True,
            )
            await asyncio.sleep(float(attempt))


async def handle_price_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Read supplier files in the admin chat; all replies obey quiet mode."""
    message = update.effective_message
    if message is None or message.document is None:
        return
    filename = message.document.file_name or ""
    if not filename.lower().endswith(ALLOWED_EXTENSIONS):
        return
    suffix = ".xlsx" if filename.lower().endswith(".xlsx") else ".xls"
    upload_key = f"telegram:{message.chat_id}:{message.message_id}"
    if getattr(message, 'edit_date', None):
        revision = str(getattr(message.document, 'file_unique_id', '')) + '\0' + (message.caption or '')
        upload_key += ':revision:' + hashlib.sha256(revision.encode()).hexdigest()[:20]
    local_path = None
    try:
        with tempfile.NamedTemporaryFile(prefix="kadads_price_",suffix=suffix,delete=False) as file:
            local_path = file.name
        try:
            await _download_price_document(context.bot,message.document.file_id,local_path)
        except Exception:
            log.exception("Price file download failed")
            receipt = await save_import(upload_key=upload_key,filename=filename,
                error="Download failed after three attempts; upload the file again.")
        else:
            try:
                receipt = await save_import(upload_key=upload_key,filename=filename,
                    path=local_path,caption=message.caption,uploaded_at=getattr(message,'date',None))
            except Exception:
                log.exception("Price import failed; transaction rolled back")
                receipt = await save_import(upload_key=upload_key,filename=filename,
                    error="Import could not be saved; no partial price update was accepted.")
        log.info("Price import %s: supplier=%s status=%s rows=%s mapped=%s reason=%s",
            receipt['id'],receipt['provider'],receipt['status'],receipt['row_count'],
            receipt['matched_rows'],receipt['reason'])
        completed = receipt['status'] == 'completed'
        text = (f"Prices imported: {receipt['row_count']} rows; {receipt['matched_rows']} stations matched. "
                f"Quote date: {receipt['effective_date']}." if completed else
                f"Prices were not activated: {receipt['reason']}")
        await safe_send(bot=context.bot,chat_id=message.chat_id,text=text,parse_mode=None,
            alert_type='price_upload_completed' if completed else 'price_upload_rejected',
            queue_on_failure=False)
    finally:
        if local_path:
            try:
                os.remove(local_path)
            except OSError:
                pass


async def pricestatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/pricestatus — last upload, account, effective date, summary stats."""
    message = update.effective_message
    if message is None:
        return

    latest = await fetch_one(
        """
        SELECT effective_date, account_number, uploaded_at
        FROM contracted_prices
        ORDER BY uploaded_at DESC
        LIMIT 1
        """
    )
    if latest is None:
        await message.reply_text("No price uploads yet.")
        return

    stats = await fetch_one(
        """
        SELECT COUNT(*) AS n,
               AVG(your_price) AS avg_price,
               MIN(your_price) AS min_price,
               MAX(your_price) AS max_price
        FROM contracted_prices
        WHERE effective_date = $1
        """,
        latest["effective_date"],
    )

    from html import escape as _esc
    last_upload_est = latest["uploaded_at"].astimezone(EST)
    avg_p = float(stats["avg_price"])
    min_p = float(stats["min_price"])
    max_p = float(stats["max_price"])
    await message.reply_text(
        f"<b>Price status</b>\n"
        f"Last upload: {last_upload_est.strftime('%Y-%m-%d %H:%M %Z')}\n"
        f"Account: {_esc(str(latest['account_number']))}\n"
        f"Effective: {latest['effective_date'].isoformat()}\n"
        f"\n"
        f"<blockquote><pre>"
        f"Stops   {stats['n']}\n"
        f"Avg     ${avg_p:.3f}/gal\n"
        f"Min     ${min_p:.3f}/gal\n"
        f"Max     ${max_p:.3f}/gal"
        f"</pre></blockquote>",
        parse_mode="HTML",
    )


async def truckstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/truckstatus <unit> — driver, latest load, live fuel level, last fueling."""
    message = update.effective_message
    if message is None:
        return
    unit = _require_one_arg(context, "Usage: /truckstatus <unit>")
    if unit is None:
        await message.reply_text("Usage: /truckstatus <unit>")
        return

    truck = await fetch_one(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        unit,
    )
    if truck is None:
        await message.reply_text(f"Truck {unit} not onboarded in trucks_drivers.")
        return

    lines = [f"Truck {truck['truck_unit']}"]
    lines.append(f"Driver: {truck['driver_full_name'] or '(unassigned)'}")
    lines.append(f"Samsara vehicle: {truck['samsara_vehicle_id'] or '(not linked)'}")

    if truck["samsara_vehicle_id"]:
        try:
            async with SamsaraClient() as samsara:
                stats = await samsara.get_vehicle_stats(truck["samsara_vehicle_id"])
        except SamsaraError as exc:
            lines.append(f"Live fuel/GPS: Samsara error — {exc}")
        else:
            lines.append(
                f"Fuel: {stats.fuel_gallons:.0f} gal  |  "
                f"GPS: {stats.lat:.4f}, {stats.lng:.4f}  |  "
                f"MPG (7d): {stats.mpg_rolling:.2f}"
                if stats.mpg_rolling is not None
                else f"Fuel: {stats.fuel_gallons:.0f} gal  |  "
                f"GPS: {stats.lat:.4f}, {stats.lng:.4f}  |  "
                f"MPG (7d): n/a"
            )
    else:
        lines.append("Live fuel/GPS: skipped — no samsara_vehicle_id")

    latest_event = await fetch_one(
        """
        SELECT load_id, status, recommended_site_id, recommended_true_cost,
               actual_site_id, actual_true_cost, dollar_impact,
               recommended_at, resolved_at
        FROM stop_events
        WHERE truck_unit = $1
        ORDER BY recommended_at DESC
        LIMIT 1
        """,
        unit,
    )
    if latest_event is None:
        lines.append("Last fueling event: none on record")
    else:
        recommended_at = latest_event["recommended_at"].astimezone(EST).strftime("%Y-%m-%d %H:%M %Z")
        lines.append(
            f"Last event: load {latest_event['load_id']} "
            f"({latest_event['status']}) at {recommended_at}"
        )
        lines.append(
            f"  recommended site {latest_event['recommended_site_id']} "
            f"@ ${float(latest_event['recommended_true_cost']):.4f}/gal"
        )
        if latest_event["actual_site_id"] is not None:
            actual_cost = (
                f"${float(latest_event['actual_true_cost']):.4f}/gal"
                if latest_event["actual_true_cost"] is not None
                else "(n/a)"
            )
            lines.append(
                f"  actual site {latest_event['actual_site_id']} @ {actual_cost}"
            )
        if latest_event["dollar_impact"] is not None:
            lines.append(f"  dollar impact: ${float(latest_event['dollar_impact']):.2f}")

    await message.reply_text("\n".join(lines))


async def whodrives(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/whodrives <unit> — currently assigned driver."""
    message = update.effective_message
    if message is None:
        return
    unit = _require_one_arg(context, "Usage: /whodrives <unit>")
    if unit is None:
        await message.reply_text("Usage: /whodrives <unit>")
        return

    truck = await fetch_one(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id, updated_at
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        unit,
    )
    if truck is None:
        await message.reply_text(f"Truck {unit} not onboarded.")
        return
    if not truck["driver_full_name"]:
        await message.reply_text(f"Truck {unit}: no driver assigned.")
        return

    updated = truck["updated_at"].astimezone(EST).strftime("%Y-%m-%d %H:%M %Z")
    tg_id = truck["driver_telegram_id"] or "(no Telegram ID)"
    await message.reply_text(
        f"Truck {unit}: {truck['driver_full_name']} "
        f"(telegram_id={tg_id}, updated {updated})"
    )


async def linkedtrucks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/linkedtrucks — list trucks linked to Telegram driver/group chats."""
    message = update.effective_message
    if message is None:
        return

    rows = await fetch_all(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id,
               samsara_vehicle_id, updated_at
        FROM trucks_drivers
        ORDER BY
            CASE WHEN driver_telegram_id IS NULL THEN 1 ELSE 0 END,
            truck_unit
        """
    )
    if not rows:
        await message.reply_text("No trucks are onboarded in trucks_drivers yet.")
        return

    linked = [row for row in rows if row["driver_telegram_id"]]
    unlinked = [row for row in rows if not row["driver_telegram_id"]]
    lines = [
        f"Linked trucks: {len(linked)} linked, {len(unlinked)} unlinked",
        "",
    ]

    if linked:
        lines.append("Linked:")
        for row in linked:
            name = row["driver_full_name"] or "(no driver name)"
            samsara = row["samsara_vehicle_id"] or "no_samsara"
            lines.append(
                f"  {row['truck_unit']} — chat {row['driver_telegram_id']} — "
                f"{name} — {samsara}"
            )

    if unlinked:
        lines.append("")
        lines.append("Unlinked:")
        for row in unlinked:
            samsara = row["samsara_vehicle_id"] or "no_samsara"
            lines.append(f"  {row['truck_unit']} — {samsara}")

    for chunk in _chunks("\n".join(lines), 3900):
        await message.reply_text(chunk)


async def setdriver(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/setdriver <unit> <driver_name...> — manual assignment.

    Upserts trucks_drivers. Preserves samsara_vehicle_id and driver_telegram_id
    if they already exist on the row.
    """
    message = update.effective_message
    if message is None:
        return
    args = context.args or []
    if len(args) < 2:
        await message.reply_text("Usage: /setdriver <unit> <driver full name>")
        return

    unit = args[0].strip()
    driver_name = " ".join(args[1:]).strip()
    if not unit or not driver_name:
        await message.reply_text("Usage: /setdriver <unit> <driver full name>")
        return

    await execute(
        """
        INSERT INTO trucks_drivers (truck_unit, driver_full_name)
        VALUES ($1, $2)
        ON CONFLICT (truck_unit) DO UPDATE SET
            driver_full_name = EXCLUDED.driver_full_name,
            updated_at = NOW()
        """,
        unit,
        driver_name,
    )
    await message.reply_text(f"Assigned truck {unit} → {driver_name}.")


async def forcebriefing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/forcebriefing <unit> — re-run optimizer and send the briefing.

    Verifies the single current trip, expires superseded advice while keeping
    its audit history, and runs the standard remaining-route planner.
    """
    from dieselup.core.load_sync import (
        LoadContextError,
        _extract_truck_unit,
        _load_context,
        _process_one_load,
        _build_samsara_unit_map,
        _select_current_loads,
        _normalize_unit,
    )
    from dieselup.core.optimizer import NoValidStopError

    message = update.effective_message
    if message is None:
        return
    unit = _require_one_arg(context, "Usage: /forcebriefing <unit>")
    if unit is None:
        await message.reply_text("Usage: /forcebriefing <unit>")
        return

    await message.reply_text(f"Searching {settings.TMS_PROVIDER} for an active order on truck {unit}...")

    try:
        async with make_tms_client() as datatruck, SamsaraClient() as samsara:
            async with asyncio.timeout(120):
                orders = [order async for order in datatruck.iter_orders()]
                unit_map = await _build_samsara_unit_map(samsara)
                selected = _select_current_loads(orders, unit_map)
                from dieselup.bot.group_link import refresh_and_verify_linked_groups
                verified_links = await refresh_and_verify_linked_groups(context.bot)
                matches = [order for order in selected if _normalize_unit(_extract_truck_unit(order)) == _normalize_unit(unit)]
                if len(matches) != 1:
                    await message.reply_text(f"Truck {unit} has no unique verified current trip. Resolve its assignment before requesting advice.")
                    return
                matching_order = await datatruck.get_order(matches[0].get("tms_order_id") or matches[0]["id"])
                ctx = _load_context(matching_order)
                from dieselup.core.trip_context import remaining_stops
                remaining_stops(matching_order)
                await execute(
                    "UPDATE stop_events SET status = 'expired', resolved_at = NOW() "
                    "WHERE truck_unit = $1 AND load_id = $2 AND status = 'pending'",
                    ctx["truck_unit"], ctx["load_id"],
                )
                outcome = await _process_one_load(matching_order, samsara=samsara,
                    samsara_by_unit=unit_map, bot=context.bot, verified_driver_links=verified_links, current_trip_verified=True)
    except NoValidStopError as exc:
        await message.reply_text(
            f"no_valid_stop: truck {exc.truck_unit} on load {exc.load_id} — "
            f"current fuel {exc.current_fuel_gallons:.1f} gal. "
            f"No stop on the route satisfies the "
            f"{settings.SAFETY_FLOOR_GALLONS} gal safety floor "
            f"and delivery reserve rule."
        )
        return
    except LoadContextError as exc:
        await message.reply_text(f"Cannot brief: {exc}")
        return
    except Exception as exc:  # noqa: BLE001 — surface every failure mode to admin
        log.exception("forcebriefing failed for truck %s", unit)
        await message.reply_text(f"Briefing failed: {type(exc).__name__}: {exc}")
        return

    if outcome == "briefed":
        await message.reply_text(
            f"Fuel plan processed for truck {unit}, load {ctx['load_id']}. Delivery follows the active messaging policy."
        )
    else:
        await message.reply_text(
            f"No briefing sent for truck {unit} (outcome={outcome})."
        )


async def refreshgraph(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/refreshgraph — snap stops and rebuild nearby Valhalla distance graph."""
    message = update.effective_message
    if message is None:
        return

    if context.application.bot_data.get("refreshgraph_running"):
        await message.reply_text("Valhalla graph refresh is already running.")
        return

    context.application.bot_data["refreshgraph_running"] = True
    chat_id = message.chat_id
    await message.reply_text(
        "Valhalla graph refresh started. I will report counts here when it finishes."
    )
    context.application.create_task(
        _refreshgraph_background(
            bot=context.bot,
            chat_id=chat_id,
            application=context.application,
        )
    )


async def _refreshgraph_background(*, bot: Bot, chat_id: int, application: Application) -> None:
    try:
        result = await refresh_stop_graph()
        metrics.incr(
            "refreshgraph_aborted" if result.aborted_reason else "refreshgraph_ok"
        )
        await safe_send(
            bot=bot,
            chat_id=chat_id,
            text=format_refresh_result(result),
            alert_type="admin_refreshgraph",
            parse_mode=None,
        )
    except Exception as exc:  # noqa: BLE001 — admin job must report and unlock
        metrics.incr("refreshgraph_error")
        log.exception("refreshgraph failed")
        await safe_send(
            bot=bot,
            chat_id=chat_id,
            text=f"Valhalla graph refresh failed: {type(exc).__name__}: {exc}",
            alert_type="admin_refreshgraph",
            parse_mode=None,
        )
    finally:
        application.bot_data["refreshgraph_running"] = False


async def fleetstats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/fleetstats — current-week fleet stats from stop_events.

    Week starts Monday 00:00 EST and ends now. Admin chat only — per
    CLAUDE.md, driver-level numbers go to admin or the driver's own chat.
    """
    message = update.effective_message
    if message is None:
        return

    now_est = datetime.now(EST)
    week_start_est = (now_est - timedelta(days=now_est.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start_utc = week_start_est.astimezone(timezone.utc)

    totals = await fetch_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE status != 'pending') AS resolved,
            COUNT(*) FILTER (WHERE status = 'saved')   AS saved,
            COUNT(*) FILTER (WHERE status = 'lost')    AS lost,
            COUNT(*) FILTER (WHERE status = 'skipped') AS skipped,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COALESCE(SUM(dollar_impact) FILTER (WHERE status = 'saved'), 0) AS saved_dollars,
            COALESCE(SUM(
                CASE
                    WHEN status = 'lost' AND dollar_impact < 0
                        THEN -dollar_impact
                    WHEN status = 'skipped'
                        THEN GREATEST(worst_candidate_true_cost - recommended_true_cost, 0) * gallons
                    ELSE 0
                END
            ), 0) AS lost_dollars
        FROM stop_events
        WHERE recommended_at >= $1
        """,
        week_start_utc,
    )

    per_driver = await fetch_all(
        """
        SELECT
            se.truck_unit,
            COALESCE(td.driver_full_name, '(unassigned)') AS driver_name,
            COUNT(*) AS recs,
            COUNT(*) FILTER (WHERE se.status = 'saved')   AS saved,
            COUNT(*) FILTER (WHERE se.status = 'lost')    AS lost,
            COUNT(*) FILTER (WHERE se.status = 'skipped') AS skipped,
            COALESCE(SUM(
                CASE
                    WHEN se.status = 'skipped'
                         AND (se.dollar_impact IS NULL OR se.dollar_impact >= 0)
                        THEN -GREATEST(se.worst_candidate_true_cost - se.recommended_true_cost, 0) * se.gallons
                    ELSE COALESCE(se.dollar_impact, 0)
                END
            ), 0) AS net_dollars
        FROM stop_events se
        LEFT JOIN trucks_drivers td ON td.truck_unit = se.truck_unit
        WHERE se.recommended_at >= $1
        GROUP BY se.truck_unit, td.driver_full_name
        ORDER BY net_dollars DESC
        LIMIT 25
        """,
        week_start_utc,
    )

    from html import escape as _esc
    resolved = int(totals["resolved"] or 0)
    saved = int(totals["saved"] or 0)
    compliance_rate = (saved / resolved * 100.0) if resolved else 0.0
    net = float(totals["saved_dollars"]) - float(totals["lost_dollars"])
    net_emoji = "✅" if net >= 0 else "🔴"

    summary = (
        f"<blockquote><pre>"
        f"Recs      {resolved + int(totals['pending'])}  "
        f"(resolved {resolved}, pending {int(totals['pending'])})\n"
        f"Saved     {saved}  Lost {int(totals['lost'])}  Skipped {int(totals['skipped'])}\n"
        f"Compliance {compliance_rate:.1f}%\n"
        f"Savings   ${float(totals['saved_dollars']):,.2f}\n"
        f"Losses    ${float(totals['lost_dollars']):,.2f}\n"
        f"Net       {net_emoji} ${net:,.2f}"
        f"</pre></blockquote>"
    )

    lines = [
        f"<b>Fleet stats — week of {week_start_est.strftime('%Y-%m-%d')} EST</b>",
        "",
        summary,
    ]

    if per_driver:
        lines.append("")
        lines.append("<b>Per truck (top by net $):</b>")
        driver_rows = []
        for row in per_driver:
            net_d = float(row["net_dollars"])
            sign = "+" if net_d >= 0 else ""
            driver_rows.append(
                f"{_esc(str(row['truck_unit'])):<10}  "
                f"s{int(row['saved'])} l{int(row['lost'])} k{int(row['skipped'])}  "
                f"{sign}${net_d:,.0f}  {_esc(str(row['driver_name']))}"
            )
        lines.append(f"<pre>{''.join(r + chr(10) for r in driver_rows)}</pre>")

    await message.reply_text("\n".join(lines), parse_mode="HTML")


async def no_valid_stop_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/no_valid_stop_alerts — trucks where the optimizer failed in last 24h."""
    message = update.effective_message
    if message is None:
        return

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    rows = await fetch_all(
        """
        SELECT truck_unit, load_id, current_fuel_gallons, raised_at
        FROM no_valid_stop_alerts
        WHERE raised_at >= $1
        ORDER BY raised_at DESC
        LIMIT 50
        """,
        cutoff,
    )
    if not rows:
        await message.reply_text("No no_valid_stop alerts in the last 24h.")
        return

    lines = [f"no_valid_stop alerts — last 24h ({len(rows)}):"]
    for row in rows:
        ts = row["raised_at"].astimezone(EST).strftime("%Y-%m-%d %H:%M %Z")
        lines.append(
            f"  {ts} — truck {row['truck_unit']}, load {row['load_id']}, "
            f"fuel {float(row['current_fuel_gallons']):.1f} gal"
        )
    await message.reply_text("\n".join(lines))


async def price_upload_reminder(bot: Bot) -> None:
    """Ping admin if no successful price upload in the last 24h. Runs daily 08:00 EST."""
    latest = await fetch_one(
        "SELECT uploaded_at FROM contracted_prices ORDER BY uploaded_at DESC LIMIT 1"
    )

    if latest is None:
        await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
            text=(
                "Reminder: no price file has ever been uploaded.\n"
                "Optimizer has no prices — please upload today's file."
            ),
            alert_type="price_upload_reminder",
            parse_mode=None,
        )
        return

    age = datetime.now(timezone.utc) - latest["uploaded_at"]
    if age < timedelta(hours=24):
        return

    days = age.days
    last_phrase = f"{days} day{'s' if days != 1 else ''} ago" if days >= 1 else "less than a day ago"
    await safe_send(
        bot=bot,
        chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
        text=(
            f"Reminder: no price file received in 24h. Last upload: {last_phrase}.\n"
            "Optimizer is using stale prices — please upload today's file."
        ),
        alert_type="price_upload_reminder",
        parse_mode=None,
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Global error handler — logs every unhandled exception and replies to the user."""
    err = context.error
    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    log.error("Unhandled handler exception:\n%s", tb)

    # Notify admin chat with full detail
    try:
        update_str = str(update)[:300] if update else "(no update)"
        await context.bot.send_message(
            chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
            text=(
                f"Bot error ({type(err).__name__}):\n"
                f"{err}\n\n"
                f"Update: {update_str}"
            ),
        )
    except Exception:
        pass

    # Reply to the user so they don't see silence
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                f"Something went wrong ({type(err).__name__}). Admin has been notified."
            )
        except Exception:
            pass


def _require_one_arg(context: ContextTypes.DEFAULT_TYPE, usage: str) -> str | None:
    args = context.args or []
    if len(args) != 1:
        return None
    value = args[0].strip()
    return value or None


async def circuits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/circuits — show CLOSED/OPEN/HALF_OPEN state for every external service."""
    message = update.effective_message
    if message is None:
        return
    provider_breaker = tms_breaker(settings.TMS_PROVIDER)
    text = (
        "Circuit breakers\n"
        f"  {settings.TMS_PROVIDER}: {provider_breaker.state()}\n"
        f"  samsara:   {samsara_breaker.state()}\n"
        f"  telegram:  {telegram_breaker.state()}"
    )
    await message.reply_text(text)


async def metrics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/metrics — dump in-process counters / gauges / timers."""
    message = update.effective_message
    if message is None:
        return
    snap = metrics.render_text().rstrip()
    if not snap:
        await message.reply_text("(no metrics recorded yet)")
        return
    # Telegram caps single messages at 4096 chars. Send in chunks if needed.
    for chunk in _chunks(f"```\n{snap}\n```", 3900):
        await message.reply_text(chunk, parse_mode="MarkdownV2")


async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/health — quick liveness + recent-sweep summary."""
    message = update.effective_message
    if message is None:
        return
    snap = metrics.snapshot()
    counters = snap.get("counters", {})
    gauges = snap.get("gauges", {})
    timers = snap.get("timers", {})

    sweep_t = timers.get("load_sync_cycle_seconds") or {}
    last_briefed = gauges.get("load_sync_last_cycle_briefed")
    last_no_stop = gauges.get("load_sync_last_cycle_no_valid_stop")
    last_errored = gauges.get("load_sync_last_cycle_errored")
    last_pending = gauges.get("compliance_last_cycle_pending")

    pending_db = await fetch_one("SELECT COUNT(*) AS n FROM stop_events WHERE status = 'pending'")

    provider_breaker = tms_breaker(settings.TMS_PROVIDER)
    lines = [
        "Bot health",
        f"  load_sync cycles: {counters.get('load_sync_cycles_total', 0)}",
        f"  compliance cycles: {counters.get('compliance_cycles_total', 0)}",
        "",
        f"  Last load_sync — briefed={int(last_briefed or 0)} "
        f"no_valid_stop={int(last_no_stop or 0)} errored={int(last_errored or 0)}",
        f"  Last compliance — pending={int(last_pending or 0)}",
        f"  Pending in DB right now: {pending_db['n'] if pending_db else '?'}",
        "",
        f"  Sweep p95: {sweep_t.get('p95_ms', '?')} ms (n={sweep_t.get('count', 0)})",
        "",
        "  Breakers:",
        f"    {settings.TMS_PROVIDER}: {provider_breaker.state()}",
        f"    samsara:   {samsara_breaker.state()}",
        f"    telegram:  {telegram_breaker.state()}",
        "",
        "  Alerts (counter totals):",
        f"    ok={counters.get('alerts_ok_total', 0)}  "
        f"circuit_open={counters.get('alerts_circuit_open_total', 0)}  "
        f"rate_limited={counters.get('alerts_rate_limited_total', 0)}  "
        f"transport_err={counters.get('alerts_transport_err_total', 0)}  "
        f"telegram_err={counters.get('alerts_telegram_err_total', 0)}",
    ]
    await message.reply_text("\n".join(lines))


async def syncdrivers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/syncdrivers — read driver names from Samsara vehicle names and populate driver_full_name."""
    from dieselup.core.load_sync import _driver_name_from_samsara

    message = update.effective_message
    if message is None:
        return

    await message.reply_text("Fetching Samsara vehicle list...")

    # Build vehicle_id → driver_name map from vehicle names (no extra scope needed)
    try:
        async with SamsaraClient() as samsara:
            vehicles = await samsara.list_vehicles()
    except SamsaraError as exc:
        await message.reply_text(f"Samsara error: {exc}")
        return

    name_map: dict[str, str] = {}
    for v in vehicles:
        dname = _driver_name_from_samsara(v.name)
        if dname:
            name_map[v.id] = dname

    # Load all trucks that have a samsara_vehicle_id
    rows = await fetch_all(
        """
        SELECT truck_unit, samsara_vehicle_id, driver_full_name
        FROM trucks_drivers
        WHERE samsara_vehicle_id IS NOT NULL
        """
    )

    updated, already_set, no_name = [], [], []
    for row in rows:
        vid = row["samsara_vehicle_id"]
        dname = name_map.get(vid)
        if not dname:
            no_name.append(row["truck_unit"])
            continue
        if row["driver_full_name"] == dname:
            already_set.append(row["truck_unit"])
            continue
        await execute(
            """
            UPDATE trucks_drivers
            SET driver_full_name = $2, updated_at = NOW()
            WHERE truck_unit = $1
            """,
            row["truck_unit"],
            dname,
        )
        updated.append(f"  {row['truck_unit']} → {dname}")

    lines = [f"Samsara driver sync — {len(vehicles)} vehicles, {len(name_map)} with driver names"]
    lines.append(f"Updated:       {len(updated)}")
    lines.append(f"Already set:   {len(already_set)}")
    lines.append(f"No name in vehicle: {len(no_name)}")
    if updated:
        lines.append("")
        lines.append("Updated:")
        lines.extend(updated)
    if no_name:
        lines.append("")
        lines.append(f"No driver in Samsara name: {', '.join(no_name)}")

    for chunk in _chunks("\n".join(lines), 3900):
        await message.reply_text(chunk)


async def testalert(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/testalert <unit> — step-by-step check: DB → Samsara → send test message to driver group."""
    message = update.effective_message
    if message is None:
        return
    unit = _require_one_arg(context, "Usage: /testalert <unit>")
    if unit is None:
        await message.reply_text("Usage: /testalert <unit>")
        return

    lines: list[str] = [f"🧪 Test alert — Truck {unit}"]
    ok = True

    # ── Step 1: DB lookup ─────────────────────────────────────────────────────
    truck = await fetch_one(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
        FROM trucks_drivers
        WHERE truck_unit = $1
        """,
        unit,
    )
    if truck is None:
        lines.append("❌ Step 1/4 DB: truck not found in trucks_drivers")
        lines.append("   → Run /addtruck to add it first")
        await message.reply_text("\n".join(lines))
        return
    lines.append("✅ Step 1/4 DB: found in trucks_drivers")
    lines.append(f"   Driver: {truck['driver_full_name'] or '(no name)'}")
    lines.append(f"   Samsara ID: {truck['samsara_vehicle_id'] or '(none)'}")
    lines.append(f"   Telegram chat: {truck['driver_telegram_id'] or '(none)'}")

    # ── Step 2: Samsara GPS + fuel ────────────────────────────────────────────
    if not truck["samsara_vehicle_id"]:
        lines.append("❌ Step 2/4 Samsara: no samsara_vehicle_id — run /setsamsara to link it")
        ok = False
    else:
        try:
            async with SamsaraClient() as samsara:
                stats = await samsara.get_vehicle_stats(truck["samsara_vehicle_id"])
            fuel_pct = round(stats.fuel_gallons / settings.TANK_CAPACITY_GALLONS * 100)
            age = f"{stats.gps_age_minutes:.0f} min ago" if stats.gps_age_minutes is not None else "unknown age"
            lines.append("✅ Step 2/4 Samsara: GPS + fuel data received")
            lines.append(f"   Fuel: {fuel_pct}%  ({stats.fuel_gallons:.0f} gal)")
            lines.append(f"   GPS:  {stats.lat:.5f}, {stats.lng:.5f}  ({age})")
            mpg = f"{stats.mpg_rolling:.2f}" if stats.mpg_rolling else "n/a"
            lines.append(f"   MPG (7d): {mpg}")
        except SamsaraError as exc:
            lines.append(f"❌ Step 2/4 Samsara: {exc}")
            ok = False

    # ── Step 3: Send test message to driver group ─────────────────────────────
    if not truck["driver_telegram_id"]:
        lines.append("❌ Step 3/4 Driver TG: no telegram chat linked")
        lines.append("   → Add bot to the driver's group or run /settgid to link manually")
        ok = False
    else:
        chat_id = int(truck["driver_telegram_id"])
        test_text = (
            f"🧪 <b>TEST ALERT — Truck {unit}</b>\n"
            f"This is a test message from admin.\n"
            f"Driver: {truck['driver_full_name'] or '(unset)'}\n"
            f"If you see this, the briefing pipeline is working ✅"
        )
        try:
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=test_text,
                parse_mode="HTML",
            )
            lines.append("✅ Step 3/4 Driver TG: test message sent to driver group")
            lines.append(f"   Chat ID: {chat_id}  |  Message ID: {msg.message_id}")
        except Exception as exc:
            lines.append(f"❌ Step 3/4 Driver TG: send failed — {type(exc).__name__}: {exc}")
            ok = False

    # ── Step 4: Send test message to dispatch group ───────────────────────────
    if settings.TELEGRAM_DISPATCH_CHAT_ID is None:
        lines.append("⚠️  Step 4/4 Dispatch TG: TELEGRAM_DISPATCH_CHAT_ID not set — skipped")
    else:
        try:
            await context.bot.send_message(
                chat_id=settings.TELEGRAM_DISPATCH_CHAT_ID,
                text=f"🧪 <b>TEST ALERT — Truck {unit}</b>\nDispatched copy working ✅",
                parse_mode="HTML",
            )
            lines.append("✅ Step 4/4 Dispatch TG: test message sent to dispatch group")
        except Exception as exc:
            lines.append(f"❌ Step 4/4 Dispatch TG: send failed — {type(exc).__name__}: {exc}")
            ok = False

    lines.append("")
    lines.append("✅ ALL CHECKS PASSED — briefings will work for this truck" if ok
                 else "❌ ISSUES FOUND — fix the steps marked above")

    await message.reply_text("\n".join(lines))


async def addtruck(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/addtruck <unit> [driver name] — create a new truck row in trucks_drivers."""
    message = update.effective_message
    if message is None:
        return
    args = context.args or []
    if not args:
        await message.reply_text("Usage: /addtruck <unit> [driver full name]")
        return

    unit = args[0].strip()
    driver_name = " ".join(args[1:]).strip() if len(args) > 1 else None

    existing = await fetch_one(
        "SELECT truck_unit FROM trucks_drivers WHERE truck_unit = $1", unit
    )
    if existing:
        await message.reply_text(
            f"Truck {unit} already exists.\n"
            f"Use /setdriver {unit} <name> to update the driver, "
            f"or /setsamsara {unit} <id> to link Samsara."
        )
        return

    await execute(
        "INSERT INTO trucks_drivers (truck_unit, driver_full_name) VALUES ($1, $2)",
        unit,
        driver_name,
    )
    reply = f"Truck {unit} added."
    if driver_name:
        reply += f" Driver: {driver_name}."
    else:
        reply += f" No driver yet — use /setdriver {unit} <name>."
    await message.reply_text(reply)


async def removetruck(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/removetruck <unit> — delete a truck from trucks_drivers."""
    message = update.effective_message
    if message is None:
        return
    unit = _require_one_arg(context, "Usage: /removetruck <unit>")
    if unit is None:
        await message.reply_text("Usage: /removetruck <unit>")
        return

    row = await fetch_one(
        "SELECT truck_unit, driver_full_name FROM trucks_drivers WHERE truck_unit = $1", unit
    )
    if row is None:
        await message.reply_text(f"Truck {unit} not found.")
        return

    await execute("DELETE FROM trucks_drivers WHERE truck_unit = $1", unit)
    driver = row["driver_full_name"] or "(no driver)"
    await message.reply_text(f"Truck {unit} ({driver}) removed.")


async def listtrucks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/listtrucks — full fleet list with Samsara + Telegram link status."""
    message = update.effective_message
    if message is None:
        return

    rows = await fetch_all(
        """
        SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
        FROM trucks_drivers
        ORDER BY truck_unit
        """
    )
    if not rows:
        await message.reply_text("No trucks in DB. Use /addtruck <unit> to add one.")
        return

    total        = len(rows)
    full_linked  = sum(1 for r in rows if r["driver_telegram_id"] and r["samsara_vehicle_id"])
    no_samsara   = sum(1 for r in rows if not r["samsara_vehicle_id"])
    no_telegram  = sum(1 for r in rows if not r["driver_telegram_id"])
    no_driver    = sum(1 for r in rows if not r["driver_full_name"])

    lines = [
        f"Fleet trucks — {total} total",
        f"✅ Fully linked: {full_linked}  |  ❌ No Samsara: {no_samsara}  |  ❌ No TG: {no_telegram}  |  ❌ No driver: {no_driver}",
        "",
    ]

    for row in rows:
        has_s = bool(row["samsara_vehicle_id"])
        has_t = bool(row["driver_telegram_id"])

        if has_s and has_t:
            icon = "✅"
        elif has_s or has_t:
            icon = "⚠️"
        else:
            icon = "❌"

        s_tag = "S✅" if has_s else "S❌"
        t_tag = "T✅" if has_t else "T❌"
        driver = row["driver_full_name"] or "(no driver)"
        lines.append(f"{icon} {row['truck_unit']:<10} {s_tag} {t_tag}  {driver}")

    for chunk in _chunks("\n".join(lines), 3900):
        await message.reply_text(chunk)


async def setsamsara(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/setsamsara <unit> <samsara_vehicle_id> — manually link a Samsara vehicle."""
    message = update.effective_message
    if message is None:
        return
    args = context.args or []
    if len(args) != 2:
        await message.reply_text("Usage: /setsamsara <unit> <samsara_vehicle_id>")
        return

    unit, vehicle_id = args[0].strip(), args[1].strip()
    await execute(
        """
        INSERT INTO trucks_drivers (truck_unit, samsara_vehicle_id)
        VALUES ($1, $2)
        ON CONFLICT (truck_unit) DO UPDATE SET
            samsara_vehicle_id = EXCLUDED.samsara_vehicle_id,
            updated_at = NOW()
        """,
        unit,
        vehicle_id,
    )
    await message.reply_text(f"Truck {unit} → Samsara vehicle ID set: {vehicle_id}")


async def settgid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/settgid <unit> <telegram_chat_id> — manually set a truck's Telegram chat ID."""
    message = update.effective_message
    if message is None:
        return
    args = context.args or []
    if len(args) != 2:
        await message.reply_text("Usage: /settgid <unit> <telegram_chat_id>")
        return

    unit = args[0].strip()
    try:
        tg_id = int(args[1].strip())
    except ValueError:
        await message.reply_text("telegram_chat_id must be a number (e.g. -1001234567890).")
        return

    await execute(
        """
        WITH unlinked AS (
            UPDATE trucks_drivers
            SET driver_telegram_id = NULL,
                driver_full_name = NULL,
                updated_at = NOW()
            WHERE driver_telegram_id = $2
              AND truck_unit <> $1
            RETURNING truck_unit
        )
        INSERT INTO trucks_drivers (truck_unit, driver_telegram_id)
        VALUES ($1, $2)
        ON CONFLICT (truck_unit) DO UPDATE SET
            driver_telegram_id = EXCLUDED.driver_telegram_id,
            updated_at = NOW()
        """,
        unit,
        tg_id,
    )
    await message.reply_text(f"Truck {unit} → Telegram chat ID set: {tg_id}")


async def movetruckchat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/movetruckchat <old_unit> <new_unit> - move a driver group to a new truck.

    This is for real-world truck swaps: the Telegram group/driver should follow
    the driver, while each truck keeps its own Samsara vehicle ID. Pending
    stop_events are left as history; the command reports their counts so admin
    can /forcebriefing the new truck when a fresh plan is needed.
    """
    message = update.effective_message
    if message is None:
        return
    args = context.args or []
    if len(args) != 2:
        await message.reply_text("Usage: /movetruckchat <old_unit> <new_unit>")
        return

    old_unit, new_unit = args[0].strip(), args[1].strip()
    if not old_unit or not new_unit or old_unit == new_unit:
        await message.reply_text("Usage: /movetruckchat <old_unit> <new_unit>")
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            old = await conn.fetchrow(
                """
                SELECT truck_unit, driver_full_name, driver_telegram_id
                FROM trucks_drivers
                WHERE truck_unit = $1
                FOR UPDATE
                """,
                old_unit,
            )
            if old is None:
                await message.reply_text(f"Old truck {old_unit} not found.")
                return

            chat_id = old["driver_telegram_id"]
            driver_name = old["driver_full_name"]
            if chat_id is None:
                await message.reply_text(
                    f"Truck {old_unit} has no Telegram chat to move. "
                    "Use /settgid if you know the chat ID."
                )
                return

            new = await conn.fetchrow(
                """
                SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
                FROM trucks_drivers
                WHERE truck_unit = $1
                FOR UPDATE
                """,
                new_unit,
            )
            if new is None:
                await conn.execute(
                    """
                    INSERT INTO trucks_drivers (truck_unit, driver_full_name)
                    VALUES ($1, NULL)
                    """,
                    new_unit,
                )
                new = await conn.fetchrow(
                    """
                    SELECT truck_unit, driver_full_name, driver_telegram_id, samsara_vehicle_id
                    FROM trucks_drivers
                    WHERE truck_unit = $1
                    FOR UPDATE
                    """,
                    new_unit,
                )

            previous_new_chat = new["driver_telegram_id"] if new else None
            previous_new_driver = new["driver_full_name"] if new else None

            await conn.execute(
                """
                UPDATE trucks_drivers
                SET driver_telegram_id = NULL,
                    driver_full_name = NULL,
                    updated_at = NOW()
                WHERE truck_unit = $1
                """,
                old_unit,
            )
            await conn.execute(
                """
                UPDATE trucks_drivers
                SET driver_telegram_id = NULL,
                    updated_at = NOW()
                WHERE driver_telegram_id = $1
                  AND truck_unit <> $2
                """,
                chat_id,
                new_unit,
            )
            await conn.execute(
                """
                UPDATE trucks_drivers
                SET driver_telegram_id = $2,
                    driver_full_name = $3,
                    updated_at = NOW()
                WHERE truck_unit = $1
                """,
                new_unit,
                chat_id,
                driver_name,
            )

            old_pending = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM stop_events
                WHERE truck_unit = $1 AND status = 'pending'
                """,
                old_unit,
            )
            new_pending = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM stop_events
                WHERE truck_unit = $1 AND status = 'pending'
                """,
                new_unit,
            )

    lines = [
        f"Moved Telegram chat {chat_id} from truck {old_unit} to {new_unit}.",
    ]
    if driver_name:
        lines.append(f"Driver moved: {driver_name}.")
    if previous_new_chat and previous_new_chat != chat_id:
        lines.append(
            f"Note: truck {new_unit} previously had Telegram chat {previous_new_chat}; it was replaced."
        )
    if previous_new_driver and previous_new_driver != driver_name:
        lines.append(
            f"Note: truck {new_unit} previously had driver {previous_new_driver}; it was replaced."
        )
    lines.append(
        f"Pending events: {old_unit}={int(old_pending or 0)}, {new_unit}={int(new_pending or 0)}."
    )
    lines.append(f"Run /forcebriefing {new_unit} to send a fresh fuel plan now.")
    await message.reply_text("\n".join(lines))


async def linkgroup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/linkgroup <chat_id> <unit> — link a drivers' group to a truck from the
    admin chat. /setgroup <unit> <chat_id> is accepted as an alias.

    For groups the bot is ALREADY in, where auto-link can't re-fire (no
    bot-added or title-change event). Runs the same logic as auto-link:
    resolves the Samsara vehicle for <unit>, upserts trucks_drivers, posts the
    confirmation into the group, and replays the latest pending briefing there.

    Get <chat_id> from the bot logs / a group-link admin notice (e.g.
    -4819078900), or just run /linktruck <unit> inside the group instead.
    """
    message = update.effective_message
    if message is None:
        return
    parsed = _parse_group_link_args(context.args or [])
    if parsed is None:
        await message.reply_text(
            "Usage: /linkgroup <chat_id> <unit> or /setgroup <unit> <chat_id>"
        )
        return
    chat_id, unit = parsed

    from dieselup.bot.group_link import _attempt_link

    command_name = ""
    if message.text:
        command_name = message.text.split(maxsplit=1)[0].lstrip("/")
    await message.reply_text(f"Linking truck {unit} -> chat {chat_id} ...")
    await _attempt_link(
        bot=context.bot,
        chat_id=chat_id,
        chat_title=f"(admin /{command_name or 'linkgroup'} unit {unit})",
        reason="admin",
        unit_override=unit,
    )
    await message.reply_text(
        f"Link attempt for truck {unit} done — see the result message above and "
        "in the group."
    )


def _parse_group_link_args(args: list[str]) -> tuple[int, str] | None:
    """Accept both historical and dispatch-friendly group-link argument order."""
    if len(args) != 2:
        return None
    first, second = args[0].strip(), args[1].strip()
    if not first or not second:
        return None

    first_chat = _parse_group_chat_id(first)
    second_chat = _parse_group_chat_id(second)
    if first_chat is not None and second_chat is None:
        return first_chat, second
    if second_chat is not None and first_chat is None:
        return second_chat, first
    return None


def _parse_group_chat_id(raw: str) -> int | None:
    try:
        chat_id = int(raw)
    except ValueError:
        return None
    return chat_id if chat_id < 0 else None


async def samsaravehicles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/samsaravehicles [search] — list Samsara vehicles with their IDs.

    Use this to find a samsara_vehicle_id (for /setsamsara) or to confirm what
    Samsara reports for a unit. Optional <search> filters by a case-insensitive
    substring of the vehicle name or its parsed unit number.
    """
    message = update.effective_message
    if message is None:
        return
    needle = " ".join(context.args or []).strip().lower()

    try:
        async with SamsaraClient() as samsara:
            vehicles = await samsara.list_vehicles()
    except SamsaraError as exc:
        await message.reply_text(f"Samsara error: {exc}")
        return

    if needle:
        vehicles = [
            v for v in vehicles
            if needle in v.name.lower() or (v.unit_digits and needle in v.unit_digits)
        ]
    if not vehicles:
        suffix = f" matching {needle!r}." if needle else "."
        await message.reply_text(f"No Samsara vehicles{suffix}")
        return

    header = f"Samsara vehicles — {len(vehicles)}"
    if needle:
        header += f" matching {needle!r}"
    lines = [header, "id · unit · name", ""]
    for v in vehicles:
        lines.append(f"{v.id} · {v.unit_digits or '?'} · {v.name}")

    for chunk in _chunks("\n".join(lines), 3900):
        await message.reply_text(chunk)


# ── Jobs that are paused/resumed by /pause and /resume ───────────────────────
# weekly_report and price_upload_reminder are intentionally excluded — they run
# infrequently and don't send driver messages, so they're safe to keep running
# during maintenance windows.
_PAUSABLE_JOBS = ("load_sync", "compliance_resolver", "dlq_retry", "fuel_brain")


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/pause [reason] — pause load_sync, compliance and dlq_retry sweeps.

    All three APScheduler jobs are paused immediately. Any sweep currently
    mid-flight completes naturally — pause only prevents the NEXT run from
    starting. The bot stays online and responds to admin commands; only the
    automated sweeps (briefings, compliance checks, alert retries) are frozen.

    Use /resume to restart them. Use /botstatus to check the current state.
    """
    message = update.effective_message
    if message is None:
        return

    bot_data = context.application.bot_data
    if bot_data.get("bot_paused"):
        paused_at = bot_data.get("paused_at")
        ago = _time_ago(paused_at)
        reason = bot_data.get("pause_reason") or "(no reason given)"
        await message.reply_text(
            f"Already paused {ago}.\nReason: {reason}\nUse /resume to restart."
        )
        return

    scheduler = bot_data.get("scheduler")
    if scheduler is None:
        await message.reply_text("Scheduler not available — cannot pause.")
        return

    reason = " ".join(context.args or []).strip() or None
    now = datetime.now(timezone.utc)

    for job_id in _PAUSABLE_JOBS:
        try:
            scheduler.pause_job(job_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("pause_cmd: could not pause job %s: %s", job_id, exc)

    bot_data["bot_paused"] = True
    metrics.gauge("bot_paused", 1)
    bot_data["pause_reason"] = reason
    bot_data["paused_at"] = now

    reason_line = f"\nReason: {reason}" if reason else ""
    reply = (
        f"⏸ Bot PAUSED at {now.astimezone(EST).strftime('%H:%M %Z')}."
        f"{reason_line}\n\n"
        f"Paused jobs: {', '.join(_PAUSABLE_JOBS)}.\n"
        "Driver briefings, compliance checks and fuel detection will NOT run.\n"
        "Use /resume to restart."
    )
    await message.reply_text(reply)
    log.warning("Bot paused by admin. Reason: %s", reason or "(none)")


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/resume — resume all paused sweeps after /pause."""
    message = update.effective_message
    if message is None:
        return

    bot_data = context.application.bot_data
    if not bot_data.get("bot_paused"):
        await message.reply_text("Bot is not paused. Nothing to resume.")
        return

    scheduler = bot_data.get("scheduler")
    if scheduler is None:
        await message.reply_text("Scheduler not available — cannot resume.")
        return

    paused_at = bot_data.get("paused_at")
    pause_duration = _time_ago(paused_at)

    for job_id in _PAUSABLE_JOBS:
        try:
            scheduler.resume_job(job_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("resume_cmd: could not resume job %s: %s", job_id, exc)

    bot_data["bot_paused"] = False
    metrics.gauge("bot_paused", 0)
    bot_data["pause_reason"] = None
    bot_data["paused_at"] = None

    await message.reply_text(
        f"▶️ Bot RESUMED after {pause_duration}.\n"
        "load_sync, compliance, and dlq_retry are running again."
    )
    log.info("Bot resumed by admin after %s.", pause_duration)


async def botstatus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/botstatus — show whether the bot sweeps are running or paused."""
    message = update.effective_message
    if message is None:
        return

    bot_data = context.application.bot_data
    paused = bot_data.get("bot_paused", False)
    scheduler = bot_data.get("scheduler")

    if paused:
        paused_at = bot_data.get("paused_at")
        reason = bot_data.get("pause_reason") or "(no reason given)"
        ago = _time_ago(paused_at)
        status_line = f"⏸ PAUSED — {ago}\nReason: {reason}"
    else:
        status_line = "▶️ RUNNING — all sweeps active"

    # Show per-job state from APScheduler
    job_lines = []
    if scheduler:
        for job_id in _PAUSABLE_JOBS:
            job = scheduler.get_job(job_id)
            if job is None:
                job_lines.append(f"  {job_id}: not found")
            else:
                next_run = job.next_run_time
                if next_run is None:
                    job_lines.append(f"  {job_id}: ⏸ paused")
                else:
                    next_est = next_run.astimezone(EST).strftime("%H:%M %Z")
                    job_lines.append(f"  {job_id}: ▶️ next run {next_est}")

    lines = [
        "Bot sweep status",
        "",
        status_line,
        "",
        "Jobs:",
    ] + job_lines

    await message.reply_text("\n".join(lines))


def _time_ago(dt: datetime | None) -> str:
    if dt is None:
        return "unknown time"
    delta = datetime.now(timezone.utc) - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s ago"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    hours = total_seconds // 3600
    mins = (total_seconds % 3600) // 60
    return f"{hours}h {mins}m ago"


def _chunks(text: str, n: int):
    for i in range(0, len(text), n):
        yield text[i:i + n]


def register_admin_handlers(application: Application) -> None:
    """Attach admin-only document upload, admin commands, and global error handler."""
    application.add_error_handler(error_handler)
    admin_chat = filters.Chat(chat_id=settings.TELEGRAM_ADMIN_CHAT_ID)
    application.add_handler(
        MessageHandler(admin_chat & filters.Document.ALL, handle_price_upload)
    )
    application.add_handler(CommandHandler("pricestatus", pricestatus, filters=admin_chat))
    application.add_handler(CommandHandler("truckstatus", truckstatus, filters=admin_chat))
    application.add_handler(CommandHandler("whodrives", whodrives, filters=admin_chat))
    application.add_handler(CommandHandler("linkedtrucks", linkedtrucks, filters=admin_chat))
    application.add_handler(CommandHandler("setdriver", setdriver, filters=admin_chat))
    application.add_handler(CommandHandler("forcebriefing", forcebriefing, filters=admin_chat))
    application.add_handler(CommandHandler("refreshgraph", refreshgraph, filters=admin_chat))
    application.add_handler(CommandHandler("fleetstats", fleetstats, filters=admin_chat))
    application.add_handler(
        CommandHandler("no_valid_stop_alerts", no_valid_stop_alerts, filters=admin_chat)
    )
    application.add_handler(CommandHandler("circuits", circuits_cmd, filters=admin_chat))
    application.add_handler(CommandHandler("metrics", metrics_cmd, filters=admin_chat))
    application.add_handler(CommandHandler("health", health_cmd, filters=admin_chat))
    # Truck management
    application.add_handler(CommandHandler("testalert", testalert, filters=admin_chat))
    application.add_handler(CommandHandler("syncdrivers", syncdrivers, filters=admin_chat))
    application.add_handler(CommandHandler("addtruck", addtruck, filters=admin_chat))
    application.add_handler(CommandHandler("removetruck", removetruck, filters=admin_chat))
    application.add_handler(CommandHandler("listtrucks", listtrucks, filters=admin_chat))
    application.add_handler(CommandHandler("listtruck", listtrucks, filters=admin_chat))
    application.add_handler(CommandHandler("setsamsara", setsamsara, filters=admin_chat))
    application.add_handler(CommandHandler("settgid", settgid, filters=admin_chat))
    application.add_handler(CommandHandler("movetruckchat", movetruckchat, filters=admin_chat))
    application.add_handler(CommandHandler("linkgroup", linkgroup, filters=admin_chat))
    application.add_handler(CommandHandler("setgroup", linkgroup, filters=admin_chat))
    application.add_handler(
        CommandHandler("samsaravehicles", samsaravehicles, filters=admin_chat)
    )
    # Bot maintenance — pause/resume scheduled sweeps
    application.add_handler(CommandHandler("pause", pause_cmd, filters=admin_chat))
    application.add_handler(CommandHandler("resume", resume_cmd, filters=admin_chat))
    application.add_handler(CommandHandler("botstatus", botstatus_cmd, filters=admin_chat))
