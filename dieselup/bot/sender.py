"""
Centralized Telegram-send wrapper.

Every alert in the codebase goes through `safe_send` instead of calling
`bot.send_message` directly. This is the single chokepoint that:

  * Routes through the Telegram circuit breaker (skip cleanly when OPEN).
  * Counts outcomes per alert type (ok / failed / circuit_open / rate_limited).
  * Logs structured one-liners with alert_type + chat_id + truck/load context.
  * Returns the `Message.message_id` on success so callers can persist it.
  * Never raises — failures return None so caller code stays linear.

Alert types currently tracked (free-form strings; new ones don't need a code
change):
  briefing, approach, delivery_complete,
  admin_no_valid_stop, admin_onboard, admin_misc,
  dispatch_briefing, dispatch_approach, dispatch_delivery,
  price_upload_summary, weekly_report, ...

Callers should pass alert_type as descriptively as possible; the more
specific, the more useful the resulting metrics breakdown.
"""
from __future__ import annotations

import logging
from typing import Any

from telegram import Bot, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError, TelegramError

from dieselup import metrics
from dieselup.circuit_breaker import CircuitOpenError, telegram_breaker
from dieselup.config import settings
from dieselup.db import execute, fetch_one
from dieselup.core.advice_guard import is_fuel_advice, validate_fuel_event
from dieselup.bot.messaging_policy import recipient_allowed

log = logging.getLogger(__name__)


async def safe_send(
    *,
    bot: Bot,
    chat_id: int,
    text: str,
    alert_type: str,
    parse_mode: str | None = ParseMode.HTML,
    disable_web_page_preview: bool = True,
    reply_markup: InlineKeyboardMarkup | None = None,
    truck_unit: str | None = None,
    load_id: str | None = None,
    extra: dict[str, Any] | None = None,
    stop_event_id: int | None = None,
    msg_id_column: str | None = None,
    queue_on_failure: bool = True,
    replace_previous_driver_alert: bool = False,
) -> int | None:
    """Send a message through the Telegram breaker, count the outcome, return msg_id or None.

    Never raises — caller code stays linear. On failure the counter and log
    line are the diagnostic surface AND (when queue_on_failure=True) the
    payload is enqueued to `alert_dlq` for the retry job to pick up.

    stop_event_id + msg_id_column are stored on the DLQ row so a successful
    retry can write the msg_id back onto the original stop_events row,
    keeping the audit trail intact across crashes/outages.

    Pass queue_on_failure=False for the retry job's own send to avoid
    infinite queueing if a DLQ row keeps failing.
    """
    ctx = _ctx(truck_unit=truck_unit, load_id=load_id, chat_id=chat_id, alert_type=alert_type, extra=extra)
    from dieselup.core import advice_audit
    async def audit(outcome, **details):
        if stop_event_id is not None:
            await advice_audit.record("message_" + outcome, truck_unit=truck_unit, load_id=load_id,
                event_id=stop_event_id, details={"alert_type": alert_type, "chat_id": chat_id, **details})

    if settings.TELEGRAM_MESSAGING_MODE == "silent":
        await audit("suppressed", reason="silent_test_mode")
        metrics.incr("alerts_suppressed_test_mode_total")
        metrics.incr(f"alerts_{alert_type}_suppressed_test_mode")
        log.info("send.suppressed.test_mode %s", ctx)
        # Suppression is deliberate, never a failed delivery to replay later.
        return None

    if not await recipient_allowed(chat_id, truck_unit):
        await audit("suppressed", reason="recipient_outside_selected_drivers")
        metrics.incr("alerts_suppressed_recipient_total")
        log.info("send.suppressed.recipient %s", ctx)
        return None  # deliberate suppression: no attempt, deletion, or retry queue

    if is_fuel_advice(alert_type) and not await validate_fuel_event(stop_event_id, truck_unit, chat_id):
        await audit("held", reason="current_route_cannot_be_verified")
        metrics.incr("alerts_suppressed_unverified_route_total")
        log.warning("send.suppressed.unverified_route %s", ctx)
        return None  # intentional hold: never enqueue or delete another alert

    error_kind: str | None = None
    error_msg: str | None = None
    await audit("attempted")
    try:
        msg = await telegram_breaker.call(
            bot.send_message,
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup,
        )
    except CircuitOpenError as exc:
        error_kind, error_msg = "circuit_open", str(exc)
        metrics.incr(f"alerts_{alert_type}_circuit_open")
        metrics.incr("alerts_circuit_open_total")
        log.warning("send.skip.circuit_open %s", ctx)
    except RetryAfter as exc:
        error_kind, error_msg = "rate_limited", f"retry_after={exc.retry_after}s"
        metrics.incr(f"alerts_{alert_type}_rate_limited")
        metrics.incr("alerts_rate_limited_total")
        log.warning("send.skip.rate_limited retry_after=%ss %s", exc.retry_after, ctx)
    except (TimedOut, NetworkError) as exc:
        error_kind, error_msg = "transport", f"{type(exc).__name__}: {exc}"
        metrics.incr(f"alerts_{alert_type}_transport_err")
        metrics.incr("alerts_transport_err_total")
        log.warning("send.fail.transport %s: %s — %s", type(exc).__name__, exc, ctx)
    except Forbidden as exc:
        if str(exc) in {
            'Telegram output is limited to the selected linked drivers',
            'Telegram messages are disabled during the test period',
        }:
            # The live link may change between this wrapper and transport.
            # A deliberate policy denial must not become a queued delivery.
            await audit("suppressed", reason="transport_recipient_policy")
            metrics.incr("alerts_suppressed_recipient_total")
            log.info("send.suppressed.recipient.transport %s", ctx)
            return None
        error_kind, error_msg = "telegram", f"{type(exc).__name__}: {exc}"
        metrics.incr(f"alerts_{alert_type}_telegram_err")
        metrics.incr("alerts_telegram_err_total")
        log.warning("send.fail.telegram %s: %s — %s", type(exc).__name__, exc, ctx)
    except TelegramError as exc:
        error_kind, error_msg = "telegram", f"{type(exc).__name__}: {exc}"
        metrics.incr(f"alerts_{alert_type}_telegram_err")
        metrics.incr("alerts_telegram_err_total")
        log.warning("send.fail.telegram %s: %s — %s", type(exc).__name__, exc, ctx)
    except Exception as exc:  # noqa: BLE001 — central wrapper must never propagate
        error_kind, error_msg = "unknown", f"{type(exc).__name__}: {exc}"
        metrics.incr(f"alerts_{alert_type}_unknown_err")
        metrics.incr("alerts_unknown_err_total")
        log.exception("send.fail.unknown %s: %s — %s", type(exc).__name__, exc, ctx)
    else:
        # Keep the last valid instruction visible until its replacement is
        # confirmed delivered. Deleting first creates a dangerous blank state
        # when Telegram times out or rejects the new message.
        if replace_previous_driver_alert:
            await _delete_previous_driver_alert(bot=bot, chat_id=chat_id, ctx=ctx)
        metrics.incr(f"alerts_{alert_type}_ok")
        metrics.incr("alerts_ok_total")
        log.info("send.ok msg_id=%s %s", msg.message_id, ctx)
        try:
            await audit("sent", message_id=msg.message_id)
        except Exception:
            # The durable attempt remains. Do not retry a delivered message
            # merely because its follow-up audit write failed.
            log.exception("send.delivered.audit_failed msg_id=%s %s", msg.message_id, ctx)
        return msg.message_id

    await audit("failed", reason=error_kind)
    # Send failed. Enqueue to DLQ unless caller opted out (retry job does that).
    if queue_on_failure:
        try:
            await execute(
                """
                INSERT INTO alert_dlq
                    (alert_type, chat_id, text, parse_mode, disable_web_page_preview,
                     truck_unit, load_id, stop_event_id, msg_id_column, last_error)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                alert_type, chat_id, text, parse_mode, disable_web_page_preview,
                truck_unit, load_id, stop_event_id, msg_id_column,
                f"{error_kind}: {error_msg}" if error_msg else error_kind,
            )
            metrics.incr("alerts_dlq_enqueued_total")
        except Exception:  # noqa: BLE001 — DLQ insert must never crash the caller
            log.exception("safe_send: failed to enqueue alert to DLQ — %s", ctx)
    return None


async def _delete_previous_driver_alert(*, bot: Bot, chat_id: int, ctx: str) -> None:
    """Delete the newest driver-facing Telegram alert without altering its DB history."""
    try:
        previous = await fetch_one(
            """
            SELECT message_id
            FROM (
                SELECT briefing_driver_msg_id AS message_id,
                       recommended_at AS sent_at
                FROM stop_events
                WHERE driver_id = $1 AND briefing_driver_msg_id IS NOT NULL
                UNION ALL
                SELECT approach_driver_msg_id AS message_id,
                       approach_ping_sent_at AS sent_at
                FROM stop_events
                WHERE driver_id = $1 AND approach_driver_msg_id IS NOT NULL
                UNION ALL
                SELECT red_flag_driver_msg_id AS message_id,
                       red_flag_sent_at AS sent_at
                FROM stop_events
                WHERE driver_id = $1 AND red_flag_driver_msg_id IS NOT NULL
                UNION ALL
                SELECT delivery_driver_msg_id AS message_id,
                       COALESCE(notified_complete_at, resolved_at) AS sent_at
                FROM stop_events
                WHERE driver_id = $1 AND delivery_driver_msg_id IS NOT NULL
            ) AS driver_alerts
            ORDER BY sent_at DESC NULLS LAST, message_id DESC
            LIMIT 1
            """,
            chat_id,
        )
    except Exception:  # noqa: BLE001 — history lookup must not block a safety alert
        log.exception("delete.previous.lookup_failed %s", ctx)
        return

    if not previous or previous["message_id"] is None:
        return

    try:
        await bot.delete_message(chat_id=chat_id, message_id=int(previous["message_id"]))
    except TelegramError as exc:
        log.warning(
            "delete.previous.telegram_failed message_id=%s error=%s — %s",
            previous["message_id"], exc, ctx,
        )
    except Exception as exc:  # noqa: BLE001 — deletion failure must not block the new alert
        log.warning(
            "delete.previous.failed message_id=%s error=%s:%s — %s",
            previous["message_id"], type(exc).__name__, exc, ctx,
        )
    else:
        log.info("delete.previous.ok message_id=%s %s", previous["message_id"], ctx)


def _ctx(**kwargs: Any) -> str:
    """Compact "key=value key=value" for structured logging."""
    parts: list[str] = []
    extra = kwargs.pop("extra", None) or {}
    for k, v in kwargs.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    for k, v in extra.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    return " ".join(parts)
