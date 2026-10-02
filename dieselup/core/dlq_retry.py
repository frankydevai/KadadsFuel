"""
Dead-letter-queue retry job.

Every 10 minutes APScheduler fires `retry_failed_alerts(bot)`. The job picks
DLQ rows that:

  * have NOT yet succeeded (`succeeded_at IS NULL`),
  * are NOT permanently failed (`permanently_failed_at IS NULL`),
  * have made fewer than MAX_ATTEMPTS attempts so far,
  * have not been retried within the last RETRY_QUIET_MINUTES (so a hot loop
    doesn't pound a flaky chat every 10 min).

For each row it re-invokes `safe_send` (with `queue_on_failure=False` so we
don't re-queue infinitely) and:

  * on success — stamps `succeeded_at`, increments `attempts`, and if a
    `(stop_event_id, msg_id_column)` was stored, UPDATEs the original
    stop_events row so the msg_id audit trail is restored;
  * on failure — increments `attempts`, updates `last_error` and
    `last_attempt_at`. After MAX_ATTEMPTS the row is marked
    `permanently_failed_at` and admin is notified once.

Crash-orphan recovery: if a bot crash happened between INSERT stop_events
and safe_send, the alert was never delivered AND no DLQ row exists either
— that is the one failure mode this module does NOT cover. The mitigation
is the live-fire retry: if a fresh sweep ever fires the same alert again
(e.g. because a load is re-dispatched), it falls through normally. To
truly close the gap, future work would persist a "send_pending" intent
before calling safe_send and reconcile on startup.
"""
from __future__ import annotations

import logging
from typing import Any

from telegram import Bot

from dieselup import metrics
from dieselup.bot.sender import safe_send
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.db import execute, fetch_all

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
RETRY_QUIET_MINUTES = 5
BATCH_LIMIT = 50


async def retry_failed_alerts(bot: Bot) -> None:
    """APScheduler entrypoint — wire to IntervalTrigger(minutes=10)."""
    log.info("dlq_retry: starting")
    metrics.incr("dlq_retry_cycles_total")

    if settings.TELEGRAM_MESSAGING_MODE == "silent":
        import time
        metrics.gauge("dlq_retry_last_heartbeat_mono", time.monotonic())
        log.info("dlq_retry: test mode; queued deliveries retained without attempts")
        return

    rows = await fetch_all(
        """
        SELECT id, alert_type, chat_id, text, parse_mode, disable_web_page_preview,
               truck_unit, load_id, stop_event_id, msg_id_column, attempts
        FROM alert_dlq
        WHERE succeeded_at IS NULL
          AND permanently_failed_at IS NULL
          AND attempts < $1
          AND ($4::text[] IS NULL OR ltrim(upper(truck_unit),'0')=ANY($4))
          AND (last_attempt_at IS NULL OR last_attempt_at < NOW() - ($2 || ' minutes')::INTERVAL)
        ORDER BY queued_at ASC
        LIMIT $3
        """,
        MAX_ATTEMPTS, str(RETRY_QUIET_MINUTES), BATCH_LIMIT, allowed_units(),
    )

    if not rows:
        log.info("dlq_retry: no rows due for retry")
        import time as _t
        metrics.gauge("dlq_retry_last_heartbeat_mono", _t.monotonic())
        return

    succeeded = failed = permanent = suppressed = 0
    for row in rows:
        if not allows(row['truck_unit']):
            continue
        try:
            outcome = await _retry_one(bot, row)
        except Exception:  # noqa: BLE001 — one bad row can't stall the job
            failed += 1
            metrics.incr("dlq_retry_errored")
            log.exception("dlq_retry: error retrying row %d", row["id"])
            continue
        if outcome == "succeeded":
            succeeded += 1
        elif outcome == "permanent":
            permanent += 1
        elif outcome == "suppressed":
            suppressed += 1
        else:
            failed += 1

    import time as _t
    metrics.gauge("dlq_retry_last_heartbeat_mono", _t.monotonic())
    metrics.gauge("dlq_retry_last_cycle_succeeded", succeeded)
    metrics.gauge("dlq_retry_last_cycle_failed", failed)
    metrics.gauge("dlq_retry_last_cycle_permanent", permanent)
    metrics.gauge("dlq_retry_last_cycle_suppressed", suppressed)
    log.info(
        "dlq_retry: done — succeeded=%d failed=%d permanent=%d suppressed=%d",
        succeeded, failed, permanent, suppressed,
    )


async def _retry_one(bot: Bot, row: Any) -> str:
    """Returns 'succeeded' | 'failed' | 'permanent' | 'suppressed'."""
    if not allows(row['truck_unit']):
        return 'suppressed'
    new_attempts = int(row["attempts"]) + 1

    if _is_suppressed_driver_alert_retry(
        str(row["alert_type"]),
        int(row["chat_id"]),
    ):
        await execute(
            """
            UPDATE alert_dlq
            SET attempts = $2,
                last_attempt_at = NOW(),
                permanently_failed_at = NOW(),
                last_error = $3
            WHERE id = $1
            """,
            int(row["id"]), new_attempts,
            "suppressed: driver one-shot alerts are not retried",
        )
        metrics.incr("dlq_retry_suppressed_driver_alert_total")
        log.info(
            "dlq_retry: suppressed queued driver alert row=%s type=%s chat_id=%s",
            row["id"], row["alert_type"], row["chat_id"],
        )
        return "suppressed"

    msg_id = await safe_send(
        bot=bot,
        chat_id=int(row["chat_id"]),
        text=row["text"],
        alert_type=f"retry_{row['alert_type']}",
        parse_mode=row["parse_mode"],
        disable_web_page_preview=bool(row["disable_web_page_preview"]),
        truck_unit=row["truck_unit"],
        load_id=row["load_id"],
        stop_event_id=row["stop_event_id"],
        queue_on_failure=False,
    )

    if msg_id is not None:
        await execute(
            """
            UPDATE alert_dlq
            SET succeeded_at = NOW(),
                last_attempt_at = NOW(),
                attempts = $2
            WHERE id = $1
            """,
            int(row["id"]), new_attempts,
        )
        # Restore msg_id on the linked stop_events row if requested.
        if row["stop_event_id"] is not None and row["msg_id_column"]:
            col = row["msg_id_column"]
            if not _is_safe_msg_id_column(col):
                log.warning("dlq_retry: refusing unsafe column name %r", col)
            else:
                await execute(
                    f"UPDATE stop_events SET {col} = $2 WHERE id = $1",
                    int(row["stop_event_id"]), int(msg_id),
                )
        metrics.incr("dlq_retry_succeeded_total")
        return "succeeded"

    # Send failed again — increment, decide permanent vs retryable.
    if new_attempts >= MAX_ATTEMPTS:
        await execute(
            """
            UPDATE alert_dlq
            SET attempts = $2,
                last_attempt_at = NOW(),
                permanently_failed_at = NOW()
            WHERE id = $1
            """,
            int(row["id"]), new_attempts,
        )
        await _notify_admin_permanent_failure(bot, row, new_attempts)
        metrics.incr("dlq_retry_permanent_total")
        return "permanent"

    await execute(
        """
        UPDATE alert_dlq
        SET attempts = $2,
            last_attempt_at = NOW()
        WHERE id = $1
        """,
        int(row["id"]), new_attempts,
    )
    metrics.incr("dlq_retry_failed_total")
    return "failed"


_SAFE_MSG_ID_COLUMNS = {
    "briefing_driver_msg_id", "briefing_dispatch_msg_id",
    "approach_driver_msg_id", "approach_dispatch_msg_id",
    "delivery_driver_msg_id", "delivery_dispatch_msg_id",
    "red_flag_driver_msg_id", "red_flag_dispatch_msg_id",
}


def _is_suppressed_driver_alert_retry(alert_type: str, chat_id: int) -> bool:
    """One-shot driver alerts must not be replayed from the retry queue."""
    if (
        settings.TELEGRAM_DISPATCH_CHAT_ID is not None
        and chat_id == settings.TELEGRAM_DISPATCH_CHAT_ID
    ):
        return False
    while alert_type.startswith("retry_"):
        alert_type = alert_type[6:]
    return alert_type in {
        "briefing",
        "delivery",
        "delivery_complete",
        "standalone_delivery",
        "approach",
        "missed_fuel_stop",
        "wrong_fuel_stop",
        "off_network_fueling",
    }


def _is_safe_msg_id_column(name: str) -> bool:
    """Guard f-string SQL — column name must be in the known allowlist."""
    return name in _SAFE_MSG_ID_COLUMNS


async def _notify_admin_permanent_failure(bot: Bot, row: Any, attempts: int) -> None:
    """Tell admin a specific alert can never be delivered. Fires once per row."""
    truck = row["truck_unit"] or "?"
    load = row["load_id"] or "?"
    snippet = (row["text"] or "")[:120].replace("\n", " ")
    await safe_send(
        bot=bot,
        chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
        text=(
            f"⚠️ DLQ permanent failure after {attempts} attempts.\n"
            f"alert_type={row['alert_type']}  truck={truck}  load={load}\n"
            f"chat_id={row['chat_id']}\n"
            f"last_error: {row['last_error']}\n"
            f"text: {snippet}..."
        ),
        alert_type="admin_dlq_permanent",
        parse_mode=None,
        queue_on_failure=False,  # don't DLQ the DLQ-failure notice
    )
