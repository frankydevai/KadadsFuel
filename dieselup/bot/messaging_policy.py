"""Enforce silent mode and selected-driver recipients at the request boundary."""
from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

from telegram.request import HTTPXRequest

from dieselup import metrics
from dieselup.config import settings
from dieselup.core.operating_scope import allowed_units, allows, unit_key
from dieselup.db import fetch_all

log = logging.getLogger(__name__)

# Allow the operations needed for polling, group verification, price downloads
# and command registration. Unknown/new message endpoints fail closed.
_SILENT_ALLOWED = frozenset({
    'getMe', 'getUpdates', 'getChat', 'getChatMember', 'getChatAdministrators',
    'getChatMemberCount', 'getMyCommands', 'setMyCommands', 'deleteMyCommands',
    'getWebhookInfo', 'deleteWebhook', 'getFile',
})
_DRIVER_OUTPUT_ALLOWED = frozenset({
    'sendMessage', 'sendDocument', 'sendPhoto', 'sendVideo', 'sendAnimation',
    'sendAudio', 'sendVoice', 'sendLocation', 'sendVenue', 'sendMediaGroup',
    'editMessageText', 'editMessageCaption', 'editMessageMedia',
    'editMessageReplyMarkup', 'deleteMessage',
})


async def recipient_allowed(chat_id: Any, truck_unit: str | None = None) -> bool:
    """Verify the current unique driver link; never cache a link across sends.

    A restricted deployment enables only driver chats, not admin/dispatch output.
    Missing, paused, conflicting, changed, or unverifiable links fail closed.
    """
    if allowed_units() is None:
        return True
    if isinstance(chat_id, bool) or not re.fullmatch(r"-?\d+", str(chat_id or "")):
        return False
    chat_id = int(chat_id)
    if chat_id in {settings.TELEGRAM_ADMIN_CHAT_ID, settings.TELEGRAM_DISPATCH_CHAT_ID}:
        return False
    try:
        rows = await fetch_all(
            """SELECT truck_unit, assignment_status, alerts_paused
               FROM trucks_drivers WHERE driver_telegram_id = $1""",
            chat_id,
        )
    except Exception as exc:
        log.warning("telegram.recipient.unverified reason=%s", type(exc).__name__)
        return False
    if len(rows) != 1:
        return False
    row = rows[0]
    return (
        allows(row["truck_unit"])
        and row["assignment_status"] == "ready"
        and row["alerts_paused"] is False
        and (truck_unit is None or unit_key(truck_unit) == unit_key(row["truck_unit"]))
    )


class MessagingPolicyRequest(HTTPXRequest):
    async def do_request(self, url: str, method: str, **kwargs: Any) -> tuple[int, bytes]:
        endpoint = url.rsplit('/', 1)[-1].split('?', 1)[0]
        file_download = method.upper() == 'GET' and urlparse(url).path.startswith('/file/bot')
        if settings.TELEGRAM_MESSAGING_MODE == 'silent' and endpoint not in _SILENT_ALLOWED and not file_download:
            metrics.incr('telegram_output_blocked_test_mode_total')
            log.info('telegram.output.blocked.test_mode endpoint=%s', endpoint)
            # No HTTP request and no fabricated successful Message/message_id.
            return 403, json.dumps({
                'ok': False, 'error_code': 403,
                'description': 'Telegram messages are disabled during the test period',
            }).encode()
        if endpoint not in _SILENT_ALLOWED and not file_download and allowed_units() is not None:
            request_data = kwargs.get('request_data')
            parameters = request_data.parameters if request_data is not None else {}
            if endpoint not in _DRIVER_OUTPUT_ALLOWED or not await recipient_allowed(parameters.get('chat_id')):
                metrics.incr('telegram_output_blocked_recipient_total')
                log.info('telegram.output.blocked.recipient endpoint=%s', endpoint)
                return 403, json.dumps({
                    'ok': False, 'error_code': 403,
                    'description': 'Telegram output is limited to the selected linked drivers',
                }).encode()
        return await super().do_request(url=url, method=method, **kwargs)
