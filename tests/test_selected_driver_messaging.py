"""Selected-driver Telegram permissions: real request shapes, no external traffic."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import Bot
from telegram.error import Forbidden
from telegram.request import HTTPXRequest, RequestData
from telegram.request._requestparameter import RequestParameter

from dieselup.bot import messaging_policy as policy
from dieselup.bot import sender
from dieselup.config import settings
from dieselup.core import dlq_retry


def mapping(unit="6682", chat_id=-1006682, **changes):
    return {
        "truck_unit": unit,
        "driver_telegram_id": chat_id,
        "assignment_status": "ready",
        "alerts_paused": False,
        **changes,
    }


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "6682,8089,8217")
    monkeypatch.setattr(settings, "TELEGRAM_MESSAGING_MODE", "live")
    monkeypatch.setattr(settings, "TELEGRAM_ADMIN_CHAT_ID", 6264960800)
    monkeypatch.setattr(settings, "TELEGRAM_DISPATCH_CHAT_ID", -1009999)
    fetch = AsyncMock(return_value=[mapping()])
    monkeypatch.setattr(policy, "fetch_all", fetch, raising=False)
    return fetch


def test_no_test_scope_keeps_full_fleet_behavior(scoped, monkeypatch):
    monkeypatch.setattr(settings, "TEST_TRUCK_UNITS", "")
    assert asyncio.run(policy.recipient_allowed(-1007777, "7777")) is True
    scoped.assert_not_awaited()


@pytest.mark.parametrize("unit", ["6682", "8089", "8217"])
def test_each_selected_ready_driver_can_receive(scoped, unit):
    chat_id = -1000000 - int(unit)
    scoped.return_value = [mapping(unit, chat_id)]
    assert asyncio.run(policy.recipient_allowed(chat_id, unit)) is True
    scoped.assert_awaited_once()
    # Looking at only the first matching assignment would hide a duplicate.
    assert "LIMIT 1" not in scoped.call_args.args[0].upper()


def test_numeric_chat_string_and_normalized_truck_match(scoped):
    scoped.return_value = [mapping("06682")]
    assert asyncio.run(policy.recipient_allowed("-1006682", "6682")) is True


@pytest.mark.parametrize("rows, truck", [
    ([], None),
    ([mapping("7777")], None),
    ([mapping(), mapping("8217")], None),
    ([mapping(), mapping()], None),
    ([mapping(assignment_status="conflict")], None),
    ([mapping(assignment_status="unlinked")], None),
    ([mapping(alerts_paused=True)], None),
    ([mapping()], "8217"),
    ([mapping()], "truck6682"),
])
def test_unmapped_duplicate_conflicted_paused_or_wrong_truck_is_denied(scoped, rows, truck):
    scoped.return_value = rows
    assert asyncio.run(policy.recipient_allowed(-1006682, truck)) is False


def test_mapping_is_checked_again_after_driver_moves(scoped):
    scoped.side_effect = [[mapping()], []]

    async def run():
        assert await policy.recipient_allowed(-1006682, "6682") is True
        assert await policy.recipient_allowed(-1006682, "6682") is False

    asyncio.run(run())
    assert scoped.await_count == 2


@pytest.mark.parametrize("chat_id", [6264960800, -1009999])
def test_admin_and_dispatch_remain_denied_even_if_wrongly_mapped(scoped, chat_id):
    scoped.return_value = [mapping(chat_id=chat_id)]
    assert asyncio.run(policy.recipient_allowed(chat_id)) is False


@pytest.mark.parametrize("chat_id", [None, "", "@driver", "bad-id", True, -1006682.5, []])
def test_invalid_chat_identifier_fails_closed_without_database(scoped, chat_id):
    assert asyncio.run(policy.recipient_allowed(chat_id)) is False
    scoped.assert_not_awaited()


def test_database_failure_fails_closed(scoped):
    scoped.side_effect = RuntimeError("simulated database outage")
    assert asyncio.run(policy.recipient_allowed(-1006682)) is False


def request_data(**parameters):
    return RequestData([
        RequestParameter.from_input(key, value)
        for key, value in parameters.items()
    ])


@pytest.mark.parametrize("endpoint", ["sendMessage", "sendDocument", "editMessageText", "deleteMessage"])
def test_direct_transport_blocks_out_of_scope_output(scoped, monkeypatch, endpoint):
    scoped.return_value = [mapping("7777", -1007777)]
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, "do_request", network)

    async def run():
        request = policy.MessagingPolicyRequest()
        try:
            result = await request.do_request(
                "https://api.telegram.org/bot123:test/" + endpoint,
                "POST", request_data=request_data(chat_id=-1007777, text="test"),
            )
            assert result[0] == 403
            network.assert_not_awaited()
        finally:
            await request.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("unit", ["6682", "8089", "8217"])
def test_real_request_parameters_allow_only_selected_chat(scoped, monkeypatch, unit):
    chat_id = -1000000 - int(unit)
    scoped.return_value = [mapping(unit, chat_id)]
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, "do_request", network)

    async def run():
        request = policy.MessagingPolicyRequest()
        try:
            assert (await request.do_request(
                "https://api.telegram.org/bot123:test/sendMessage", "POST",
                request_data=request_data(chat_id=chat_id, text="test"),
            ))[0] == 200
            network.assert_awaited_once()
        finally:
            await request.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("endpoint, parameters", [
    ("sendMessage", {"text": "test"}),
    ("answerCallbackQuery", {"callback_query_id": "test"}),
    ("editMessageText", {"inline_message_id": "test", "text": "test"}),
    ("futureOutputEndpoint", {}),
])
def test_output_without_target_chat_fails_closed(scoped, monkeypatch, endpoint, parameters):
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, "do_request", network)

    async def run():
        request = policy.MessagingPolicyRequest()
        try:
            assert (await request.do_request(
                "https://api.telegram.org/bot123:test/" + endpoint, "POST",
                request_data=request_data(**parameters),
            ))[0] == 403
            network.assert_not_awaited()
        finally:
            await request.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("endpoint, method", [
    ("getUpdates", "POST"), ("getFile", "POST"), ("getChat", "POST"),
    ("setMyCommands", "POST"), ("deleteWebhook", "POST"),
    ("/file/bot123:test/documents/prices.xlsx", "GET"),
])
def test_scoped_live_mode_keeps_reads_and_price_downloads(scoped, monkeypatch, endpoint, method):
    network = AsyncMock(return_value=(200, b"read result"))
    monkeypatch.setattr(HTTPXRequest, "do_request", network)
    url = "https://api.telegram.org" + endpoint if endpoint.startswith("/") else "https://api.telegram.org/bot123:test/" + endpoint

    async def run():
        request = policy.MessagingPolicyRequest()
        try:
            assert await request.do_request(url, method) == (200, b"read result")
            network.assert_awaited_once()
            scoped.assert_not_awaited()
        finally:
            await request.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("method", ["send_message", "send_document"])
@pytest.mark.parametrize("chat_id", [6264960800, -1007777])
def test_actual_bot_direct_output_cannot_bypass_recipient_policy(scoped, monkeypatch, method, chat_id):
    scoped.return_value = []
    output = AsyncMock()

    async def upstream(self, url, method, **kwargs):
        if url.endswith("/getMe"):
            return 200, b'{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"Test"}}'
        await output(url, method, **kwargs)
        return 200, b'{"ok":true,"result":true}'

    monkeypatch.setattr(HTTPXRequest, "do_request", upstream)

    async def run():
        async with Bot(token="123:test", request=policy.MessagingPolicyRequest()) as bot:
            with pytest.raises(Forbidden):
                if method == "send_message":
                    await bot.send_message(chat_id=chat_id, text="must not send")
                else:
                    await bot.send_document(chat_id=chat_id, document=b"fake test sheet")
        output.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("rows, truck", [
    ([], "6682"),
    ([mapping("7777")], "7777"),
    ([mapping()], "8217"),
    ([mapping(alerts_paused=True)], "6682"),
])
def test_denied_sender_never_validates_sends_deletes_or_queues(scoped, monkeypatch, rows, truck):
    scoped.return_value = rows
    network = AsyncMock(side_effect=AssertionError("recipient must be rejected before delivery"))
    queue = AsyncMock()
    validate = AsyncMock(return_value=True)
    breaker = AsyncMock(side_effect=AssertionError("recipient must be rejected before breaker"))
    monkeypatch.setattr(sender, "execute", queue)
    monkeypatch.setattr(sender, "fetch_one", queue)
    monkeypatch.setattr(sender, "validate_fuel_event", validate)
    monkeypatch.setattr(sender.telegram_breaker, "call", breaker)
    bot = SimpleNamespace(send_message=network, delete_message=network)
    result = asyncio.run(sender.safe_send(
        bot=bot, chat_id=-1006682, text="test", alert_type="briefing",
        truck_unit=truck, stop_event_id=42, replace_previous_driver_alert=True,
    ))
    assert result is None
    validate.assert_not_awaited()
    breaker.assert_not_awaited()
    network.assert_not_awaited()
    queue.assert_not_awaited()


def test_allowed_non_fuel_sender_delivers_selected_driver(scoped, monkeypatch):
    bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=123)))

    async def call(fn, **kwargs):
        return await fn(**kwargs)

    monkeypatch.setattr(sender.telegram_breaker, "call", call)
    assert asyncio.run(sender.safe_send(
        bot=bot, chat_id=-1006682, text="test", alert_type="test_notification", truck_unit="6682",
    )) == 123
    bot.send_message.assert_awaited_once()


def test_unknown_output_endpoint_with_valid_selected_chat_is_denied(scoped, monkeypatch):
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, "do_request", network)

    async def run():
        request = policy.MessagingPolicyRequest()
        try:
            assert (await request.do_request(
                "https://api.telegram.org/bot123:test/futureOutputEndpoint", "POST",
                request_data=request_data(chat_id=-1006682, text="test"),
            ))[0] == 403
            network.assert_not_awaited()
        finally:
            await request.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("alert_type", [
    "off_network_fueling", "retry_off_network_fueling", "retry_retry_briefing",
])
def test_queued_one_shot_driver_alert_is_retired_without_resending(scoped, monkeypatch, alert_type):
    send = AsyncMock(return_value=123)
    write = AsyncMock()
    monkeypatch.setattr(dlq_retry, "safe_send", send)
    monkeypatch.setattr(dlq_retry, "execute", write)
    row = {
        "id": 10,
        "alert_type": alert_type,
        "chat_id": -1006682,
        "text": "old driver instruction",
        "parse_mode": None,
        "disable_web_page_preview": True,
        "truck_unit": "6682",
        "load_id": "test-load",
        "stop_event_id": 42,
        "msg_id_column": "briefing_driver_msg_id",
        "attempts": 0,
    }
    assert asyncio.run(dlq_retry._retry_one(object(), row)) == "suppressed"
    send.assert_not_awaited()
    write.assert_awaited_once()
    assert "permanently_failed_at = NOW()" in write.call_args.args[0]
    assert write.call_args.args[3] == "suppressed: driver one-shot alerts are not retried"


@pytest.mark.parametrize("alert_type", [
    "off_network_fueling", "retry_off_network_fueling", "retry_retry_briefing",
])
def test_one_shot_queue_classification_still_excludes_dispatch(scoped, alert_type):
    # Recipient permissions independently keep dispatch silent in selected mode.
    assert dlq_retry._is_suppressed_driver_alert_retry(alert_type, -1009999) is False


def test_link_change_between_sender_and_transport_is_not_queued(scoped, monkeypatch):
    scoped.side_effect = [[mapping()], []]
    queue = AsyncMock()
    network_output = AsyncMock()
    monkeypatch.setattr(sender, "execute", queue)

    async def call(fn, **kwargs):
        return await fn(**kwargs)

    async def upstream(self, url, method, **kwargs):
        if url.endswith("/getMe"):
            return 200, b'{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"Test"}}'
        await network_output(url, method, **kwargs)
        return 403, b'{"ok":false,"error_code":403,"description":"bot was kicked"}'

    monkeypatch.setattr(sender.telegram_breaker, "call", call)
    monkeypatch.setattr(HTTPXRequest, "do_request", upstream)

    async def run():
        async with Bot(token="123:test", request=policy.MessagingPolicyRequest()) as bot:
            assert await sender.safe_send(
                bot=bot, chat_id=-1006682, text="test", alert_type="test_notification", truck_unit="6682",
            ) is None
        queue.assert_not_awaited()
        network_output.assert_not_awaited()
        assert scoped.await_count == 2

    asyncio.run(run())


def test_real_telegram_forbidden_still_queues_delivery_failure(scoped, monkeypatch):
    queue = AsyncMock()
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=Forbidden("bot was kicked")))
    monkeypatch.setattr(sender, "execute", queue)

    async def call(fn, **kwargs):
        return await fn(**kwargs)

    monkeypatch.setattr(sender.telegram_breaker, "call", call)
    assert asyncio.run(sender.safe_send(
        bot=bot, chat_id=-1006682, text="test", alert_type="test_notification", truck_unit="6682",
    )) is None
    scoped.assert_awaited_once()
    bot.send_message.assert_awaited_once()
    queue.assert_awaited_once()
    assert "INSERT INTO alert_dlq" in queue.call_args.args[0]
    assert "bot was kicked" in queue.call_args.args[-1]
