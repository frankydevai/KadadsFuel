import asyncio
from unittest.mock import AsyncMock

import pytest
from telegram import Bot
from telegram.error import Forbidden
from telegram.request import HTTPXRequest

from dieselup.bot import sender
from dieselup.bot.messaging_policy import MessagingPolicyRequest
from dieselup.config import settings
from dieselup.core import dlq_retry


@pytest.mark.parametrize('endpoint', [
    'sendMessage', 'sendDocument', 'sendPhoto', 'sendLocation', 'sendMediaGroup',
    'editMessageText', 'editMessageCaption', 'deleteMessage', 'forwardMessage',
    'copyMessage', 'answerCallbackQuery', 'futureOutputEndpoint',
])
def test_silent_mode_blocks_output_before_any_http_request(monkeypatch, endpoint):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    network = AsyncMock(side_effect=AssertionError('Telegram output must not reach the network'))
    monkeypatch.setattr(HTTPXRequest, 'do_request', network)
    async def run():
        request = MessagingPolicyRequest()
        try:
            status, body = await request.do_request('https://api.telegram.org/bot123:test/'+endpoint, 'POST')
            assert status == 403
            assert b'"ok": false' in body
            network.assert_not_awaited()
        finally: await request.shutdown()
    asyncio.run(run())


@pytest.mark.parametrize('endpoint', ['getMe', 'getUpdates', 'getChat', 'getChatMember', 'getFile', 'setMyCommands', 'deleteWebhook'])
def test_silent_mode_keeps_polling_and_connection_reads(monkeypatch, endpoint):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, 'do_request', network)
    async def run():
        request = MessagingPolicyRequest()
        try:
            assert (await request.do_request('https://api.telegram.org/bot123:test/'+endpoint, 'POST'))[0] == 200
            network.assert_awaited_once()
        finally: await request.shutdown()
    asyncio.run(run())


def test_silent_sender_does_not_send_queue_delete_or_mark_success(monkeypatch):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    network = AsyncMock()
    database = AsyncMock()
    monkeypatch.setattr(sender, 'execute', database)
    monkeypatch.setattr(sender, 'fetch_one', database)
    class FakeBot:
        send_message = network
        delete_message = network
    async def run():
        assert await sender.safe_send(bot=FakeBot(), chat_id=-100, text='SIMULATION',
            alert_type='briefing', replace_previous_driver_alert=True) is None
    asyncio.run(run())
    network.assert_not_awaited()
    database.assert_not_awaited()


def test_silent_retry_keeps_existing_queue_untouched(monkeypatch):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    database = AsyncMock(side_effect=AssertionError('Do not consume retry attempts in test mode'))
    monkeypatch.setattr(dlq_retry, 'fetch_all', database)
    monkeypatch.setattr(dlq_retry, 'execute', database)
    asyncio.run(dlq_retry.retry_failed_alerts(object()))
    database.assert_not_awaited()


def test_silent_mode_allows_read_only_price_file_download(monkeypatch):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    network = AsyncMock(return_value=(200, b'price-file-content'))
    monkeypatch.setattr(HTTPXRequest, 'do_request', network)
    async def run():
        request = MessagingPolicyRequest()
        try:
            assert await request.do_request('https://api.telegram.org/file/bot123:test/documents/file.xlsx', 'GET') == (200, b'price-file-content')
            network.assert_awaited_once()
        finally: await request.shutdown()
    asyncio.run(run())


def test_live_mode_still_uses_real_transport(monkeypatch):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'live')
    network = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
    monkeypatch.setattr(HTTPXRequest, 'do_request', network)
    async def run():
        request = MessagingPolicyRequest()
        try:
            assert (await request.do_request('https://api.telegram.org/bot123:test/sendMessage', 'POST'))[0] == 200
            network.assert_awaited_once()
        finally: await request.shutdown()
    asyncio.run(run())


def test_bot_cannot_bypass_policy_with_direct_send_message(monkeypatch):
    monkeypatch.setattr(settings, 'TELEGRAM_MESSAGING_MODE', 'silent')
    async def upstream(self, url, method, **kwargs):
        assert url.endswith('/getMe')
        return 200, b'{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"Test"}}'
    monkeypatch.setattr(HTTPXRequest, 'do_request', upstream)
    async def run():
        async with Bot(token='123:test', request=MessagingPolicyRequest()) as bot:
            with pytest.raises(Forbidden, match='disabled during the test period'):
                await bot.send_message(chat_id=-100, text='SHOULD NEVER BE SENT')
    asyncio.run(run())
