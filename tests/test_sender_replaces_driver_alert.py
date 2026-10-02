import asyncio

from telegram.error import TimedOut

from dieselup.bot import sender


class _Message:
    message_id = 901


class _Bot:
    def __init__(self):
        self.deleted = []
        self.sent = []

    async def delete_message(self, **kwargs):
        self.deleted.append(kwargs)

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return _Message()


def test_driver_alert_deletes_only_telegram_copy_and_sends_new(monkeypatch):
    async def fake_fetch_one(sql, *args):
        assert "briefing_driver_msg_id" in sql
        assert args == (-100123,)
        return {"message_id": 777}

    bot = _Bot()
    monkeypatch.setattr(sender, "fetch_one", fake_fetch_one)

    message_id = asyncio.run(sender.safe_send(
        bot=bot,
        chat_id=-100123,
        text="new alert",
        alert_type="test_verified_notification",
        replace_previous_driver_alert=True,
        queue_on_failure=False,
    ))

    assert bot.deleted == [{"chat_id": -100123, "message_id": 777}]
    assert len(bot.sent) == 1
    assert message_id == 901


def test_dispatch_send_does_not_delete_any_message(monkeypatch):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("dispatch sends must not inspect driver history")

    bot = _Bot()
    monkeypatch.setattr(sender, "fetch_one", unexpected_fetch)

    asyncio.run(sender.safe_send(
        bot=bot,
        chat_id=-200456,
        text="dispatch copy",
        alert_type="test_dispatch_notification",
        queue_on_failure=False,
    ))

    assert bot.deleted == []
    assert len(bot.sent) == 1


def test_failed_replacement_keeps_previous_driver_alert(monkeypatch):
    async def unexpected_fetch(*_args, **_kwargs):
        raise AssertionError("old alert must not be looked up after send failure")

    class FailingBot(_Bot):
        async def send_message(self, **kwargs):
            self.sent.append(kwargs)
            raise TimedOut("telegram unavailable")

    bot = FailingBot()
    monkeypatch.setattr(sender, "fetch_one", unexpected_fetch)

    message_id = asyncio.run(sender.safe_send(
        bot=bot,
        chat_id=-100123,
        text="replacement that fails",
        alert_type="test_verified_notification",
        replace_previous_driver_alert=True,
        queue_on_failure=False,
    ))

    assert message_id is None
    assert bot.deleted == []
    assert len(bot.sent) == 1
