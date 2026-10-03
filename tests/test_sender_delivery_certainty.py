"""A lost Telegram reply cannot be treated as proof that no post was accepted."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, Forbidden, NetworkError, TelegramError, TimedOut

from dieselup.bot import sender
from dieselup.core import advice_audit


@pytest.mark.parametrize("error", [TimedOut(), NetworkError("connection lost"),
                                  TelegramError("unknown response"), RuntimeError("unknown")])
def test_potentially_accepted_fuel_post_is_uncertain_and_never_queued(monkeypatch, error):
    posted = []
    async def accepted_then_lost_reply(**kwargs):
        posted.append(kwargs["text"])
        raise error
    bot = SimpleNamespace(send_message=accepted_then_lost_reply)
    recorder, writer = AsyncMock(), AsyncMock()
    monkeypatch.setattr(sender.settings, "TELEGRAM_MESSAGING_MODE", "live")
    monkeypatch.setattr(sender, "recipient_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(sender, "validate_fuel_event", AsyncMock(return_value=True))
    monkeypatch.setattr(sender.telegram_breaker, "call", lambda fn, **kwargs: fn(**kwargs))
    monkeypatch.setattr(advice_audit, "record", recorder)
    monkeypatch.setattr(sender, "execute", writer)
    result = asyncio.run(sender.safe_send(bot=bot, chat_id=-1006682, text="Synthetic fuel advice",
                                         alert_type="briefing", truck_unit="6682", stop_event_id=42))
    assert result is None
    assert len(posted) == 1
    assert [call.args[0] for call in recorder.call_args_list] == ["message_attempted", "message_uncertain"]
    writer.assert_not_awaited()


@pytest.mark.parametrize("error", [BadRequest("invalid input"), Forbidden("bot removed")])
def test_explicit_telegram_rejection_is_distinguished_from_transport_uncertainty(monkeypatch, error):
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=error))
    recorder = AsyncMock()
    monkeypatch.setattr(sender.settings, "TELEGRAM_MESSAGING_MODE", "live")
    monkeypatch.setattr(sender, "recipient_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(sender, "validate_fuel_event", AsyncMock(return_value=True))
    monkeypatch.setattr(sender.telegram_breaker, "call", lambda fn, **kwargs: fn(**kwargs))
    monkeypatch.setattr(advice_audit, "record", recorder)
    result = asyncio.run(sender.safe_send(bot=bot, chat_id=-1006682, text="Synthetic fuel advice",
                                         alert_type="briefing", truck_unit="6682", stop_event_id=42,
                                         queue_on_failure=False))
    assert result is None
    assert recorder.call_args.args[0] == "message_failed"
    assert recorder.call_args.kwargs["details"]["reason"] == "telegram_rejected"
