import asyncio

from dieselup.core import dlq_retry


def test_dlq_retry_suppresses_queued_driver_delivery_alert(monkeypatch):
    sent: list[dict] = []
    executed: list[tuple[str, tuple]] = []

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 123

    async def fake_execute(sql, *args):
        executed.append((sql, args))
        return None

    monkeypatch.setattr(dlq_retry, "safe_send", fake_safe_send)
    monkeypatch.setattr(dlq_retry, "execute", fake_execute)

    row = {
        "id": 10,
        "alert_type": "standalone_delivery",
        "chat_id": 111,
        "text": "Delivery Complete - No next load dispatched yet",
        "parse_mode": None,
        "disable_web_page_preview": True,
        "truck_unit": "702658",
        "load_id": None,
        "stop_event_id": None,
        "msg_id_column": None,
        "attempts": 0,
    }

    outcome = asyncio.run(dlq_retry._retry_one(bot=object(), row=row))

    assert outcome == "suppressed"
    assert sent == []
    assert len(executed) == 1
    assert "permanently_failed_at = NOW()" in executed[0][0]
    assert executed[0][1][2] == "suppressed: driver one-shot alerts are not retried"


def test_dlq_retry_suppresses_queued_driver_red_flag(monkeypatch):
    executed: list[tuple[str, tuple]] = []

    async def fake_execute(sql, *args):
        executed.append((sql, args))
        return None

    monkeypatch.setattr(dlq_retry, "execute", fake_execute)

    row = {
        "id": 11,
        "alert_type": "missed_fuel_stop",
        "chat_id": 111,
        "text": "MISSED FUEL STOP",
        "parse_mode": None,
        "disable_web_page_preview": True,
        "truck_unit": "702658",
        "load_id": "LOAD-1",
        "stop_event_id": None,
        "msg_id_column": None,
        "attempts": 0,
    }

    outcome = asyncio.run(dlq_retry._retry_one(bot=object(), row=row))

    assert outcome == "suppressed"
    assert executed[0][1][2] == "suppressed: driver one-shot alerts are not retried"


def test_dlq_retry_suppresses_queued_driver_approach_alert(monkeypatch):
    monkeypatch.setattr(dlq_retry.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)

    suppressed = dlq_retry._is_suppressed_driver_alert_retry("approach", 111)

    assert suppressed is True


def test_dlq_retry_does_not_suppress_dispatch_delivery_alert(monkeypatch):
    monkeypatch.setattr(dlq_retry.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)

    suppressed = dlq_retry._is_suppressed_driver_alert_retry(
        "delivery_complete",
        222,
    )

    assert suppressed is False
