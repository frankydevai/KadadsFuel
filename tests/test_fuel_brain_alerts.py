import asyncio

from dieselup.core import fuel_brain


def test_no_plan_fuel_event_is_recorded_without_telegram(monkeypatch):
    sent: list[dict] = []
    claimed: list[dict] = []
    executed: list[tuple[str, tuple]] = []

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 1

    async def fake_claim_alert_fingerprint(**kwargs):
        claimed.append(kwargs)
        return True

    async def fake_execute(sql, *args):
        executed.append((sql, args))
        return None

    monkeypatch.setattr(fuel_brain.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)
    monkeypatch.setattr(fuel_brain, "safe_send", fake_safe_send)
    monkeypatch.setattr(fuel_brain, "_claim_alert_fingerprint", fake_claim_alert_fingerprint)
    monkeypatch.setattr(fuel_brain, "execute", fake_execute)

    asyncio.run(
        fuel_brain._alert_for_fuel_event(
            bot=object(),
            event_id=99,
            classification="off_network",
            truck_unit="551566",
            gallons=13.0,
            nearby=None,
            pending=None,
            truck_row={"driver_telegram_id": 111},
        )
    )

    assert sent == []
    assert claimed == []
    assert executed == []


def test_no_plan_contracted_fuel_event_is_quiet(monkeypatch):
    sent: list[dict] = []

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 1

    monkeypatch.setattr(fuel_brain.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)
    monkeypatch.setattr(fuel_brain, "safe_send", fake_safe_send)

    asyncio.run(
        fuel_brain._alert_for_fuel_event(
            bot=object(),
            event_id=100,
            classification="contracted_other",
            truck_unit="545978",
            gallons=108.0,
            nearby={"site_id": 1, "station_name": "Pilot Travel Center"},
            pending=None,
            truck_row={"driver_telegram_id": 111},
        )
    )

    assert sent == []


def test_active_plan_off_network_records_silently(monkeypatch):
    sent: list[dict] = []
    executed: list[tuple[str, tuple]] = []
    claimed: list[dict] = []

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 900 + len(sent)

    async def fake_claim_alert_fingerprint(**kwargs):
        claimed.append(kwargs)
        return True

    async def fake_execute(sql, *args):
        executed.append((sql, args))
        return None

    monkeypatch.setattr(fuel_brain.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)
    monkeypatch.setattr(fuel_brain, "safe_send", fake_safe_send)
    monkeypatch.setattr(fuel_brain, "_claim_alert_fingerprint", fake_claim_alert_fingerprint)
    monkeypatch.setattr(fuel_brain, "execute", fake_execute)

    asyncio.run(
        fuel_brain._alert_for_fuel_event(
            bot=object(),
            event_id=101,
            classification="off_network",
            truck_unit="702658",
            gallons=80.0,
            nearby=None,
            pending={
                "id": 10,
                "load_id": "LOAD-1",
                "recommended_site_id": 77,
                "recommended_true_cost": 3.50,
                "worst_candidate_true_cost": 4.00,
                "candidates": "[]",
            },
            truck_row={"driver_telegram_id": 111},
        )
    )

    assert sent == []
    assert claimed == []
    assert executed == []
