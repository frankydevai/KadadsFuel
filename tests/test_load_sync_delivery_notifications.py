import asyncio
from types import SimpleNamespace

from dieselup.core import load_sync
from dieselup.clients.samsara import VehicleSummary


def test_driver_briefing_fingerprint_is_per_truck_load_stop():
    first = load_sync._driver_briefing_fingerprint(
        alert_kind="briefing",
        truck_unit="702658",
        load_id="LOAD-1",
        recommended_site_id=123,
    )
    second = load_sync._driver_briefing_fingerprint(
        alert_kind="briefing",
        truck_unit="702658",
        load_id="LOAD-1",
        recommended_site_id=123,
    )
    different_stop = load_sync._driver_briefing_fingerprint(
        alert_kind="briefing",
        truck_unit="702658",
        load_id="LOAD-1",
        recommended_site_id=456,
    )

    assert first == second
    assert first != different_stop


def test_driver_briefing_fingerprint_tracks_phase_context_quantity_and_fill_mode():
    base = dict(alert_kind="briefing", truck_unit="6682", load_id="LOAD",
                recommended_site_id=1, route_phase="pickup_then_delivery",
                route_context_sha256="shipper-context", planned_gallons=80, fill_to_full=False)
    first = load_sync._driver_briefing_fingerprint(**base)
    for change in ({"route_phase": "delivery_only"}, {"route_context_sha256": "delivery-context"},
                   {"planned_gallons": 100}, {"fill_to_full": True}):
        assert load_sync._driver_briefing_fingerprint(**{**base, **change}) != first
    assert load_sync._driver_briefing_fingerprint(**{**base, "planned_gallons": 80.0}) == first


class _FakeBot:
    async def delete_message(self, *args, **kwargs):
        return None


class _FakeSamsara:
    async def get_vehicle_stats(self, vehicle_id):
        return SimpleNamespace(
            lat=40.0,
            lng=-74.0,
            fuel_gallons=100,
            mpg_rolling=6.5,
            gps_age_minutes=1,
            fuel_age_minutes=1,
        )

    async def get_vehicle_fuel(self, vehicle_id):
        return 110


def test_delivery_followup_does_not_send_to_driver_chat(monkeypatch):
    sent: list[dict] = []

    def fake_load_context(order):
        return {
            "datatruck_order_id": 123,
            "load_id": "LOAD-2",
            "truck_unit": "702658",
            "destination_lat": 41.0,
            "destination_lng": -75.0,
            "destination_label": "Delivery",
            "origin_label": "Pickup",
        }

    async def fake_ensure_truck_onboarded(**kwargs):
        return {
            "driver_telegram_id": 111,
            "samsara_vehicle_id": "veh_702658",
        }

    async def fake_build_routed_leg(*, ctx, stats, is_first_plan=False,
                                    delivery_complete_followup=False):
        assert delivery_complete_followup is True
        return {
            "recommended_site_id": "pilot_1",
            "recommended_true_cost": 3.10,
            "worst_true_cost": 3.75,
            "gallons": 150,
            "candidates_json": "[{\"site_id\": 1}]",
            "briefing_text": "Delivery Complete - updated fuel plan",
            "alert_kind": "delivery",
        }

    async def fake_fetch_one(sql, *args):
        if "INSERT INTO stop_events" in sql:
            return {"id": 42}
        return None

    async def fake_execute(*args, **kwargs):
        return None

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 900 + len(sent)

    monkeypatch.setattr(load_sync.settings, "ORS_API_KEY", "")
    monkeypatch.setattr(load_sync.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)
    monkeypatch.setattr(load_sync, "_load_context", fake_load_context)
    monkeypatch.setattr(load_sync, "_ensure_truck_onboarded", fake_ensure_truck_onboarded)
    monkeypatch.setattr(load_sync, "_build_routed_leg", fake_build_routed_leg)
    monkeypatch.setattr(load_sync, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(load_sync, "execute", fake_execute)
    monkeypatch.setattr(load_sync, "safe_send", fake_safe_send)

    result = asyncio.run(
        load_sync._process_one_load(
            {"id": 123, "truck_unit_number": "702658"},
            samsara=_FakeSamsara(),
            samsara_by_unit={"702658": [VehicleSummary("veh_702658", "702658 - Driver", "702658")]},
            bot=_FakeBot(),
            delivery_complete_followup=True,
            current_trip_verified=True,
        )
    )

    assert result == "briefed"
    assert [call["chat_id"] for call in sent] == [222]
    assert sent[0]["alert_type"] == "dispatch_delivery"


def test_standalone_delivery_complete_does_not_send_to_driver_chat(monkeypatch):
    sent: list[dict] = []

    async def fake_fetch_one(sql, *args):
        return {
            "driver_telegram_id": 111,
            "samsara_vehicle_id": "veh_702658",
        }

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 700 + len(sent)

    monkeypatch.setattr(load_sync.settings, "TELEGRAM_DISPATCH_CHAT_ID", 222)
    monkeypatch.setattr(load_sync, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(load_sync, "safe_send", fake_safe_send)

    asyncio.run(
        load_sync._send_standalone_delivery_complete(
            bot=_FakeBot(),
            samsara=_FakeSamsara(),
            truck_unit="702658",
        )
    )

    assert [call["chat_id"] for call in sent] == [222]
    assert sent[0]["alert_type"] == "dispatch_standalone_delivery"
