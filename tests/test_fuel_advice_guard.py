import asyncio
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.core import advice_guard, load_sync
from dieselup.bot import sender, handlers
from dieselup.clients.samsara import VehicleSummary
from dieselup.core.lane_plan import LaneBuyPlan, BuyLeg
from dieselup.core.optimizer import CandidateStop


def event(age=0):
    proof = {
        "model": "remaining_route_v1",
        "complete_candidate_coverage": True,
        "trip_verified": True,
        "tms_order_id": "trip",
        "samsara_vehicle_id": "v1",
        "checked_at": (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat(),
    }
    return {
        "id": 1,
        "truck_unit": "100",
        "load_id": "L1",
        "tms_order_id": "trip",
        "status": "pending",
        "recommended_site_id": 1,
        "gallons": 80,
        "samsara_vehicle_id": "v1",
        "roster_vehicle_id": "v1",
        "driver_telegram_id": -101,
        "driver_full_name": "Jane Driver",
        "candidates": [{"plan": {"route_evidence": proof}, "fill_to_full": False}],
    }


@pytest.mark.parametrize(
    "change", ["legacy", "wrong_group", "changed_vehicle", "resolved"]
)
def test_legacy_or_reassigned_or_resolved_advice_cannot_send(monkeypatch, change):
    row = event()
    chat = -101
    if change == "legacy":
        row["candidates"] = [{}]
    if change == "wrong_group":
        chat = -999
    if change == "changed_vehicle":
        row["roster_vehicle_id"] = "other"
    if change == "resolved":
        row["status"] = "saved"
    monkeypatch.setattr(advice_guard, "fetch_one", AsyncMock(return_value=row))
    assert not asyncio.run(advice_guard.validate_fuel_event(1, "100", chat))


def test_just_generated_verified_plan_can_be_sent_immediately(monkeypatch):
    monkeypatch.setattr(advice_guard, "fetch_one", AsyncMock(return_value=event()))
    assert asyncio.run(advice_guard.validate_fuel_event(1, "100", -101))


@pytest.mark.parametrize(
    "alert",
    [
        "briefing",
        "dispatch_briefing",
        "approach",
        "retry_dispatch_approach",
        "delivery",
    ],
)
def test_failed_advice_validation_never_sends_deletes_or_queues(monkeypatch, alert):
    monkeypatch.setattr(sender.settings, "TELEGRAM_MESSAGING_MODE", "live")
    monkeypatch.setattr(sender, "validate_fuel_event", AsyncMock(return_value=False))
    bot = SimpleNamespace(send_message=AsyncMock(), delete_message=AsyncMock())
    queue = AsyncMock()
    monkeypatch.setattr(sender, "execute", queue)
    assert (
        asyncio.run(
            sender.safe_send(
                bot=bot,
                chat_id=-101,
                text="saved advice",
                alert_type=alert,
                stop_event_id=1,
                truck_unit="100",
                replace_previous_driver_alert=True,
            )
        )
        is None
    )
    bot.send_message.assert_not_called()
    bot.delete_message.assert_not_called()
    queue.assert_not_called()


@pytest.mark.parametrize(
    "case", ["passed_stop", "wrong_load", "changed_gallons", "same_best"]
)
def test_cached_advice_is_replanned_against_fresh_trip_and_gps(monkeypatch, case):
    row = event(600)
    order = {
        "id": "trip",
        "load_number": "L1",
        "status": "in_transit",
        "truck_unit_number": "100",
        "driver_full_name": "Jane Driver",
        "stops": [
            {"type": "delivery", "latitude": 40, "longitude": -99, "completed": False}
        ],
    }

    class Tms:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def iter_orders(self):
            yield {**order, "id": "other"} if case == "wrong_load" else order

        async def get_order(self, _):
            return order

    class Samsara:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def get_vehicle_stats(self, _):
            return SimpleNamespace(
                lat=40,
                lng=-99.5,
                gps_age_minutes=1,
                fuel_age_minutes=1,
                mpg_rolling=6.5,
            )

    stop = CandidateStop(
        2 if case == "passed_stop" else 1, "Pilot", None, "X", "NJ", 40, -99.4, 4, 4.5
    )
    planner = AsyncMock(
        return_value=LaneBuyPlan(
            [BuyLeg(stop, 90 if case == "changed_gallons" else 80, 10, 0, 4)], 4
        )
    )
    monkeypatch.setattr(advice_guard, "fetch_one", AsyncMock(return_value=row))
    monkeypatch.setattr(advice_guard, "make_tms_client", Tms)
    monkeypatch.setattr(advice_guard, "SamsaraClient", Samsara)
    monkeypatch.setattr(advice_guard, "plan_remaining_route", planner)
    monkeypatch.setattr(
        load_sync,
        "_build_samsara_unit_map",
        AsyncMock(
            return_value={"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]}
        ),
    )
    assert asyncio.run(advice_guard.validate_fuel_event(1, "100", -101)) == (
        case == "same_best"
    )
    if case != "wrong_load":
        assert planner.call_args.kwargs["stats"].lng == -99.5


def test_manual_briefing_cannot_bypass_advice_guard(monkeypatch):
    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(
        effective_message=message, effective_chat=SimpleNamespace(id=-101)
    )
    monkeypatch.setattr(
        handlers, "_resolve_driver", AsyncMock(return_value={"truck_unit": "100"})
    )
    monkeypatch.setattr(
        handlers, "_latest_pending_event", AsyncMock(return_value=event(600))
    )
    monkeypatch.setattr(handlers, "validate_fuel_event", AsyncMock(return_value=False))
    asyncio.run(handlers.briefing(update, SimpleNamespace()))
    assert message.reply_text.call_count == 1
    assert "cannot be verified" in message.reply_text.call_args.args[0]
    assert "Pilot" not in message.reply_text.call_args.args[0]
