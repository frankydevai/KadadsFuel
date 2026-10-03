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
        "telemetry_checked_at": (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat(),
        "gps_age_minutes_at_check": 1,
        "fuel_age_minutes_at_check": 1,
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


@pytest.mark.parametrize("age", [0, 600])
@pytest.mark.parametrize("change", ["phase", "target", "unchanged", "truck", "driver", "reserved", "legacy_proof", "unresolved_context", "assignment_conflict", "lookup_delayed", "telemetry_stale", "missing_telemetry"])
def test_quickmanage_phase_is_rechecked_even_when_same_pump_and_quantity(monkeypatch, age, change):
    from copy import deepcopy
    from dieselup.core.trip_context import route_context_signature

    original = {
        "id": "trip", "load_number": "L1", "status": "dispatched", "raw_status": "dispatched",
        "truck_unit_number": "100", "driver_full_name": "Jane Driver", "tms_provider": "quickmanage",
        "route_phase": "pickup_then_delivery", "route_phase_status": "dispatched",
        "route_context_source": "quickmanage_status",
        "stops": [
            {"id": "pickup", "type": "pickup", "latitude": 40, "longitude": -99.8},
            {"id": "delivery", "type": "delivery", "latitude": 40, "longitude": -99},
        ],
    }
    row = event(age)
    proof = row["candidates"][0]["plan"]["route_evidence"]
    proof.update({key: original[key] for key in (
        "tms_provider", "route_phase", "route_phase_status", "route_context_source")})
    proof["route_context_sha256"] = route_context_signature(original)
    fresh = deepcopy(original)
    if change == "phase":
        fresh.update(status="in_transit", raw_status="in_transit", route_phase="delivery_only", route_phase_status="in_transit")
    elif change == "target":
        fresh["stops"][0]["longitude"] = -99.7
    elif change == "truck":
        fresh["truck_unit_number"] = "200"
    elif change == "driver":
        fresh["driver_full_name"] = "Other Driver"
    elif change == "reserved":
        fresh.update(status="reserved", raw_status="reserved", route_phase="reserved", route_phase_status="reserved")
    elif change == "legacy_proof":
        proof.pop("route_context_sha256")
    elif change == "unresolved_context":
        fresh["stops"][0]["latitude"] = None
    elif change == "assignment_conflict":
        fresh["assignment_conflict"] = True
    elif change == "telemetry_stale":
        proof["gps_age_minutes_at_check"] = 6
    elif change == "missing_telemetry":
        proof.pop("telemetry_checked_at")
    order_lookup = AsyncMock(return_value=fresh)
    if change == "lookup_delayed":
        async def delayed_lookup(*_):
            proof["checked_at"] = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()
            return fresh
        order_lookup.side_effect = delayed_lookup

    class Tms:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def iter_orders(self): yield fresh
        get_order = staticmethod(order_lookup)

    class Samsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        get_vehicle_stats = staticmethod(AsyncMock(return_value=SimpleNamespace(
            lat=40, lng=-99.9, gps_age_minutes=1, fuel_age_minutes=1, mpg_rolling=6.5)))

    planner = AsyncMock(return_value=LaneBuyPlan([BuyLeg(
        CandidateStop(1, "Pilot", None, "X", "NJ", 40, -99.4, 4, 4.5), 80, 10, 0, 4)], 4))
    monkeypatch.setattr(advice_guard, "fetch_one", AsyncMock(return_value=row))
    monkeypatch.setattr(advice_guard, "make_tms_client", Tms)
    monkeypatch.setattr(advice_guard, "SamsaraClient", Samsara)
    monkeypatch.setattr(advice_guard, "plan_remaining_route", planner)
    monkeypatch.setattr(load_sync, "_build_samsara_unit_map", AsyncMock(return_value={
        "100": [VehicleSummary("v1", "100 - Jane Driver", "100")]}))

    refreshed = change in {"lookup_delayed", "telemetry_stale", "missing_telemetry"}
    assert asyncio.run(advice_guard.validate_fuel_event(1, "100", -101)) == (change == "unchanged" or refreshed)
    if age == 0 and not refreshed:
        order_lookup.assert_awaited_once_with("trip")
    if refreshed:
        planner.assert_awaited_once()
    if change in {"phase", "target", "legacy_proof", "unresolved_context"}:
        planner.assert_not_awaited()
