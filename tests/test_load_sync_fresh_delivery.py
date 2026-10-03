"""An undelivered current plan is retried from fresh evidence, never queued text."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.clients.samsara import VehicleSummary
from dieselup.core import load_sync


@pytest.fixture
def workflow(monkeypatch):
    candidate = {"site_id": 1, "distance_miles": 10,
                 "plan": {"route_evidence": {"model": "remaining_route_v1"}}}
    state = {"id": 42, "recommended_site_id": 1, "gallons": 80,
             "candidates": [candidate], "briefing_driver_msg_id": None, "accepted_audit": False,
             "driver_id": -1006682, "samsara_vehicle_id": "v6682"}
    inserted = []
    written = []

    async def fetch(sql, *args):
        if "INSERT INTO stop_events" in sql:
            inserted.append(args)
            return {"id": 43}
        if sql.lstrip().startswith("UPDATE stop_events"):
            written.append((sql, args))
            return {"id": 42}
        if "FROM stop_events" in sql and "status = 'pending'" in sql:
            return state.copy()
        if "FROM fuel_advice_audit" in sql:
            return {"id": 123} if state["accepted_audit"] else None
        raise AssertionError("Unexpected database query in delivery simulation")

    async def execute(sql, *args):
        written.append((sql, args))
        if "COALESCE($2, briefing_driver_msg_id)" in sql:
            state["briefing_driver_msg_id"] = args[1]
        return "UPDATE 1"

    driver = {"driver_full_name": "Jane Driver", "driver_telegram_id": -1006682,
              "samsara_vehicle_id": "v6682"}
    stats = SimpleNamespace(lat=40, lng=-100, fuel_gallons=60, mpg_rolling=6.5,
                            speed_mph=55, gps_age_minutes=1, fuel_age_minutes=1)
    order = {"id": "trip", "load_number": "LOAD", "truck_unit_number": "6682",
             "driver_full_name": "Jane Driver", "status": "in_transit",
             "stops": [{"id": "delivery", "type": "delivery", "completed": False,
                        "latitude": 41, "longitude": -99}]}
    leg = {"recommended_site_id": 1, "recommended_true_cost": 3,
           "worst_true_cost": 4, "gallons": 80, "candidates_json": json.dumps([candidate]),
           "briefing_text": "FRESH VALIDATED INSTRUCTION", "alert_kind": "briefing"}
    sender = AsyncMock(return_value=901)
    claim = AsyncMock(return_value=True)
    parked = AsyncMock(return_value=False)
    monkeypatch.setattr(load_sync, "fetch_one", fetch)
    monkeypatch.setattr(load_sync, "execute", execute)
    monkeypatch.setattr(load_sync, "_ensure_truck_onboarded", AsyncMock(return_value=driver))
    monkeypatch.setattr(load_sync, "_build_routed_leg", AsyncMock(return_value=leg))
    monkeypatch.setattr(load_sync, "safe_send", sender)
    monkeypatch.setattr(load_sync, "_claim_driver_briefing_fingerprint", claim)
    monkeypatch.setattr(load_sync, "_suppress_driver_briefing_for_parked_truck", parked)
    monkeypatch.setattr(load_sync, "fuel_plan_keyboard", lambda **kwargs: None)
    monkeypatch.setattr(load_sync.settings, "TELEGRAM_DISPATCH_CHAT_ID", None)
    monkeypatch.setattr(load_sync.settings, "TELEGRAM_MESSAGING_MODE", "live")
    kwargs = {"samsara": SimpleNamespace(get_vehicle_stats=AsyncMock(return_value=stats)),
              "samsara_by_unit": {"6682": [VehicleSummary("v6682", "6682 - Jane Driver", "6682")]},
              "bot": object(), "verified_driver_links": {"6682": -1006682},
              "current_trip_verified": True}
    return SimpleNamespace(state=state, inserted=inserted, written=written, order=order,
                           kwargs=kwargs, sender=sender, claim=claim, parked=parked, stats=stats, leg=leg)


def test_undelivered_same_plan_reuses_event_and_fresh_text_then_stops_after_acceptance(workflow):
    async def run():
        assert await load_sync._process_one_load(workflow.order, **workflow.kwargs) == "briefed"
        assert not workflow.inserted
        sent = workflow.sender.call_args.kwargs
        assert sent["stop_event_id"] == 42
        assert sent["text"] == "FRESH VALIDATED INSTRUCTION"
        assert sent["queue_on_failure"] is False
        assert await load_sync._process_one_load(workflow.order, **workflow.kwargs) == "skipped"
        assert workflow.sender.await_count == 1
    asyncio.run(run())


@pytest.mark.parametrize("suppression", ["parked", "resting", "silent", "unverified_group"])
def test_fresh_same_plan_is_not_retried_while_driver_is_ineligible(workflow, monkeypatch, suppression):
    if suppression == "parked":
        workflow.parked.return_value = True
    elif suppression == "resting":
        monkeypatch.setattr(load_sync, "_is_driver_resting", lambda stats: True)
    elif suppression == "silent":
        monkeypatch.setattr(load_sync.settings, "TELEGRAM_MESSAGING_MODE", "silent")
    else:
        workflow.kwargs["verified_driver_links"] = {}
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "skipped"
    workflow.sender.assert_not_awaited()
    workflow.claim.assert_not_awaited()
    assert not workflow.inserted


def test_failed_fresh_delivery_can_be_rechecked_without_duplicate_event(workflow):
    workflow.sender.side_effect = [None, 902]
    async def run():
        await load_sync._process_one_load(workflow.order, **workflow.kwargs)
        await load_sync._process_one_load(workflow.order, **workflow.kwargs)
    asyncio.run(run())
    assert workflow.sender.await_count == 2
    assert not workflow.inserted
    assert all(call.kwargs["stop_event_id"] == 42 for call in workflow.sender.call_args_list)


def test_current_moving_speed_does_not_count_as_parked_at_previous_plan_origin(monkeypatch):
    read = AsyncMock(return_value={"candidates": [{"truck_latitude": 40, "truck_longitude": -100}]})
    monkeypatch.setattr(load_sync, "fetch_one", read)
    stats = SimpleNamespace(lat=40, lng=-100, speed_mph=55, gps_age_minutes=1)
    assert not asyncio.run(load_sync._suppress_driver_briefing_for_parked_truck(truck_unit="6682", stats=stats))


@pytest.mark.parametrize("column", ["briefing_driver_msg_id", "approach_driver_msg_id", "delivery_driver_msg_id", "red_flag_driver_msg_id"])
def test_any_accepted_driver_fuel_alert_prevents_duplicate_delivery(workflow, column):
    workflow.state[column] = 900
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "skipped"
    workflow.sender.assert_not_awaited()
    assert not workflow.inserted


def test_durable_sent_audit_prevents_replay_when_id_column_write_was_lost(workflow):
    workflow.state["accepted_audit"] = True
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "skipped"
    workflow.sender.assert_not_awaited()
    workflow.claim.assert_not_awaited()


def test_unrenewable_claim_never_sends_or_queues_stored_text(workflow):
    workflow.claim.return_value = False
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "skipped"
    workflow.sender.assert_not_awaited()
    assert not workflow.inserted


def test_distance_changes_refresh_an_unsent_instruction_without_new_event(workflow):
    fresh = json.loads(workflow.leg["candidates_json"])
    fresh[0]["distance_miles"] = 25
    workflow.leg["candidates_json"] = json.dumps(fresh)
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "briefed"
    assert not workflow.inserted
    refreshed = [args for sql, args in workflow.written if "SET candidates=" in sql]
    assert json.loads(refreshed[0][1])[0]["distance_miles"] == 25


def test_phase_change_expires_old_instruction_without_missed_or_loss_scoring(workflow, monkeypatch):
    workflow.order.update(tms_provider="quickmanage", raw_status="in_transit", route_phase="delivery_only",
                          route_phase_status="in_transit", route_context_source="quickmanage_status")
    old = workflow.state["candidates"][0]["plan"]["route_evidence"]
    old.update(route_context_sha256="old-context", route_phase="pickup_then_delivery")
    fresh = json.loads(workflow.leg["candidates_json"])
    fresh[0]["plan"]["route_evidence"].update(route_context_sha256="new-context", route_phase="delivery_only")
    workflow.leg["candidates_json"] = json.dumps(fresh)
    expired = AsyncMock()
    monkeypatch.setattr(load_sync.advice_audit, "expire_pending", expired)
    from dieselup.core import route_progress
    passed = AsyncMock(side_effect=AssertionError("A phase change is not a missed stop"))
    monkeypatch.setattr(route_progress, "passed_stop_evidence", passed)
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "briefed"
    expired.assert_awaited_once_with(42, "route_context_changed")
    passed.assert_not_called()
    assert len(workflow.inserted) == 1


@pytest.mark.parametrize("identity", ["driver_id", "samsara_vehicle_id"])
def test_reconnected_group_or_vehicle_gets_new_event_without_rewriting_old_identity(workflow, monkeypatch, identity):
    workflow.state[identity] = -1009999 if identity == "driver_id" else "old-vehicle"
    expired = AsyncMock()
    monkeypatch.setattr(load_sync.advice_audit, "expire_pending", expired)
    assert asyncio.run(load_sync._process_one_load(workflow.order, **workflow.kwargs)) == "briefed"
    expired.assert_awaited_once_with(42, "driver_delivery_identity_changed")
    assert len(workflow.inserted) == 1
    assert not [sql for sql, _ in workflow.written if "SET candidates=" in sql]
    assert workflow.sender.call_args.kwargs["stop_event_id"] == 43
    assert workflow.sender.call_args.kwargs["text"] == "FRESH VALIDATED INSTRUCTION"


def test_stored_telemetry_age_includes_planning_time_independently_of_route_timestamp(monkeypatch):
    from datetime import datetime
    from dieselup.core.lane_plan import BuyLeg, LaneBuyPlan
    from dieselup.core.optimizer import CandidateStop

    stop = CandidateStop(1, "Synthetic Pilot", None, "X", "KY", 38, -85, 3.5, 4)
    planner = AsyncMock(return_value=LaneBuyPlan(
        [BuyLeg(stop, 80, 42, 0, 3.5)], 3.5,
        route_evidence={"model": "remaining_route_v1", "checked_at": "2000-01-01T00:00:00+00:00"}))
    monkeypatch.setattr(load_sync, "plan_remaining_route", planner)
    clock = iter([100, 100, 160, 160, 160])
    monkeypatch.setattr(load_sync, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    order = {"id": "SYNTHETIC", "truck_unit_number": "6682", "stops": [{
        "id": "delivery", "type": "delivery", "completed": False, "latitude": 39, "longitude": -85}]}
    stats = SimpleNamespace(lat=37, lng=-85, fuel_gallons=60, mpg_rolling=6.5,
                            gps_age_minutes=2, fuel_age_minutes=3)
    leg = asyncio.run(load_sync._build_routed_leg(ctx=load_sync._load_context(order), stats=stats))
    proof = json.loads(leg["candidates_json"])[0]["plan"]["route_evidence"]
    assert proof["gps_age_minutes_at_check"] == 3
    assert proof["fuel_age_minutes_at_check"] == 4
    assert datetime.fromisoformat(proof["telemetry_checked_at"]).tzinfo is not None
    assert proof["checked_at"] == "2000-01-01T00:00:00+00:00"
