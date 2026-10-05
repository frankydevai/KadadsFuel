"""Driver outcomes require an accepted instruction, not a stored plan."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.core import advice_audit, compliance, fuel_replan, stop_visits
from dieselup.core.trip_context import route_context_signature


@pytest.fixture
def monitored_plan(monkeypatch):
    order = {"id": "synthetic-trip", "load_number": "SYNTHETIC", "status": "in_transit",
        "raw_status": "in_transit", "tms_provider": "quickmanage", "truck_unit_number": "6682",
        "driver_full_name": "Synthetic Driver", "route_phase": "delivery_only",
        "route_phase_status": "in_transit", "route_context_source": "quickmanage_status",
        "stops": [{"id": "delivery", "type": "delivery", "latitude": 33, "longitude": -97}]}
    route = {"model": "remaining_route_v1", "trip_verified": True,
        "complete_candidate_coverage": True, "samsara_vehicle_id": "synthetic-vehicle",
        "tms_provider": "quickmanage", "route_phase": "delivery_only", "route_phase_status": "in_transit",
        "route_context_source": "quickmanage_status", "route_context_sha256": route_context_signature(order),
        "monitor": {"shape": [[32, -97], [32.5, -97], [33, -97]], "first_fuel_progress_miles": 34.547}}
    event = {"id": 42, "truck_unit": "6682", "load_id": "SYNTHETIC", "driver_id": -1006682,
        "tms_order_id": "synthetic-trip", "samsara_vehicle_id": "synthetic-vehicle",
        "candidates": [{"latitude": 32.5, "longitude": -97, "plan": {"route_evidence": route}}],
        "recommended_site_id": 1, "recommended_true_cost": 3, "worst_candidate_true_cost": 4,
        "gallons": 80, "fuel_pct_before": 50, "approach_ping_sent_at": None,
        "recommended_at": datetime.now(timezone.utc)}
    state = {"accepted_audit": None, "observed": None}
    async def read(sql, *args):
        if "FROM trucks_drivers" in sql:
            return {"samsara_vehicle_id": "synthetic-vehicle", "driver_telegram_id": -1006682,
                    "driver_full_name": "Synthetic Driver"}
        if "FROM fuel_events" in sql:
            return state["observed"]
        if "FROM fuel_advice_audit" in sql:
            return state["accepted_audit"]
        raise AssertionError("Unexpected database read")
    location = SimpleNamespace(lat=32.55, lng=-97, gps_age_minutes=1, speed_mph=55)
    samsara = SimpleNamespace(get_vehicle_location=AsyncMock(return_value=location),
        get_vehicle_stats=AsyncMock(return_value=SimpleNamespace(fuel_age_minutes=1)),
        get_vehicle_fuel=AsyncMock(return_value=110))
    marked = AsyncMock()
    warning = AsyncMock()
    replan = AsyncMock(return_value=True)
    monkeypatch.setattr(compliance, "fetch_one", read)
    monkeypatch.setattr(compliance, "execute", AsyncMock())
    monkeypatch.setattr(compliance, "_mark_resolved", marked)
    monkeypatch.setattr(compliance, "_send_missed_stop_alert", warning)
    monkeypatch.setattr(compliance, "_send_approach_reminder", AsyncMock())
    monkeypatch.setattr(compliance, "_stamp_fuel_delta", AsyncMock())
    monkeypatch.setattr(stop_visits, "observe_visit", AsyncMock())
    monkeypatch.setattr(fuel_replan, "replan_truck", replan)
    monkeypatch.setattr(advice_audit, "fetch_one", AsyncMock(return_value=None))
    from dieselup.core import observed_fueling
    monkeypatch.setattr(observed_fueling, "analysis", AsyncMock(return_value=({
        "price_status": "pending", "saving": None, "extra_cost": None}, None)))
    return SimpleNamespace(event=event, state=state, marked=marked, warning=warning, replan=replan,
        kwargs={"samsara": samsara, "datatruck": SimpleNamespace(get_order=AsyncMock(return_value=order)), "bot": object()})


@pytest.mark.parametrize("non_delivery", [None, "driver_delivery_claimed", "message_attempted", "message_failed", "message_uncertain", "message_suppressed", "red_flag_id", "warning_sent", "wrong_group"])
def test_unadvised_passage_expires_neutrally_without_miss_or_warning(monitored_plan, non_delivery):
    plan = monitored_plan
    if non_delivery == "red_flag_id":
        plan.event["red_flag_driver_msg_id"] = 900
    elif non_delivery is not None:
        plan.state["accepted_audit"] = {"kind": "message_sent" if non_delivery in {"warning_sent", "wrong_group"} else non_delivery,
            "message_id": "900", "alert_type": "missed_fuel_stop" if non_delivery == "warning_sent" else "briefing",
            "chat_id": "-1009999" if non_delivery == "wrong_group" else "-1006682"}
    assert asyncio.run(compliance._resolve_one(plan.event, **plan.kwargs)) == "expired"
    plan.warning.assert_not_awaited()
    assert plan.marked.call_args.kwargs["status"] == "expired"
    assert plan.marked.call_args.kwargs["dollar_impact"] == 0
    assert plan.marked.call_args.kwargs["evidence"]["reason"] == "advice_not_delivered_before_passage"
    plan.replan.assert_awaited_once_with(plan.kwargs["bot"], "6682", 42)


@pytest.mark.parametrize("column", ["briefing_driver_msg_id", "approach_driver_msg_id", "delivery_driver_msg_id"])
def test_accepted_instruction_column_allows_verified_miss(monitored_plan, column):
    plan = monitored_plan
    plan.event[column] = 900
    assert asyncio.run(compliance._resolve_one(plan.event, **plan.kwargs)) == "skipped"
    plan.warning.assert_awaited_once()
    assert plan.marked.call_args.kwargs["status"] == "skipped"


@pytest.mark.parametrize("alert_type", ["briefing", "approach", "delivery", "delivery_complete", "retry_briefing"])
def test_durable_driver_instruction_acceptance_survives_missing_id_column(monitored_plan, alert_type):
    plan = monitored_plan
    plan.state["accepted_audit"] = {"kind": "message_sent", "chat_id": "-1006682",
        "alert_type": alert_type, "message_id": "900"}
    assert asyncio.run(compliance._resolve_one(plan.event, **plan.kwargs)) == "skipped"
    plan.warning.assert_awaited_once()


@pytest.mark.parametrize("classification", ["recommended", "contracted_other", "off_network"])
def test_unadvised_fueling_is_physical_evidence_without_driver_savings_or_loss(monitored_plan, classification):
    plan = monitored_plan
    plan.state["observed"] = {"classification": classification, "gallons": 60, "site_id": 1,
        "fuel_pct_end": 75, "finalized_at": datetime.now(timezone.utc)}
    assert asyncio.run(compliance._resolve_one(plan.event, **plan.kwargs)) == "expired"
    assert plan.marked.call_args.kwargs["status"] == "expired"
    assert plan.marked.call_args.kwargs["dollar_impact"] == 0
    assert plan.marked.call_args.kwargs["evidence"]["reason"] == "advice_not_delivered_before_fueling"
    plan.warning.assert_not_awaited()


def test_driver_warning_passes_event_metadata_to_durable_sender(monkeypatch):
    sender = AsyncMock(return_value=900)
    monkeypatch.setattr(compliance, "safe_send", sender)
    monkeypatch.setattr(compliance, "_claim_red_flag_event", AsyncMock(return_value=True))
    monkeypatch.setattr(compliance, "_claim_alert_fingerprint", AsyncMock(return_value=True))
    monkeypatch.setattr(compliance, "execute", AsyncMock())
    monkeypatch.setattr(compliance.settings, "TELEGRAM_DISPATCH_CHAT_ID", None)
    asyncio.run(compliance._send_red_flag_alert(bot=object(), driver_telegram_id=-1006682,
        text="Synthetic verified warning", alert_type="missed_fuel_stop", truck_unit="6682",
        load_id="SYNTHETIC", event_id=42, dedupe_key="site:1"))
    assert sender.call_args.kwargs["stop_event_id"] == 42
    assert sender.call_args.kwargs["msg_id_column"] == "red_flag_driver_msg_id"
