import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dieselup.core.route_progress import project_progress, passed_stop_evidence
from dieselup.core import compliance, fuel_replan, load_sync, advice_audit
from dieselup.clients.samsara import VehicleSummary


def proof():
    return {"model": "remaining_route_v1", "trip_verified":True,"complete_candidate_coverage":True,"samsara_vehicle_id":"v1", "monitor": {
        "shape": [[32, -97], [32.5, -97], [33, -97]],
        "first_fuel_progress_miles": 34.547}}


@pytest.mark.parametrize("lat,missed", [(32.4, False), (32.505, False), (32.55, True)])
def test_north_south_miss_uses_route_progress_not_longitude(lat, missed):
    assert bool(passed_stop_evidence(proof(), SimpleNamespace(lat=lat, lng=-97))) == missed


def test_off_route_and_ambiguous_revisited_roads_do_not_prove_a_miss():
    assert project_progress([[32, -97], [33, -97]], (32.6, -96)) is None
    assert project_progress([[32, -97], [33, -97], [32, -97]], (32.6, -97)) is None


@pytest.mark.parametrize("case,expected", [("passed", "skipped"), ("before", None),
    ("stale_gps", None), ("stale_fuel", None), ("filled_then_left", "saved"),
    ("fuel_elsewhere", "lost"), ("trip_changed", "expired"), ("legacy", "expired")])
def test_monitor_checks_freshness_and_persisted_fueling_before_scoring_a_miss(monkeypatch, case, expected):
    route = {} if case == "legacy" else proof()
    stop = {"latitude": 32.5, "longitude": -97, "plan": {"route_evidence": route}}
    event = {"id": 10, "truck_unit": "100", "load_id": "L1", "tms_order_id": "trip",
        "samsara_vehicle_id": "v1", "candidates": json.dumps([stop]), "recommended_site_id": 1,
        "recommended_true_cost": 3, "worst_candidate_true_cost": 4, "gallons": 80,
        "fuel_pct_before": 50, "approach_ping_sent_at": None, "briefing_driver_msg_id": 900,
        "recommended_at": datetime.now(timezone.utc)}
    observed = None
    if case in {"filled_then_left", "fuel_elsewhere"}:
        observed = {"classification": "recommended" if case == "filled_then_left" else "contracted_other",
                    "gallons": 60, "site_id": 1, "fuel_pct_end": 75,"finalized_at":datetime.now(timezone.utc)}
    async def fetch(sql, *args):
        return observed if "FROM fuel_events" in sql else {"samsara_vehicle_id": "v1", "driver_telegram_id": -100}
    location = SimpleNamespace(lat=32.4 if case == "before" else 32.55, lng=-97,
        gps_age_minutes=10 if case == "stale_gps" else 1, speed_mph=55)
    client = SimpleNamespace(get_vehicle_location=AsyncMock(return_value=location),
        get_vehicle_stats=AsyncMock(return_value=SimpleNamespace(fuel_age_minutes=60 if case == "stale_fuel" else 1)),
        get_vehicle_fuel=AsyncMock(return_value=110))
    order = {"status": "in_transit", "truck_unit_number": "other" if case == "trip_changed" else "100"}
    tms = SimpleNamespace(get_order=AsyncMock(return_value=order))
    from dieselup.core import observed_fueling
    monkeypatch.setattr(observed_fueling,"fetch_one",AsyncMock(return_value=None))
    monkeypatch.setattr(compliance, "fetch_one", fetch)
    monkeypatch.setattr(compliance, "_nearby_priced_stop", AsyncMock(return_value=None))
    monkeypatch.setattr(compliance, "_detect_off_network_fueling", AsyncMock(return_value=None))
    for name in ("_send_approach_reminder", "_send_missed_stop_alert", "_stamp_fuel_delta"):
        monkeypatch.setattr(compliance, name, AsyncMock())
    marked = AsyncMock()
    monkeypatch.setattr(compliance, "_mark_resolved", marked)
    monkeypatch.setattr(compliance, "_expire_event", AsyncMock(return_value="expired"))
    result = asyncio.run(compliance._resolve_one(event, samsara=client, datatruck=tms, bot=object()))
    assert result == expected
    if expected == "skipped":
        assert marked.call_args.kwargs["dollar_impact"] == 0
        assert marked.call_args.kwargs["status"] == "skipped"
    elif expected == "lost":
        assert marked.call_args.kwargs["status"] == "lost"
        assert marked.call_args.kwargs["dollar_impact"] == 0
        assert marked.call_args.kwargs["evidence"]["price_status"] == "pending"
    elif expected == "saved":
        assert marked.call_args.kwargs["status"] == "saved"
    else:
        marked.assert_not_called()


@pytest.mark.parametrize("failed", [False, True])
def test_immediate_replan_requires_complete_current_assignment_and_links_history(monkeypatch, failed):
    order = {"id": "trip", "status": "in_transit", "truck_unit_number": "100", "driver_full_name": "Jane Driver"}
    class Tms:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def iter_orders(self, **kwargs):
            yield order
            if failed: raise RuntimeError("partial enumeration")
        async def get_order(self, _): return order
    class Samsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
    import dieselup.clients.tms as tms_module
    import dieselup.clients.samsara as samsara_module
    import dieselup.bot.group_link as links
    monkeypatch.setattr(tms_module, "make_tms_client", Tms)
    monkeypatch.setattr(samsara_module, "SamsaraClient", Samsara)
    monkeypatch.setattr(links, "refresh_and_verify_linked_groups", AsyncMock(return_value={"100": -100}))
    monkeypatch.setattr(load_sync, "_build_samsara_unit_map", AsyncMock(return_value={"100": [VehicleSummary("v1", "100 - Jane Driver", "100")]}))
    processor = AsyncMock(return_value="briefed")
    monkeypatch.setattr(load_sync, "_process_one_load", processor)
    recorder = AsyncMock()
    monkeypatch.setattr(advice_audit, "record", recorder)
    assert asyncio.run(fuel_replan.replan_truck(object(), "100", 10)) == (not failed)
    if failed:
        processor.assert_not_called()
        assert recorder.call_args.args[0] == "replan_held"
    else:
        assert processor.call_args.kwargs["replaces_event_id"] == 10
        assert processor.call_args.kwargs["current_trip_verified"] is True
        assert recorder.call_args.args[0] == "replan_completed"


def test_silent_advice_is_audited_without_sending(monkeypatch):
    from dieselup.bot import sender
    recorder = AsyncMock()
    monkeypatch.setattr(advice_audit, "record", recorder)
    monkeypatch.setattr(sender.settings, "TELEGRAM_MESSAGING_MODE", "silent")
    bot = SimpleNamespace(send_message=AsyncMock())
    asyncio.run(sender.safe_send(bot=bot, chat_id=-100, text="test", alert_type="briefing", truck_unit="100", stop_event_id=10))
    bot.send_message.assert_not_called()
    assert recorder.call_args.args[0] == "message_suppressed"
    assert recorder.call_args.kwargs["details"]["reason"] == "silent_test_mode"


@pytest.mark.parametrize("change", ["phase", "phase_unresolved", "target", "legacy_proof", "unresolved_context", "missing_fresh_metadata", "resolver_outage", "assignment_conflict", "late_assignment_conflict"])
def test_quickmanage_context_change_or_unavailability_never_penalizes_driver(monkeypatch, change):
    from copy import deepcopy
    from dieselup.core import stop_visits, advice_guard
    from dieselup.core.trip_context import route_context_signature, TripContextError

    original = {
        "id": "trip", "load_number": "L1", "status": "dispatched", "raw_status": "dispatched",
        "truck_unit_number": "100", "driver_full_name": "Jane Driver", "tms_provider": "quickmanage",
        "route_phase": "pickup_then_delivery", "route_phase_status": "dispatched",
        "route_context_source": "quickmanage_status",
        "stops": [
            {"id": "pickup", "type": "pickup", "latitude": 32.6, "longitude": -97},
            {"id": "delivery", "type": "delivery", "latitude": 33, "longitude": -97},
        ],
    }
    route = proof()
    route.update({key: original[key] for key in (
        "tms_provider", "route_phase", "route_phase_status", "route_context_source")})
    route["route_context_sha256"] = route_context_signature(original)
    fresh = deepcopy(original)
    if change in {"phase", "phase_unresolved"}:
        fresh.update(status="in_transit", raw_status="in_transit", route_phase="delivery_only", route_phase_status="in_transit")
        if change == "phase_unresolved":
            fresh["stops"][1]["latitude"] = None
    elif change == "target":
        fresh["stops"][0]["latitude"] = 32.7
    elif change == "legacy_proof":
        route.pop("route_context_sha256")
    elif change == "unresolved_context":
        fresh["stops"][0]["latitude"] = None
    elif change == "missing_fresh_metadata":
        fresh.pop("route_phase")
    elif change in {"assignment_conflict", "late_assignment_conflict"}:
        fresh["assignment_conflict"] = True
    else:
        def unavailable_context(order):
            raise TripContextError("Temporary location resolver outage")
        monkeypatch.setattr(advice_guard, "route_context_signature", unavailable_context)
    event = {
        "id": 10, "truck_unit": "100", "load_id": "L1", "tms_order_id": "trip",
        "samsara_vehicle_id": "v1", "candidates": [{"latitude": 32.5, "longitude": -97,
            "plan": {"route_evidence": route}}], "recommended_site_id": 1,
        "recommended_true_cost": 3, "worst_candidate_true_cost": 4, "gallons": 80,
        "fuel_pct_before": 50, "approach_ping_sent_at": None,
        "recommended_at": datetime.now(timezone.utc),
    }
    async def fetch(sql, *args):
        if "FROM fuel_events" in sql:
            return None
        return {"samsara_vehicle_id": "v1", "driver_telegram_id": -100, "driver_full_name": "Jane Driver"}
    location = SimpleNamespace(lat=32.55, lng=-97, gps_age_minutes=1, speed_mph=55)
    client = SimpleNamespace(get_vehicle_location=AsyncMock(return_value=location),
        get_vehicle_stats=AsyncMock(return_value=SimpleNamespace(fuel_age_minutes=1)))
    tms = SimpleNamespace(get_order=AsyncMock(
        side_effect=[original, fresh] if change == "late_assignment_conflict" else None,
        return_value=fresh))
    monkeypatch.setattr(compliance, "fetch_one", fetch)
    monkeypatch.setattr(compliance, "execute", AsyncMock())
    monkeypatch.setattr(compliance, "_fuel_delta_since_recommendation", AsyncMock(return_value=None))
    monkeypatch.setattr(compliance, "_send_approach_reminder", AsyncMock())
    missed_alert = AsyncMock()
    monkeypatch.setattr(compliance, "_send_missed_stop_alert", missed_alert)
    visit = AsyncMock()
    monkeypatch.setattr(stop_visits, "observe_visit", visit)
    marked = AsyncMock()
    monkeypatch.setattr(compliance, "_mark_resolved", marked)
    recorder = AsyncMock()
    monkeypatch.setattr(advice_audit, "record", recorder)

    result = asyncio.run(compliance._resolve_one(event, samsara=client, datatruck=tms, bot=object()))
    if change in {"unresolved_context", "missing_fresh_metadata", "resolver_outage", "assignment_conflict", "late_assignment_conflict"}:
        assert result is None
        marked.assert_not_awaited()
        assert recorder.call_args.args[0] == "monitor_held"
        expected_reason = ("current_assignment_conflict" if change in {
            "assignment_conflict", "late_assignment_conflict"} else "current_route_context_unverified")
        assert recorder.call_args.kwargs["details"]["reason"] == expected_reason
    else:
        assert result == "expired"
        assert marked.call_args.kwargs["status"] == "expired"
        assert marked.call_args.kwargs["dollar_impact"] == 0
        expected_reason = "legacy_plan_requires_route_refresh" if change == "legacy_proof" else "current_route_phase_changed"
        assert marked.call_args.kwargs["evidence"]["reason"] == expected_reason
    missed_alert.assert_not_awaited()
    if change == "late_assignment_conflict":
        # The first observation used verified assignment; the second check
        # became uncertain and must not resolve or score the stored route.
        visit.assert_awaited_once()
    else:
        compliance._send_approach_reminder.assert_not_awaited()
        visit.assert_not_awaited()
    # Resolution is mocked separately: no expiry may delete delivery claims.
    compliance.execute.assert_not_awaited()
