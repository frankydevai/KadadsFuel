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
        "fuel_pct_before": 50, "approach_ping_sent_at": None,
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
