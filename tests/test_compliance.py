"""
Tests for compliance.py pure functions:
  * compute_dollar_impact — saved / lost / skipped formulas
  * _truck_past_stop — longitude-based skip trigger
  * haversine_km — great-circle distance in km
"""
import asyncio

import pytest

from dieselup.core import compliance
from dieselup.config import settings
from dieselup.core.compliance import (
    FUELED_EVENT_GALLONS,
    GEOFENCE_RADIUS_KM,
    PAST_LONGITUDE_BUFFER_DEGREES,
    _fuel_delta_since_recommendation,
    _alert_admin_error,
    _missed_stop_distance_reliable,
    _red_flag_fingerprint,
    compute_dollar_impact,
    haversine_km,
)
from dieselup.core.compliance import _truck_past_stop


def test_nearby_priced_stop_retries_transient_database_timeout(monkeypatch):
    calls = []

    async def fake_fetch_all(query, *args):
        calls.append((query, args))
        if len(calls) == 1:
            raise TimeoutError
        return []

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(compliance, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(compliance.asyncio, "sleep", no_wait)

    result = asyncio.run(
        compliance._nearby_priced_stop(40.0, -86.0, exclude_site_id=123)
    )

    assert result is None
    assert len(calls) == 2
    assert "nearby_stops AS MATERIALIZED" in calls[0][0]
    normalized = " ".join(calls[0][0].split()).lower()
    assert "q.provider='fts'" in normalized
    assert "join fuel_stops fs on fs.id=q.fuel_stop_id" in normalized
    assert "lower(cp.city) = lower(fs.city)" not in normalized
    assert calls[0][1] == calls[1][1]


def test_repeated_compliance_error_alert_is_rate_limited(monkeypatch):
    sent = []

    async def fake_send_admin(bot, text):
        sent.append(text)

    monkeypatch.setattr(compliance, "_safe_send_admin", fake_send_admin)
    compliance._recent_error_alerts.clear()
    event = {"id": 42, "truck_unit": "3044", "load_id": "L-1"}

    asyncio.run(_alert_admin_error(object(), event, TimeoutError()))
    asyncio.run(_alert_admin_error(object(), event, TimeoutError()))

    assert len(sent) == 1
    assert "compliance error on event 42" in sent[0]


# ---------------------------------------------------------------------------
# compute_dollar_impact
# ---------------------------------------------------------------------------

class TestComputeDollarImpact:
    """
    Signed impact formulas:
      saved   : (worst - recommended) * gallons
      lost    : (recommended - actual) * gallons
      skipped : 0 (no fuel purchase proven)
    """

    def test_saved_formula(self):
        impact = compute_dollar_impact(
            status="saved",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=3.80,
            actual_true_cost=None,
            gallons=100,
        )
        assert impact == pytest.approx((3.80 - 3.50) * 100)

    def test_lost_formula(self):
        impact = compute_dollar_impact(
            status="lost",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=3.80,
            actual_true_cost=3.80,
            gallons=100,
        )
        # actual > recommended -> driver paid more than the assigned stop.
        assert impact == pytest.approx((3.50 - 3.80) * 100)

    def test_skipped_does_not_invent_financial_loss(self):
        impact = compute_dollar_impact(
            status="skipped",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=3.80,
            actual_true_cost=None,
            gallons=100,
        )
        assert impact == 0.0

    def test_saved_with_zero_spread_is_zero(self):
        # worst == recommended → no savings opportunity
        impact = compute_dollar_impact(
            status="saved",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=3.50,
            actual_true_cost=None,
            gallons=200,
        )
        assert impact == 0.0

    def test_lost_requires_actual_cost(self):
        with pytest.raises(ValueError):
            compute_dollar_impact(
                status="lost",
                recommended_true_cost=3.50,
                worst_candidate_true_cost=3.80,
                actual_true_cost=None,
                gallons=100,
            )

    def test_unknown_status_raises(self):
        with pytest.raises(ValueError):
            compute_dollar_impact(
                status="pending",
                recommended_true_cost=3.50,
                worst_candidate_true_cost=3.80,
                actual_true_cost=None,
                gallons=100,
            )

    def test_gallons_scales_impact(self):
        base = compute_dollar_impact(
            status="saved",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=4.00,
            actual_true_cost=None,
            gallons=100,
        )
        double = compute_dollar_impact(
            status="saved",
            recommended_true_cost=3.50,
            worst_candidate_true_cost=4.00,
            actual_true_cost=None,
            gallons=200,
        )
        assert double == pytest.approx(base * 2)


# ---------------------------------------------------------------------------
# _truck_past_stop
# ---------------------------------------------------------------------------

class TestTruckPastStop:
    """The longitude-based 'has the truck bypassed the stop?' check."""

    def test_eastbound_truck_past_stop(self):
        # Destination is east (higher lng). Stop at -74.0. Truck at -73.5 (east of stop).
        assert _truck_past_stop(truck_lng=-73.5, rec_lng=-74.0, dest_lng=-72.0)

    def test_eastbound_truck_not_past_stop(self):
        # Truck at -74.5 (west of stop at -74.0, heading east).
        assert not _truck_past_stop(truck_lng=-74.5, rec_lng=-74.0, dest_lng=-72.0)

    def test_westbound_truck_past_stop(self):
        # Destination is west (lower lng). Stop at -74.0. Truck at -74.5 (west of stop).
        assert _truck_past_stop(truck_lng=-74.5, rec_lng=-74.0, dest_lng=-76.0)

    def test_westbound_truck_not_past_stop(self):
        # Truck still east of stop heading west.
        assert not _truck_past_stop(truck_lng=-73.5, rec_lng=-74.0, dest_lng=-76.0)

    def test_gps_jitter_buffer_prevents_false_positive(self):
        # Truck is only PAST_LONGITUDE_BUFFER_DEGREES east of stop — inside the jitter buffer
        barely_past = -74.0 + PAST_LONGITUDE_BUFFER_DEGREES * 0.5
        assert not _truck_past_stop(truck_lng=barely_past, rec_lng=-74.0, dest_lng=-72.0)

    def test_dest_equals_rec_returns_false(self):
        # Edge case: stop is at delivery longitude
        assert not _truck_past_stop(truck_lng=-74.0, rec_lng=-74.0, dest_lng=-74.0)


class TestMissedStopDistanceReliability:
    def test_reasonable_bypass_distance_can_alert(self):
        assert _missed_stop_distance_reliable(30.0)

    def test_far_away_pending_event_is_stale_not_missed(self):
        assert not _missed_stop_distance_reliable(1972.0)


# ---------------------------------------------------------------------------
# haversine_km
# ---------------------------------------------------------------------------

class TestHaversineKm:
    def test_same_point(self):
        assert haversine_km(40.0, -74.0, 40.0, -74.0) == pytest.approx(0.0, abs=0.001)

    def test_geofence_radius_makes_sense(self):
        # The 1 km geofence should be about 0.009 degrees at mid-latitudes
        # Two points 1 km apart should trigger the geofence
        lat1, lng1 = 40.0, -74.0
        # 1 km north ≈ 0.009 degrees latitude
        lat2 = lat1 + 0.009
        d = haversine_km(lat1, lng1, lat2, lng1)
        assert d <= GEOFENCE_RADIUS_KM + 0.1

    def test_1km_geofence_constants_correct(self):
        assert GEOFENCE_RADIUS_KM == 1.0


# ---------------------------------------------------------------------------
# red-flag dedupe
# ---------------------------------------------------------------------------

class TestRedFlagDedupe:
    def test_same_stop_alert_has_same_fingerprint_even_if_text_changes(self):
        first = _red_flag_fingerprint(
            alert_type="missed_fuel_stop",
            truck_unit="702658",
            load_id="LOAD-1",
            dedupe_key="site:123",
        )
        second = _red_flag_fingerprint(
            alert_type="missed_fuel_stop",
            truck_unit="702658",
            load_id="LOAD-1",
            dedupe_key="site:123",
        )
        assert first == second

    def test_different_stop_has_different_fingerprint(self):
        first = _red_flag_fingerprint(
            alert_type="missed_fuel_stop",
            truck_unit="702658",
            load_id="LOAD-1",
            dedupe_key="site:123",
        )
        second = _red_flag_fingerprint(
            alert_type="missed_fuel_stop",
            truck_unit="702658",
            load_id="LOAD-1",
            dedupe_key="site:456",
        )
        assert first != second


# ---------------------------------------------------------------------------
# fuel-delta gate
# ---------------------------------------------------------------------------

class TestFuelDeltaGate:
    def test_ignores_small_fuel_rise(self):
        class FakeSamsara:
            async def get_vehicle_fuel(self, _vehicle_id):
                return 120.0

        before_pct = 100.0 / settings.TANK_CAPACITY_GALLONS * 100.0
        result = asyncio.run(
            _fuel_delta_since_recommendation(
                event={"fuel_pct_before": before_pct},
                samsara=FakeSamsara(),
                vehicle_id="vid-1",
            )
        )

        assert result is None

    def test_accepts_30_gallon_or_larger_rise(self):
        class FakeSamsara:
            async def get_vehicle_fuel(self, _vehicle_id):
                return 130.0

        before_pct = 100.0 / settings.TANK_CAPACITY_GALLONS * 100.0
        result = asyncio.run(
            _fuel_delta_since_recommendation(
                event={"fuel_pct_before": before_pct},
                samsara=FakeSamsara(),
                vehicle_id="vid-1",
            )
        )

        assert result is not None
        _pct_after, gallons = result
        assert gallons >= FUELED_EVENT_GALLONS
        assert gallons == pytest.approx(30.0)
