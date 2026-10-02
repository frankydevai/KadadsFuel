"""
Tests for Telegram message templates in bot/messages.py.

Verifies:
  * HTML escaping of user-controlled fields (driver names with <>&)
  * All required fields rendered in output
  * Sequential and legacy format dispatch
  * briefing_message backward-compat wrapper
  * status_message and delivery_complete_message
"""
import pytest
from html import escape

from dieselup.bot.messages import (
    briefing_message,
    delivery_complete_message,
    fuel_plan_message,
    missed_fuel_stop_message,
    sequential_fuel_plan_message,
    status_message,
    wrong_fuel_stop_message,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stop_dict(**kwargs) -> dict:
    defaults = {
        "station_name": "Pilot Travel Center",
        "address": "1 Fuel Lane",
        "city": "Trenton",
        "state": "NJ",
        "latitude": 40.22,
        "longitude": -74.77,
        "your_price": 3.499,
        "retail_price": 4.099,
        "distance_miles": 45.0,
        "fuel_at_arrival": 55.0,
        "gallons_to_pump": 160,
        "true_cost_per_gallon": 3.499,
        "projected_tank_at_delivery": 110.0,
        "distance_stop_to_destination_miles": 200.0,
        "origin_label": "Newark, NJ",
        "destination_label": "Columbus, OH",
        "current_fuel_gallons": 100.0,
        "flags": [],
        "truck_latitude": 40.7,
        "truck_longitude": -74.0,
        "gallons_to_pump": 160,
        "in_sweet_spot": True,
        "savings_per_gallon": 0.60,
        "total_savings": 96.0,
        "total_trip_cost": 559.84,
        "gallons_to_stop": 6.9,
    }
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# fuel_plan_message
# ---------------------------------------------------------------------------

class TestFuelPlanMessage:
    def test_contains_complete_stop_address(self):
        text = fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "1 Fuel Lane, Trenton, NJ" in text

    def test_contains_load_id(self):
        text = fuel_plan_message(
            load_id="ZAM-9981",
            truck_unit="702658",
            origin_label="Newark, NJ",
            destination_label="Columbus, OH",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "ZAM-9981" in text

    def test_contains_truck_unit(self):
        text = fuel_plan_message(
            load_id="12345",
            truck_unit="702658",
            origin_label="Newark, NJ",
            destination_label="Columbus, OH",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "702658" in text

    def test_html_escapes_station_name(self):
        malicious_stop = _stop_dict(station_name="<script>alert('xss')</script>")
        text = fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=malicious_stop,
            gallons_to_pump=160,
        )
        assert "<script>" not in text
        assert "&lt;script&gt;" in text

    def test_html_escapes_load_id(self):
        text = fuel_plan_message(
            load_id="<LOAD>",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "<LOAD>" not in text
        assert "&lt;LOAD&gt;" in text

    def test_contains_price(self):
        text = fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(your_price=3.499),
            gallons_to_pump=160,
        )
        assert "3.499" in text

    def test_contains_directions_url(self):
        text = fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "maps.google" in text or "google.com/maps" in text

    def test_mpg_fallback_flag_shown(self):
        text = fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
            flags=["mpg_fallback_used"],
        )
        assert "mpg_fallback_used" in text


# ---------------------------------------------------------------------------
# sequential_fuel_plan_message
# ---------------------------------------------------------------------------

class TestSequentialFuelPlanMessage:
    def test_shows_stop_number(self):
        text = sequential_fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_buy=80,
            stop_number=1,
            stop_count=3,
            is_final_leg=False,
        )
        assert "STOP 1 of 3" in text

    def test_shows_exact_gallons_not_full_tank(self):
        text = sequential_fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_buy=80,
            stop_number=1,
            stop_count=2,
            is_final_leg=False,
        )
        assert "80 gal" in text

    def test_final_leg_says_delivery_reserve(self):
        text = sequential_fuel_plan_message(
            load_id="1",
            truck_unit="T1",
            origin_label="A",
            destination_label="B",
            current_fuel_gallons=100.0,
            stop=_stop_dict(),
            gallons_to_buy=80,
            stop_number=1,
            stop_count=1,
            is_final_leg=True,
        )
        assert "delivery" in text.lower()


# ---------------------------------------------------------------------------
# briefing_message (backward-compat wrapper)
# ---------------------------------------------------------------------------

class TestBriefingMessage:
    def test_empty_candidates_graceful(self):
        text = briefing_message("T1", "L1", [])
        assert "T1" in text
        assert "L1" in text

    def test_sequential_format_when_stop_number_present(self):
        stop = _stop_dict(stop_number=1, stop_count=2)
        text = briefing_message("T1", "L1", [stop])
        assert "STOP 1 of 2" in text

    def test_legacy_format_when_no_stop_number(self):
        stop = _stop_dict()  # no stop_number/stop_count
        text = briefing_message("T1", "L1", [stop])
        assert "FUEL PLAN" in text


# ---------------------------------------------------------------------------
# status_message
# ---------------------------------------------------------------------------

class TestStatusMessage:
    def test_missing_fuel_plan_does_not_claim_trip_is_inactive(self):
        # An active trip held for missing route evidence has no pending advice.
        text = status_message("8217", None, None)
        assert "8217" in text
        assert "Load status not verified" in text
        assert "No verified fuel plan yet" in text
        assert "none active" not in text.lower()

    def test_with_stop(self):
        text = status_message("702658", "ZAM-9981", _stop_dict())
        assert "ZAM-9981" in text
        assert "Pilot" in text


# ---------------------------------------------------------------------------
# red-flag messages
# ---------------------------------------------------------------------------

class TestRedFlagMessages:
    def test_missed_stop_shows_estimated_loss(self):
        text = missed_fuel_stop_message(
            truck_unit="702658",
            stop=_stop_dict(),
            distance_miles=12.0,
            current_fuel_percent=44,
            gallons_to_pump=160,
            estimated_loss_dollars=48.25,
            truck_lat=40.0,
            truck_lng=-74.0,
        )
        assert "MISSED FUEL STOP" in text
        assert "Missed savings" in text
        assert "$48.25" in text

    def test_wrong_stop_shows_estimated_loss(self):
        text = wrong_fuel_stop_message(
            truck_unit="702658",
            advised_stop=_stop_dict(station_name="Assigned Pilot"),
            actual_stop=_stop_dict(station_name="Other Pilot"),
            actual_price=3.899,
            estimated_loss_dollars=64.0,
        )
        assert "WRONG FUEL STOP" in text
        assert "Estimated loss: $64.00" in text


# ---------------------------------------------------------------------------
# delivery_complete_message
# ---------------------------------------------------------------------------

class TestDeliveryCompleteMessage:
    def test_no_stop_shows_no_next_load(self):
        text = delivery_complete_message(
            truck_unit="702658",
            next_load_id=None,
            current_fuel_percent=45,
            distance_to_next_stop_miles=None,
            stop=None,
            gallons_to_pump=220,
        )
        assert "702658" in text
        assert "next load" in text.lower() or "No next" in text

    def test_with_stop_shows_station(self):
        text = delivery_complete_message(
            truck_unit="702658",
            next_load_id="NEW-123",
            current_fuel_percent=45,
            distance_to_next_stop_miles=80.0,
            stop=_stop_dict(),
            gallons_to_pump=160,
        )
        assert "Pilot" in text
        assert "702658" in text

    def test_html_escapes_truck_unit_with_special_chars(self):
        text = delivery_complete_message(
            truck_unit="702<658>",
            next_load_id=None,
            current_fuel_percent=45,
            distance_to_next_stop_miles=None,
            stop=None,
            gallons_to_pump=220,
        )
        assert "702<658>" not in text
        assert "702&lt;658&gt;" in text
