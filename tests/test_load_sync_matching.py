"""
Tests for driver-name extraction and matching logic in load_sync.

Covers:
  * _driver_name_from_samsara — cleans Samsara vehicle names into driver names
  * _name_similarity — fuzzy name comparison
  * _match_vehicle_by_driver_name — selects best Samsara vehicle for a DT driver
  * _extract_truck_unit — pulls truck unit from DataTruck order shapes

These functions are the primary pipeline for matching DataTruck orders to
Samsara vehicles when unit numbers don't align cleanly.
"""
import asyncio

import pytest

from dieselup.clients.samsara import VehicleSummary, extract_samsara_index_keys, extract_unit_digits
from dieselup.core.load_sync import (
    _alert_admin_error,
    _driver_name_from_samsara,
    _extract_truck_unit,
    _match_vehicle_by_driver_name,
    _name_similarity,
    _is_verified_driver_assignment,
)
from dieselup.core import load_sync


# ---------------------------------------------------------------------------
# _driver_name_from_samsara
# ---------------------------------------------------------------------------

class TestDriverNameFromSamsara:
    """Verify name extraction from every naming format seen in the fleet."""

    @pytest.mark.parametrize("vehicle_name,expected", [
        # plain "unit - NAME" format
        ("702658 - MILTON MEDINA",               "MILTON MEDINA"),
        ("401000 - BRUNOSAIRE GABRIEL",           "BRUNOSAIRE GABRIEL"),
        ("541910 - RUKUNDO YVAN",                 "RUKUNDO YVAN"),
        ("567667 - NZEYIMANA KAMALI",             "NZEYIMANA KAMALI"),
        ("585979 - PIERRE ANDRE",                 "PIERRE ANDRE"),
        ("431666 - Rodny Louis Jacques",          "Rodny Louis Jacques"),
        ("9298 - GLORIA NIWEMUGENI",              "GLORIA NIWEMUGENI"),
        ("567663 - Jetta Itangiteka",             "Jetta Itangiteka"),
        ("702662 - D'Arcy Richardson",            "D'Arcy Richardson"),
        ("529847 (203602) - PATRICK KAMALI",      "PATRICK KAMALI"),
        ("545978 (551569) - BERNAVIL FLEURY",     "BERNAVIL FLEURY"),
        ("401998 - THEOGENE WALCOTT",             "THEOGENE WALCOTT"),
        ("476604 - EMMANUEL ISHIMWE",             "EMMANUEL ISHIMWE"),
        ("452431 - Therron Johnson",              "Therron Johnson"),
        ("1646 - Jackson Marcelin",               "Jackson Marcelin"),
        ("3044 - Jimmy Brown",                    "Jimmy Brown"),
        ("3440 - ALEXANDER MATTIS",               "ALEXANDER MATTIS"),
        ("581621 - LATHO GUEU",                   "LATHO GUEU"),
        # UNIT# prefix
        ("UNIT# 3044 - Jimmy Brown",              "Jimmy Brown"),
        ("UNIT - 567667 - NZEYIMANA KAMALI",      "NZEYIMANA KAMALI"),
        # SUBUNIT format
        ("SUBUNIT# 727424 (551802) - JETSON ANDRE",   "JETSON ANDRE"),
        ("SUBUNIT - 898725(551566) WALNES DORESTAL",  "WALNES DORESTAL"),
        # short unit numbers
        ("9299 - RUBEN HAMPTON",                  "RUBEN HAMPTON"),
        ("2477 - MINANI JEAN",                    "MINANI JEAN"),
        ("7764 - Ahmed Ben-Marzouk",              "Ahmed Ben-Marzouk"),
        ("777N - HECTOR MARTIN",                  "HECTOR MARTIN"),
    ])
    def test_extracts_driver_name(self, vehicle_name, expected):
        result = _driver_name_from_samsara(vehicle_name)
        assert result == expected, (
            f"_driver_name_from_samsara({vehicle_name!r}) returned {result!r}, "
            f"expected {expected!r}"
        )

    def test_inactive_vehicle_returns_none(self):
        assert _driver_name_from_samsara("Inactive") is None

    def test_new_vehicle_returns_none(self):
        assert _driver_name_from_samsara("NEW") is None

    def test_no_driver_vehicle_returns_none(self):
        assert _driver_name_from_samsara("3044 - No Driver") is None

    def test_totalled_vehicle_returns_none(self):
        assert _driver_name_from_samsara("5555 - Totalled") is None

    def test_empty_string_returns_none(self):
        assert _driver_name_from_samsara("") is None

    def test_null_name_returns_none(self):
        assert _driver_name_from_samsara(None) is None

    def test_unit_only_returns_none(self):
        # Only digits + separators, no name part
        assert _driver_name_from_samsara("3044 - ") is None or \
               _driver_name_from_samsara("3044") is None


def test_quickmanage_assignment_requires_verified_group_truck_and_driver():
    driver = {
        "driver_telegram_id": -100123,
        "driver_full_name": "JANE DRIVER / JOHN TEAMMATE",
    }
    links = {"8143": -100123}
    assert _is_verified_driver_assignment(
        driver=driver,
        truck_unit="8143",
        quickmanage_driver="Jane Driver",
        verified_driver_links=links,
    )
    assert not _is_verified_driver_assignment(
        driver=driver,
        truck_unit="8143",
        quickmanage_driver="Wrong Driver",
        verified_driver_links=links,
    )
    assert not _is_verified_driver_assignment(
        driver=driver,
        truck_unit="9999",
        quickmanage_driver="Jane Driver",
        verified_driver_links=links,
    )


def test_repeated_load_error_alert_is_rate_limited(monkeypatch):
    sent = []

    async def fake_send_admin(bot, text, *, alert_type="admin_misc"):
        sent.append((text, alert_type))

    monkeypatch.setattr(load_sync, "_safe_send_admin", fake_send_admin)
    load_sync._recent_error_alerts.clear()
    order = {"id": "bad-order"}

    asyncio.run(_alert_admin_error(object(), order, ValueError("bad state")))
    asyncio.run(_alert_admin_error(object(), order, ValueError("bad state")))

    assert sent == [(
        "load_sync error on order bad-order: ValueError: bad state",
        "admin_load_sync_error",
    )]


# ---------------------------------------------------------------------------
# _name_similarity
# ---------------------------------------------------------------------------

class TestNameSimilarity:
    """Similarity scores must push real matches above 0.85 and mismatches below."""

    def test_exact_match(self):
        assert _name_similarity("MILTON MEDINA", "MILTON MEDINA") == 1.0

    def test_case_insensitive(self):
        score = _name_similarity("milton medina", "MILTON MEDINA")
        assert score >= 0.85

    def test_darcy_apostrophe(self):
        # "D'Arcy Richardson" vs "DARCY RICHARDSON" — apostrophe must not kill the match
        score = _name_similarity("D'Arcy Richardson", "DARCY RICHARDSON")
        assert score >= 0.85, f"D'Arcy match score {score:.2f} < 0.85"

    def test_darcy_matches_darcy(self):
        score = _name_similarity("D'Arcy Richardson", "D'Arcy Richardson")
        assert score == 1.0

    def test_name_with_hyphen(self):
        # "Ahmed Ben-Marzouk" vs "AHMED BEN MARZOUK"
        score = _name_similarity("Ahmed Ben-Marzouk", "AHMED BEN MARZOUK")
        assert score >= 0.85, f"Hyphen match score {score:.2f} < 0.85"

    def test_different_names_score_low(self):
        score = _name_similarity("MILTON MEDINA", "JEAN THEODORE")
        assert score < 0.85

    def test_partial_name_overlap(self):
        # Last name matches but first name differs
        score = _name_similarity("JOHN ANDRE", "PIERRE ANDRE")
        # Should NOT be above threshold just because last name matches
        assert score < 0.85

    @pytest.mark.parametrize("dt_name,samsara_name", [
        ("MILTON MEDINA",       "MILTON MEDINA"),
        ("BRUNOSAIRE GABRIEL",  "BRUNOSAIRE GABRIEL"),
        ("RUKUNDO YVAN",        "RUKUNDO YVAN"),
        ("NZEYIMANA KAMALI",    "NZEYIMANA KAMALI"),
        ("PIERRE ANDRE",        "PIERRE ANDRE"),
        ("Rodny Louis Jacques", "RODNY LOUIS JACQUES"),
        ("GLORIA NIWEMUGENI",   "GLORIA NIWEMUGENI"),
        ("PATRICK KAMALI",      "PATRICK KAMALI"),
        ("BERNAVIL FLEURY",     "BERNAVIL FLEURY"),
        ("THEOGENE WALCOTT",    "THEOGENE WALCOTT"),
        ("EMMANUEL ISHIMWE",    "EMMANUEL ISHIMWE"),
        ("Therron Johnson",     "THERRON JOHNSON"),
        ("Jackson Marcelin",    "JACKSON MARCELIN"),
        ("Jimmy Brown",         "JIMMY BROWN"),
        ("ALEXANDER MATTIS",    "ALEXANDER MATTIS"),
        ("LATHO GUEU",          "LATHO GUEU"),
    ])
    def test_customer_driver_names_match(self, dt_name, samsara_name):
        score = _name_similarity(dt_name, samsara_name)
        assert score >= 0.85, (
            f"Name match failed: DT={dt_name!r} Samsara={samsara_name!r} score={score:.2f}"
        )


# ---------------------------------------------------------------------------
# _match_vehicle_by_driver_name
# ---------------------------------------------------------------------------

def _make_fleet(names: list[str]) -> dict[str, list[VehicleSummary]]:
    """Build a samsara_by_unit map from a list of vehicle names."""
    out: dict[str, list[VehicleSummary]] = {}
    for i, name in enumerate(names):
        v = VehicleSummary(id=f"vid_{i}", name=name, unit_digits=extract_unit_digits(name))
        for key in extract_samsara_index_keys(name):
            out.setdefault(key, []).append(v)
    return out


class TestMatchVehicleByDriverName:
    def test_exact_match(self):
        idx = _make_fleet(["702658 - MILTON MEDINA", "401000 - BRUNOSAIRE GABRIEL"])
        result = _match_vehicle_by_driver_name("MILTON MEDINA", idx)
        assert result is not None
        assert result.name == "702658 - MILTON MEDINA"

    def test_case_insensitive_match(self):
        idx = _make_fleet(["3044 - Jimmy Brown"])
        result = _match_vehicle_by_driver_name("jimmy brown", idx)
        assert result is not None

    def test_no_match_returns_none(self):
        idx = _make_fleet(["702658 - MILTON MEDINA"])
        result = _match_vehicle_by_driver_name("JEAN THEODORE", idx)
        assert result is None

    def test_trailing_letter_unit_matches_numeric_quickmanage_unit(self):
        idx = _make_fleet(["777N - HECTOR MARTIN"])
        result = _match_vehicle_by_driver_name("HECTOR MARTIN", idx)
        assert result is not None
        assert result.name == "777N - HECTOR MARTIN"

    def test_ambiguous_tie_returns_none(self):
        # Two vehicles with the exact same driver name — can't pick one
        idx = _make_fleet([
            "111111 - JOHN DOE",
            "222222 - JOHN DOE",
        ])
        result = _match_vehicle_by_driver_name("JOHN DOE", idx)
        assert result is None

    def test_inactive_vehicle_ignored(self):
        idx = _make_fleet(["3044 - Inactive", "702658 - MILTON MEDINA"])
        result = _match_vehicle_by_driver_name("MILTON MEDINA", idx)
        assert result is not None
        assert result.name == "702658 - MILTON MEDINA"


# ---------------------------------------------------------------------------
# _extract_truck_unit — DataTruck order parsing
# ---------------------------------------------------------------------------

class TestExtractTruckUnit:
    """Verify unit extraction from every DataTruck order shape variant."""

    def test_trip_truck_unit_number(self):
        order = {"trip": {"truck__unit_number": "702658"}}
        assert _extract_truck_unit(order) == "702658"

    def test_assigned_driver_n_truck(self):
        order = {"assigned_driver_n_truck": {"truck_unit_number": "401000"}}
        assert _extract_truck_unit(order) == "401000"

    def test_tractor_unit(self):
        order = {"tractor": {"unit": "3044"}}
        assert _extract_truck_unit(order) == "3044"

    def test_top_level_truck_unit_number(self):
        order = {"truck_unit_number": "567667"}
        assert _extract_truck_unit(order) == "567667"

    def test_top_level_unit(self):
        order = {"unit": "9299"}
        assert _extract_truck_unit(order) == "9299"

    def test_subunit_format(self):
        order = {"assigned_driver_n_truck": {"truck_unit_number": "SUBUNIT# 727424 (551802)"}}
        assert _extract_truck_unit(order) == "SUBUNIT# 727424 (551802)"

    def test_compound_with_paren(self):
        order = {"assigned_driver_n_truck": {"truck_unit_number": "529847 (203602)"}}
        assert _extract_truck_unit(order) == "529847 (203602)"

    def test_missing_returns_none(self):
        assert _extract_truck_unit({}) is None

    def test_integer_unit_converted_to_string(self):
        order = {"trip": {"truck__unit_number": 3044}}
        assert _extract_truck_unit(order) == "3044"

    def test_trip_nested_truck_dict(self):
        order = {"trip": {"truck": {"unit_number": "476604"}}}
        assert _extract_truck_unit(order) == "476604"

    def test_whitespace_stripped(self):
        order = {"trip": {"truck__unit_number": "  702658  "}}
        assert _extract_truck_unit(order) == "702658"
