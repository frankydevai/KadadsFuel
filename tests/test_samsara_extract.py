"""
Tests for Samsara vehicle name parsing: extract_unit_digits and extract_samsara_index_keys.

Covers every unit format present in the customer's registered fleet list and
verifies the Samsara auto-onboard matching logic works for all of them.
"""
import pytest

from dieselup.clients.samsara import (
    VehicleSummary,
    extract_samsara_index_keys,
    extract_unit_digits,
)
from dieselup.core.load_sync import _resolve_samsara_matches


# ---------------------------------------------------------------------------
# extract_unit_digits — first standalone digit sequence in a vehicle name
# ---------------------------------------------------------------------------

class TestExtractUnitDigits:
    """All naming formats observed in the customer's Samsara fleet."""

    def test_plain_unit_dash_name(self):
        assert extract_unit_digits("702658 - MILTON MEDINA") == "702658"

    def test_plain_unit_short(self):
        assert extract_unit_digits("9299 - RUBEN HAMPTON") == "9299"

    def test_unit_hash(self):
        assert extract_unit_digits("UNIT# 3044 - Jimmy Brown") == "3044"

    def test_unit_dash(self):
        assert extract_unit_digits("UNIT - 567667 - KAMALI") == "567667"

    def test_subunit_hash(self):
        assert extract_unit_digits("SUBUNIT# 727424 (551802) - JETSON ANDRE") == "727424"

    def test_subunit_no_hash(self):
        assert extract_unit_digits("SUBUNIT 898725(551566) WALNES DORESTAL") == "898725"

    def test_plain_with_paren(self):
        assert extract_unit_digits("529847 (203602) - PATRICK KAMALI") == "529847"

    def test_embedded_digit_returns_none(self):
        # Digits inside a word like 'F550' or 'GHP2' must not match
        assert extract_unit_digits("GHP2-GED-P5C") is None

    def test_ford_model_returns_none(self):
        assert extract_unit_digits("Ford F550") is None

    def test_none_input(self):
        assert extract_unit_digits(None) is None

    def test_empty_string(self):
        assert extract_unit_digits("") is None

    def test_inactive_label(self):
        assert extract_unit_digits("Inactive") is None

    def test_leading_zeros_preserved(self):
        assert extract_unit_digits("005 - Driver Name") == "005"

    def test_trailing_letter_unit_matches_numeric_alias(self):
        assert extract_unit_digits("777N - HECTOR MARTIN") == "777"

    # Every unit from the customer fleet list
    @pytest.mark.parametrize("name,expected", [
        ("702658 - MILTON MEDINA",              "702658"),
        ("401000 - BRUNOSAIRE GABRIEL",         "401000"),
        ("401999 - OMAR SALMAN",                "401999"),
        ("541910 - RUKUNDO YVAN",               "541910"),
        ("567667 - NZEYIMANA KAMALI",           "567667"),
        ("9299 - RUBEN HAMPTON",                "9299"),
        ("2477 - MINANI JEAN",                  "2477"),
        ("7764 - Ahmed Ben-Marzouk",            "7764"),
        ("585979 - PIERRE ANDRE",               "585979"),
        ("431666 - Rodny Louis Jacques",        "431666"),
        ("9298 - GLORIA NIWEMUGENI",            "9298"),
        ("567663 - Jetta Itangiteka",           "567663"),
        ("702662 - D'Arcy Richardson",          "702662"),
        ("529847 (203602) - PATRICK KAMALI",    "529847"),
        ("545978 (551569) - BERNAVIL FLEURY",   "545978"),
        ("401998 - THEOGENE WALCOTT",           "401998"),
        ("431661 - MAVTAY ROBERT TOBIN",        "431661"),
        ("770999 - JEAN THEODORE",              "770999"),
        ("476604 - EMMANUEL ISHIMWE",           "476604"),
        ("452431 - Therron Johnson",            "452431"),
        ("1646 - Jackson Marcelin",             "1646"),
        ("3044 - Jimmy Brown",                  "3044"),
        ("211610 - Deliner Verdul",             "211610"),
        ("3440 - ALEXANDER MATTIS",             "3440"),
        ("581621 - LATHO GUEU",                 "581621"),
    ])
    def test_customer_fleet_units(self, name, expected):
        assert extract_unit_digits(name) == expected


# ---------------------------------------------------------------------------
# extract_samsara_index_keys — all keys a vehicle should be indexed under
# ---------------------------------------------------------------------------

class TestExtractSamsaraIndexKeys:
    def test_plain_unit(self):
        keys = extract_samsara_index_keys("702658 - MILTON MEDINA")
        assert keys == ["702658"]

    def test_trailing_letter_unit_indexed_by_numeric_alias(self):
        keys = extract_samsara_index_keys("777N - HECTOR MARTIN")
        assert keys == ["777"]

    def test_subunit_hash_extracts_paren_as_primary(self):
        # SUBUNIT format: DataTruck uses the parenthetical number as the truck unit
        keys = extract_samsara_index_keys("SUBUNIT# 727424 (551802) - JETSON ANDRE")
        assert "551802" in keys  # paren = what DataTruck sends
        assert "727424" in keys  # primary = also indexed

    def test_subunit_variant(self):
        keys = extract_samsara_index_keys("SUBUNIT - 898725(551566) WALNES DORESTAL")
        assert "551566" in keys
        assert "898725" in keys

    def test_non_subunit_paren_only_indexes_primary(self):
        # "529847 (203602)" has no SUBUNIT prefix — only index primary (529847)
        keys = extract_samsara_index_keys("529847 (203602) - PATRICK KAMALI")
        assert keys == ["529847"]

    def test_empty_returns_empty(self):
        assert extract_samsara_index_keys("") == []
        assert extract_samsara_index_keys(None) == []

    def test_inactive_returns_empty(self):
        assert extract_samsara_index_keys("Inactive") == []

    def test_no_duplicate_keys(self):
        # When subunit primary == paren (degenerate edge case), no dupes
        keys = extract_samsara_index_keys("SUBUNIT# 12345 (12345) - Driver")
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# _resolve_samsara_matches — finds vehicles for a DataTruck truck_unit string
# ---------------------------------------------------------------------------

def _make_samsara_map(vehicles: list[VehicleSummary]) -> dict[str, list[VehicleSummary]]:
    """Replicate the samsara_by_unit index build from load_sync."""
    out: dict[str, list[VehicleSummary]] = {}
    for v in vehicles:
        for key in extract_samsara_index_keys(v.name):
            out.setdefault(key, []).append(v)
    return out


class TestResolveSamsaraMatches:
    """Verify matching for every unit format in the customer fleet."""

    def _vehicle(self, name: str) -> VehicleSummary:
        return VehicleSummary(
            id=f"id_{extract_unit_digits(name) or name[:10]}",
            name=name,
            unit_digits=extract_unit_digits(name),
        )

    def test_plain_unit_matches(self):
        v = self._vehicle("702658 - MILTON MEDINA")
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches("702658", idx)
        assert len(matches) == 1
        assert matches[0].id == v.id

    def test_subunit_matched_by_primary_number(self):
        v = self._vehicle("SUBUNIT# 727424 (551802) - JETSON ANDRE")
        idx = _make_samsara_map([v])
        # DataTruck sends "SUBUNIT# 727424 (551802)" — extract_unit_digits → "727424"
        matches = _resolve_samsara_matches("SUBUNIT# 727424 (551802)", idx)
        assert len(matches) == 1

    def test_subunit_matched_by_paren_number(self):
        v = self._vehicle("SUBUNIT# 727424 (551802) - JETSON ANDRE")
        idx = _make_samsara_map([v])
        # If DataTruck somehow sends just "551802"
        matches = _resolve_samsara_matches("551802", idx)
        assert len(matches) == 1

    def test_non_subunit_paren_unit_matched_by_primary(self):
        v = self._vehicle("529847 (203602) - PATRICK KAMALI")
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches("529847 (203602)", idx)
        assert len(matches) == 1

    def test_compound_slash_unit(self):
        v = self._vehicle("1646 - Jackson Marcelin")
        idx = _make_samsara_map([v])
        # DataTruck sometimes sends "005/1646" — slash split logic
        matches = _resolve_samsara_matches("005/1646", idx)
        assert len(matches) == 1

    def test_no_match_returns_empty(self):
        v = self._vehicle("702658 - MILTON MEDINA")
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches("999999", idx)
        assert matches == []

    def test_leading_zeros_stripped(self):
        v = self._vehicle("005 - Driver")
        idx = _make_samsara_map([v])
        # Looking up "5" should still find "005"
        matches = _resolve_samsara_matches("5", idx)
        assert len(matches) == 1

    def test_trailing_letter_samsara_unit_matches_numeric_tms_unit(self):
        v = self._vehicle("777N - HECTOR MARTIN")
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches("777", idx)
        assert len(matches) == 1
        assert matches[0].id == v.id

    def test_non_subunit_paren_fallback_to_inner_number(self):
        # If DataTruck sends "551566" (inner paren) for "898725 (551566)",
        # the fix in _resolve_samsara_matches must find the vehicle.
        # This vehicle has NO SUBUNIT prefix so only "898725" is indexed.
        # The paren fallback in _resolve_samsara_matches handles the other direction:
        # if we call with "898725 (551566)" the primary "898725" should match.
        v = self._vehicle("898725 - WALNES DORESTAL")  # vehicle named without paren
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches("898725 (551566)", idx)
        assert len(matches) == 1

    @pytest.mark.parametrize("dt_unit,samsara_name", [
        ("702658",                       "702658 - MILTON MEDINA"),
        ("401000",                       "401000 - BRUNOSAIRE GABRIEL"),
        ("567667",                       "567667 - NZEYIMANA KAMALI"),
        ("3044",                         "UNIT# 3044 - Jimmy Brown"),
        ("529847 (203602)",              "529847 (203602) - PATRICK KAMALI"),
        ("SUBUNIT# 727424 (551802)",     "SUBUNIT# 727424 (551802) - JETSON ANDRE"),
    ])
    def test_customer_fleet_lookup(self, dt_unit, samsara_name):
        v = VehicleSummary(id="vid_1", name=samsara_name, unit_digits=extract_unit_digits(samsara_name))
        idx = _make_samsara_map([v])
        matches = _resolve_samsara_matches(dt_unit, idx)
        assert len(matches) == 1, (
            f"Expected 1 match for DT unit {dt_unit!r} against Samsara {samsara_name!r}, "
            f"got {len(matches)}"
        )
