"""
IFTA diesel tax rates by US state (dollars per gallon).

Source: https://www.iftach.org/taxmatrix4.php — official IFTA quarterly tax
matrix. IFTA rates change every quarter; update this file on the first
business day of each quarter from the official matrix or your fleet's IFTA
broker. The optimizer's true-cost ranking is only as accurate as the rates
below.

Last updated: 2026-Q2 (effective 2026-04-01).

All 48 contiguous states are present. Ontario is included because live loads
and contracted fuel locations cross into Canada. Alaska and Hawaii are
excluded because the fleet does not operate there.
"""
from __future__ import annotations

# State (2-letter code) -> diesel IFTA tax rate, USD per gallon.
# Values stored at 4-decimal precision to match the schema's NUMERIC(8,4).
IFTA_DIESEL_RATES: dict[str, float] = {
    "AL": 0.2900,
    "AZ": 0.2600,
    "AR": 0.2850,
    "CA": 0.9410,
    "CO": 0.2450,
    "CT": 0.4920,
    "DE": 0.2200,
    "FL": 0.3645,
    "GA": 0.3480,
    "ID": 0.3300,
    "IL": 0.6710,
    "IN": 0.5700,
    "IA": 0.3250,
    "KS": 0.2600,
    "KY": 0.2470,
    "LA": 0.2000,
    "ME": 0.3290,
    "MD": 0.4625,
    "MA": 0.2400,
    "MI": 0.4760,
    "MN": 0.2850,
    "MS": 0.1800,
    "MO": 0.2450,
    "MT": 0.2945,
    "NE": 0.2860,
    "NV": 0.2700,
    "NH": 0.2380,
    "NJ": 0.4950,
    "NM": 0.2300,
    "NY": 0.4035,
    "NC": 0.4040,
    "ND": 0.2300,
    "OH": 0.4700,
    "OK": 0.2000,
    "OR": 0.4000,
    "PA": 0.7470,
    "RI": 0.3500,
    "SC": 0.2800,
    "SD": 0.3000,
    "TN": 0.2740,
    "TX": 0.2000,
    "UT": 0.3850,
    "VT": 0.3200,
    "VA": 0.3020,
    "WA": 0.4940,
    "WV": 0.3550,
    "WI": 0.3290,
    "WY": 0.2400,
    # IFTA publishes Canadian rates converted to USD per US gallon.
    "ON": 0.2490,
}

assert len(IFTA_DIESEL_RATES) == 49, (
    f"IFTA rate table should cover 48 contiguous states plus Ontario, found {len(IFTA_DIESEL_RATES)}. "
    "Did a state get removed during a quarterly update?"
)


def get_rate(state_code: str) -> float:
    """Return the IFTA diesel rate for a 2-letter state code (case-insensitive)."""
    code = (state_code or "").strip().upper()
    if code not in IFTA_DIESEL_RATES:
        raise ValueError(f"No IFTA diesel rate on file for state {code!r}")
    return IFTA_DIESEL_RATES[code]
