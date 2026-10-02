"""Builds the full unit-dashboard payload — pure assembly + classification, no I/O.

`build_unit_view` takes already-fetched inputs (route, corridor-split stations with prices and
road miles, truck/load facts) and returns the JSON the frontend renders: six stat cards, the
three left-column cards, the route shape, and a classified `stations[]` with a `tier` per stop.

Classification reads each station's `ifta_adjusted` (IFTA true net cost: card price + home-state
rate − stop-state rate) — it never recomputes the IFTA math. The caller picks which price feeds
`ifta_adjusted` via the Retail/Discount toggle and passes it in; re-toggling re-runs only this
function, not the route.

Tiers:  best=green · valid=amber · rejected=red · out_of_range=gray
  best         — lowest ifta_adjusted among usable in-corridor stops
  valid        — in corridor, within REJECT_OVER $/gal of best
  rejected     — in corridor but more than REJECT_OVER over best
  out_of_range — off corridor, or unusable (no price / not truck_accessible)
"""

from __future__ import annotations

from typing import Any


def _r(v, n=2):
    return round(float(v), n) if v is not None else None


def _usable(s: dict) -> bool:
    return bool(s.get("truck_accessible", True)) and s.get("ifta_adjusted") is not None


def build_unit_view(
    *,
    unit: str,
    date: str,
    current_station: str | None,
    price_mode: str,                 # "discount" | "retail"
    truck: dict,                     # {driver, tank_size, avg_mpg}
    load: dict,                      # {load_id, pickup:{address,time}, delivery:{address,time}}
    route: dict,                     # {shape, miles}
    in_stations: list[dict],         # in-corridor; each has ifta_adjusted, your_price, retail_price, miles_to_stop, cross_track_miles
    off_stations: list[dict],        # off-corridor / excluded
    fuel_at_send_pct: float,
    gallons_to_buy: float,
    reject_over: float,
    truck_pos: tuple[float, float] | None = None,
) -> dict[str, Any]:
    usable = [s for s in in_stations if _usable(s)]
    best = min(usable, key=lambda s: s["ifta_adjusted"]) if usable else None

    def tier_for(s: dict) -> str:
        if best is None or not _usable(s):
            return "out_of_range"
        if s["site_id"] == best["site_id"]:
            return "best"
        return "rejected" if (s["ifta_adjusted"] - best["ifta_adjusted"]) > reject_over else "valid"

    stations_out: list[dict] = []
    for s in in_stations:
        t = tier_for(s)
        price_shown = s.get("retail_price") if price_mode == "retail" else s.get("your_price")
        stations_out.append({
            "site_id": s.get("site_id"),
            "name": s.get("name"),
            "lat": _r(s.get("lat"), 6),
            "lon": _r(s.get("lon"), 6),
            "price": _r(price_shown, 3),
            "ifta_adjusted": _r(s.get("ifta_adjusted"), 3),
            "miles_to_stop": _r(s.get("miles_to_stop"), 1),
            "cross_track_miles": _r(s.get("cross_track_miles"), 1),
            "tier": t,
            "label": f"${_r(price_shown, 3):.3f}" if price_shown is not None else "—",
        })
    for s in off_stations:
        if s.get("lat") is None or s.get("lon") is None:
            continue  # never emit phantom pins
        price_shown = s.get("retail_price") if price_mode == "retail" else s.get("your_price")
        stations_out.append({
            "site_id": s.get("site_id"), "name": s.get("name"),
            "lat": _r(s.get("lat"), 6), "lon": _r(s.get("lon"), 6),
            "price": _r(price_shown, 3), "ifta_adjusted": _r(s.get("ifta_adjusted"), 3),
            "miles_to_stop": None, "cross_track_miles": _r(s.get("cross_track_miles"), 1),
            "tier": "out_of_range",
            "label": f"${_r(price_shown, 3):.3f}" if price_shown is not None else "—",
        })

    # ── Stat cards ──────────────────────────────────────────────────────────────
    contracted_per_gal = _r(best["your_price"], 3) if best else None
    retails = [s["retail_price"] for s in in_stations if s.get("retail_price") is not None]
    route_avg_per_gal = _r(sum(retails) / len(retails), 3) if retails else None
    savings_per_gal = (_r(route_avg_per_gal - contracted_per_gal, 3)
                       if (route_avg_per_gal is not None and contracted_per_gal is not None) else None)
    total_savings = (_r(savings_per_gal * gallons_to_buy, 2) if savings_per_gal is not None else None)
    miles_to_stop = _r(best.get("miles_to_stop"), 1) if best else None

    station_card = None
    if best:
        station_card = {
            "site_id": best.get("site_id"), "name": best.get("name"),
            "city": best.get("city"), "state": best.get("state"),
            "your_price": _r(best.get("your_price"), 3),
            "retail_price": _r(best.get("retail_price"), 3),
            "ifta_adjusted": _r(best.get("ifta_adjusted"), 3),
            "miles_to_stop": miles_to_stop,
        }

    return {
        "unit": unit,
        "current_station": current_station,
        "date": date,
        "price_mode": price_mode,
        "stats": {
            "contracted_per_gal": contracted_per_gal,
            "route_avg_per_gal": route_avg_per_gal,
            "savings_per_gal": savings_per_gal,
            "total_savings": total_savings,
            "fuel_at_send_pct": _r(fuel_at_send_pct, 0),
            "miles_to_stop": miles_to_stop,
        },
        "truck": {
            "unit": unit,
            "driver": truck.get("driver"),
            "tank_size": _r(truck.get("tank_size"), 0),
            "avg_mpg": _r(truck.get("avg_mpg"), 1),
            "fuel_at_send_pct": _r(fuel_at_send_pct, 0),
            "gps_age_minutes": _r(truck.get("gps_age_minutes"), 1),
        },
        "load": {
            "load_id": load.get("load_id"),
            "miles": _r(route.get("miles"), 0),
            "pickup": load.get("pickup"),
            "delivery": load.get("delivery"),
        },
        "station": station_card,
        "route": {"shape": route.get("shape"), "miles": _r(route.get("miles"), 1),
                  "mode": route.get("mode", "valhalla"),
                  "approximate": route.get("mode") == "approximate"},
        "truck_pos": ({"lat": _r(truck_pos[0], 6), "lng": _r(truck_pos[1], 6)} if truck_pos else None),
        "stations": stations_out,
        "counts": {"in_range": len(in_stations), "off_corridor": len(off_stations)},
    }
