"""Stops-along-route geometry — pure, no I/O, no Valhalla calls.

Given a Valhalla route shape (an encoded polyline at **precision 6**) and a list of fuel stops,
decide which stops sit inside the route corridor. "In corridor" is a geometric test: the stop's
perpendicular distance to the route *line* (the actual road geometry Valhalla returned), in miles —
NOT the straight-line distance from the truck to the stop. This keeps the corridor split to one
already-computed route + plain geometry (no per-station routing).

The single Valhalla matrix call (truck → in-corridor stops) provides the real road miles-to-stop
separately; this module only decides corridor membership.
"""

from __future__ import annotations

import math
from typing import Any

EARTH_RADIUS_MILES = 3958.7613


def decode_polyline(encoded: str, precision: int = 6) -> list[tuple[float, float]]:
    """Decode an encoded polyline to [(lat, lng), …].

    Valhalla encodes shapes at precision 6 (1e-6); the Google default is precision 5. Decoding at
    the wrong precision collapses or scatters the line — always pass precision=6 for Valhalla.
    """
    if not encoded:
        return []
    factor = float(10 ** precision)
    index = lat = lng = 0
    coords: list[tuple[float, float]] = []
    length = len(encoded)
    while index < length:
        for is_lng in (False, True):
            shift = result = 0
            while True:
                if index >= length:
                    return coords
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lng:
                lng += delta
            else:
                lat += delta
        coords.append((lat / factor, lng / factor))
    return coords


def encode_polyline(coords: list[tuple[float, float]], precision: int = 6) -> str:
    """Encode [(lat, lng), …] to an encoded polyline at the given precision (6 = Valhalla).

    Used to synthesize a straight-line route shape when Valhalla is not configured, so the frontend
    can draw it with the same precision-6 decoder it uses for real routes.
    """
    factor = 10 ** precision
    out: list[str] = []
    plat = plng = 0

    def _enc(v: int) -> str:
        v <<= 1
        if v < 0:
            v = ~v
        s = ""
        while v >= 0x20:
            s += chr((0x20 | (v & 0x1F)) + 63)
            v >>= 5
        return s + chr(v + 63)

    for lat, lng in coords:
        ilat, ilng = round(lat * factor), round(lng * factor)
        out.append(_enc(ilat - plat))
        out.append(_enc(ilng - plng))
        plat, plng = ilat, ilng
    return "".join(out)


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_MILES * math.asin(min(1.0, math.sqrt(a)))


def _point_to_segment_miles(plat, plng, alat, alng, blat, blng) -> float:
    """Perpendicular distance (miles) from point P to segment A–B, using a local
    equirectangular projection around P (accurate at corridor scales)."""
    lat0 = math.radians(plat)
    mx = EARTH_RADIUS_MILES * math.cos(lat0) * math.pi / 180.0  # miles per degree lng at this lat
    my = EARTH_RADIUS_MILES * math.pi / 180.0                   # miles per degree lat
    ax, ay = (alng - plng) * mx, (alat - plat) * my
    bx, by = (blng - plng) * mx, (blat - plat) * my
    # Distance from origin (P) to segment A–B in this local planar frame.
    dx, dy = bx - ax, by - ay
    seg2 = dx * dx + dy * dy
    if seg2 <= 1e-12:
        return math.hypot(ax, ay)
    t = max(0.0, min(1.0, -(ax * dx + ay * dy) / seg2))
    cx, cy = ax + t * dx, ay + t * dy
    return math.hypot(cx, cy)


def cross_track_miles(lat: float, lng: float, polyline: list[tuple[float, float]]) -> float:
    """Minimum perpendicular distance (miles) from a point to the route polyline.

    Checks every segment (not just the nearest vertex), so a stop between sparse shape points is
    measured correctly. Returns inf for an empty/one-point line.
    """
    if len(polyline) < 2:
        if polyline:
            return haversine_miles(lat, lng, polyline[0][0], polyline[0][1])
        return float("inf")
    best = float("inf")
    for (alat, alng), (blat, blng) in zip(polyline, polyline[1:]):
        d = _point_to_segment_miles(lat, lng, alat, alng, blat, blng)
        if d < best:
            best = d
    return best


def split_corridor(
    stations: list[dict[str, Any]],
    polyline: list[tuple[float, float]],
    corridor_miles: float,
    lat_key: str = "lat",
    lng_key: str = "lon",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split stations into (in_corridor, off_corridor) by cross-track miles to the route.

    Stations missing coordinates are treated as off-corridor (never plotted as phantom pins). Each
    returned station dict gets a `cross_track_miles` field added.
    """
    in_corridor: list[dict[str, Any]] = []
    off_corridor: list[dict[str, Any]] = []
    for s in stations:
        lat, lng = s.get(lat_key), s.get(lng_key)
        if lat is None or lng is None:
            off_corridor.append({**s, "cross_track_miles": None})
            continue
        d = cross_track_miles(float(lat), float(lng), polyline)
        rec = {**s, "cross_track_miles": round(d, 2) if d != float("inf") else None}
        (in_corridor if d <= corridor_miles else off_corridor).append(rec)
    return in_corridor, off_corridor
