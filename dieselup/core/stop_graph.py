"""Valhalla-backed Pilot/FJ stop snapping and nearby stop-distance graph.

This module is deliberately batch-oriented. It is run by the admin
`/refreshgraph` command or the CLI script, not inside the load-sync hot path.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any, Iterable

from dieselup.clients.valhalla import MatrixCell, SnappedStop, ValhallaClient
from dieselup.config import settings
from dieselup.core.optimizer import haversine_miles
from dieselup.db import get_pool

log = logging.getLogger(__name__)

MAX_GRAPH_PAIR_MILES = 60.0
MAX_MATRIX_TARGETS = 50
PREFLIGHT_SAMPLES = 5


@dataclass(frozen=True)
class StopGraphRefreshResult:
    stops_seen: int = 0
    stops_snapped: int = 0
    stops_inaccessible: int = 0
    pairs_considered: int = 0
    pairs_stored: int = 0
    pairs_failed: int = 0
    aborted_reason: str | None = None


@dataclass(frozen=True)
class _StopNode:
    id: int
    lat: float
    lon: float


async def refresh_stop_graph(
    *,
    router: Any | None = None,
    pool: Any | None = None,
    max_pair_miles: float = MAX_GRAPH_PAIR_MILES,
    preflight_samples: int = PREFLIGHT_SAMPLES,
    max_matrix_targets: int = MAX_MATRIX_TARGETS,
) -> StopGraphRefreshResult:
    """Snap fuel stops and refresh directed Valhalla stop-to-stop distances.

    The function uses the existing asyncpg pool and never opens a standalone DB
    connection. Valhalla failures abort safely before destructive graph cleanup,
    so a bad config or missing tiles cannot wipe a known-good graph.
    """
    if router is None:
        if not settings.VALHALLA_CONFIG:
            return StopGraphRefreshResult(
                aborted_reason="VALHALLA_CONFIG is not set"
            )
        router = ValhallaClient(settings.VALHALLA_CONFIG)
    elif getattr(router, "available", True) is False:
        return StopGraphRefreshResult(aborted_reason="Valhalla router unavailable")

    db_pool = pool or await get_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, station_name, latitude, longitude
            FROM fuel_stops
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY id
            """
        )

        if not rows:
            return StopGraphRefreshResult()

        preflight = await _preflight_snaps(
            router=router,
            rows=rows[: max(1, preflight_samples)],
        )
        if preflight and not any(snap is not None for snap in preflight.values()):
            return StopGraphRefreshResult(
                stops_seen=len(rows),
                aborted_reason=(
                    "Valhalla could not snap any preflight stops; "
                    "check tiles/config before marking stops inaccessible"
                ),
            )

        snapped_nodes: list[_StopNode] = []
        stops_snapped = 0
        stops_inaccessible = 0
        for row in rows:
            stop_id = int(row["id"])
            snap = preflight.get(stop_id)
            if stop_id not in preflight:
                snap = await router.snap_stop(
                    float(row["latitude"]),
                    float(row["longitude"]),
                )

            if snap is None:
                stops_inaccessible += 1
                await _mark_stop_inaccessible(conn, stop_id)
                continue

            stops_snapped += 1
            await _mark_stop_snapped(conn, stop_id, snap)
            snapped_nodes.append(_StopNode(id=stop_id, lat=snap.lat, lon=snap.lon))

        result = await _refresh_distance_pairs(
            conn=conn,
            router=router,
            nodes=snapped_nodes,
            max_pair_miles=max_pair_miles,
            max_matrix_targets=max_matrix_targets,
        )
        return StopGraphRefreshResult(
            stops_seen=len(rows),
            stops_snapped=stops_snapped,
            stops_inaccessible=stops_inaccessible,
            pairs_considered=result.pairs_considered,
            pairs_stored=result.pairs_stored,
            pairs_failed=result.pairs_failed,
            aborted_reason=result.aborted_reason,
        )


def format_refresh_result(result: StopGraphRefreshResult) -> str:
    """Compact human-readable result for Telegram/CLI."""
    if result.aborted_reason:
        return (
            "Valhalla graph refresh aborted\n"
            f"Reason: {result.aborted_reason}\n"
            f"Stops seen: {result.stops_seen}"
        )
    return (
        "Valhalla graph refresh complete\n"
        f"Stops snapped: {result.stops_snapped}/{result.stops_seen}\n"
        f"Inaccessible stops: {result.stops_inaccessible}\n"
        f"Pairs stored: {result.pairs_stored} "
        f"(failed {result.pairs_failed}, considered {result.pairs_considered})"
    )


async def _preflight_snaps(
    *,
    router: Any,
    rows: Iterable[Any],
) -> dict[int, SnappedStop | None]:
    cache: dict[int, SnappedStop | None] = {}
    for row in rows:
        stop_id = int(row["id"])
        cache[stop_id] = await router.snap_stop(
            float(row["latitude"]),
            float(row["longitude"]),
        )
    return cache


async def _mark_stop_snapped(conn: Any, stop_id: int, snap: SnappedStop) -> None:
    await conn.execute(
        """
        UPDATE fuel_stops
        SET truck_accessible = TRUE,
            snapped_lat = $2,
            snapped_lon = $3,
            road_name = $4,
            valhalla_checked_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
        """,
        stop_id,
        snap.lat,
        snap.lon,
        snap.road_name,
    )


async def _mark_stop_inaccessible(conn: Any, stop_id: int) -> None:
    await conn.execute(
        """
        UPDATE fuel_stops
        SET truck_accessible = FALSE,
            snapped_lat = NULL,
            snapped_lon = NULL,
            road_name = NULL,
            valhalla_checked_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
        """,
        stop_id,
    )


async def _refresh_distance_pairs(
    *,
    conn: Any,
    router: Any,
    nodes: list[_StopNode],
    max_pair_miles: float,
    max_matrix_targets: int,
) -> StopGraphRefreshResult:
    built_at = datetime.now(timezone.utc)
    pairs_considered = 0
    pairs_stored = 0
    pairs_failed = 0

    for origin in nodes:
        targets = [
            node
            for node in nodes
            if node.id != origin.id
            and haversine_miles(origin.lat, origin.lon, node.lat, node.lon)
            <= max_pair_miles
        ]
        for chunk in _chunks(targets, max_matrix_targets):
            pairs_considered += len(chunk)
            cells = await router.matrix(
                (origin.lat, origin.lon),
                [(target.lat, target.lon) for target in chunk],
            )
            if cells is None or len(cells) != len(chunk):
                pairs_failed += len(chunk)
                log.warning(
                    "stop_graph: Valhalla matrix failed for stop_id=%s (%d targets)",
                    origin.id,
                    len(chunk),
                )
                continue

            batch: list[tuple[int, int, float, int | None, datetime]] = []
            for target, cell in zip(chunk, cells):
                if not isinstance(cell, MatrixCell):
                    pairs_failed += 1
                    continue
                batch.append(
                    (
                        origin.id,
                        target.id,
                        cell.distance_miles,
                        cell.duration_seconds,
                        built_at,
                    )
                )

            if not batch:
                continue
            await conn.executemany(
                """
                INSERT INTO stop_distances
                    (from_stop_id, to_stop_id, distance_miles,
                     duration_seconds, updated_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (from_stop_id, to_stop_id) DO UPDATE SET
                    distance_miles = EXCLUDED.distance_miles,
                    duration_seconds = EXCLUDED.duration_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                batch,
            )
            pairs_stored += len(batch)

    # Only prune stale rows after at least one pair succeeded. If Valhalla was
    # broken during matrixing, keep the previous graph instead of wiping it.
    if pairs_stored > 0:
        await conn.execute("DELETE FROM stop_distances WHERE updated_at < $1", built_at)

    return StopGraphRefreshResult(
        pairs_considered=pairs_considered,
        pairs_stored=pairs_stored,
        pairs_failed=pairs_failed,
    )


def _chunks(items: list[_StopNode], n: int) -> Iterable[list[_StopNode]]:
    for i in range(0, len(items), max(1, n)):
        yield items[i : i + max(1, n)]
