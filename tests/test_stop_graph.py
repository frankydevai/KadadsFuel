import asyncio
from pathlib import Path

from dieselup.clients.valhalla import MatrixCell, SnappedStop
from dieselup.core.stop_graph import (
    format_refresh_result,
    refresh_stop_graph,
)


SCHEMA = (Path(__file__).resolve().parent.parent / "schema.sql").read_text()


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_exc):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.batches = []

    async def fetch(self, _query):
        return self.rows

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "OK"

    async def executemany(self, query, batch):
        self.batches.append((query, list(batch)))
        return "OK"


class FakeRouter:
    available = True

    def __init__(self, snaps):
        self.snaps = snaps
        self.snap_calls = []
        self.matrix_calls = []

    async def snap_stop(self, lat, lon):
        self.snap_calls.append((lat, lon))
        return self.snaps.get((lat, lon))

    async def matrix(self, origin, targets):
        self.matrix_calls.append((origin, targets))
        return [MatrixCell(distance_miles=12.5, duration_seconds=900) for _ in targets]


def test_schema_has_valhalla_stop_graph_contract():
    assert "truck_accessible" in SCHEMA
    assert "snapped_lat" in SCHEMA
    assert "snapped_lon" in SCHEMA
    assert "road_name" in SCHEMA
    assert "CREATE TABLE IF NOT EXISTS stop_distances" in SCHEMA
    assert "PRIMARY KEY (from_stop_id, to_stop_id)" in SCHEMA


def test_refresh_aborts_when_preflight_cannot_snap_any_stop():
    rows = [
        {"id": 1, "latitude": 40.0, "longitude": -80.0},
        {"id": 2, "latitude": 40.1, "longitude": -80.0},
    ]
    conn = FakeConn(rows)
    router = FakeRouter(snaps={})

    result = asyncio.run(
        refresh_stop_graph(router=router, pool=FakePool(conn), preflight_samples=2)
    )

    assert result.aborted_reason is not None
    assert result.stops_seen == 2
    assert conn.executed == []
    assert conn.batches == []


def test_refresh_snaps_stops_and_stores_pairs_within_60_miles():
    rows = [
        {"id": 1, "latitude": 40.0, "longitude": -80.0},
        {"id": 2, "latitude": 40.1, "longitude": -80.0},
        {"id": 3, "latitude": 42.0, "longitude": -80.0},
    ]
    snaps = {
        (40.0, -80.0): SnappedStop(40.0, -80.0, "I-80"),
        (40.1, -80.0): SnappedStop(40.1, -80.0, "I-80"),
        (42.0, -80.0): SnappedStop(42.0, -80.0, "I-80"),
    }
    conn = FakeConn(rows)
    router = FakeRouter(snaps=snaps)

    result = asyncio.run(
        refresh_stop_graph(router=router, pool=FakePool(conn), preflight_samples=1)
    )

    assert result.aborted_reason is None
    assert result.stops_seen == 3
    assert result.stops_snapped == 3
    assert result.stops_inaccessible == 0
    assert result.pairs_considered == 2
    assert result.pairs_stored == 2
    stored_pairs = [row[:2] for _query, batch in conn.batches for row in batch]
    assert stored_pairs == [(1, 2), (2, 1)]
    assert "Pairs stored: 2" in format_refresh_result(result)
