"""
Async PostgreSQL connection pool plus thin query helpers.

The pool is a process-wide singleton created on the first call to get_pool().
run_schema() is invoked once at that point and applies schema.sql, which uses
CREATE TABLE IF NOT EXISTS and named constraints throughout — re-running on
every cold start is safe.

All query helpers take a SQL string plus positional asyncpg parameters
($1, $2, ...). SQL is never built with f-strings or % formatting.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import Any

import asyncpg

from dieselup.config import settings

_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"
_FUEL_STOPS_PATH = Path(__file__).resolve().parent.parent / "data" / "pilot_locations.csv"

# A SQL command timeout does not bound waiting for a free pool connection.
# asyncpg also uses this limit for its cancellation-safe connection release.
POOL_ACQUIRE_TIMEOUT_SECONDS = 30.0

_pool: asyncpg.Pool | None = None
_lock = asyncio.Lock()
log = logging.getLogger(__name__)


async def get_pool() -> asyncpg.Pool:
    """Return the process-wide asyncpg pool, creating it on first call."""
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is not None:
            return _pool
        pool = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        try:
            if _run_schema_on_startup():
                await run_schema(pool)
            else:
                log.info(
                    "database schema startup pass disabled; use reviewed SQL "
                    "or RUN_SCHEMA_ON_STARTUP=true for schema changes"
                )
            await seed_bundled_fuel_stops(pool)
        except Exception:
            await pool.close()
            raise
        _pool = pool
        return _pool


def _run_schema_on_startup() -> bool:
    """Allow mature deployments to avoid redundant DDL locks on every restart."""
    value = os.getenv("RUN_SCHEMA_ON_STARTUP", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


async def seed_bundled_fuel_stops(pool: asyncpg.Pool) -> int:
    """Seed the price-free Pilot/Flying J location network on first boot.

    Contracted prices arrive separately from the carrier's daily Pilot file.
    Keeping coordinates in the image means a fresh Railway project needs only
    credentials and the price mailbox/upload; there is no manual SQL seed step.
    """
    if not _FUEL_STOPS_PATH.exists():
        return 0
    async with pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM fuel_stops")
        if int(existing or 0) > 0:
            return 0

        rows = _bundled_fuel_stop_rows()
        if not rows:
            raise RuntimeError(f"No usable fuel stops found in {_FUEL_STOPS_PATH}")

        await conn.executemany(
            """
            INSERT INTO fuel_stops
                (id, pilot_site_id, station_name, address, city, state, latitude, longitude)
            VALUES ($1::integer, $1::integer, $2, NULL, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                pilot_site_id = EXCLUDED.pilot_site_id,
                station_name = EXCLUDED.station_name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                updated_at = NOW()
            """,
            rows,
        )
        await conn.execute(
            "SELECT setval(pg_get_serial_sequence('fuel_stops', 'id'), "
            "GREATEST((SELECT MAX(id) FROM fuel_stops), 1), true)"
        )
        return len(rows)


def _bundled_fuel_stop_rows() -> list[tuple[int, str, str, str, float, float]]:
    """Parse the bundled stop network, accepting its canonical CSV header."""
    if not _FUEL_STOPS_PATH.exists():
        return []
    with _FUEL_STOPS_PATH.open(newline="", encoding="utf-8") as handle:
        rows = []
        for row in csv.DictReader(handle):
            site_id = row.get("pilot_site_id") or row.get("site_id")
            if not site_id or not row.get("latitude") or not row.get("longitude"):
                continue
            rows.append((
                int(site_id),
                row["station_name"],
                row.get("city") or "",
                row.get("state") or "",
                float(row["latitude"]),
                float(row["longitude"]),
            ))
    return rows


async def close_pool() -> None:
    """Close the pool. Safe to call multiple times."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


async def run_schema(pool: asyncpg.Pool) -> None:
    """Apply schema.sql. Idempotent — safe to run on every start.

    Statements are executed ONE AT A TIME (split on top-level semicolons)
    instead of as a single multi-statement blob, for two reasons:
      1. A failure now reports exactly WHICH statement died — a blob execute
         only surfaces the bare Postgres error with no location.
      2. Each DDL statement's effects are visible to the next one without
         any cross-statement parse/analysis ordering surprises.
    The whole script still runs inside one transaction.
    """
    if not _SCHEMA_PATH.exists():
        raise RuntimeError(f"schema.sql not found at {_SCHEMA_PATH}")
    sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql)
    async with pool.acquire() as conn:
        async with conn.transaction():
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                except Exception as exc:
                    summary = " ".join(stmt.split())[:200]
                    raise RuntimeError(
                        f"schema.sql failed on statement: {summary!r} — "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc


def _match_dollar_tag(sql: str, i: int) -> str | None:
    """If a PostgreSQL dollar-quote tag opens at index `i`, return it.

    A tag is ``$$`` or ``$identifier$`` where identifier is
    ``[A-Za-z_][A-Za-z0-9_]*`` (e.g. ``$body$``). Returns the full tag string
    including both ``$``, or None when `i` is not the start of a dollar-quote
    tag — so positional params like ``$1`` are NOT mistaken for a tag.
    """
    if i >= len(sql) or sql[i] != "$":
        return None
    j = i + 1
    n = len(sql)
    if j < n and (sql[j].isalpha() or sql[j] == "_"):
        j += 1
        while j < n and (sql[j].isalnum() or sql[j] == "_"):
            j += 1
    if j < n and sql[j] == "$":
        return sql[i : j + 1]
    return None


def _split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script on semicolons outside quotes, comments, and
    dollar-quoted bodies.

    Handles single-quoted string literals, line comments (--), and PostgreSQL
    dollar-quoted strings ($$...$$ and $tag$...$tag$). The dollar-quote support
    is required for the ``DO $$ ... $$`` self-heal blocks in schema.sql: their
    internal semicolons must NOT be treated as statement terminators (doing so
    splits the block into unterminated fragments and run_schema crashes the
    process on every cold start with "unterminated dollar-quoted string").
    """
    statements: list[str] = []
    buf: list[str] = []
    in_string = False
    in_line_comment = False
    dollar_tag: str | None = None  # current open dollar-quote tag, e.g. "$$"
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
        elif dollar_tag is not None:
            # Inside a dollar-quoted body: only the matching closing tag ends it.
            # Semicolons, quotes and -- are all literal text here.
            if ch == "$" and sql.startswith(dollar_tag, i):
                buf.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
            else:
                buf.append(ch)
                i += 1
        elif in_string:
            buf.append(ch)
            if ch == "'":
                # '' is an escaped quote inside a string literal
                if i + 1 < n and sql[i + 1] == "'":
                    buf.append("'")
                    i += 2
                else:
                    in_string = False
                    i += 1
            else:
                i += 1
        elif ch == "-" and sql[i : i + 2] == "--":
            in_line_comment = True
            buf.append(ch)
            i += 1
        elif ch == "'":
            in_string = True
            buf.append(ch)
            i += 1
        elif ch == "$" and (_dt := _match_dollar_tag(sql, i)) is not None:
            dollar_tag = _dt
            buf.append(_dt)
            i += len(_dt)
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt and not _is_only_comments(stmt):
                statements.append(stmt)
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    tail = "".join(buf).strip()
    if tail and not _is_only_comments(tail):
        statements.append(tail)
    return statements


def _is_only_comments(stmt: str) -> bool:
    return all(
        not line.strip() or line.strip().startswith("--")
        for line in stmt.splitlines()
    )


async def fetch_one(query: str, *args: Any) -> asyncpg.Record | None:
    """Run a parameterized SELECT and return the first row, or None."""
    pool = await get_pool()
    async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as conn:
        return await conn.fetchrow(query, *args)


async def fetch_all(query: str, *args: Any) -> list[asyncpg.Record]:
    """Run a parameterized SELECT and return all rows."""
    pool = await get_pool()
    async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as conn:
        return await conn.fetch(query, *args)


async def execute(query: str, *args: Any) -> str:
    """Run a parameterized INSERT/UPDATE/DELETE; returns the status string."""
    pool = await get_pool()
    async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as conn:
        return await conn.execute(query, *args)
