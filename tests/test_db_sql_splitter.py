"""Regression tests for the schema.sql statement splitter.

Guards the production crash where `DO $$ ... $$` self-heal blocks were split at
their internal semicolons, producing unterminated dollar-quoted fragments that
made run_schema() abort startup ("unterminated dollar-quoted string").
"""
import sys
import types
from pathlib import Path

# Stub asyncpg + dieselup.config so db.py imports without a real DB/driver.
if "asyncpg" not in sys.modules:
    sys.modules["asyncpg"] = types.ModuleType("asyncpg")

from dieselup.db import (  # noqa: E402
    _bundled_fuel_stop_rows,
    _match_dollar_tag,
    _run_schema_on_startup,
    _split_sql_statements,
)

SCHEMA = (Path(__file__).resolve().parent.parent / "schema.sql").read_text()


def test_schema_startup_switch_defaults_on_and_accepts_false(monkeypatch):
    monkeypatch.delenv("RUN_SCHEMA_ON_STARTUP", raising=False)
    assert _run_schema_on_startup() is True
    monkeypatch.setenv("RUN_SCHEMA_ON_STARTUP", "false")
    assert _run_schema_on_startup() is False


def test_do_blocks_kept_whole_and_balanced():
    stmts = _split_sql_statements(SCHEMA)
    do_blocks = [s for s in stmts if "DO $$" in s]
    assert len(do_blocks) == 4, "all DO $$ blocks must survive splitting"
    for block in do_blocks:
        assert block.count("$$") == 2, "each DO block must keep a balanced $$ pair"
        assert block.rstrip().endswith("END $$")
        assert ";" in block, "internal semicolons must stay inside the block"


def test_no_orphan_fragments():
    stmts = _split_sql_statements(SCHEMA)
    orphans = [s for s in stmts if s.strip() in ("END IF", "END $$", "END")]
    assert orphans == []


def test_every_statement_has_balanced_dollar_quotes():
    # An odd count of $$ in any single statement is exactly what triggers the
    # asyncpg "unterminated dollar-quoted string" error.
    for stmt in _split_sql_statements(SCHEMA):
        assert stmt.count("$$") % 2 == 0


def test_plain_statements_still_split():
    sql = "CREATE TABLE a (id int); CREATE TABLE b (id int);"
    assert _split_sql_statements(sql) == [
        "CREATE TABLE a (id int)",
        "CREATE TABLE b (id int)",
    ]


def test_semicolon_inside_dollar_block_not_split():
    sql = "DO $$ BEGIN PERFORM 1; PERFORM 2; END $$; SELECT 1;"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2
    assert stmts[0] == "DO $$ BEGIN PERFORM 1; PERFORM 2; END $$"
    assert stmts[1] == "SELECT 1"


def test_named_dollar_tag():
    sql = "CREATE FUNCTION f() RETURNS int AS $body$ BEGIN RETURN 1; END $body$ LANGUAGE plpgsql;"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 1


def test_positional_param_not_treated_as_dollar_tag():
    # $1 is a parameter placeholder, not a dollar-quote tag.
    assert _match_dollar_tag("WHERE id = $1;", 11) is None
    sql = "SELECT $1; SELECT $2;"
    assert _split_sql_statements(sql) == ["SELECT $1", "SELECT $2"]


def test_shared_schema_contains_provider_neutral_event_identity():
    assert "datatruck_order_id BIGINT" in SCHEMA
    assert "tms_order_id TEXT" in SCHEMA
    assert "quickmanage_trip_id TEXT" in SCHEMA


def test_all_deployment_entrypoints_use_the_shared_dieselup_brain():
    root = Path(__file__).resolve().parent.parent
    assert "python -m dieselup.main" in (root / "Procfile").read_text()
    assert '"python", "-m", "dieselup.main"' in (root / "Dockerfile").read_text()
    assert '"startCommand": "python -m dieselup.main"' in (root / "railway.json").read_text()
    assert 'cmd = "python -m dieselup.main"' in (root / "nixpacks.toml").read_text()


def test_bundled_pilot_network_is_parseable_on_fresh_database():
    rows = _bundled_fuel_stop_rows()
    assert len(rows) >= 700
    assert all(site_id > 0 for site_id, *_rest in rows)
    assert all(-125 <= lng <= -67 for *_prefix, lng in rows)


def test_bootstrap_mode_prevents_telegram_and_scheduler_startup():
    root = Path(__file__).resolve().parent.parent
    main = (root / "dieselup" / "main.py").read_text()
    bootstrap_gate = main.index('settings.BOT_MODE == "bootstrap"')
    telegram_start = main.index("ApplicationBuilder().token")
    scheduler_start = main.index("scheduler = AsyncIOScheduler")
    assert bootstrap_gate < telegram_start
    assert bootstrap_gate < scheduler_start
