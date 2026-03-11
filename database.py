"""
database.py  -  PostgreSQL connection + schema + all queries.

Tables:
  trucks          - maps Samsara vehicle_name → telegram_group_id
  fuel_stops      - all Pilot + Love's locations with diesel prices
  truck_states    - current state of each truck (persisted across restarts)
  fuel_alerts     - one row per low-fuel alert event
  ca_reminders    - tracks California border reminders sent (cooldown)
  bot_config      - key/value store for config (pilot locations cache etc.)
"""

import logging
import time
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from datetime import datetime
from config import DATABASE_URL

log = logging.getLogger(__name__)


# -- Connection ---------------------------------------------------------------

def get_connection(retries: int = 3, delay: float = 2.0):
    """Connect to PostgreSQL with automatic retry on connection failure."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                DATABASE_URL,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            log.warning(f"DB connection attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_err


@contextmanager
def db_cursor():
    """Yields a dict cursor; commits on success, rolls back on error."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -- Schema -------------------------------------------------------------------

SCHEMA_SQL = """
-- trucks: manually inserted to map Samsara name → Telegram group
CREATE TABLE IF NOT EXISTS trucks (
    id                  SERIAL PRIMARY KEY,
    vehicle_name        TEXT    NOT NULL UNIQUE,
    telegram_group_id   TEXT,
    tank_capacity_gal   REAL    NOT NULL DEFAULT 150,
    avg_mpg             REAL    NOT NULL DEFAULT 6.5,
    tank_size_known     BOOLEAN NOT NULL DEFAULT FALSE,
    mpg_known           BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_stops: seeded from Pilot CSV + Love's XLSX
CREATE TABLE IF NOT EXISTS fuel_stops (
    id              SERIAL PRIMARY KEY,
    source          TEXT    NOT NULL,
    store_id        TEXT    NOT NULL,
    store_name      TEXT    NOT NULL,
    brand           TEXT    NOT NULL,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    latitude        REAL    NOT NULL,
    longitude       REAL    NOT NULL,
    phone           TEXT,
    diesel_price    REAL,
    price_updated   TIMESTAMPTZ,
    has_diesel      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, store_id)
);

CREATE INDEX IF NOT EXISTS idx_fuel_stops_lat_lng ON fuel_stops (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_state   ON fuel_stops (state);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_source  ON fuel_stops (source);

-- truck_states: full state persisted to survive Railway redeploys
CREATE TABLE IF NOT EXISTS truck_states (
    vehicle_id              TEXT PRIMARY KEY,
    vehicle_name            TEXT,
    state                   TEXT        NOT NULL DEFAULT 'UNKNOWN',
    fuel_pct                REAL,
    latitude                REAL,
    longitude               REAL,
    speed_mph               REAL,
    heading                 REAL,
    next_poll               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parked_since            TIMESTAMPTZ,
    alert_sent              BOOLEAN     NOT NULL DEFAULT FALSE,
    overnight_alert_sent    BOOLEAN     NOT NULL DEFAULT FALSE,
    open_alert_id           INTEGER,
    assigned_stop_id        INTEGER,
    assigned_stop_name      TEXT,
    assigned_stop_lat       REAL,
    assigned_stop_lng       REAL,
    assignment_time         TIMESTAMPTZ,
    in_yard                 BOOLEAN     NOT NULL DEFAULT FALSE,
    yard_name               TEXT,
    sleeping                BOOLEAN     NOT NULL DEFAULT FALSE,
    fuel_when_parked        REAL,
    ca_reminder_sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    prev_truck_group        TEXT,
    prev_truck_msg_id       BIGINT,
    prev_dispatcher_msg_id  BIGINT,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_alerts: history of every alert sent
CREATE TABLE IF NOT EXISTS fuel_alerts (
    id              SERIAL PRIMARY KEY,
    vehicle_id      TEXT    NOT NULL,
    vehicle_name    TEXT,
    fuel_pct        REAL    NOT NULL,
    latitude        REAL    NOT NULL,
    longitude       REAL    NOT NULL,
    heading         REAL,
    speed_mph       REAL,
    best_stop_id    INTEGER,
    best_stop_name  TEXT,
    best_stop_price REAL,
    alt_stop_id     INTEGER,
    alt_stop_name   TEXT,
    alt_stop_price  REAL,
    savings_usd     REAL,
    alert_type      TEXT    NOT NULL DEFAULT 'low_fuel',
    status          TEXT    NOT NULL DEFAULT 'open',
    alerted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fuel_alerts_vehicle ON fuel_alerts (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fuel_alerts_status  ON fuel_alerts (status);

-- ca_reminders: prevent duplicate CA border reminders
CREATE TABLE IF NOT EXISTS ca_reminders (
    id          SERIAL PRIMARY KEY,
    vehicle_id  TEXT        NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def init_db():
    """Create all tables if they don't exist. Runs migrations for existing DBs."""
    log.info("Initializing PostgreSQL schema...")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    # Migrations for existing DBs
    cur.execute("ALTER TABLE trucks ALTER COLUMN telegram_group_id DROP NOT NULL")
    for col, coltype in [
        ("prev_truck_group",       "TEXT"),
        ("prev_truck_msg_id",      "BIGINT"),
        ("prev_dispatcher_msg_id", "BIGINT"),
    ]:
        cur.execute(f"ALTER TABLE truck_states ADD COLUMN IF NOT EXISTS {col} {coltype}")
    conn.commit()
    conn.close()
    log.info("✅ Database schema ready.")


# -- Config -------------------------------------------------------------------

def set_config_value(key: str, value: str):
    sql = """
        INSERT INTO bot_config (key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, (key, value))


def get_config_value(key: str) -> str | None:
    with db_cursor() as cur:
        cur.execute("SELECT value FROM bot_config WHERE key=%s", (key,))
        row = cur.fetchone()
        return row["value"] if row else None


# -- Helpers ------------------------------------------------------------------

def _row(r):
    return dict(r) if r else None

def _rows(rs):
    return [dict(r) for r in rs]

def _dt(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


# -- trucks -------------------------------------------------------------------

def get_truck_group(vehicle_name: str) -> str | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT telegram_group_id FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return row["telegram_group_id"] if row else None


def get_truck_config(vehicle_name: str) -> dict | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        return _row(cur.fetchone())


def get_all_registered_trucks() -> list:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM trucks WHERE is_active = TRUE ORDER BY vehicle_name")
        return _rows(cur.fetchall())


def auto_register_truck(vehicle_id: str, vehicle_name: str) -> bool:
    """Auto-register a truck seen from Samsara if not already in DB. Returns True if newly registered."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM trucks WHERE vehicle_name = %s", (vehicle_name,))
        if cur.fetchone():
            return False
        cur.execute(
            "INSERT INTO trucks (vehicle_name, is_active) VALUES (%s, TRUE)",
            (vehicle_name,)
        )
        log.info(f"Auto-registered new truck: {vehicle_name}")
        return True


def upsert_truck_group(vehicle_name: str, group_id: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "UPDATE trucks SET telegram_group_id = %s WHERE vehicle_name = %s",
            (group_id, vehicle_name)
        )
        return cur.rowcount > 0


def deactivate_truck(vehicle_name: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "UPDATE trucks SET is_active = FALSE WHERE vehicle_name = %s",
            (vehicle_name,)
        )
        return cur.rowcount > 0


# -- fuel_stops ---------------------------------------------------------------

def upsert_fuel_stop(row: dict):
    bulk_upsert_fuel_stops([row])


def bulk_upsert_fuel_stops(records: list[dict]) -> int:
    """Bulk insert/update fuel stops. Returns count inserted/updated."""
    if not records:
        return 0
    sql = """
        INSERT INTO fuel_stops
            (source, store_id, store_name, brand, address, city, state, zip,
             latitude, longitude, phone, diesel_price, price_updated, has_diesel)
        VALUES
            (%(source)s, %(store_id)s, %(store_name)s, %(brand)s,
             %(address)s, %(city)s, %(state)s, %(zip)s,
             %(latitude)s, %(longitude)s, %(phone)s,
             %(diesel_price)s, %(price_updated)s, %(has_diesel)s)
        ON CONFLICT (source, store_id) DO UPDATE SET
            store_name    = EXCLUDED.store_name,
            brand         = EXCLUDED.brand,
            address       = EXCLUDED.address,
            city          = EXCLUDED.city,
            state         = EXCLUDED.state,
            latitude      = EXCLUDED.latitude,
            longitude     = EXCLUDED.longitude,
            diesel_price  = EXCLUDED.diesel_price,
            price_updated = EXCLUDED.price_updated,
            has_diesel    = EXCLUDED.has_diesel
    """
    with db_cursor() as cur:
        cur.executemany(sql, records)
    return len(records)


def get_all_diesel_stops() -> list:
    """Return all stops that have diesel and a known price."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT * FROM fuel_stops
            WHERE has_diesel = TRUE AND diesel_price IS NOT NULL
            ORDER BY state, city
        """)
        return _rows(cur.fetchall())


def get_stops_count() -> int:
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM fuel_stops WHERE has_diesel = TRUE")
        return cur.fetchone()["cnt"]


def get_price_last_updated() -> datetime | None:
    with db_cursor() as cur:
        cur.execute("SELECT MAX(price_updated) as latest FROM fuel_stops")
        row = cur.fetchone()
        return row["latest"] if row else None


# -- truck_states -------------------------------------------------------------

def load_all_truck_states() -> dict:
    """Load all truck states from DB. Returns {vehicle_id: state_dict}."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM truck_states")
        rows = cur.fetchall()

    states = {}
    for row in rows:
        r = dict(row)
        vid = r["vehicle_id"]
        states[vid] = {
            "vehicle_id":             vid,
            "vehicle_name":           r["vehicle_name"],
            "state":                  r["state"],
            "fuel_pct":               r["fuel_pct"],
            "lat":                    r["latitude"],
            "lng":                    r["longitude"],
            "speed_mph":              r["speed_mph"],
            "heading":                r["heading"],
            "next_poll":              _dt(r["next_poll"]),
            "parked_since":           _dt(r["parked_since"]),
            "alert_sent":             bool(r["alert_sent"]),
            "overnight_alert_sent":   bool(r["overnight_alert_sent"]),
            "open_alert_id":          r["open_alert_id"],
            "assigned_stop_id":       r["assigned_stop_id"],
            "assigned_stop_name":     r["assigned_stop_name"],
            "assigned_stop_lat":      r["assigned_stop_lat"],
            "assigned_stop_lng":      r["assigned_stop_lng"],
            "assignment_time":        _dt(r["assignment_time"]),
            "in_yard":                bool(r["in_yard"]),
            "yard_name":              r["yard_name"],
            "sleeping":               bool(r["sleeping"]),
            "fuel_when_parked":       r["fuel_when_parked"],
            "ca_reminder_sent":       bool(r["ca_reminder_sent"]),
            "prev_truck_group":       r.get("prev_truck_group"),
            "prev_truck_msg_id":      r.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": r.get("prev_dispatcher_msg_id"),
        }
    return states


def save_truck_state(state: dict):
    """Upsert a single truck state to DB."""
    sql = """
        INSERT INTO truck_states (
            vehicle_id, vehicle_name, state, fuel_pct,
            latitude, longitude, speed_mph, heading,
            next_poll, parked_since, alert_sent, overnight_alert_sent,
            open_alert_id, assigned_stop_id, assigned_stop_name,
            assigned_stop_lat, assigned_stop_lng, assignment_time,
            in_yard, yard_name, sleeping, fuel_when_parked,
            ca_reminder_sent, prev_truck_group, prev_truck_msg_id,
            prev_dispatcher_msg_id, last_updated
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(state)s, %(fuel_pct)s,
            %(lat)s, %(lng)s, %(speed_mph)s, %(heading)s,
            %(next_poll)s, %(parked_since)s, %(alert_sent)s, %(overnight_alert_sent)s,
            %(open_alert_id)s, %(assigned_stop_id)s, %(assigned_stop_name)s,
            %(assigned_stop_lat)s, %(assigned_stop_lng)s, %(assignment_time)s,
            %(in_yard)s, %(yard_name)s, %(sleeping)s, %(fuel_when_parked)s,
            %(ca_reminder_sent)s, %(prev_truck_group)s, %(prev_truck_msg_id)s,
            %(prev_dispatcher_msg_id)s, NOW()
        )
        ON CONFLICT (vehicle_id) DO UPDATE SET
            vehicle_name           = EXCLUDED.vehicle_name,
            state                  = EXCLUDED.state,
            fuel_pct               = EXCLUDED.fuel_pct,
            latitude               = EXCLUDED.latitude,
            longitude              = EXCLUDED.longitude,
            speed_mph              = EXCLUDED.speed_mph,
            heading                = EXCLUDED.heading,
            next_poll              = EXCLUDED.next_poll,
            parked_since           = EXCLUDED.parked_since,
            alert_sent             = EXCLUDED.alert_sent,
            overnight_alert_sent   = EXCLUDED.overnight_alert_sent,
            open_alert_id          = EXCLUDED.open_alert_id,
            assigned_stop_id       = EXCLUDED.assigned_stop_id,
            assigned_stop_name     = EXCLUDED.assigned_stop_name,
            assigned_stop_lat      = EXCLUDED.assigned_stop_lat,
            assigned_stop_lng      = EXCLUDED.assigned_stop_lng,
            assignment_time        = EXCLUDED.assignment_time,
            in_yard                = EXCLUDED.in_yard,
            yard_name              = EXCLUDED.yard_name,
            sleeping               = EXCLUDED.sleeping,
            fuel_when_parked       = EXCLUDED.fuel_when_parked,
            ca_reminder_sent       = EXCLUDED.ca_reminder_sent,
            prev_truck_group       = EXCLUDED.prev_truck_group,
            prev_truck_msg_id      = EXCLUDED.prev_truck_msg_id,
            prev_dispatcher_msg_id = EXCLUDED.prev_dispatcher_msg_id,
            last_updated           = NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":             state["vehicle_id"],
            "vehicle_name":           state.get("vehicle_name"),
            "state":                  state.get("state", "UNKNOWN"),
            "fuel_pct":               state.get("fuel_pct"),
            "lat":                    state.get("lat"),
            "lng":                    state.get("lng"),
            "speed_mph":              state.get("speed_mph"),
            "heading":                state.get("heading"),
            "next_poll":              state.get("next_poll"),
            "parked_since":           state.get("parked_since"),
            "alert_sent":             bool(state.get("alert_sent", False)),
            "overnight_alert_sent":   bool(state.get("overnight_alert_sent", False)),
            "open_alert_id":          state.get("open_alert_id"),
            "assigned_stop_id":       state.get("assigned_stop_id"),
            "assigned_stop_name":     state.get("assigned_stop_name"),
            "assigned_stop_lat":      state.get("assigned_stop_lat"),
            "assigned_stop_lng":      state.get("assigned_stop_lng"),
            "assignment_time":        state.get("assignment_time"),
            "in_yard":                bool(state.get("in_yard", False)),
            "yard_name":              state.get("yard_name"),
            "sleeping":               bool(state.get("sleeping", False)),
            "fuel_when_parked":       state.get("fuel_when_parked"),
            "ca_reminder_sent":       bool(state.get("ca_reminder_sent", False)),
            "prev_truck_group":       state.get("prev_truck_group"),
            "prev_truck_msg_id":      state.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": state.get("prev_dispatcher_msg_id"),
        })


def save_all_truck_states(states: dict):
    for state in states.values():
        save_truck_state(state)


def reset_truck_states():
    with db_cursor() as cur:
        cur.execute("DELETE FROM truck_states")
    log.info("✅ Truck states reset.")


# -- fuel_alerts --------------------------------------------------------------

def create_fuel_alert(vehicle_id, vehicle_name, fuel_pct, lat, lng,
                      heading, speed_mph, alert_type="low_fuel",
                      best_stop=None, alt_stop=None, savings_usd=None) -> int:
    sql = """
        INSERT INTO fuel_alerts (
            vehicle_id, vehicle_name, fuel_pct, latitude, longitude,
            heading, speed_mph, alert_type,
            best_stop_id, best_stop_name, best_stop_price,
            alt_stop_id,  alt_stop_name,  alt_stop_price, savings_usd
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(fuel_pct)s, %(lat)s, %(lng)s,
            %(heading)s, %(speed_mph)s, %(alert_type)s,
            %(best_stop_id)s, %(best_stop_name)s, %(best_stop_price)s,
            %(alt_stop_id)s,  %(alt_stop_name)s,  %(alt_stop_price)s, %(savings_usd)s
        ) RETURNING id
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":      vehicle_id,
            "vehicle_name":    vehicle_name,
            "fuel_pct":        fuel_pct,
            "lat":             lat,
            "lng":             lng,
            "heading":         heading,
            "speed_mph":       speed_mph,
            "alert_type":      alert_type,
            "best_stop_id":    best_stop["id"]          if best_stop else None,
            "best_stop_name":  best_stop["store_name"]  if best_stop else None,
            "best_stop_price": best_stop["diesel_price"] if best_stop else None,
            "alt_stop_id":     alt_stop["id"]           if alt_stop else None,
            "alt_stop_name":   alt_stop["store_name"]   if alt_stop else None,
            "alt_stop_price":  alt_stop["diesel_price"]  if alt_stop else None,
            "savings_usd":     savings_usd,
        })
        return cur.fetchone()["id"]


def resolve_alert(alert_id: int):
    with db_cursor() as cur:
        cur.execute(
            "UPDATE fuel_alerts SET status='resolved', resolved_at=NOW() WHERE id=%s",
            (alert_id,)
        )


# -- Aliases ------------------------------------------------------------------

def get_bot_config(key: str) -> str | None:
    return get_config_value(key)

def set_bot_config(key: str, value: str):
    set_config_value(key, value)
