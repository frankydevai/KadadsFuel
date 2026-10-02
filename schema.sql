-- DieselUp shared carrier-brain PostgreSQL schema
-- Idempotent: safe to run on every deploy (CREATE IF NOT EXISTS + ALTER ADD COLUMN IF NOT EXISTS).
-- TMS-neutral: DataTruck and QuickManage identities are stored side-by-side.

CREATE TABLE IF NOT EXISTS fuel_stops (
    id BIGSERIAL PRIMARY KEY,
    station_name TEXT NOT NULL,
    address TEXT,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fuel_stops_unique UNIQUE (city, state, address)
);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_state ON fuel_stops(state);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_geo ON fuel_stops(latitude, longitude);

-- Valhalla-cleaned truck-road coordinates. Planning and compliance prefer
-- snapped_* when present; stops with truck_accessible=FALSE are excluded.
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS truck_accessible BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS snapped_lat DOUBLE PRECISION;
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS snapped_lon DOUBLE PRECISION;
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS road_name TEXT;
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS valhalla_checked_at TIMESTAMPTZ;
ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS pilot_site_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_fuel_stops_truck_accessible ON fuel_stops(truck_accessible);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fuel_stops_pilot_site_id
    ON fuel_stops(pilot_site_id) WHERE pilot_site_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fuel_stops_snapped_geo
    ON fuel_stops(snapped_lat, snapped_lon)
    WHERE truck_accessible = TRUE
      AND snapped_lat IS NOT NULL
      AND snapped_lon IS NOT NULL;

-- Directed road distances between nearby fuel stops.
-- Populated by a Valhalla graph job; lane planner falls back to geometry on cache miss.
CREATE TABLE IF NOT EXISTS stop_distances (
    from_stop_id BIGINT NOT NULL REFERENCES fuel_stops(id) ON DELETE CASCADE,
    to_stop_id BIGINT NOT NULL REFERENCES fuel_stops(id) ON DELETE CASCADE,
    distance_miles NUMERIC(8,2) NOT NULL,
    duration_seconds INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (from_stop_id, to_stop_id),
    CONSTRAINT stop_distances_not_self CHECK (from_stop_id <> to_stop_id)
);
CREATE INDEX IF NOT EXISTS idx_stop_distances_from ON stop_distances(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_distances_to ON stop_distances(to_stop_id);

-- Contracted diesel prices (uploaded from Pilot/FJ price sheets).
CREATE TABLE IF NOT EXISTS contracted_prices (
    id BIGSERIAL PRIMARY KEY,
    site_id INTEGER NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    your_price NUMERIC(8,4) NOT NULL,
    retail_price NUMERIC(8,4),
    cost NUMERIC(8,4),
    effective_date DATE NOT NULL,
    account_number TEXT NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT contracted_prices_unique UNIQUE (site_id, effective_date)
);
CREATE INDEX IF NOT EXISTS idx_contracted_prices_effective ON contracted_prices(effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_contracted_prices_state ON contracted_prices(state);

-- Driver ↔ truck assignment, onboarded by /setdriver admin command.
CREATE TABLE IF NOT EXISTS trucks_drivers (
    id BIGSERIAL PRIMARY KEY,
    truck_unit TEXT NOT NULL UNIQUE,
    driver_full_name TEXT,
    driver_telegram_id BIGINT,
    samsara_vehicle_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Shared dashboard assignment metadata (already present in production).
ALTER TABLE trucks_drivers ADD COLUMN IF NOT EXISTS telegram_group_name TEXT;
ALTER TABLE trucks_drivers ADD COLUMN IF NOT EXISTS assignment_status TEXT NOT NULL DEFAULT 'unlinked';
ALTER TABLE trucks_drivers ADD COLUMN IF NOT EXISTS alerts_paused BOOLEAN NOT NULL DEFAULT FALSE;

-- One pending row per active load. Tracks which fuel stop was recommended,
-- who complied, and the resulting dollar impact.
CREATE TABLE IF NOT EXISTS stop_events (
    id BIGSERIAL PRIMARY KEY,
    truck_unit TEXT NOT NULL,
    driver_id BIGINT,
    load_id TEXT NOT NULL,
    datatruck_order_id BIGINT,
    tms_order_id TEXT,
    quickmanage_trip_id TEXT,                   -- legacy QM-specific identity
    recommended_site_id INTEGER NOT NULL,
    recommended_true_cost NUMERIC(8,4) NOT NULL,
    candidates JSONB NOT NULL,                  -- ranked stop list with full stop metadata
    actual_site_id INTEGER,
    actual_true_cost NUMERIC(8,4),
    worst_candidate_true_cost NUMERIC(8,4) NOT NULL,
    gallons INTEGER NOT NULL DEFAULT 200,
    dollar_impact NUMERIC(10,2),
    status TEXT NOT NULL DEFAULT 'pending',
    recommended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    CONSTRAINT status_check CHECK (status IN ('pending', 'saved', 'lost', 'skipped', 'expired'))
);
CREATE INDEX IF NOT EXISTS idx_stop_events_status ON stop_events(status);
CREATE INDEX IF NOT EXISTS idx_stop_events_truck ON stop_events(truck_unit);
CREATE INDEX IF NOT EXISTS idx_stop_events_recommended_at ON stop_events(recommended_at);

-- Kadads operational history: bot writes; authenticated dashboard server reads.
-- No public Data API policies; direct database access uses the existing server role.
CREATE TABLE IF NOT EXISTS fuel_advice_audit (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    truck_unit TEXT,
    load_id TEXT,
    stop_event_id BIGINT REFERENCES stop_events(id),
    related_event_id BIGINT REFERENCES stop_events(id),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_advice_audit_created ON fuel_advice_audit(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_advice_audit_truck_created ON fuel_advice_audit(truck_unit, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_advice_audit_event ON fuel_advice_audit(stop_event_id, kind);
CREATE INDEX IF NOT EXISTS idx_advice_audit_related ON fuel_advice_audit(related_event_id);
CREATE INDEX IF NOT EXISTS idx_advice_audit_missed ON fuel_advice_audit(truck_unit, created_at DESC)
    WHERE kind='missed_detected';
CREATE INDEX IF NOT EXISTS idx_advice_audit_replan_requests ON fuel_advice_audit(truck_unit,created_at DESC)
    WHERE kind IN ('missed_detected','stop_visited_no_fill','stop_lost');
ALTER TABLE fuel_advice_audit ENABLE ROW LEVEL SECURITY;

-- Additional columns added incrementally (safe to re-run):

-- QuickManage-native TMS identity. Older databases may still have the former
-- DataTruck column; keeping this as an additive migration avoids destructive
-- deploy-time rewrites while all live code uses QuickManage exclusively.
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS quickmanage_trip_id TEXT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS datatruck_order_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS tms_order_id TEXT;
CREATE INDEX IF NOT EXISTS idx_stop_events_quickmanage_trip
    ON stop_events(quickmanage_trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_events_tms_order
    ON stop_events(tms_order_id);

-- CREATE TABLE IF NOT EXISTS does not update an older status constraint.
-- Rebuild it so the operational 'expired' safety-valve state is always legal.
DO $$ BEGIN
    ALTER TABLE stop_events DROP CONSTRAINT IF EXISTS status_check;
    ALTER TABLE stop_events ADD CONSTRAINT status_check
        CHECK (status IN ('pending', 'saved', 'lost', 'skipped', 'expired'));
END $$;

-- Delivery-complete dedup: stamp after notification sent.
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS notified_complete_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_stop_events_notified_complete
    ON stop_events(notified_complete_at) WHERE notified_complete_at IS NULL;

-- 30-mile approach reminder dedup.
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS approach_ping_sent_at TIMESTAMPTZ;

-- Telegram message IDs for editing/audit trail.
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS briefing_driver_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS briefing_dispatch_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS approach_driver_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS approach_dispatch_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS delivery_driver_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS delivery_dispatch_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS red_flag_sent_at TIMESTAMPTZ;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS red_flag_alert_type TEXT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS red_flag_driver_msg_id BIGINT;
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS red_flag_dispatch_msg_id BIGINT;

-- Fuel tracking: actual gallons pumped (Samsara ~85% accuracy).
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS fuel_pct_before NUMERIC(5,2);
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS fuel_pct_after NUMERIC(5,2);
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS actual_gallons NUMERIC(7,1);
CREATE INDEX IF NOT EXISTS idx_stop_events_fuel_verify
    ON stop_events(resolved_at)
    WHERE status = 'saved' AND fuel_pct_after IS NULL;

-- Stable Samsara vehicle ID (truck_unit varies per load; vehicle ID does not).
ALTER TABLE stop_events ADD COLUMN IF NOT EXISTS samsara_vehicle_id TEXT;
CREATE INDEX IF NOT EXISTS idx_stop_events_samsara_vehicle ON stop_events(samsara_vehicle_id);

-- Deduplication: prevents same exact alert being sent twice.
CREATE TABLE IF NOT EXISTS alert_send_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    alert_type TEXT NOT NULL,
    truck_unit TEXT,
    load_id TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dead-letter queue: failed Telegram sends retried by dlq_retry job.
-- After max attempts → permanently_failed_at stamped, admin notified once.
CREATE TABLE IF NOT EXISTS alert_dlq (
    id BIGSERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    chat_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    parse_mode TEXT,
    disable_web_page_preview BOOLEAN NOT NULL DEFAULT TRUE,
    truck_unit TEXT,
    load_id TEXT,
    stop_event_id BIGINT,
    msg_id_column TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    succeeded_at TIMESTAMPTZ,
    permanently_failed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_alert_dlq_retry_candidates
    ON alert_dlq(last_attempt_at)
    WHERE succeeded_at IS NULL AND permanently_failed_at IS NULL;

-- ── Truck telemetry ────────────────────────────────────────────────────────────
-- One row per 5-min fuel_brain poll. Pruned after SNAPSHOT_RETENTION_DAYS.
CREATE TABLE IF NOT EXISTS truck_snapshots (
    id BIGSERIAL PRIMARY KEY,
    samsara_vehicle_id TEXT NOT NULL,
    truck_unit TEXT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    fuel_pct NUMERIC(5,2),
    speed_mph NUMERIC(6,2),
    heading NUMERIC(5,1),
    taken_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Self-heal: add any columns that may be missing from older schema versions.
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'truck_snapshots' AND column_name = 'id'
  ) THEN
    ALTER TABLE truck_snapshots ADD COLUMN id BIGSERIAL PRIMARY KEY;
  END IF;
END $$;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS samsara_vehicle_id TEXT;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS truck_unit TEXT;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS fuel_pct NUMERIC(5,2);
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS speed_mph NUMERIC(6,2);
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS heading NUMERIC(5,1);
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS taken_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS gps_observed_at TIMESTAMPTZ;
ALTER TABLE truck_snapshots ADD COLUMN IF NOT EXISTS fuel_observed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_truck_snapshots_vehicle_time
    ON truck_snapshots(samsara_vehicle_id, taken_at DESC);
CREATE INDEX IF NOT EXISTS idx_truck_snapshots_taken_at
    ON truck_snapshots(taken_at);

-- ── Fueling events ────────────────────────────────────────────────────────────
-- Every fueling detected from a fuel% jump ≥30 gal. Classification:
--   recommended      — at the pending stop_event's assigned site
--   contracted_other — at a contracted Pilot/FJ stop not assigned
--   off_network      — nowhere near any contracted stop
CREATE TABLE IF NOT EXISTS fuel_events (
    id BIGSERIAL PRIMARY KEY,
    samsara_vehicle_id TEXT NOT NULL,
    truck_unit TEXT,
    load_id TEXT,
    stop_event_id BIGINT,
    site_id INTEGER,
    station_name TEXT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    fuel_pct_start NUMERIC(5,2) NOT NULL,
    fuel_pct_end NUMERIC(5,2) NOT NULL,
    gallons NUMERIC(7,1) NOT NULL,
    classification TEXT NOT NULL
        CHECK (classification IN ('recommended', 'contracted_other', 'off_network')),
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finalized_at TIMESTAMPTZ,
    alert_sent_at TIMESTAMPTZ
);

-- Self-heal for existing fuel_events tables with different shapes.
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'fuel_events' AND column_name = 'id'
  ) THEN
    ALTER TABLE fuel_events ADD COLUMN id BIGSERIAL PRIMARY KEY;
  END IF;
END $$;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS samsara_vehicle_id TEXT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS truck_unit TEXT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS load_id TEXT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS stop_event_id BIGINT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS site_id INTEGER;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS station_name TEXT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS fuel_pct_start NUMERIC(5,2);
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS fuel_pct_end NUMERIC(5,2);
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS gallons NUMERIC(7,1);
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS classification TEXT;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ;
ALTER TABLE fuel_events ADD COLUMN IF NOT EXISTS alert_sent_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fuel_events_vehicle_time
    ON fuel_events(samsara_vehicle_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_fuel_events_advice_detected
    ON fuel_events(stop_event_id, detected_at) WHERE stop_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fuel_events_open
    ON fuel_events(samsara_vehicle_id) WHERE finalized_at IS NULL;

-- No-valid-stop alerts log (admin notification when corridor has no candidates).
CREATE TABLE IF NOT EXISTS no_valid_stop_alerts (
    id BIGSERIAL PRIMARY KEY,
    truck_unit TEXT NOT NULL,
    load_id TEXT NOT NULL,
    current_fuel_gallons NUMERIC(8,2) NOT NULL,
    raised_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_no_valid_stop_alerts_raised_at ON no_valid_stop_alerts(raised_at DESC);
CREATE INDEX IF NOT EXISTS idx_no_valid_stop_alerts_truck ON no_valid_stop_alerts(truck_unit);

-- Additive supplier price storage. Existing advice and legacy prices are retained.
CREATE TABLE IF NOT EXISTS price_file_imports (
    id BIGSERIAL PRIMARY KEY,
    upload_key TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    file_sha256 TEXT,
    provider TEXT CHECK(provider IN ('pilot','loves','fts')),
    account_number TEXT,
    effective_date DATE,
    status TEXT NOT NULL CHECK(status IN ('processing','completed','held','failed')),
    date_source TEXT NOT NULL DEFAULT 'unknown' CHECK(date_source IN ('supplier','caption','upload_day','unknown')),
    reason TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    matched_rows INTEGER NOT NULL DEFAULT 0,
    excluded_rows INTEGER NOT NULL DEFAULT 0,
    row_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_price_file_imports_recent ON price_file_imports(uploaded_at DESC);
ALTER TABLE price_file_imports ADD COLUMN IF NOT EXISTS date_source TEXT NOT NULL DEFAULT 'unknown'
    CHECK(date_source IN ('supplier','caption','upload_day','unknown'));
CREATE TABLE IF NOT EXISTS price_feed_quotes (
    id BIGSERIAL PRIMARY KEY,
    import_id BIGINT NOT NULL REFERENCES price_file_imports(id),
    provider TEXT NOT NULL CHECK(provider IN ('pilot','loves','fts')),
    account_number TEXT NOT NULL,
    station_key TEXT NOT NULL,
    fuel_stop_id BIGINT REFERENCES fuel_stops(id),
    station_name TEXT,
    address TEXT,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    your_price NUMERIC(8,4) NOT NULL CHECK(your_price BETWEEN 2 AND 8),
    retail_price NUMERIC(8,4),
    effective_date DATE,
    UNIQUE(import_id,station_key)
);
CREATE INDEX IF NOT EXISTS idx_price_feed_quotes_station ON price_feed_quotes
    (provider,account_number,fuel_stop_id,effective_date DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_price_feed_quotes_import ON price_feed_quotes(import_id);
ALTER TABLE price_file_imports ENABLE ROW LEVEL SECURITY;
ALTER TABLE price_feed_quotes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON price_file_imports,price_feed_quotes FROM PUBLIC;
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
        REVOKE ALL ON price_file_imports,price_feed_quotes FROM anon;
    END IF;
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
        REVOKE ALL ON price_file_imports,price_feed_quotes FROM authenticated;
    END IF;
END $$;
