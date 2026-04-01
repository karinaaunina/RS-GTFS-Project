-- Idempotent upgrade script for an existing database.
-- Use this only if you already have tables from an earlier project version
-- and want to align them with the current unified backend schema.

ALTER TABLE IF EXISTS feed_snapshots
    ADD COLUMN IF NOT EXISTS feed_header_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS raw_pb BYTEA,
    ADD COLUMN IF NOT EXISTS raw_pb_size INTEGER,
    ADD COLUMN IF NOT EXISTS trip_updates_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stop_time_updates_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS vehicle_positions_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS alerts_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dqi NUMERIC,
    ADD COLUMN IF NOT EXISTS penalty INTEGER,
    ADD COLUMN IF NOT EXISTS issues JSONB;

ALTER TABLE IF EXISTS trip_updates_history
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS route_short_name TEXT,
    ADD COLUMN IF NOT EXISTS route_long_name TEXT,
    ADD COLUMN IF NOT EXISTS start_date TEXT,
    ADD COLUMN IF NOT EXISTS start_time TEXT,
    ADD COLUMN IF NOT EXISTS direction_id INTEGER,
    ADD COLUMN IF NOT EXISTS trip_headsign TEXT,
    ADD COLUMN IF NOT EXISTS shape_id TEXT,
    ADD COLUMN IF NOT EXISTS schedule_relationship INTEGER,
    ADD COLUMN IF NOT EXISTS vehicle_label TEXT,
    ADD COLUMN IF NOT EXISTS trip_update_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS feed_header_timestamp TIMESTAMPTZ;

ALTER TABLE IF EXISTS stop_time_updates_history
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_label TEXT,
    ADD COLUMN IF NOT EXISTS route_short_name TEXT,
    ADD COLUMN IF NOT EXISTS route_long_name TEXT,
    ADD COLUMN IF NOT EXISTS direction_id INTEGER,
    ADD COLUMN IF NOT EXISTS trip_headsign TEXT,
    ADD COLUMN IF NOT EXISTS stop_name TEXT,
    ADD COLUMN IF NOT EXISTS stop_lat DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS stop_lon DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS arrival_delay_sec INTEGER,
    ADD COLUMN IF NOT EXISTS departure_delay_sec INTEGER,
    ADD COLUMN IF NOT EXISTS scheduled_arrival_time TEXT,
    ADD COLUMN IF NOT EXISTS scheduled_departure_time TEXT,
    ADD COLUMN IF NOT EXISTS schedule_relationship INTEGER,
    ADD COLUMN IF NOT EXISTS feed_header_timestamp TIMESTAMPTZ;

ALTER TABLE IF EXISTS vehicle_positions_history
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_label TEXT,
    ADD COLUMN IF NOT EXISTS route_short_name TEXT,
    ADD COLUMN IF NOT EXISTS route_long_name TEXT,
    ADD COLUMN IF NOT EXISTS direction_id INTEGER,
    ADD COLUMN IF NOT EXISTS trip_headsign TEXT,
    ADD COLUMN IF NOT EXISTS current_status INTEGER,
    ADD COLUMN IF NOT EXISTS stop_name TEXT,
    ADD COLUMN IF NOT EXISTS feed_header_timestamp TIMESTAMPTZ;

ALTER TABLE IF EXISTS alerts_history
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS feed_header_timestamp TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_feed_snapshots_source_received_at ON feed_snapshots (source, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_feed_snapshots_source_header_ts ON feed_snapshots (source, feed_header_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_trip_updates_source_recorded_at ON trip_updates_history (source, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_trip_updates_source_trip_id ON trip_updates_history (source, trip_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_trip_updates_source_route_id ON trip_updates_history (source, route_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_trip_updates_source_vehicle_id ON trip_updates_history (source, vehicle_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_stop_updates_source_recorded_at ON stop_time_updates_history (source, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_stop_updates_source_stop_id ON stop_time_updates_history (source, stop_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_stop_updates_source_trip_id ON stop_time_updates_history (source, trip_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_stop_updates_source_vehicle_id ON stop_time_updates_history (source, vehicle_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_stop_updates_source_route_id ON stop_time_updates_history (source, route_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_vehicle_history_source_recorded_at ON vehicle_positions_history (source, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_history_source_vehicle_id ON vehicle_positions_history (source, vehicle_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_history_source_trip_id ON vehicle_positions_history (source, trip_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_history_source_route_id ON vehicle_positions_history (source, route_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_history_source_stop_id ON vehicle_positions_history (source, stop_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_history_source_recorded_at ON alerts_history (source, recorded_at DESC);
