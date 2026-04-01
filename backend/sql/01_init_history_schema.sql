CREATE TABLE IF NOT EXISTS feed_snapshots (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feed_header_timestamp TIMESTAMPTZ,
    raw_pb BYTEA,
    raw_pb_size INTEGER,
    trip_updates_count INTEGER NOT NULL DEFAULT 0,
    stop_time_updates_count INTEGER NOT NULL DEFAULT 0,
    vehicle_positions_count INTEGER NOT NULL DEFAULT 0,
    alerts_count INTEGER NOT NULL DEFAULT 0,
    dqi NUMERIC,
    penalty INTEGER,
    issues JSONB
);

CREATE TABLE IF NOT EXISTS trip_updates_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES feed_snapshots(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    entity_id TEXT,
    trip_id TEXT,
    vehicle_id TEXT,
    vehicle_label TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    start_date TEXT,
    start_time TEXT,
    direction_id INTEGER,
    trip_headsign TEXT,
    shape_id TEXT,
    schedule_relationship INTEGER,
    trip_update_timestamp TIMESTAMPTZ,
    feed_header_timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS stop_time_updates_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES feed_snapshots(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    entity_id TEXT,
    trip_id TEXT,
    vehicle_id TEXT,
    vehicle_label TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    direction_id INTEGER,
    trip_headsign TEXT,
    stop_sequence INTEGER,
    stop_id TEXT,
    stop_name TEXT,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    arrival_time TIMESTAMPTZ,
    arrival_delay_sec INTEGER,
    departure_time TIMESTAMPTZ,
    departure_delay_sec INTEGER,
    scheduled_arrival_time TEXT,
    scheduled_departure_time TEXT,
    schedule_relationship INTEGER,
    feed_header_timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS vehicle_positions_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES feed_snapshots(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    entity_id TEXT,
    vehicle_id TEXT,
    vehicle_label TEXT,
    trip_id TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    direction_id INTEGER,
    trip_headsign TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bearing DOUBLE PRECISION,
    speed_mps DOUBLE PRECISION,
    current_status INTEGER,
    stop_id TEXT,
    stop_name TEXT,
    vehicle_timestamp TIMESTAMPTZ,
    feed_header_timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS alerts_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES feed_snapshots(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    entity_id TEXT,
    header TEXT,
    description TEXT,
    cause INTEGER,
    effect INTEGER,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    feed_header_timestamp TIMESTAMPTZ
);

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
