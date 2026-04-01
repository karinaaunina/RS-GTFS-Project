ALTER TABLE IF EXISTS stop_time_updates_history
    ADD COLUMN IF NOT EXISTS vehicle_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_label TEXT;

CREATE INDEX IF NOT EXISTS idx_stop_updates_source_vehicle_id
    ON stop_time_updates_history (source, vehicle_id, recorded_at DESC);
