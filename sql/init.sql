CREATE TABLE IF NOT EXISTS raw_telemetry (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned_telemetry (
    id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    spacecraft_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    fuel_level DOUBLE PRECISION,
    battery_voltage DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    oxygen_level DOUBLE PRECISION,
    is_valid BOOLEAN NOT NULL,
    validation_notes TEXT[],
    anomaly_flags TEXT[]
);

CREATE TABLE IF NOT EXISTS telemetry_alerts (
    id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    spacecraft_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cleaned_telemetry_event_time
ON cleaned_telemetry(event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_event_time
ON telemetry_alerts(event_timestamp DESC);
