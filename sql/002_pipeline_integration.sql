ALTER TABLE sensor_metrics_5m ADD COLUMN IF NOT EXISTS batch_id TEXT;
ALTER TABLE machine_risk_latest ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ;
