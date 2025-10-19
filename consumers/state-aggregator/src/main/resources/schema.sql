CREATE TABLE IF NOT EXISTS device_state (
  device_id VARCHAR(64) PRIMARY KEY,
  device_type VARCHAR(32) NOT NULL,
  region VARCHAR(32),
  cpu_percent DOUBLE PRECISION,
  memory_percent DOUBLE PRECISION,
  battery_health DOUBLE PRECISION,
  network_latency_ms DOUBLE PRECISION,
  os_compliant BOOLEAN,
  last_seen TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_device_state_region ON device_state(region);
CREATE INDEX IF NOT EXISTS idx_device_state_last_seen ON device_state(last_seen);
