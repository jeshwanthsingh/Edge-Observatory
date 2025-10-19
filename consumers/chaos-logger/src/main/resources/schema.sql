CREATE TABLE IF NOT EXISTS chaos_incidents (
  id BIGSERIAL PRIMARY KEY,
  scenario VARCHAR(64) NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  recovered_at TIMESTAMPTZ,
  recovery_duration_ms BIGINT,
  affected_component VARCHAR(128),
  details JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chaos_incidents_scenario ON chaos_incidents(scenario);
CREATE INDEX IF NOT EXISTS idx_chaos_incidents_started_at ON chaos_incidents(started_at DESC);
