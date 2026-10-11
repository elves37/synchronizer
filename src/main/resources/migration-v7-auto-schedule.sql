-- Optional manual migration for databases created by v6 or earlier.
-- Execute this file once when spring.sql.init.mode=never.
ALTER TABLE sync_source_config
    ADD COLUMN IF NOT EXISTS schedule_interval_minutes INTEGER NOT NULL DEFAULT 5;
ALTER TABLE sync_source_config
    ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP;
ALTER TABLE sync_source_config
    ADD COLUMN IF NOT EXISTS last_scheduled_at TIMESTAMP;

UPDATE sync_source_config
SET next_run_at = CURRENT_TIMESTAMP
    + (schedule_interval_minutes * INTERVAL '1 minute')
WHERE enabled = TRUE
  AND schedule_enabled = TRUE
  AND next_run_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sync_source_schedule_due
ON sync_source_config (next_run_at, id)
WHERE enabled = TRUE AND schedule_enabled = TRUE;
