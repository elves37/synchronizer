CREATE TABLE IF NOT EXISTS target_user (
    account_key        VARCHAR(200) PRIMARY KEY,
    login_id           VARCHAR(200),
    name               VARCHAR(200),
    email              VARCHAR(300),
    phone              VARCHAR(100),
    address_text       TEXT,
    organization_name  VARCHAR(300),
    extra_payload      JSONB,
    status             VARCHAR(50),
    source_updated_at  TIMESTAMP,
    sync_mode          VARCHAR(50) NOT NULL DEFAULT 'SYNC',
    last_synced_at     TIMESTAMP,
    manual_updated_at  TIMESTAMP,
    manual_updated_by  VARCHAR(100),
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE target_user ADD COLUMN IF NOT EXISTS address_text TEXT;
ALTER TABLE target_user ADD COLUMN IF NOT EXISTS organization_name VARCHAR(300);
ALTER TABLE target_user ADD COLUMN IF NOT EXISTS extra_payload JSONB;

CREATE TABLE IF NOT EXISTS sync_source_db (
    db_key              VARCHAR(100) PRIMARY KEY,
    name                VARCHAR(200) NOT NULL,
    jdbc_url            TEXT NOT NULL,
    username            VARCHAR(200) NOT NULL,
    password_secret_key VARCHAR(500) NOT NULL,
    driver_class_name   VARCHAR(300) NOT NULL DEFAULT 'org.postgresql.Driver',
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sync_source_config (
    id                   BIGSERIAL PRIMARY KEY,
    source_db_key         VARCHAR(100) NOT NULL REFERENCES sync_source_db(db_key),
    name                 VARCHAR(200) NOT NULL,
    incremental_sql      TEXT NOT NULL,
    fetch_one_sql        TEXT,
    key_alias            VARCHAR(100) NOT NULL DEFAULT 'account_key',
    updated_at_alias     VARCHAR(100) NOT NULL DEFAULT 'sync_updated_at',
    upsert_sql           TEXT NOT NULL,
    chunk_size           INTEGER NOT NULL DEFAULT 500,
    fetch_size           INTEGER NOT NULL DEFAULT 500,
    safety_lag_seconds   INTEGER NOT NULL DEFAULT 60,
    overlap_seconds      INTEGER NOT NULL DEFAULT 300,
    enabled                    BOOLEAN NOT NULL DEFAULT TRUE,
    schedule_enabled           BOOLEAN NOT NULL DEFAULT FALSE,
    schedule_interval_minutes  INTEGER NOT NULL DEFAULT 5 CHECK (schedule_interval_minutes BETWEEN 1 AND 10080),
    next_run_at                TIMESTAMP,
    last_scheduled_at          TIMESTAMP,
    created_at                 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

-- Upgrade support for databases created by v6 or earlier.
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

CREATE TABLE IF NOT EXISTS sync_cursor (
    source_id         BIGINT PRIMARY KEY REFERENCES sync_source_config(id) ON DELETE CASCADE,
    last_updated_at   TIMESTAMP NOT NULL DEFAULT TIMESTAMP '1970-01-01 00:00:00',
    last_account_key  VARCHAR(200) NOT NULL DEFAULT '',
    last_cutoff_at    TIMESTAMP,
    last_run_id       BIGINT,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sync_excluded_account (
    id               BIGSERIAL PRIMARY KEY,
    account_key      VARCHAR(200) NOT NULL,
    source_id        BIGINT NULL REFERENCES sync_source_config(id) ON DELETE CASCADE,
    reason           TEXT,
    manual_editable  BOOLEAN NOT NULL DEFAULT TRUE,
    created_by       VARCHAR(100),
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by       VARCHAR(100),
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sync_excluded_global
ON sync_excluded_account (account_key)
WHERE source_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_sync_excluded_source
ON sync_excluded_account (account_key, source_id)
WHERE source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sync_excluded_source
ON sync_excluded_account (source_id, account_key);

CREATE TABLE IF NOT EXISTS sync_skipped_snapshot (
    source_id        BIGINT NOT NULL REFERENCES sync_source_config(id) ON DELETE CASCADE,
    account_key      VARCHAR(200) NOT NULL,
    source_payload   JSONB NOT NULL,
    row_hash         VARCHAR(128) NOT NULL,
    sync_updated_at  TIMESTAMP NOT NULL,
    last_seen_run_id BIGINT NOT NULL,
    last_seen_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, account_key)
);

CREATE TABLE IF NOT EXISTS sync_account_state (
    source_id          BIGINT NOT NULL REFERENCES sync_source_config(id) ON DELETE CASCADE,
    account_key        VARCHAR(200) NOT NULL,
    row_hash           VARCHAR(128),
    status             VARCHAR(50) NOT NULL,
    last_seen_run_id   BIGINT,
    last_seen_at       TIMESTAMP,
    last_synced_at     TIMESTAMP,
    error_message      TEXT,
    PRIMARY KEY (source_id, account_key)
);

CREATE TABLE IF NOT EXISTS sync_run (
    id                       BIGSERIAL PRIMARY KEY,
    source_id                BIGINT NOT NULL REFERENCES sync_source_config(id) ON DELETE CASCADE,
    trigger_type             VARCHAR(50) NOT NULL DEFAULT 'MANUAL',
    status                   VARCHAR(50) NOT NULL,
    started_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at              TIMESTAMP,
    total_count              INTEGER NOT NULL DEFAULT 0,
    upserted_count           INTEGER NOT NULL DEFAULT 0,
    unchanged_count          INTEGER NOT NULL DEFAULT 0,
    skipped_excluded_count   INTEGER NOT NULL DEFAULT 0,
    failed_count             INTEGER NOT NULL DEFAULT 0,
    error_message            TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_run_source_started
ON sync_run (source_id, started_at DESC);

CREATE TABLE IF NOT EXISTS sync_manual_change_log (
    id              BIGSERIAL PRIMARY KEY,
    account_key     VARCHAR(200) NOT NULL,
    changed_by      VARCHAR(100),
    before_payload  JSONB,
    after_payload   JSONB,
    reason          TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sync_job_lock (
    lock_key     VARCHAR(200) PRIMARY KEY,
    locked_until TIMESTAMP NOT NULL,
    locked_by    VARCHAR(200) NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
