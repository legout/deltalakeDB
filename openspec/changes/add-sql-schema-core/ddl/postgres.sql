-- Minimal Postgres DDL for SQL-backed Delta metadata (PRD §7)
-- Scope: core tables, head tracking, mirror status, and recommended indexes

-- Optional: enum for mirror status
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dl_mirror_state') THEN
    CREATE TYPE dl_mirror_state AS ENUM ('PENDING','FAILED','SUCCEEDED');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS dl_tables (
  table_id UUID PRIMARY KEY,
  name TEXT,
  location TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  protocol_min_reader INT NOT NULL,
  protocol_min_writer INT NOT NULL,
  properties JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Current head for optimistic concurrency and fast lookup
CREATE TABLE IF NOT EXISTS dl_table_heads (
  table_id UUID PRIMARY KEY REFERENCES dl_tables(table_id) ON DELETE CASCADE,
  current_version BIGINT NOT NULL CHECK (current_version >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dl_table_versions (
  table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
  version BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  committer TEXT,
  operation TEXT,
  operation_params JSONB,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_add_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  size_bytes BIGINT,
  partition_values JSONB,
  stats JSONB,
  data_change BOOLEAN DEFAULT true,
  modification_time BIGINT,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_remove_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  deletion_timestamp BIGINT,
  data_change BOOLEAN DEFAULT true,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_metadata_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  schema_json JSONB NOT NULL,
  partition_columns TEXT[],
  table_properties JSONB,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_protocol_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  min_reader_version INT NOT NULL,
  min_writer_version INT NOT NULL,
  PRIMARY KEY (table_id, version)
);

-- Optional app-id progress markers
CREATE TABLE IF NOT EXISTS dl_txn_actions (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  app_id TEXT NOT NULL,
  last_update BIGINT NOT NULL,
  PRIMARY KEY (table_id, version, app_id)
);

-- Mirror status & reconciliation queue
CREATE TABLE IF NOT EXISTS dl_mirror_status (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  status dl_mirror_state NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  digest TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (table_id, version)
);

-- Recommended indexes
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_tid_ver
  ON dl_table_versions (table_id, version DESC);

CREATE INDEX IF NOT EXISTS idx_dl_add_files_tid_path
  ON dl_add_files (table_id, path);

CREATE INDEX IF NOT EXISTS idx_dl_remove_files_tid_path
  ON dl_remove_files (table_id, path);

-- Optional JSONB GIN indexes (predicate planning)
CREATE INDEX IF NOT EXISTS idx_dl_add_files_stats_gin
  ON dl_add_files USING GIN (stats);

CREATE INDEX IF NOT EXISTS idx_dl_add_files_partvals_gin
  ON dl_add_files USING GIN (partition_values);

CREATE INDEX IF NOT EXISTS idx_dl_mirror_status_status
  ON dl_mirror_status (status, updated_at);

