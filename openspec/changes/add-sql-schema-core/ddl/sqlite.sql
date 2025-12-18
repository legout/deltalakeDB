-- Minimal SQLite DDL for SQL-backed Delta metadata (PRD §7)
-- Notes: JSON stored as TEXT; booleans as INTEGER 0/1; timestamps as INTEGER (epoch ms) or TEXT

CREATE TABLE IF NOT EXISTS dl_tables (
  table_id TEXT PRIMARY KEY,
  name TEXT,
  location TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  protocol_min_reader INTEGER NOT NULL,
  protocol_min_writer INTEGER NOT NULL,
  properties TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS dl_table_heads (
  table_id TEXT PRIMARY KEY,
  current_version INTEGER NOT NULL CHECK (current_version >= 0),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dl_table_versions (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  committed_at TEXT NOT NULL DEFAULT (datetime('now')),
  committer TEXT,
  operation TEXT,
  operation_params TEXT,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_add_files (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  path TEXT NOT NULL,
  size_bytes INTEGER,
  partition_values TEXT,
  stats TEXT,
  data_change INTEGER DEFAULT 1,
  modification_time INTEGER,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_remove_files (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  path TEXT NOT NULL,
  deletion_timestamp INTEGER,
  data_change INTEGER DEFAULT 1,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_metadata_updates (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  schema_json TEXT NOT NULL,
  partition_columns TEXT, -- JSON array encoded as TEXT
  table_properties TEXT,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_protocol_updates (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  min_reader_version INTEGER NOT NULL,
  min_writer_version INTEGER NOT NULL,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_txn_actions (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  app_id TEXT NOT NULL,
  last_update INTEGER NOT NULL,
  PRIMARY KEY (table_id, version, app_id)
);

CREATE TABLE IF NOT EXISTS dl_mirror_status (
  table_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','FAILED','SUCCEEDED')) DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  digest TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (table_id, version)
);

-- Recommended indexes
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_tid_ver
  ON dl_table_versions (table_id, version);

CREATE INDEX IF NOT EXISTS idx_dl_add_files_tid_path
  ON dl_add_files (table_id, path);

CREATE INDEX IF NOT EXISTS idx_dl_remove_files_tid_path
  ON dl_remove_files (table_id, path);

CREATE INDEX IF NOT EXISTS idx_dl_mirror_status_status
  ON dl_mirror_status (status, updated_at);

