//! SQL schema definitions for Delta Lake metadata storage.
//!
//! Provides database-specific DDL for creating the relational schema that stores
//! Delta Lake transaction log metadata across Postgres, SQLite, and DuckDB.

use std::fmt::Write;

/// Database engine types supported by the SQL backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEngine {
    /// PostgreSQL database
    Postgres,
    /// SQLite database  
    Sqlite,
    /// DuckDB database
    DuckDB,
}

impl DatabaseEngine {
    /// Get the database engine name as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseEngine::Postgres => "postgres",
            DatabaseEngine::Sqlite => "sqlite",
            DatabaseEngine::DuckDB => "duckdb",
        }
    }
}

/// SQL schema generator for Delta Lake metadata.
pub struct SchemaGenerator {
    engine: DatabaseEngine,
}

impl SchemaGenerator {
    /// Create a new schema generator for the specified database engine.
    pub fn new(engine: DatabaseEngine) -> Self {
        Self { engine }
    }

    /// Generate the complete DDL for all Delta Lake tables.
    pub fn generate_ddl(&self) -> String {
        let mut ddl = String::new();
        
        // Add table creation statements in dependency order
        writeln!(ddl, "-- Delta Lake SQL Schema for {}", self.engine.as_str()).unwrap();
        writeln!(ddl, "-- Generated automatically").unwrap();
        writeln!(ddl).unwrap();

        writeln!(ddl, "{}", self.create_dl_tables_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_table_versions_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_add_files_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_remove_files_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_metadata_updates_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_protocol_updates_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirror_status_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_multi_table_transactions_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirroring_alerts_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirroring_summaries_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirroring_config_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirroring_escalations_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_dl_mirroring_retry_schedule_ddl()).unwrap();
        writeln!(ddl).unwrap();
        writeln!(ddl, "{}", self.create_indexes_ddl()).unwrap();

        ddl
    }

    /// Create DDL for the dl_tables table.
    fn create_dl_tables_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    location TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB,
    UNIQUE(name, location)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_tables (
    table_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6)))),
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    metadata TEXT,
    UNIQUE(name, location)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_tables (
    table_id UUID PRIMARY KEY DEFAULT uuid(),
    name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    metadata JSON,
    UNIQUE(name, location)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_table_versions table.
    fn create_dl_table_versions_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_table_versions (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    committed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    committer VARCHAR(255),
    operation VARCHAR(100),
    operation_params JSONB,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_table_versions (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    committed_at TEXT NOT NULL,
    committer TEXT,
    operation TEXT,
    operation_params TEXT,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_table_versions (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    committed_at TIMESTAMP NOT NULL,
    committer VARCHAR,
    operation VARCHAR,
    operation_params JSON,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_add_files table.
    fn create_dl_add_files_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_add_files (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    path TEXT NOT NULL,
    size BIGINT NOT NULL,
    modification_time BIGINT NOT NULL,
    data_change BOOLEAN NOT NULL DEFAULT true,
    partition_values JSONB NOT NULL DEFAULT '{}',
    stats TEXT,
    tags JSONB,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_add_files (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    modification_time INTEGER NOT NULL,
    data_change INTEGER NOT NULL DEFAULT 1,
    partition_values TEXT NOT NULL DEFAULT '{}',
    stats TEXT,
    tags TEXT,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_add_files (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    size BIGINT NOT NULL,
    modification_time BIGINT NOT NULL,
    data_change BOOLEAN NOT NULL DEFAULT true,
    partition_values JSON NOT NULL DEFAULT '{}',
    stats VARCHAR,
    tags JSON,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_remove_files table.
    fn create_dl_remove_files_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_remove_files (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    path TEXT NOT NULL,
    deletion_timestamp BIGINT,
    data_change BOOLEAN NOT NULL DEFAULT true,
    extended_file_metadata BOOLEAN,
    partition_values JSONB,
    size BIGINT,
    tags JSONB,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_remove_files (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    path TEXT NOT NULL,
    deletion_timestamp INTEGER,
    data_change INTEGER NOT NULL DEFAULT 1,
    extended_file_metadata INTEGER,
    partition_values TEXT,
    size INTEGER,
    tags TEXT,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_remove_files (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    deletion_timestamp BIGINT,
    data_change BOOLEAN NOT NULL DEFAULT true,
    extended_file_metadata BOOLEAN,
    partition_values JSON,
    size BIGINT,
    tags JSON,
    PRIMARY KEY (table_id, version, path)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_metadata_updates table.
    fn create_dl_metadata_updates_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_metadata_updates (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    id TEXT NOT NULL,
    format JSONB NOT NULL,
    schema_string TEXT NOT NULL,
    partition_columns TEXT[] NOT NULL DEFAULT '{}',
    configuration JSONB NOT NULL DEFAULT '{}',
    created_time BIGINT,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_metadata_updates (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    id TEXT NOT NULL,
    format TEXT NOT NULL,
    schema_string TEXT NOT NULL,
    partition_columns TEXT NOT NULL DEFAULT '[]',
    configuration TEXT NOT NULL DEFAULT '{}',
    created_time INTEGER,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_metadata_updates (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    id VARCHAR NOT NULL,
    format JSON NOT NULL,
    schema_string VARCHAR NOT NULL,
    partition_columns VARCHAR[] NOT NULL DEFAULT [],
    configuration JSON NOT NULL DEFAULT '{}',
    created_time BIGINT,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_protocol_updates table.
    fn create_dl_protocol_updates_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_protocol_updates (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    min_reader_version INTEGER NOT NULL,
    min_writer_version INTEGER NOT NULL,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_protocol_updates (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    min_reader_version INTEGER NOT NULL,
    min_writer_version INTEGER NOT NULL,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_protocol_updates (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    min_reader_version INTEGER NOT NULL,
    min_writer_version INTEGER NOT NULL,
    PRIMARY KEY (table_id, version)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for the dl_mirror_status table.
    fn create_dl_mirror_status_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirror_status (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    artifact_type VARCHAR(20) NOT NULL, -- 'json' or 'checkpoint'
    artifact_path TEXT NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'pending', 'completed', 'failed'
    error_message TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, version, artifact_type)
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirror_status (
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    artifact_type TEXT NOT NULL, -- 'json' or 'checkpoint'
    artifact_path TEXT NOT NULL,
    status TEXT NOT NULL, -- 'pending', 'completed', 'failed'
    error_message TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (table_id, version, artifact_type)
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirror_status (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    artifact_type VARCHAR NOT NULL, -- 'json' or 'checkpoint'
    artifact_path VARCHAR NOT NULL,
    status VARCHAR NOT NULL, -- 'pending', 'completed', 'failed'
    error_message VARCHAR,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, version, artifact_type)
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_multi_table_transactions table.
    fn create_dl_multi_table_transactions_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_multi_table_transactions (
    transaction_id UUID PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    table_count INTEGER NOT NULL,
    total_action_count INTEGER NOT NULL,
    state VARCHAR(20) NOT NULL, -- 'ACTIVE', 'COMMITTED', 'FAILED', 'ROLLED_BACK'
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_multi_table_transactions (
    transaction_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    table_count INTEGER NOT NULL,
    total_action_count INTEGER NOT NULL,
    state TEXT NOT NULL, -- 'ACTIVE', 'COMMITTED', 'FAILED', 'ROLLED_BACK'
    error_message TEXT,
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_multi_table_transactions (
    transaction_id UUID PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    table_count INTEGER NOT NULL,
    total_action_count INTEGER NOT NULL,
    state VARCHAR NOT NULL, -- 'ACTIVE', 'COMMITTED', 'FAILED', 'ROLLED_BACK'
    error_message VARCHAR,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_mirroring_alerts table.
    fn create_dl_mirroring_alerts_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    affected_tables JSONB,
    resolved BOOLEAN NOT NULL DEFAULT false,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_alerts (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    affected_tables TEXT,
    resolved INTEGER NOT NULL DEFAULT 0,
    resolved_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_alerts (
    id UUID PRIMARY KEY,
    alert_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    message VARCHAR NOT NULL,
    affected_tables VARCHAR,
    resolved BOOLEAN NOT NULL DEFAULT false,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_mirroring_summaries table.
    fn create_dl_mirroring_summaries_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    total_tables INTEGER NOT NULL,
    successful_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    avg_duration_ms BIGINT NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_summaries (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    total_tables INTEGER NOT NULL,
    successful_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    total_duration_ms INTEGER NOT NULL,
    avg_duration_ms INTEGER NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_summaries (
    id UUID PRIMARY KEY,
    total_tables INTEGER NOT NULL,
    successful_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    avg_duration_ms BIGINT NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_mirroring_config table.
    fn create_dl_mirroring_config_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_config (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    paused_at TIMESTAMP WITH TIME ZONE,
    resumed_at TIMESTAMP WITH TIME ZONE,
    config JSONB,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_config (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    status TEXT NOT NULL DEFAULT 'active',
    paused_at TEXT,
    resumed_at TEXT,
    config TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_config (
    id UUID PRIMARY KEY,
    status VARCHAR NOT NULL DEFAULT 'active',
    paused_at TIMESTAMP,
    resumed_at TIMESTAMP,
    config VARCHAR,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_mirroring_escalations table.
    fn create_dl_mirroring_escalations_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_escalations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    escalation_type VARCHAR(50) NOT NULL,
    root_cause TEXT,
    failure_summary JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    assigned_to TEXT,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_escalations (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    escalation_type TEXT NOT NULL,
    root_cause TEXT,
    failure_summary TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    assigned_to TEXT,
    resolved_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_escalations (
    id UUID PRIMARY KEY,
    escalation_type VARCHAR NOT NULL,
    root_cause VARCHAR,
    failure_summary VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'pending',
    assigned_to VARCHAR,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for dl_mirroring_retry_schedule table.
    fn create_dl_mirroring_retry_schedule_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_retry_schedule (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_retry_schedule (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    table_id TEXT NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    scheduled_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'scheduled',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
CREATE TABLE IF NOT EXISTS dl_mirroring_retry_schedule (
    id UUID PRIMARY KEY,
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    status VARCHAR NOT NULL DEFAULT 'scheduled',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#
                .to_string()
            }
        }
    }

    /// Create DDL for database-specific indexes.
    fn create_indexes_ddl(&self) -> String {
        match self.engine {
            DatabaseEngine::Postgres => {
                r#"
-- Performance indexes for Postgres
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_table_id_version ON dl_table_versions(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_committed_at ON dl_table_versions(committed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_table_id_version ON dl_add_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_table_id_version ON dl_remove_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_path ON dl_add_files(path);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_path ON dl_remove_files(path);
"#
                .to_string()
            }
            DatabaseEngine::Sqlite => {
                r#"
-- Performance indexes for SQLite
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_table_id_version ON dl_table_versions(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_committed_at ON dl_table_versions(committed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_table_id_version ON dl_add_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_table_id_version ON dl_remove_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_path ON dl_add_files(path);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_path ON dl_remove_files(path);
"#
                .to_string()
            }
            DatabaseEngine::DuckDB => {
                r#"
-- Performance indexes for DuckDB
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_table_id_version ON dl_table_versions(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_table_versions_committed_at ON dl_table_versions(committed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_table_id_version ON dl_add_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_table_id_version ON dl_remove_files(table_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dl_add_files_path ON dl_add_files(path);
CREATE INDEX IF NOT EXISTS idx_dl_remove_files_path ON dl_remove_files(path);
CREATE INDEX IF NOT EXISTS idx_dl_mirror_status_table_version ON dl_mirror_status(table_id, version);
CREATE INDEX IF NOT EXISTS idx_dl_mirror_status_status ON dl_mirror_status(status);
CREATE INDEX IF NOT EXISTS idx_dl_mirror_status_created_at ON dl_mirror_status(created_at);
CREATE INDEX IF NOT EXISTS idx_dl_multi_table_transactions_started_at ON dl_multi_table_transactions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_dl_multi_table_transactions_state ON dl_multi_table_transactions(state);
"#
                .to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_schema_generation() {
        let generator = SchemaGenerator::new(DatabaseEngine::Postgres);
        let ddl = generator.generate_ddl();
        
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dl_tables"));
        assert!(ddl.contains("JSONB"));
        assert!(ddl.contains("TEXT[]"));
        assert!(ddl.contains("TIMESTAMP WITH TIME ZONE"));
        assert!(ddl.contains("gen_random_uuid()"));
    }

    #[test]
    fn test_sqlite_schema_generation() {
        let generator = SchemaGenerator::new(DatabaseEngine::Sqlite);
        let ddl = generator.generate_ddl();
        
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dl_tables"));
        assert!(ddl.contains("TEXT PRIMARY KEY"));
        assert!(ddl.contains("datetime('now')"));
        // SQLite should not have JSONB or array types
        assert!(!ddl.contains("JSONB"));
        assert!(!ddl.contains("TEXT[]"));
    }

    #[test]
    fn test_duckdb_schema_generation() {
        let generator = SchemaGenerator::new(DatabaseEngine::DuckDB);
        let ddl = generator.generate_ddl();
        
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dl_tables"));
        assert!(ddl.contains("JSON"));
        assert!(ddl.contains("uuid()"));
        assert!(ddl.contains("VARCHAR[]"));
    }

    #[test]
    fn test_database_engine_as_str() {
        assert_eq!(DatabaseEngine::Postgres.as_str(), "postgres");
        assert_eq!(DatabaseEngine::Sqlite.as_str(), "sqlite");
        assert_eq!(DatabaseEngine::DuckDB.as_str(), "duckdb");
    }
}