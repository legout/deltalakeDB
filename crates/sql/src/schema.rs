//! Database schema definitions for Delta Lake metadata storage

use serde_json::Value;
use std::collections::HashMap;

/// Database schema version
pub const SCHEMA_VERSION: i64 = 1;

/// SQL schema definitions for different database types
pub struct SchemaManager;

impl SchemaManager {
    /// Get table creation statements for a specific database type
    pub fn get_schema_sql(database_type: &str) -> Vec<String> {
        match database_type {
            "postgresql" => Self::postgres_schema(),
            "sqlite" => Self::sqlite_schema(),
            "duckdb" => Self::duckdb_schema(),
            _ => panic!("Unsupported database type: {}", database_type),
        }
    }

    /// Get PostgreSQL-specific schema
    fn postgres_schema() -> Vec<String> {
        vec![
            // Schema migration tracking
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (version > 0)
            )
            "#.trim().to_string(),

            // Delta tables
            r#"
            CREATE TABLE IF NOT EXISTS delta_tables (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_path VARCHAR(1000) NOT NULL UNIQUE,
                table_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata_version BIGINT NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT true,
                table_uuid UUID NOT NULL,
                CHECK (metadata_version >= 0)
            )
            "#.trim().to_string(),

            // Table protocols
            r#"
            CREATE TABLE IF NOT EXISTS delta_protocols (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(table_id),
                CHECK (min_reader_version >= 1),
                CHECK (min_writer_version >= 1)
            )
            "#.trim().to_string(),

            // Table metadata
            r#"
            CREATE TABLE IF NOT EXISTS delta_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                metadata_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                configuration JSONB NOT NULL DEFAULT '{}',
                partition_columns TEXT[] NOT NULL DEFAULT '{}',
                UNIQUE(table_id)
            )
            "#.trim().to_string(),

            // Commits
            r#"
            CREATE TABLE IF NOT EXISTS delta_commits (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                version BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                operation_type VARCHAR(50) NOT NULL,
                operation_parameters JSONB NOT NULL DEFAULT '{}',
                isolation_level VARCHAR(50) NOT NULL DEFAULT 'Serializable',
                is_blind_append BOOLEAN NOT NULL DEFAULT false,
                engine_info VARCHAR(255),
                transaction_id UUID,
                commit_info JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(table_id, version),
                CHECK (version >= 0)
            )
            "#.trim().to_string(),

            // Commit actions
            r#"
            CREATE TABLE IF NOT EXISTS delta_commit_actions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                commit_id UUID NOT NULL REFERENCES delta_commits(id) ON DELETE CASCADE,
                action_type VARCHAR(50) NOT NULL,
                action_data JSONB NOT NULL,
                file_path VARCHAR(1000),
                data_change BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#.trim().to_string(),

            // Files for fast metadata access
            r#"
            CREATE TABLE IF NOT EXISTS delta_files (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                file_path VARCHAR(1000) NOT NULL,
                file_size BIGINT NOT NULL,
                modification_time TIMESTAMPTZ NOT NULL,
                data_change BOOLEAN NOT NULL DEFAULT true,
                file_stats JSONB NOT NULL DEFAULT '{}',
                partition_values JSONB NOT NULL DEFAULT '{}',
                tags JSONB NOT NULL DEFAULT '{}',
                commit_version BIGINT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(table_id, file_path, commit_version),
                CHECK (file_size >= 0),
                CHECK (commit_version >= 0)
            )
            "#.trim().to_string(),
        ]
    }

    /// Get SQLite-specific schema
    fn sqlite_schema() -> Vec<String> {
        vec![
            // Schema migration tracking
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                applied_at TEXT NOT NULL DEFAULT (datetime('now')),
                CHECK (version > 0)
            )
            "#.trim().to_string(),

            // Delta tables
            r#"
            CREATE TABLE IF NOT EXISTS delta_tables (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                table_path TEXT NOT NULL UNIQUE,
                table_name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                metadata_version INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                table_uuid TEXT NOT NULL,
                CHECK (metadata_version >= 0),
                CHECK (is_active IN (0, 1))
            )
            "#.trim().to_string(),

            // Table protocols
            r#"
            CREATE TABLE IF NOT EXISTS delta_protocols (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                table_id TEXT NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(table_id),
                CHECK (min_reader_version >= 1),
                CHECK (min_writer_version >= 1)
            )
            "#.trim().to_string(),

            // Table metadata
            r#"
            CREATE TABLE IF NOT EXISTS delta_metadata (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                table_id TEXT NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                configuration TEXT NOT NULL DEFAULT '{}',
                partition_columns TEXT NOT NULL DEFAULT '[]',
                UNIQUE(table_id)
            )
            "#.trim().to_string(),

            // Commits
            r#"
            CREATE TABLE IF NOT EXISTS delta_commits (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                table_id TEXT NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                version INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                operation_parameters TEXT NOT NULL DEFAULT '{}',
                isolation_level TEXT NOT NULL DEFAULT 'Serializable',
                is_blind_append INTEGER NOT NULL DEFAULT 0,
                engine_info TEXT,
                transaction_id TEXT,
                commit_info TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(table_id, version),
                CHECK (version >= 0),
                CHECK (is_blind_append IN (0, 1))
            )
            "#.trim().to_string(),

            // Commit actions
            r#"
            CREATE TABLE IF NOT EXISTS delta_commit_actions (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                commit_id TEXT NOT NULL REFERENCES delta_commits(id) ON DELETE CASCADE,
                action_type TEXT NOT NULL,
                action_data TEXT NOT NULL,
                file_path TEXT,
                data_change INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                CHECK (data_change IN (0, 1))
            )
            "#.trim().to_string(),

            // Files for fast metadata access
            r#"
            CREATE TABLE IF NOT EXISTS delta_files (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                table_id TEXT NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                modification_time TEXT NOT NULL,
                data_change INTEGER NOT NULL DEFAULT 1,
                file_stats TEXT NOT NULL DEFAULT '{}',
                partition_values TEXT NOT NULL DEFAULT '{}',
                tags TEXT NOT NULL DEFAULT '{}',
                commit_version INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(table_id, file_path, commit_version),
                CHECK (file_size >= 0),
                CHECK (commit_version >= 0),
                CHECK (is_active IN (0, 1))
            )
            "#.trim().to_string(),
        ]
    }

    /// Get DuckDB-specific schema
    fn duckdb_schema() -> Vec<String> {
        vec![
            // Schema migration tracking
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version UBIGINT PRIMARY KEY,
                name VARCHAR NOT NULL,
                description VARCHAR,
                applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
                CHECK (version > 0)
            )
            "#.trim().to_string(),

            // Delta tables
            r#"
            CREATE TABLE IF NOT EXISTS delta_tables (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_path VARCHAR NOT NULL UNIQUE,
                table_name VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                metadata_version UBIGINT NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT true,
                table_uuid UUID NOT NULL,
                CHECK (metadata_version >= 0)
            )
            "#.trim().to_string(),

            // Table protocols
            r#"
            CREATE TABLE IF NOT EXISTS delta_protocols (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE(table_id),
                CHECK (min_reader_version >= 1),
                CHECK (min_writer_version >= 1)
            )
            "#.trim().to_string(),

            // Table metadata
            r#"
            CREATE TABLE IF NOT EXISTS delta_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                metadata_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                configuration JSON NOT NULL DEFAULT '{}',
                partition_columns VARCHAR[] NOT NULL DEFAULT '[]',
                UNIQUE(table_id)
            )
            "#.trim().to_string(),

            // Commits
            r#"
            CREATE TABLE IF NOT EXISTS delta_commits (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                version UBIGINT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                operation_type VARCHAR NOT NULL,
                operation_parameters JSON NOT NULL DEFAULT '{}',
                isolation_level VARCHAR NOT NULL DEFAULT 'Serializable',
                is_blind_append BOOLEAN NOT NULL DEFAULT false,
                engine_info VARCHAR,
                transaction_id UUID,
                commit_info JSON NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE(table_id, version),
                CHECK (version >= 0)
            )
            "#.trim().to_string(),

            // Commit actions
            r#"
            CREATE TABLE IF NOT EXISTS delta_commit_actions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                commit_id UUID NOT NULL REFERENCES delta_commits(id) ON DELETE CASCADE,
                action_type VARCHAR NOT NULL,
                action_data JSON NOT NULL,
                file_path VARCHAR,
                data_change BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            "#.trim().to_string(),

            // Files for fast metadata access
            r#"
            CREATE TABLE IF NOT EXISTS delta_files (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES delta_tables(id) ON DELETE CASCADE,
                file_path VARCHAR NOT NULL,
                file_size UBIGINT NOT NULL,
                modification_time TIMESTAMP NOT NULL,
                data_change BOOLEAN NOT NULL DEFAULT true,
                file_stats JSON NOT NULL DEFAULT '{}',
                partition_values JSON NOT NULL DEFAULT '{}',
                tags JSON NOT NULL DEFAULT '{}',
                commit_version UBIGINT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE(table_id, file_path, commit_version),
                CHECK (file_size >= 0),
                CHECK (commit_version >= 0)
            )
            "#.trim().to_string(),
        ]
    }

    /// Get index creation statements for a specific database type
    pub fn get_index_sql(database_type: &str) -> Vec<String> {
        match database_type {
            "postgresql" => Self::postgres_indexes(),
            "sqlite" => Self::sqlite_indexes(),
            "duckdb" => Self::duckdb_indexes(),
            _ => panic!("Unsupported database type: {}", database_type),
        }
    }

    /// PostgreSQL-specific indexes
    fn postgres_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_table_path ON delta_tables(table_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_created_at ON delta_tables(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_is_active ON delta_tables(is_active)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_id ON delta_commits(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_version ON delta_commits(table_id, version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_timestamp ON delta_commits(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_operation_type ON delta_commits(operation_type)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_commit_id ON delta_commit_actions(commit_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_action_type ON delta_commit_actions(action_type)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_file_path ON delta_commit_actions(file_path)",

            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_id ON delta_files(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_active ON delta_files(table_id, is_active)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_file_path ON delta_files(file_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_commit_version ON delta_files(commit_version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_data_change ON delta_files(data_change)",
        ].iter().map(|s| s.to_string()).collect()
    }

    /// SQLite-specific indexes
    fn sqlite_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_table_path ON delta_tables(table_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_created_at ON delta_tables(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_is_active ON delta_tables(is_active)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_id ON delta_commits(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_version ON delta_commits(table_id, version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_timestamp ON delta_commits(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_operation_type ON delta_commits(operation_type)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_commit_id ON delta_commit_actions(commit_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_action_type ON delta_commit_actions(action_type)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_file_path ON delta_commit_actions(file_path)",

            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_id ON delta_files(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_active ON delta_files(table_id, is_active)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_file_path ON delta_files(file_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_commit_version ON delta_files(commit_version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_data_change ON delta_files(data_change)",
        ].iter().map(|s| s.to_string()).collect()
    }

    /// DuckDB-specific indexes
    fn duckdb_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_table_path ON delta_tables(table_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_created_at ON delta_tables(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_delta_tables_is_active ON delta_tables(is_active)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_id ON delta_commits(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_table_version ON delta_commits(table_id, version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_timestamp ON delta_commits(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commits_operation_type ON delta_commits(operation_type)",

            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_commit_id ON delta_commit_actions(commit_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_action_type ON delta_commit_actions(action_type)",
            "CREATE INDEX IF NOT EXISTS idx_delta_commit_actions_file_path ON delta_commit_actions(file_path)",

            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_id ON delta_files(table_id)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_table_active ON delta_files(table_id, is_active)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_file_path ON delta_files(file_path)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_commit_version ON delta_files(commit_version)",
            "CREATE INDEX IF NOT EXISTS idx_delta_files_data_change ON delta_files(data_change)",
        ].iter().map(|s| s.to_string()).collect()
    }

    /// Get initial migration records
    pub fn get_initial_migrations() -> Vec<(i64, String, String)> {
        vec![
            (1, "initial_schema".to_string(), "Create initial database schema for Delta Lake metadata".to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_generation() {
        let postgres_schema = SchemaManager::get_schema_sql("postgresql");
        let sqlite_schema = SchemaManager::get_schema_sql("sqlite");
        let duckdb_schema = SchemaManager::get_schema_sql("duckdb");

        assert!(!postgres_schema.is_empty());
        assert!(!sqlite_schema.is_empty());
        assert!(!duckdb_schema.is_empty());

        // Check that all schemas have the same number of tables
        assert_eq!(postgres_schema.len(), sqlite_schema.len());
        assert_eq!(postgres_schema.len(), duckdb_schema.len());

        // Check that all schemas contain the tables table
        assert!(postgres_schema.iter().any(|s| s.contains("delta_tables")));
        assert!(sqlite_schema.iter().any(|s| s.contains("delta_tables")));
        assert!(duckdb_schema.iter().any(|s| s.contains("delta_tables")));
    }

    #[test]
    fn test_index_generation() {
        let postgres_indexes = SchemaManager::get_index_sql("postgresql");
        let sqlite_indexes = SchemaManager::get_index_sql("sqlite");
        let duckdb_indexes = SchemaManager::get_index_sql("duckdb");

        assert!(!postgres_indexes.is_empty());
        assert!(!sqlite_indexes.is_empty());
        assert!(!duckdb_indexes.is_empty());

        // Check that all indexes are created
        assert_eq!(postgres_indexes.len(), sqlite_indexes.len());
        assert_eq!(postgres_indexes.len(), duckdb_indexes.len());

        // Check that table path index exists
        assert!(postgres_indexes.iter().any(|s| s.contains("idx_delta_tables_table_path")));
        assert!(sqlite_indexes.iter().any(|s| s.contains("idx_delta_tables_table_path")));
        assert!(duckdb_indexes.iter().any(|s| s.contains("idx_delta_tables_table_path")));
    }

    #[test]
    fn test_initial_migrations() {
        let migrations = SchemaManager::get_initial_migrations();
        assert_eq!(migrations.len(), 1);

        let (version, name, description) = &migrations[0];
        assert_eq!(version, &1);
        assert_eq!(name, "initial_schema");
        assert!(description.contains("initial database schema"));
    }

    #[test]
    fn test_unsupported_database() {
        let result = std::panic::catch_unwind(|| {
            SchemaManager::get_schema_sql("unsupported");
        });
        assert!(result.is_err());
    }
}