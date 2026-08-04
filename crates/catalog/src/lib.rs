//! The SQL shape of the catalog, owned in one place.
//!
//! A [`Dialect`] is a supported [`Engine`] backend (Postgres, SQLite, DuckDB).
//! Each read-path query is a [`CatalogQuery`]: the SQL text for that dialect
//! plus the ordered list of [`CatalogParam`]s a caller must bind. The bind
//! order travels with the text, so a dialect's parameter cardinality is data,
//! not tribal knowledge hidden in an adapter — e.g. the active-files CTE binds
//! two parameters under Postgres (named `$1`/`$2`) but four under SQLite/DuckDB
//! (positional `?`, one pair per table reference).
//!
//! [`schema_ddl`] and [`schema_drop_ddl`] are the canonical catalog DDL,
//! relocated verbatim from the per-engine test fixtures that used to duplicate
//! them. [`table_names`] is the full table inventory; it is the single source
//! of truth when a caller needs to enumerate the catalog (e.g. to clear a
//! table's rows on re-import).
//!
//! This crate does no execution and has no sqlx or duckdb dependency. Adapters
//! keep execution and row mapping; they call here for text and bind order, then
//! build their own `TableSnapshot` / `Protocol` / `TableMetadata` from core.

/// A supported catalog backend. Named to match the domain term in `CONTEXT.md`.
///
/// [`Engine`]: CONTEXT.md#metadata-plane
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    Postgres,
    Sqlite,
    DuckDb,
}

/// A parameter a caller must bind, in order, when executing a [`CatalogQuery`].
///
/// The concrete value is supplied by the caller (a table id and a version);
/// this enum only describes *which* one goes in each bind slot and *how many*
/// slots there are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogParam {
    /// The `table_id` column value.
    TableId,
    /// The `version` column value.
    Version,
}

/// A read-path catalog query for one [`Dialect`]: its SQL text and the ordered
/// parameters a caller must bind against it.
#[derive(Debug, Clone, Copy)]
pub struct CatalogQuery {
    /// The SQL text, with placeholders in the dialect's own style
    /// (`$1`/`$2` for Postgres, positional `?` for SQLite and DuckDB).
    pub sql: &'static str,
    /// The parameters to bind, in positional order.
    pub params: &'static [CatalogParam],
}

/// Latest `TableMetadata` row at or before a version.
///
/// Postgres and SQLite share one query body and differ only in placeholder
/// style; DuckDB's JSON typing forces `CAST`/`to_json`/`CASE` expressions to
/// unify the select shapes — structurally different, not merely a placeholder
/// swap.
pub fn latest_metadata_query(dialect: Dialect) -> CatalogQuery {
    match dialect {
        Dialect::Postgres => CatalogQuery {
            sql: METADATA_POSTGRES,
            params: PARAMS_TABLE_ID_VERSION,
        },
        Dialect::Sqlite => CatalogQuery {
            sql: METADATA_POSITIONAL,
            params: PARAMS_TABLE_ID_VERSION,
        },
        Dialect::DuckDb => CatalogQuery {
            sql: METADATA_DUCKDB,
            params: PARAMS_TABLE_ID_VERSION,
        },
    }
}

/// Latest `Protocol` row at or before a version.
///
/// Postgres and SQLite/DuckDB share one body and differ only in placeholder
/// style.
pub fn latest_protocol_query(dialect: Dialect) -> CatalogQuery {
    match dialect {
        Dialect::Postgres => CatalogQuery {
            sql: PROTOCOL_POSTGRES,
            params: PARAMS_TABLE_ID_VERSION,
        },
        Dialect::Sqlite | Dialect::DuckDb => CatalogQuery {
            sql: PROTOCOL_POSITIONAL,
            params: PARAMS_TABLE_ID_VERSION,
        },
    }
}

/// The set of [`ActiveFile`]s surviving (adds minus removes) at or before a
/// version: latest action per path wins, keeps only live adds.
///
/// Postgres binds two named parameters (`$1`/`$2`, each referenced twice);
/// SQLite and DuckDB bind four positional parameters (`table_id, version,
/// table_id, version`) because `?` is positional and each table reference
/// repeats the filter.
pub fn active_files_query(dialect: Dialect) -> CatalogQuery {
    match dialect {
        Dialect::Postgres => CatalogQuery {
            sql: ACTIVE_FILES_POSTGRES,
            params: PARAMS_TABLE_ID_VERSION,
        },
        Dialect::Sqlite => CatalogQuery {
            sql: ACTIVE_FILES_SQLITE,
            params: PARAMS_TABLE_ID_VERSION_TABLE_ID_VERSION,
        },
        Dialect::DuckDb => CatalogQuery {
            sql: ACTIVE_FILES_DUCKDB,
            params: PARAMS_TABLE_ID_VERSION_TABLE_ID_VERSION,
        },
    }
}

/// The canonical catalog `CREATE TABLE` statements for a dialect.
///
/// **DuckDB returns the reader subset** (six tables) that the DuckDB reader
/// path and its tests require; the catalog has no DuckDB writer today. The
/// Postgres and SQLite sets are the full catalog.
pub fn schema_ddl(dialect: Dialect) -> &'static [&'static str] {
    match dialect {
        Dialect::Postgres => SCHEMA_DDL_POSTGRES,
        Dialect::Sqlite => SCHEMA_DDL_SQLITE,
        Dialect::DuckDb => SCHEMA_DDL_DUCKDB,
    }
}

/// `DROP TABLE IF EXISTS` statements for the catalog, in foreign-key-safe
/// order (dependents before parents). Universal across dialects.
pub fn schema_drop_ddl() -> &'static [&'static str] {
    SCHEMA_DROP
}

/// The full inventory of catalog tables, in a stable, dependency-safe order.
///
/// Single source of truth for callers that enumerate the catalog (e.g. a CLI
/// clearing a table's rows before re-import — it skips `dl_tables`, the row it
/// is upserting, rather than maintaining its own copy of this list).
pub fn table_names() -> &'static [&'static str] {
    TABLE_NAMES
}

// ---------------------------------------------------------------------------
// Parameter sets
// ---------------------------------------------------------------------------

const PARAMS_TABLE_ID_VERSION: &[CatalogParam] = &[CatalogParam::TableId, CatalogParam::Version];
const PARAMS_TABLE_ID_VERSION_TABLE_ID_VERSION: &[CatalogParam] = &[
    CatalogParam::TableId,
    CatalogParam::Version,
    CatalogParam::TableId,
    CatalogParam::Version,
];

// ---------------------------------------------------------------------------
// Metadata queries — relocated verbatim from each adapter
// ---------------------------------------------------------------------------

const METADATA_POSTGRES: &str = r#"
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = $1 AND version <= $2
            ORDER BY version DESC
            LIMIT 1
            "#;

const METADATA_POSITIONAL: &str = r#"
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = ? AND version <= ?
            ORDER BY version DESC
            LIMIT 1
            "#;

const METADATA_DUCKDB: &str = r#"
            SELECT
                CAST(schema_json AS VARCHAR) AS schema_json,
                CASE
                    WHEN partition_columns IS NULL THEN NULL
                    ELSE to_json(partition_columns)
                END AS partition_columns_json,
                CASE
                    WHEN table_properties IS NULL THEN NULL
                    ELSE CAST(table_properties AS VARCHAR)
                END AS table_properties
            FROM dl_metadata_updates
            WHERE table_id = ? AND version <= ?
            ORDER BY version DESC
            LIMIT 1
            "#;

// ---------------------------------------------------------------------------
// Protocol queries — relocated verbatim from each adapter
// ---------------------------------------------------------------------------

const PROTOCOL_POSTGRES: &str = r#"
            SELECT min_reader_version, min_writer_version
            FROM dl_protocol_updates
            WHERE table_id = $1 AND version <= $2
            ORDER BY version DESC
            LIMIT 1
            "#;

const PROTOCOL_POSITIONAL: &str = r#"
            SELECT min_reader_version, min_writer_version
            FROM dl_protocol_updates
            WHERE table_id = ? AND version <= ?
            ORDER BY version DESC
            LIMIT 1
            "#;

// ---------------------------------------------------------------------------
// Active-files CTE — relocated verbatim from each adapter
// ---------------------------------------------------------------------------

const ACTIVE_FILES_POSTGRES: &str = r#"
            WITH actions AS (
                SELECT path,
                       version,
                       TRUE AS is_add,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = $1 AND version <= $2
                UNION ALL
                SELECT path,
                       version,
                       FALSE AS is_add,
                       NULL::BIGINT AS size_bytes,
                       NULL::JSONB AS partition_values,
                       NULL::BIGINT AS modification_time
                FROM dl_remove_files
                WHERE table_id = $1 AND version <= $2
            ), ranked AS (
                SELECT path,
                       size_bytes,
                       partition_values,
                       modification_time,
                       is_add,
                       ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) AS rn
                FROM actions
            )
            SELECT path, size_bytes, partition_values, modification_time
            FROM ranked
            WHERE rn = 1 AND is_add = TRUE
            ORDER BY path
            "#;

const ACTIVE_FILES_SQLITE: &str = r#"
            WITH actions AS (
                SELECT path,
                       version,
                       1 AS is_add,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = ? AND version <= ?
                UNION ALL
                SELECT path,
                       version,
                       0 AS is_add,
                       NULL AS size_bytes,
                       NULL AS partition_values,
                       NULL AS modification_time
                FROM dl_remove_files
                WHERE table_id = ? AND version <= ?
            ), ranked AS (
                SELECT path,
                       size_bytes,
                       partition_values,
                       modification_time,
                       is_add,
                       ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) AS rn
                FROM actions
            )
            SELECT path, size_bytes, partition_values, modification_time
            FROM ranked
            WHERE rn = 1 AND is_add = 1
            ORDER BY path
            "#;

const ACTIVE_FILES_DUCKDB: &str = r#"
                WITH actions AS (
                SELECT path,
                       version,
                       TRUE AS is_add,
                       size_bytes,
                       CAST(partition_values AS VARCHAR) AS partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = ? AND version <= ?
                UNION ALL
                SELECT path,
                       version,
                       FALSE AS is_add,
                       NULL AS size_bytes,
                       NULL AS partition_values,
                       NULL AS modification_time
                    FROM dl_remove_files
                    WHERE table_id = ? AND version <= ?
                ), ranked AS (
                    SELECT path,
                           size_bytes,
                           partition_values,
                           modification_time,
                           is_add,
                           ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) AS rn
                    FROM actions
                )
                SELECT path,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM ranked
                WHERE rn = 1 AND is_add
                ORDER BY path
                "#;

// ---------------------------------------------------------------------------
// Schema DDL — relocated verbatim per dialect
// ---------------------------------------------------------------------------

const SCHEMA_DDL_POSTGRES: &[&str] = &[
    r#"CREATE TABLE dl_tables (
                table_id UUID PRIMARY KEY,
                name TEXT,
                location TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                protocol_min_reader INT NOT NULL,
                protocol_min_writer INT NOT NULL,
                properties JSONB NOT NULL DEFAULT '{}'::jsonb
            )"#,
    r#"CREATE TABLE dl_table_heads (
                table_id UUID PRIMARY KEY REFERENCES dl_tables(table_id) ON DELETE CASCADE,
                current_version BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
    r#"CREATE TABLE dl_table_versions (
                table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
                version BIGINT NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                committer TEXT,
                operation TEXT,
                operation_params JSONB,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE dl_add_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path TEXT NOT NULL,
                size_bytes BIGINT,
                partition_values JSONB,
                stats JSONB,
                data_change BOOLEAN DEFAULT TRUE,
                modification_time BIGINT,
                PRIMARY KEY (table_id, version, path)
            )"#,
    r#"CREATE TABLE dl_remove_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path TEXT NOT NULL,
                deletion_timestamp BIGINT,
                data_change BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (table_id, version, path)
            )"#,
    r#"CREATE TABLE dl_metadata_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                schema_json JSONB NOT NULL,
                partition_columns TEXT[],
                table_properties JSONB,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE dl_protocol_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                min_reader_version INT NOT NULL,
                min_writer_version INT NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE dl_txn_actions (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                app_id TEXT NOT NULL,
                last_update BIGINT NOT NULL,
                PRIMARY KEY (table_id, version, app_id)
            )"#,
    r#"CREATE TABLE dl_mirror_status (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INT NOT NULL DEFAULT 0,
                last_error TEXT,
                digest TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_id, version)
            )"#,
];

const SCHEMA_DDL_SQLITE: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS dl_tables (
                table_id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                protocol_min_reader INTEGER NOT NULL,
                protocol_min_writer INTEGER NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}'
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_table_heads (
                table_id TEXT PRIMARY KEY,
                current_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_table_versions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                committed_at TEXT NOT NULL,
                committer TEXT,
                operation TEXT,
                operation_params TEXT,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_add_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                size_bytes INTEGER,
                partition_values TEXT,
                stats TEXT,
                data_change INTEGER,
                modification_time INTEGER,
                PRIMARY KEY (table_id, version, path)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_remove_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                deletion_timestamp INTEGER,
                data_change INTEGER,
                PRIMARY KEY (table_id, version, path)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_metadata_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                schema_json TEXT NOT NULL,
                partition_columns TEXT,
                table_properties TEXT,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_protocol_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_txn_actions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                app_id TEXT NOT NULL,
                last_update INTEGER NOT NULL,
                PRIMARY KEY (table_id, version, app_id)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_mirror_status (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                digest TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (table_id, version)
            )"#,
];

/// DuckDB schema — the **reader subset**. The catalog has no DuckDB writer
/// path today, so only the tables the reader joins on are defined here.
const SCHEMA_DDL_DUCKDB: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS dl_table_heads (
                table_id UUID PRIMARY KEY,
                current_version BIGINT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_table_versions (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                committed_at TIMESTAMP NOT NULL,
                committer VARCHAR,
                operation VARCHAR,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_metadata_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                schema_json JSON NOT NULL,
                partition_columns VARCHAR[],
                table_properties JSON,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_protocol_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_add_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path VARCHAR NOT NULL,
                size_bytes BIGINT,
                partition_values JSON,
                modification_time BIGINT,
                PRIMARY KEY (table_id, version, path)
            )"#,
    r#"CREATE TABLE IF NOT EXISTS dl_remove_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path VARCHAR NOT NULL,
                deletion_timestamp BIGINT,
                data_change BOOLEAN,
                PRIMARY KEY (table_id, version, path)
            )"#,
];

const SCHEMA_DROP: &[&str] = &[
    "DROP TABLE IF EXISTS dl_mirror_status",
    "DROP TABLE IF EXISTS dl_txn_actions",
    "DROP TABLE IF EXISTS dl_protocol_updates",
    "DROP TABLE IF EXISTS dl_metadata_updates",
    "DROP TABLE IF EXISTS dl_remove_files",
    "DROP TABLE IF EXISTS dl_add_files",
    "DROP TABLE IF EXISTS dl_table_versions",
    "DROP TABLE IF EXISTS dl_table_heads",
    "DROP TABLE IF EXISTS dl_tables",
];

const TABLE_NAMES: &[&str] = &[
    "dl_tables",
    "dl_table_heads",
    "dl_table_versions",
    "dl_add_files",
    "dl_remove_files",
    "dl_metadata_updates",
    "dl_protocol_updates",
    "dl_txn_actions",
    "dl_mirror_status",
];
