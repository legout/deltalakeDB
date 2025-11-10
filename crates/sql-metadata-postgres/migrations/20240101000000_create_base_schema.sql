-- Migration: Create base Delta Lake metadata schema
-- This migration creates the core tables and indexes for SQL-backed Delta Lake metadata.
-- 
-- Tables created:
-- - dl_tables: Registry of Delta tables with metadata
-- - dl_table_versions: Version history tracking for each table
-- - dl_add_files: File addition actions
-- - dl_remove_files: File removal actions
-- - dl_metadata_updates: Schema and metadata changes
-- - dl_protocol_updates: Protocol version changes
-- - dl_txn_actions: Transaction/streaming progress tracking

-- Create extension for UUID support (if not already created)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Main Tables
-- ============================================================================

-- dl_tables: Registry of Delta tables
-- Stores the base information about each Delta table including location,
-- protocol versions, and current version tracking for optimistic concurrency.
CREATE TABLE dl_tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Table identification
    table_name TEXT NOT NULL,
    location TEXT NOT NULL UNIQUE,  -- Object storage location (s3://bucket/path)
    
    -- Version tracking for optimistic concurrency control
    current_version BIGINT NOT NULL DEFAULT 0,
    
    -- Protocol requirements
    min_reader_version INT NOT NULL DEFAULT 1,
    min_writer_version INT NOT NULL DEFAULT 2,
    
    -- Properties: arbitrary metadata as JSON
    -- Examples: {"created_by": "app_v1", "owner": "team_x"}
    properties JSONB DEFAULT '{}'::JSONB,
    
    -- Lifecycle tracking
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CHECK (current_version >= 0),
    CHECK (min_reader_version > 0),
    CHECK (min_writer_version > 0)
);

CREATE INDEX idx_dl_tables_location ON dl_tables(location);
CREATE INDEX idx_dl_tables_table_name ON dl_tables(table_name);

-- ============================================================================
-- Version Tracking
-- ============================================================================

-- dl_table_versions: Complete version history for each table
-- Records every commit to track lineage, operations, and committer information.
CREATE TABLE dl_table_versions (
    table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    
    -- Timestamp of this commit (milliseconds since epoch, from Delta JSON)
    commit_timestamp BIGINT NOT NULL,
    
    -- Operation type: "AddFile", "RemoveFile", "SetTransaction", "Metadata", "Protocol", etc.
    -- This helps understand what type of commit this was
    operation_type TEXT NOT NULL,
    
    -- Number of actions in this version (for stats)
    num_actions INT NOT NULL DEFAULT 0,
    
    -- Optional metadata about the commit
    -- Example: {"committer": "spark", "timestamp": "...", "isolation_level": "..."}
    commit_info JSONB DEFAULT '{}'::JSONB,
    
    -- Lifecycle tracking
    recorded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (table_id, version),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    CHECK (version >= 0),
    CHECK (commit_timestamp >= 0)
);

-- Index for efficient "latest version" queries
CREATE INDEX idx_dl_table_versions_latest 
    ON dl_table_versions(table_id, version DESC);

-- Index for timestamp-based time travel queries
CREATE INDEX idx_dl_table_versions_timestamp 
    ON dl_table_versions(table_id, commit_timestamp DESC);

-- ============================================================================
-- Add File Actions
-- ============================================================================

-- dl_add_files: Track file additions in each version
-- Represents AddFile actions in Delta transaction log.
-- One row per file per version where file was added.
CREATE TABLE dl_add_files (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- File identification
    file_path TEXT NOT NULL,  -- e.g., "path/to/file.parquet"
    file_size_bytes BIGINT NOT NULL,
    
    -- Modification timestamp (milliseconds since epoch)
    modification_time BIGINT NOT NULL,
    
    -- Partitioning information (JSONB for flexibility across partition schemes)
    -- Example for partitioned by date/region:
    -- {"year": 2024, "month": 1, "day": 15, "region": "us-west"}
    partition_values JSONB,
    
    -- Statistics about file contents (JSONB, can include min/max/null_count per column)
    -- Example: {"row_count": 1000, "columns": {"id": {"min": 1, "max": 999}}}
    stats JSONB,
    
    -- Whether stats are accurate (may be truncated in some cases)
    stats_truncated BOOLEAN DEFAULT FALSE,
    
    -- Additional tags/metadata
    -- Example: {"encoding": "snappy", "created_by": "spark"}
    tags JSONB,
    
    PRIMARY KEY (table_id, version, file_path),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, version) REFERENCES dl_table_versions(table_id, version) ON DELETE CASCADE,
    CHECK (file_size_bytes >= 0),
    CHECK (modification_time >= 0)
);

-- Index for file existence lookups: "does this file exist at this version?"
CREATE INDEX idx_dl_add_files_lookup 
    ON dl_add_files(table_id, file_path, version DESC);

-- Index for version-based file queries: "what files exist at version X?"
CREATE INDEX idx_dl_add_files_version 
    ON dl_add_files(table_id, version DESC, file_path);

-- GIN index on stats for predicate pushdown
CREATE INDEX idx_dl_add_files_stats 
    ON dl_add_files USING GIN (stats);

-- GIN index on partition values for partition pruning
CREATE INDEX idx_dl_add_files_partitions 
    ON dl_add_files USING GIN (partition_values);

-- ============================================================================
-- Remove File Actions
-- ============================================================================

-- dl_remove_files: Track file removals (deletions/compactions)
-- Represents RemoveFile actions in Delta transaction log.
CREATE TABLE dl_remove_files (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- File identification
    file_path TEXT NOT NULL,
    
    -- Deletion timestamp (when the remove action was recorded, millis since epoch)
    deletion_timestamp BIGINT NOT NULL,
    
    -- Whether this removal changed data (vs. just cleanup)
    data_change BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Extended file metadata (optional) for more detailed tracking
    extended_file_metadata BOOLEAN,
    
    -- Deletion vector for row-level deletion tracking (if applicable)
    -- Format depends on Delta version; stored as JSON for flexibility
    deletion_vector JSONB,
    
    PRIMARY KEY (table_id, version, file_path),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, version) REFERENCES dl_table_versions(table_id, version) ON DELETE CASCADE,
    CHECK (deletion_timestamp >= 0)
);

-- Index for efficient removed file lookups
CREATE INDEX idx_dl_remove_files_lookup 
    ON dl_remove_files(table_id, file_path, version DESC);

-- Index for version queries
CREATE INDEX idx_dl_remove_files_version 
    ON dl_remove_files(table_id, version DESC);

-- ============================================================================
-- Metadata Updates
-- ============================================================================

-- dl_metadata_updates: Track schema and metadata changes
-- Represents Metadata actions in Delta transaction log.
-- Records schema evolution, table properties, and description changes.
CREATE TABLE dl_metadata_updates (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- Table description
    description TEXT,
    
    -- Schema in JSON format (Delta StructType)
    -- Example: {"type":"struct","fields":[{"name":"id","type":"long"},...]}
    schema_json JSONB,
    
    -- Partition columns (array of column names)
    -- Example: ["year", "month", "day"]
    partition_columns TEXT[],
    
    -- Configuration properties
    -- Example: {"delta.columnMapping.mode": "name"}
    configuration JSONB DEFAULT '{}'::JSONB,
    
    -- Creation timestamp (milliseconds since epoch, from Delta)
    created_time BIGINT,
    
    PRIMARY KEY (table_id, version),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, version) REFERENCES dl_table_versions(table_id, version) ON DELETE CASCADE
);

-- Index for efficient metadata lookups
CREATE INDEX idx_dl_metadata_updates_version 
    ON dl_metadata_updates(table_id, version DESC);

-- ============================================================================
-- Protocol Updates
-- ============================================================================

-- dl_protocol_updates: Track protocol version changes
-- Represents Protocol actions in Delta transaction log.
-- Records minimum reader/writer versions required.
CREATE TABLE dl_protocol_updates (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- Minimum protocol version for readers
    min_reader_version INT,
    
    -- Minimum protocol version for writers
    min_writer_version INT,
    
    PRIMARY KEY (table_id, version),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, version) REFERENCES dl_table_versions(table_id, version) ON DELETE CASCADE,
    CHECK (min_reader_version IS NULL OR min_reader_version > 0),
    CHECK (min_writer_version IS NULL OR min_writer_version > 0)
);

CREATE INDEX idx_dl_protocol_updates_version 
    ON dl_protocol_updates(table_id, version DESC);

-- ============================================================================
-- Transaction/Streaming Actions
-- ============================================================================

-- dl_txn_actions: Track transaction and streaming progress
-- Represents SetTransaction actions in Delta transaction log.
-- Used for tracking streaming application progress and multi-table transactions.
CREATE TABLE dl_txn_actions (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- Application identifier (e.g., "streaming-app-v1")
    app_id TEXT NOT NULL,
    
    -- Last value/offset processed by the streaming application
    -- Type depends on the source (e.g., Kafka offset, file sequence number)
    last_update_value TEXT,
    
    PRIMARY KEY (table_id, version, app_id),
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, version) REFERENCES dl_table_versions(table_id, version) ON DELETE CASCADE
);

CREATE INDEX idx_dl_txn_actions_lookup 
    ON dl_txn_actions(table_id, app_id, version DESC);

-- ============================================================================
-- Summary and Stats
-- ============================================================================

-- Helper view: Active files (files added but not yet removed)
-- This view represents the current snapshot of active files in each table.
CREATE VIEW v_active_files AS
SELECT 
    af.table_id,
    af.version,
    af.file_path,
    af.file_size_bytes,
    af.partition_values,
    af.stats,
    af.modification_time
FROM dl_add_files af
WHERE NOT EXISTS (
    SELECT 1 FROM dl_remove_files rf 
    WHERE rf.table_id = af.table_id 
    AND rf.file_path = af.file_path
    AND rf.version > af.version
);

-- ============================================================================
-- Grants (if needed for production deployments)
-- ============================================================================

-- Example: Create read-only role for queries (uncomment as needed)
-- CREATE ROLE delta_reader WITH LOGIN PASSWORD 'password';
-- GRANT CONNECT ON DATABASE delta_metadata TO delta_reader;
-- GRANT USAGE ON SCHEMA public TO delta_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO delta_reader;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO delta_reader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO delta_reader;
