## ADDED Requirements

### Requirement: Tables Registry
The system SHALL provide a `dl_tables` table to register Delta tables with their metadata.

#### Scenario: Registering new table
- **WHEN** a new Delta table is created
- **THEN** a row is inserted with UUID, name, location, and protocol versions

#### Scenario: Storing table properties
- **WHEN** table properties are set
- **THEN** properties are stored in JSONB column for flexible schema

### Requirement: Version Tracking
The system SHALL provide a `dl_table_versions` table to track each commit with operation metadata.

#### Scenario: Recording commit
- **WHEN** a commit succeeds
- **THEN** version, timestamp, committer, and operation details are recorded

#### Scenario: Querying version history
- **WHEN** listing table history
- **THEN** versions are retrievable in descending order with operation context

### Requirement: Add File Actions
The system SHALL provide a `dl_add_files` table to store file addition actions.

#### Scenario: Adding files with stats
- **WHEN** files are added in a commit
- **THEN** path, size, partition values, and statistics are stored per version

#### Scenario: Storing partition values
- **WHEN** files are partitioned
- **THEN** partition values are stored as JSONB for flexible partition schemes

### Requirement: Remove File Actions
The system SHALL provide a `dl_remove_files` table to track file removals.

#### Scenario: Removing files
- **WHEN** files are removed (compaction, delete)
- **THEN** removal is recorded with version, path, and deletion timestamp

### Requirement: Metadata Updates
The system SHALL provide a `dl_metadata_updates` table for schema changes.

#### Scenario: Schema evolution
- **WHEN** table schema changes
- **THEN** new schema JSON, partition columns, and properties are stored by version

### Requirement: Protocol Updates
The system SHALL provide a `dl_protocol_updates` table for protocol version changes.

#### Scenario: Protocol upgrade
- **WHEN** protocol version increases
- **THEN** min_reader and min_writer versions are recorded for that commit

### Requirement: Transaction Actions
The system SHALL provide a `dl_txn_actions` table for streaming progress.

#### Scenario: Recording streaming progress
- **WHEN** streaming app makes progress
- **THEN** app_id and last_update value are stored per version

### Requirement: Fast Lookups
The system SHALL provide indexes for common query patterns.

#### Scenario: Latest version lookup
- **WHEN** querying for latest table version
- **THEN** lookup completes in O(1) time using (table_id, version DESC) index

#### Scenario: File path lookup
- **WHEN** checking if file exists
- **THEN** lookup completes efficiently using (table_id, path) index

#### Scenario: Stats filtering
- **WHEN** applying predicate pushdown
- **THEN** stats column uses GIN index for JSON filtering

### Requirement: Referential Integrity
The system SHALL enforce foreign key constraints from action tables to tables registry.

#### Scenario: Orphan prevention
- **WHEN** attempting to add actions for non-existent table
- **THEN** database rejects the operation with foreign key violation

### Requirement: Migration Support
The system SHALL provide versioned SQL migrations using sqlx.

#### Scenario: Initial setup
- **WHEN** setting up new database
- **THEN** `sqlx migrate run` creates all tables and indexes

#### Scenario: Schema rollback
- **WHEN** migration needs to be reversed
- **THEN** down migration cleanly removes changes
