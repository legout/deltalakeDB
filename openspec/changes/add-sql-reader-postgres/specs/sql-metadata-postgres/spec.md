## ADDED Requirements

### Requirement: SQL-Based Snapshot Reading
The system SHALL implement `SqlTxnLogReader` that reconstructs table snapshots from PostgreSQL.

#### Scenario: Reading latest snapshot
- **WHEN** opening a table without version specified
- **THEN** reader queries latest version from `dl_table_versions` and returns current state

#### Scenario: Fast version lookup
- **WHEN** querying for latest version
- **THEN** lookup completes in < 50ms using indexed query

### Requirement: Active Files Listing
The system SHALL efficiently list active files by computing adds minus removes.

#### Scenario: Computing active file set
- **WHEN** requesting files for a version
- **THEN** reader returns paths present in `dl_add_files` but not in `dl_remove_files` up to that version

#### Scenario: Large file sets
- **WHEN** table has 100k+ active files
- **THEN** file listing completes in < 800ms (p95) with batched queries

### Requirement: Schema Retrieval
The system SHALL retrieve current table schema from metadata updates.

#### Scenario: Getting current schema
- **WHEN** requesting table schema
- **THEN** reader returns latest `dl_metadata_updates` entry with partition columns and schema JSON

### Requirement: Protocol Version Checking
The system SHALL check protocol compatibility from stored protocol updates.

#### Scenario: Protocol validation
- **WHEN** reader opens a table
- **THEN** system checks min_reader_version against supported versions

### Requirement: Time Travel by Version
The system SHALL support reading table state at any historical version.

#### Scenario: Reading old version
- **WHEN** requesting snapshot at version N
- **THEN** reader reconstructs state using only actions up to version N

#### Scenario: Invalid version
- **WHEN** requesting version that doesn't exist
- **THEN** reader returns error indicating valid version range

### Requirement: Time Travel by Timestamp
The system SHALL support reading table state at a specific timestamp.

#### Scenario: Timestamp-based lookup
- **WHEN** requesting state at timestamp T
- **THEN** reader finds latest commit with `committed_at <= T` and returns that snapshot

#### Scenario: Timestamp before first commit
- **WHEN** timestamp is before table creation
- **THEN** reader returns error indicating table didn't exist

### Requirement: Connection Pooling
The system SHALL use sqlx connection pool for efficient database access.

#### Scenario: Concurrent reads
- **WHEN** multiple readers access the same database
- **THEN** connections are reused from pool without creating new connections per query

#### Scenario: Connection failure recovery
- **WHEN** database connection fails
- **THEN** reader retries with exponential backoff up to configured limit

### Requirement: Prepared Statements
The system SHALL use compile-time checked prepared statements with sqlx.

#### Scenario: Query safety
- **WHEN** executing queries
- **THEN** queries are validated at compile time using `sqlx::query!` macro

### Requirement: Query Performance
The system SHALL optimize queries for sub-second table opens.

#### Scenario: Cold open benchmark
- **WHEN** opening table with 100k files on local PostgreSQL
- **THEN** p95 latency is < 800ms

#### Scenario: Warm open benchmark
- **WHEN** opening table with cached connections
- **THEN** p50 latency is < 200ms
