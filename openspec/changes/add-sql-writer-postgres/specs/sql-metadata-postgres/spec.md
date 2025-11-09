## ADDED Requirements

### Requirement: Atomic Commit
The system SHALL implement `SqlTxnLogWriter` that commits Delta actions atomically to PostgreSQL.

#### Scenario: Successful commit
- **WHEN** writer submits valid actions for version N+1
- **THEN** all actions are inserted and version is incremented in a single transaction

#### Scenario: Transaction rollback
- **WHEN** any error occurs during commit
- **THEN** entire transaction is rolled back leaving database unchanged

### Requirement: Optimistic Concurrency Control
The system SHALL enforce version-based optimistic concurrency.

#### Scenario: Version validation
- **WHEN** writer attempts to commit version N+1
- **THEN** system validates current version is exactly N before proceeding

#### Scenario: Concurrent writer conflict
- **WHEN** two writers attempt to commit same version
- **THEN** first writer succeeds and second receives `VersionConflict` error

#### Scenario: Row-level locking
- **WHEN** validating version
- **THEN** writer acquires row lock using `SELECT ... FOR UPDATE` to prevent races

### Requirement: Action Persistence
The system SHALL insert all action types into normalized tables.

#### Scenario: Add file actions
- **WHEN** commit includes add file actions
- **THEN** rows are inserted into `dl_add_files` with path, size, stats, and partition values

#### Scenario: Remove file actions
- **WHEN** commit includes remove file actions
- **THEN** rows are inserted into `dl_remove_files` with path and deletion timestamp

#### Scenario: Metadata update
- **WHEN** commit changes schema or properties
- **THEN** row is inserted into `dl_metadata_updates` with schema JSON and partition columns

#### Scenario: Protocol update
- **WHEN** commit changes protocol requirements
- **THEN** row is inserted into `dl_protocol_updates` with min reader/writer versions

#### Scenario: Transaction action
- **WHEN** streaming application reports progress
- **THEN** row is inserted into `dl_txn_actions` with app_id and last_update

### Requirement: Batch Operations
The system SHALL support efficient batch insertion for commits with many files.

#### Scenario: Large commit
- **WHEN** commit adds 1000+ files
- **THEN** writer uses batch INSERT or COPY for performance

#### Scenario: Batch performance
- **WHEN** inserting 10k files
- **THEN** operation completes in < 5 seconds on local PostgreSQL

### Requirement: Idempotency
The system SHALL reject duplicate commit attempts safely.

#### Scenario: Duplicate commit
- **WHEN** writer retries same version after failure
- **THEN** system returns error without corrupting state

### Requirement: Validation
The system SHALL validate actions before insertion.

#### Scenario: Invalid action schema
- **WHEN** action has invalid or missing required fields
- **THEN** writer returns validation error before starting transaction

#### Scenario: Protocol compatibility
- **WHEN** writer attempts incompatible protocol change
- **THEN** system rejects commit with clear error

### Requirement: Version Tracking
The system SHALL insert commit metadata into `dl_table_versions`.

#### Scenario: Recording operation
- **WHEN** commit succeeds
- **THEN** row is added with version, timestamp, committer, operation type, and parameters

#### Scenario: Preserving history
- **WHEN** querying version history
- **THEN** all past commits are preserved with full metadata

### Requirement: Error Reporting
The system SHALL provide detailed error messages for failures.

#### Scenario: Version conflict details
- **WHEN** optimistic concurrency check fails
- **THEN** error includes expected version, actual version, and table identifier

#### Scenario: Constraint violation
- **WHEN** database constraint is violated
- **THEN** error explains which constraint and provides context for debugging

### Requirement: Transaction Isolation
The system SHALL use appropriate isolation level for consistency.

#### Scenario: Read committed isolation
- **WHEN** writer commits
- **THEN** transaction uses at least READ COMMITTED isolation to prevent dirty reads

#### Scenario: Serializable for conflicts
- **WHEN** detecting concurrent writers
- **THEN** version check prevents write skew with row-level locking
