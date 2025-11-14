# rust-workspace Specification

## Purpose
TBD - created by archiving change add-repo-skeleton. Update Purpose after archive.
## Requirements
### Requirement: Cargo Workspace
The repository SHALL define a Cargo workspace at the root that includes planned crates for core domain, SQL adapters, and log mirroring.

#### Scenario: Workspace members declared
- **WHEN** viewing `Cargo.toml` at the repository root
- **THEN** a `[workspace]` with `members = ["crates/core", "crates/sql", "crates/mirror"]` is present

#### Scenario: Minimal compilable crates
- **WHEN** running `cargo check`
- **THEN** each workspace member compiles with a minimal `lib.rs` that builds successfully

### Requirement: Crate Naming and Structure
Crate names and directories MUST follow the planned architecture.

#### Scenario: Crate names
- **WHEN** viewing `crates/*/Cargo.toml`
- **THEN** crates are named `deltalakedb-core`, `deltalakedb-sql`, and `deltalakedb-mirror`

### Requirement: Transaction Log Abstractions
The system SHALL provide pluggable traits for Delta transaction log operations to support both file-based and SQL-backed implementations.

#### Scenario: Read latest snapshot via trait
- **WHEN** a TxnLogReader implementation is used to read the latest snapshot
- **THEN** it returns the current table state including active files, schema, and protocol

#### Scenario: Write commit via trait  
- **WHEN** a TxnLogWriter implementation is used to commit actions
- **THEN** it atomically persists the actions and returns the new version

#### Scenario: File-based implementation compatibility
- **WHEN** FileTxnLogReader/FileTxnLogWriter are used
- **THEN** they maintain exact compatibility with existing delta-rs file operations

#### Scenario: Error handling
- **WHEN** a transaction log operation fails
- **THEN** it returns a structured error with context about the failure type

### Requirement: Delta Table Import Tool
The system SHALL provide a CLI tool to import existing Delta tables from `_delta_log/` into the SQL metadata catalog.

#### Scenario: Import existing table
- **WHEN** a Delta table path is provided to the import tool
- **THEN** it replays the checkpoint and JSON logs to populate SQL tables

#### Scenario: Large table migration
- **WHEN** importing a table with many versions
- **THEN** it provides progress reporting and can resume if interrupted

#### Scenario: Migration validation
- **WHEN** an import is completed
- **THEN** it validates that SQL-derived snapshot matches file-based snapshot

### Requirement: Multi-Table ACID Transactions
The system SHALL support atomic commits that update multiple Delta tables within a single SQL transaction.

#### Scenario: Atomic multi-table commit
- **WHEN** actions are staged for multiple tables and committed
- **THEN** all tables are updated atomically within a single SQL transaction

#### Scenario: Transaction rollback
- **WHEN** a multi-table transaction fails
- **THEN** no changes are persisted to any of the involved tables

#### Scenario: Cross-table consistency
- **WHEN** related tables are updated together
- **THEN** referential integrity is maintained across the transaction

### Requirement: SQL Transaction Log Reader
The system SHALL provide a SqlTxnLogReader implementation that reconstructs Delta table snapshots from relational metadata.

#### Scenario: Read latest snapshot from SQL
- **WHEN** SqlTxnLogReader reads the latest version
- **THEN** it returns the current active files, schema, and protocol from SQL tables

#### Scenario: Time travel by version
- **WHEN** a specific version is requested
- **THEN** it reconstructs the table state as of that version using historical data

#### Scenario: Time travel by timestamp
- **WHEN** a timestamp is provided
- **THEN** it finds the latest version committed at or before that timestamp

### Requirement: SQL Transaction Log Writer
The system SHALL provide a SqlTxnLogWriter that commits Delta actions to SQL with optimistic concurrency control.

#### Scenario: Single table commit
- **WHEN** actions are committed via SqlTxnLogWriter
- **THEN** they are inserted into SQL tables within a transaction with version validation

#### Scenario: Optimistic concurrency
- **WHEN** concurrent writers attempt to commit
- **THEN** only one succeeds per version, others retry with fresh snapshot

#### Scenario: Writer idempotency
- **WHEN** the same commit is retried
- **THEN** it is detected and handled gracefully without duplication

