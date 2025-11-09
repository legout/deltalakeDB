# sql-metadata-core Specification

## Purpose
TBD - created by archiving change add-txnlog-traits. Update Purpose after archive.
## Requirements
### Requirement: Transaction Log Reader Trait
The system SHALL provide a `TxnLogReader` trait that abstracts reading operations from Delta transaction logs.

#### Scenario: Reading latest version
- **WHEN** a reader requests the latest version of a table
- **THEN** the trait returns the current version number without listing all commits

#### Scenario: Reading snapshot at version
- **WHEN** a reader requests table state at a specific version
- **THEN** the trait returns complete snapshot including active files, schema, and metadata

#### Scenario: Time travel by timestamp
- **WHEN** a reader requests table state at a specific timestamp
- **THEN** the trait returns the snapshot corresponding to the latest commit before or at that timestamp

### Requirement: Transaction Log Writer Trait
The system SHALL provide a `TxnLogWriter` trait that abstracts write operations to Delta transaction logs.

#### Scenario: Beginning a commit
- **WHEN** a writer begins a new commit
- **THEN** the trait validates current version and prepares for optimistic concurrency

#### Scenario: Writing actions
- **WHEN** a writer submits add/remove/metadata actions
- **THEN** the trait validates and stages the actions for commit

#### Scenario: Finalizing commit
- **WHEN** a writer finalizes a commit
- **THEN** the trait ensures atomicity and increments version by exactly one

#### Scenario: Concurrent write conflict
- **WHEN** two writers attempt to commit the same version
- **THEN** the trait rejects one writer with a conflict error

### Requirement: Async Support
The system SHALL implement traits with async methods using Tokio runtime.

#### Scenario: Non-blocking operations
- **WHEN** any trait method is called
- **THEN** the operation executes asynchronously without blocking the runtime

### Requirement: Error Handling
The system SHALL define specific error types for transaction log operations.

#### Scenario: Version conflict error
- **WHEN** optimistic concurrency check fails
- **THEN** the trait returns a `VersionConflict` error with expected and actual versions

#### Scenario: IO error
- **WHEN** underlying storage fails
- **THEN** the trait wraps the error with context about the operation

