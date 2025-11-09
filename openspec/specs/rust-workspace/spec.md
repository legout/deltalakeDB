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

