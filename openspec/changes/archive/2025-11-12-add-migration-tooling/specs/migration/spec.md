## ADDED Requirements

### Requirement: Metadata Import from _delta_log
The system SHALL provide a tool to bootstrap SQL metadata from existing Delta logs.

#### Scenario: Bootstrap from checkpoint and JSON
- **WHEN** importing an existing Delta table
- **THEN** the tool SHALL read the latest checkpoint (if present) and replay subsequent JSON commits
- **AND** SHALL populate normalized SQL tables with table, version, and file action records

#### Scenario: Initialize current version
- **WHEN** import completes successfully
- **THEN** the tool SHALL set the table's current_version to the latest imported version
- **AND** SHALL record import metadata for audit and repeatability

### Requirement: Parity Validation
The system SHALL validate parity between SQL-derived and `_delta_log`-derived snapshots.

#### Scenario: Snapshot diff verification
- **WHEN** validating a table
- **THEN** the tool SHALL compare active files, schema, protocol, and table properties from both sources
- **AND** SHALL report any differences with machine-readable and human-readable outputs

#### Scenario: Drift alerting
- **WHEN** differences persist beyond configured thresholds
- **THEN** the system SHALL emit alerts and non-zero exit codes
- **AND** SHALL provide guidance for reconciliation

### Requirement: Writer Cutover and Rollback
The system SHALL provide a documented cutover to SQL-backed writers and a safe rollback path.

#### Scenario: Writer cutover to SQL-backed
- **WHEN** writers are switched to `deltasql://` URIs
- **THEN** the system SHALL continue to mirror to `_delta_log` for external compatibility
- **AND** SHALL verify parity post-cutover

#### Scenario: Safe rollback to file-backed writers
- **WHEN** rollback is initiated
- **THEN** writers SHALL be reconfigured to file-backed paths
- **AND** SHALL ensure `_delta_log` remains authoritative and consistent

### Requirement: Idempotent Operations
The migration tooling SHALL support idempotent imports and validations.

#### Scenario: Re-running import
- **WHEN** `dl import` is executed multiple times for the same table
- **THEN** the tool SHALL not duplicate records nor corrupt state
- **AND** SHALL resume from the last consistent point when possible

### Requirement: Operational Reporting
The system SHALL provide reporting and metrics for migration activities.

#### Scenario: Progress and summary reporting
- **WHEN** running import or validation
- **THEN** the tool SHALL print progress and summary statistics (versions processed, files added/removed)
- **AND** SHALL emit structured logs for observability

