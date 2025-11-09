## ADDED Requirements

### Requirement: Delta JSON Commit Mirroring
The system SHALL write canonical Delta JSON commit files to object storage after SQL commits.

#### Scenario: Mirroring successful commit
- **WHEN** SQL commit for version N succeeds
- **THEN** system writes `_delta_log/00000000000000000N.json` with actions

#### Scenario: Newline-delimited format
- **WHEN** writing JSON commit
- **THEN** each action is serialized as one JSON object per line per Delta spec

#### Scenario: Action ordering
- **WHEN** serializing multiple action types
- **THEN** actions are ordered deterministically (protocol, metadata, txn, add, remove)

### Requirement: Parquet Checkpoint Generation
The system SHALL generate Parquet checkpoints at configured intervals.

#### Scenario: Checkpoint cadence
- **WHEN** version is multiple of checkpoint_interval (default 10)
- **THEN** system generates `_delta_log/00000000000000000N.checkpoint.parquet`

#### Scenario: Checkpoint schema
- **WHEN** writing checkpoint
- **THEN** Parquet schema matches Delta protocol specification

#### Scenario: Checkpoint compression
- **WHEN** generating checkpoint
- **THEN** file uses Snappy compression for compatibility

### Requirement: Post-Commit Mirroring
The system SHALL mirror only after SQL transaction commits successfully.

#### Scenario: Sequencing guarantee
- **WHEN** SQL commit fails
- **THEN** no mirror artifacts are written to object storage

#### Scenario: Mirroring order
- **WHEN** SQL commit succeeds
- **THEN** mirroring executes immediately after commit confirmation

### Requirement: Idempotent Mirroring
The system SHALL support safe retries of mirror operations.

#### Scenario: Duplicate mirror attempt
- **WHEN** mirroring retries due to transient failure
- **THEN** duplicate writes are detected and safely handled

#### Scenario: Overwrite safety
- **WHEN** file already exists in object storage
- **THEN** system verifies content matches before skipping

### Requirement: Mirror Status Tracking
The system SHALL track mirror status per table and version.

#### Scenario: Recording success
- **WHEN** mirror completes successfully
- **THEN** status is recorded as `SUCCESS` with timestamp

#### Scenario: Recording failure
- **WHEN** mirror fails
- **THEN** status is recorded as `FAILED` with error details and retry count

#### Scenario: Pending status
- **WHEN** mirror is in progress
- **THEN** status is marked as `PENDING` to prevent duplicate attempts

### Requirement: Mirror Lag Monitoring
The system SHALL expose metrics for mirror lag.

#### Scenario: Lag calculation
- **WHEN** SQL version ahead of mirrored version
- **THEN** lag is computed as difference between SQL and last successful mirror

#### Scenario: Lag alerting
- **WHEN** mirror lag exceeds threshold (default 60 seconds)
- **THEN** system emits alert metric

### Requirement: Background Reconciliation
The system SHALL provide reconciler to repair mirror gaps.

#### Scenario: Gap detection
- **WHEN** reconciler runs
- **THEN** it identifies versions with `FAILED` or missing mirror status

#### Scenario: Automatic retry
- **WHEN** failed mirror is found
- **THEN** reconciler retries with exponential backoff up to max attempts

#### Scenario: Gap notification
- **WHEN** mirror remains failed after max retries
- **THEN** system logs error and emits metric for manual intervention

### Requirement: Atomic Object Store Operations
The system SHALL use atomic write operations where supported.

#### Scenario: Conditional put
- **WHEN** object store supports put-if-absent
- **THEN** system uses it to prevent overwrites

#### Scenario: Atomic rename
- **WHEN** object store supports atomic rename
- **THEN** system writes to temp key and renames atomically

### Requirement: Canonical Serialization
The system SHALL produce byte-for-byte compatible Delta artifacts.

#### Scenario: JSON field ordering
- **WHEN** serializing actions
- **THEN** JSON fields are ordered per Delta spec

#### Scenario: Timestamp formatting
- **WHEN** serializing timestamps
- **THEN** system uses Unix epoch milliseconds as integers

#### Scenario: Null handling
- **WHEN** optional fields are absent
- **THEN** they are omitted from JSON (not serialized as null)

### Requirement: Conformance Validation
The system SHALL validate mirrored artifacts against Delta protocol.

#### Scenario: JSON validation
- **WHEN** mirror writes commit file
- **THEN** output passes Delta JSON schema validation

#### Scenario: Checkpoint validation
- **WHEN** mirror writes checkpoint
- **THEN** Parquet schema matches Delta checkpoint specification

#### Scenario: External reader test
- **WHEN** validating compatibility
- **THEN** mirrored table is readable by Spark and DuckDB without errors
