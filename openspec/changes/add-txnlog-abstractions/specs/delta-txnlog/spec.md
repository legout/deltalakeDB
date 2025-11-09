## ADDED Requirements
### Requirement: Transaction Log Abstractions
The system SHALL expose `TxnLogReader` and `TxnLogWriter` traits that decouple metadata storage from behavior, allowing multiple implementations (file-backed, SQL-backed) without changing call sites.

#### Scenario: Backward compatibility via file-backed
- **WHEN** a table is opened via a file/object-store URI (e.g., `s3://bucket/path`)
- **THEN** the system uses `FileTxnLogReader` and returns the same snapshot as the current file-backed behavior

#### Scenario: Reader trait capabilities
- **WHEN** using a `TxnLogReader`
- **THEN** the caller can obtain current version, time-travel by version/timestamp, active files, schema, partition columns, protocol, and table properties

#### Scenario: Writer trait concurrency
- **WHEN** a `TxnLogWriter` commits a set of actions for version `N+1`
- **THEN** the write enforces optimistic concurrency (`current_version == N`) and returns commit metadata including the new version

