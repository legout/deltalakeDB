## ADDED Requirements
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
