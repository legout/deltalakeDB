## ADDED Requirements
### Requirement: SQL Read Path (SQLite)
The system SHALL provide a `SqlTxnLogReader` for SQLite with parity to the Postgres reader for table open and time travel.

#### Scenario: Table open (SQLite)
- **WHEN** opening a table backed by SQLite schema
- **THEN** the reader returns latest version and active files

#### Scenario: Time travel by version (SQLite)
- **WHEN** requesting snapshot for version `V`
- **THEN** the reader returns metadata/protocol/properties and active files as of `V`

