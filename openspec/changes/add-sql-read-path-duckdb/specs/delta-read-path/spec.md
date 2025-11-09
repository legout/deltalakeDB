## ADDED Requirements
### Requirement: SQL Read Path (DuckDB)
The system SHALL provide a `SqlTxnLogReader` for DuckDB with parity to the Postgres reader for table open and time travel.

#### Scenario: Table open (DuckDB)
- **WHEN** opening a table backed by DuckDB
- **THEN** the reader returns latest version and active files

#### Scenario: Time travel by version (DuckDB)
- **WHEN** requesting snapshot for version `V`
- **THEN** the reader returns metadata/protocol/properties and active files as of `V`

