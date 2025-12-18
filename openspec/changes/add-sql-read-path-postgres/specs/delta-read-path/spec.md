## ADDED Requirements
### Requirement: SQL Read Path (Postgres)
The system SHALL provide a `SqlTxnLogReader` for Postgres that reconstructs table snapshots solely from SQL data.

#### Scenario: Table open
- **WHEN** opening a table backed by Postgres schema
- **THEN** the reader returns the latest committed version and active files derived from `dl_add_files` minus `dl_remove_files`

#### Scenario: Time travel by version
- **WHEN** requesting snapshot for version `V`
- **THEN** the reader returns schema, protocol, properties, and active files as of `V`

#### Scenario: Time travel by timestamp
- **WHEN** requesting snapshot for timestamp `T`
- **THEN** the reader resolves the greatest `version` with `committed_at <= T` and returns that snapshot

