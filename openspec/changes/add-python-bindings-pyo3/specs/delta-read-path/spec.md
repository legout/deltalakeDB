## MODIFIED Requirements

### Requirement: SQL Read Path (Postgres)
The system SHALL provide a `SqlTxnLogReader` for Postgres that reconstructs table snapshots solely from SQL data. The reader MUST be accessible from both Rust and Python via type bindings.

#### Scenario: Table open (Rust)
- **WHEN** opening a table backed by Postgres schema in Rust
- **THEN** the reader returns the latest committed version and active files derived from `dl_add_files` minus `dl_remove_files`

#### Scenario: Table open (Python)
- **WHEN** opening a table backed by Postgres via `table = conn.open_table("my_table")`
- **THEN** the Python Table object wraps the Rust reader and returns metadata

#### Scenario: Time travel by version
- **WHEN** requesting snapshot for version `V` (Rust or Python)
- **THEN** the reader returns schema, protocol, properties, and active files as of `V`

#### Scenario: Time travel by timestamp
- **WHEN** requesting snapshot for timestamp `T` (Rust or Python)
- **THEN** the reader resolves the greatest `version` with `committed_at <= T` and returns that snapshot

### Requirement: SQL Read Path (SQLite)
The system SHALL provide a `SqlTxnLogReader` for SQLite, accessible from Rust and Python.

#### Scenario: Table open (SQLite, Rust or Python)
- **WHEN** opening a table backed by SQLite schema
- **THEN** the reader returns the latest committed version and active files

#### Scenario: Time travel (SQLite, Rust or Python)
- **WHEN** requesting snapshot by version or timestamp
- **THEN** the reader returns the snapshot as of that point in time

### Requirement: SQL Read Path (DuckDB)
The system SHALL provide a `SqlTxnLogReader` for DuckDB, accessible from Rust and Python.

#### Scenario: Table open (DuckDB, Rust or Python)
- **WHEN** opening a table backed by DuckDB schema
- **THEN** the reader returns the latest committed version and active files

#### Scenario: Time travel (DuckDB, Rust or Python)
- **WHEN** requesting snapshot by version or timestamp
- **THEN** the reader returns the snapshot as of that point in time

