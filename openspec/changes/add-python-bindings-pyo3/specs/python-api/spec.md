## ADDED Requirements

### Requirement: Python Type System and Imports
The Python package `delkalakedb` SHALL expose core Rust types via pyo3 bindings with full type hints compatible with PEP 484 and mypy strict mode.

#### Scenario: Import core types
- **WHEN** importing `from delkalakedb import DeltaSQL, Table, Transaction`
- **THEN** each type is available with documented docstrings and type stubs

#### Scenario: Type checking
- **WHEN** running `mypy --strict` over user code calling delkalakedb
- **THEN** all type assertions pass with no errors or `# type: ignore` required for valid usage

### Requirement: DeltaSQL Connection and Table Discovery
The system SHALL provide a `DeltaSQL` class that connects to a SQL metadata backend and discovers tables.

#### Scenario: Connect to Postgres
- **WHEN** creating `conn = DeltaSQL("deltasql://postgres://user:pass@localhost/mydb")`
- **THEN** the connection establishes without error and is ready for table operations

#### Scenario: Connect to SQLite
- **WHEN** creating `conn = DeltaSQL("deltasql://sqlite:///path/to/db.sqlite")`
- **THEN** the connection establishes and opens the SQLite database

#### Scenario: List tables
- **WHEN** calling `conn.list_tables()` or `conn.list_tables(schema="myschema")`
- **THEN** returns a list of `Table` names discoverable in the catalog

### Requirement: Table Read Operations
The system SHALL provide a `Table` class supporting snapshot and time-travel reads.

#### Scenario: Open table and get snapshot
- **WHEN** calling `table = conn.open_table("my_table")` or `table.snapshot()`
- **THEN** returns metadata including current version, schema, protocol, and active files

#### Scenario: Time travel by version
- **WHEN** calling `table.version(v).snapshot()`
- **THEN** returns the snapshot (schema, files) as of version `v`

#### Scenario: Time travel by timestamp
- **WHEN** calling `table.at_timestamp("2024-12-18T10:00:00Z").snapshot()`
- **THEN** returns the snapshot at the given timestamp (resolved to nearest committed version)

#### Scenario: Iterate active files
- **WHEN** calling `for file in table.snapshot().files()`
- **THEN** yields `ActiveFile` objects with path, size, modification_time, and action details

### Requirement: Single-Table Write Operations
The system SHALL provide a `Transaction` class for single-table commits.

#### Scenario: Stage and commit actions
- **WHEN** creating `txn = conn.begin_transaction()` and calling `txn.add_file(...); txn.commit()`
- **THEN** actions are persisted atomically and new version is assigned

#### Scenario: Time travel after commit
- **WHEN** committing a transaction and then calling `table.version(new_v).snapshot()`
- **THEN** the new version reflects the committed actions

### Requirement: Multi-Table Transaction Operations
The system SHALL provide transaction staging and atomic multi-table commits.

#### Scenario: Stage actions for multiple tables
- **WHEN** creating `builder = conn.transaction_builder()` and calling `builder.add_table_actions(...)`
- **THEN** actions are staged per table_id without committing

#### Scenario: Atomic multi-table commit
- **WHEN** calling `builder.commit()` with actions for tables A, B, C
- **THEN** all tables are updated atomically (all or nothing) or a `ConcurrencyError` is raised if any table head mismatches

### Requirement: Configuration and Error Handling
The system SHALL provide consistent error types and connection configuration.

#### Scenario: Connection error
- **WHEN** connecting to an unreachable database
- **THEN** raises a clear `ConnectionError` with diagnostic message

#### Scenario: Concurrency conflict
- **WHEN** committing with a mismatched `expected_version`
- **THEN** raises a `ConcurrencyError` with details (actual vs. expected)

#### Scenario: Invalid URI
- **WHEN** creating `DeltaSQL("deltasql://invalid_scheme://...")`
- **THEN** raises a `ValueError` with schema guidance

