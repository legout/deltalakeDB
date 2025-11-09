## ADDED Requirements
### Requirement: DeltaSQL URI Scheme
The system SHALL accept a `deltasql://` URI scheme to identify SQL-backed Delta tables and the target database engine.

#### Scenario: Postgres URI
- **WHEN** opening `deltasql://postgres/mydb/public/mytable`
- **THEN** the system connects to Postgres database `mydb`, schema `public`, table `mytable`

#### Scenario: SQLite URI
- **WHEN** opening `deltasql://sqlite//var/data/meta.db?table=mytable`
- **THEN** the system opens SQLite at `/var/data/meta.db` and targets table `mytable`

#### Scenario: DuckDB URI
- **WHEN** opening `deltasql://duckdb//var/data/catalog.duckdb?table=mytable`
- **THEN** the system opens DuckDB file `/var/data/catalog.duckdb` and targets table `mytable`

#### Scenario: Invalid URI error
- **WHEN** a `deltasql://` URI is missing a table identifier or uses an unknown engine
- **THEN** the system returns a clear error describing the required URI forms

