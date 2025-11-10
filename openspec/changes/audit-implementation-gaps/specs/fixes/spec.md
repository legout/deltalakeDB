## ADDED Requirements

### Requirement: Python pyo3 Integration
The system SHALL provide functional Python bindings for multi-table transactions using pyo3.

#### Scenario: Python context manager usage
- **WHEN** user calls `with deltalakedb.begin() as tx:`
- **THEN** context manager works correctly with auto-commit on exit or rollback on exception

#### Scenario: Python staging
- **WHEN** user calls `tx.write(table_id, df, mode="append")`
- **THEN** actions are staged and available for commit

#### Scenario: Python transaction commit
- **WHEN** context manager exits normally
- **THEN** transaction commits all staged tables atomically

### Requirement: URI Routing to Real Readers
The system SHALL route deltasql:// URIs to functional reader implementations.

#### Scenario: PostgreSQL URI routing
- **WHEN** user opens `deltasql://postgres/localhost/mydb/public/table`
- **THEN** returns functional PostgresReader (not mock)

#### Scenario: SQLite URI routing
- **WHEN** user opens `deltasql://sqlite//path/to/db.db?table=mytable`
- **THEN** returns functional SqliteReader (not mock)

#### Scenario: DuckDB URI routing
- **WHEN** user opens `deltasql://duckdb/:memory:?table=mytable`
- **THEN** returns functional DuckDbReader (not mock)

#### Scenario: Real data operations via URI
- **WHEN** reader created from URI performs get_latest_version()
- **THEN** actual data is read from database (not mocked)

### Requirement: Mirror Engine Implementation
The system SHALL actually mirror multi-table commits to _delta_log files.

#### Scenario: Spark mirroring
- **WHEN** multi-table transaction commits successfully
- **THEN** system writes _delta_log entries to object store for each table

#### Scenario: DuckDB mirroring
- **WHEN** multi-table transaction commits successfully
- **THEN** system registers tables in DuckDB catalog

#### Scenario: Mirror failure tracking
- **WHEN** mirroring to one engine fails
- **THEN** other engines continue and failure is tracked in dl_mirror_status

### Requirement: Delta Protocol Schema Compliance
The system SHALL include all required Delta protocol fields in schema.

#### Scenario: RemoveFile data_change field
- **WHEN** removing files from table
- **THEN** dl_remove_files has data_change field indicating data vs metadata removal

#### Scenario: Migration creates missing field
- **WHEN** migration runs on existing database
- **THEN** data_change field is added with correct default

### Requirement: Complete Python Staging
The system SHALL fully implement Python staging of actions.

#### Scenario: DataFrame conversion
- **WHEN** user stages DataFrame via Python API
- **THEN** DataFrame is converted to Actions and recorded

#### Scenario: Staged actions recorded
- **WHEN** transaction stages 3 tables
- **THEN** all staged actions are available for retrieval

### Requirement: Test Fixture Reliability
The system SHALL fail fast with clear error messages in tests.

#### Scenario: Missing database URL
- **WHEN** TEST_DATABASE_URL environment variable not set
- **THEN** test fails immediately with clear error message (not silent failure)

#### Scenario: Connection string validation
- **WHEN** test attempts to run
- **THEN** connection string is validated before test execution

### Requirement: Performance Assertion Accuracy
The system SHALL enforce actual performance targets in tests.

#### Scenario: Single-file commit latency
- **WHEN** performance test for single-file commit runs
- **THEN** assertion validates < 50ms (not lenient threshold)

#### Scenario: Throughput targets
- **WHEN** throughput test runs
- **THEN** assertion validates > 10 commits/sec and > 1000 files/sec

### Requirement: Consistent Error Types
The system SHALL use consistent error naming and hierarchy.

#### Scenario: Error type hierarchy
- **WHEN** error is generated in any module
- **THEN** error follows consistent type hierarchy with clear From trait implementations

#### Scenario: Error message consistency
- **WHEN** user encounters error
- **THEN** error message format is consistent across all modules

### Requirement: Specification-Implementation Alignment
The system SHALL have all implementations match their specifications.

#### Scenario: Python API functional
- **WHEN** user follows Python API specification
- **THEN** code works as documented without placeholder stubs

#### Scenario: URI routing functional
- **WHEN** user follows URI routing specification
- **THEN** real data operations work (not mocked)

#### Scenario: Mirror operations functional
- **WHEN** user follows mirror specification
- **THEN** actual files are created in _delta_log (not placeholder)

## MODIFIED Requirements

### Requirement: Multi-Table Transaction API
The existing system SHALL have all placeholder implementations replaced with functional code.

#### Scenario: Python API working
- **WHEN** Python API is used per add-multi-table-txn specification
- **THEN** all features work end-to-end (not placeholder stubs)

#### Scenario: Mirror integration complete
- **WHEN** multi-table transaction commits
- **THEN** actual mirroring occurs (not placeholder Ok(()))

## Acceptance Criteria

- All 8 issues fixed and tested
- 100% test pass rate
- No performance regression from original targets
- All specifications match implementations
- Code review approved
- Documentation updated
