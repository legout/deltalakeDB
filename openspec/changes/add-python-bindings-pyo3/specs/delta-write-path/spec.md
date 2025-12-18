## MODIFIED Requirements

### Requirement: SQL Write Path (Postgres)
The system SHALL provide a `SqlTxnLogWriter` for Postgres accessible from both Rust and Python.

#### Scenario: Commit from Rust
- **WHEN** committing actions in Rust via `SqlTxnLogWriter::commit()`
- **THEN** actions are persisted atomically with optimistic concurrency (CAS) and mirror status enqueued

#### Scenario: Commit from Python
- **WHEN** calling `transaction.commit()` from Python
- **THEN** the Python binding marshals actions to Rust and invokes the writer atomically

#### Scenario: Concurrency error handling (Rust)
- **WHEN** CAS fails due to unexpected version
- **THEN** returns a `ConcurrencyError` indicating the conflict

#### Scenario: Concurrency error handling (Python)
- **WHEN** CAS fails on a Python-initiated commit
- **THEN** raises a Python `ConcurrencyError` with details

