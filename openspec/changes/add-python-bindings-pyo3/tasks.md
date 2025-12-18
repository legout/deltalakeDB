## 1. Foundation & Setup

- [x] 1.1 Add `pyo3` and `maturin` dependencies to `Cargo.toml`
- [x] 1.2 Configure `pyproject.toml` to use `maturin` as build backend
- [x] 1.3 Create `crates/pyo3_bindings/` crate (or repurpose existing) as Python binding surface
- [x] 1.4 Ensure all Rust core crates re-export public types and traits

## 2. Type Bindings & Core API

- [x] 2.1 Create pyo3 bindings for error types (`ConcurrencyError`, `ConnectionError`, etc.)
- [x] 2.2 Bind URI types (`DeltasqlUri`, `DeltasqlEngine`) 
- [x] 2.3 Bind schema types (`TableMetadata`, `Protocol`, `ActiveFile`, `RemovedFile`)
- [ ] 2.4 Bind action types (`DeltaAction` and variants) (defer to Phase 5+)
- [x] 2.5 Create Python `__init__.pyi` type stub file for IDE/mypy support

## 3. High-Level Python API

- [x] 3.1 Implement `DeltaSQL` class wrapping connection logic
  - [x] 3.1a Add `__init__(uri: str)` constructor
  - [x] 3.1b Add `open_table(name: str) -> Table` method
  - [x] 3.1c Add `list_tables(schema: str | None) -> List[str]` method (stub)
  - [ ] 3.1d Add async support (optional for MVP, or use blocking Rust)
- [x] 3.2 Implement `Table` class wrapping read operations
  - [x] 3.2a Add `snapshot() -> Snapshot` method (stub)
  - [x] 3.2b Add `version(v: int) -> Table` for version time-travel
  - [x] 3.2c Add `at_timestamp(ts: str) -> Table` for timestamp time-travel
  - [x] 3.2d Add properties: `current_version`, `location` (stubs)
- [x] 3.3 Implement `Snapshot` class
  - [x] 3.3a Add `version: int` property
  - [x] 3.3b Add `files() -> Iterator[ActiveFile]` method
  - [x] 3.3c Add properties
  - [x] 3.3d Add `protocol: Protocol` property
  - [x] 3.3e Add `metadata: TableMetadata` property
- [x] 3.4 Implement `Transaction` class for single-table writes
  - [x] 3.4a Add `add_file(path, size, modification_time, ...)` method (stub)
  - [x] 3.4b Add `remove_file(path)` method (stub)
  - [x] 3.4c Add `set_metadata(metadata)` method
  - [x] 3.4d Add `commit() -> Version` method (stub)
- [x] 3.5 Implement `TransactionBuilder` for multi-table writes
  - [x] 3.5a Add `add_table_actions(table_id, actions)` method (stub)
  - [x] 3.5b Add `staged_tables() -> List[UUID]` method (stub)
  - [x] 3.5c Add `commit() -> Dict[UUID, Version]` method (stub)

## 4. Integration & Error Handling

- [x] 4.1 Map Rust error types to Python exceptions
- [x] 4.2 Add comprehensive error messages with troubleshooting hints (stub messages in place)
- [x] 4.3 Implement `__repr__` and `__str__` for user-facing types
- [x] 4.4 Add docstrings to all public classes and methods

## 5. Testing

- [ ] 5.1 Write unit tests for type bindings
- [ ] 5.2 Write integration tests covering:
  - [ ] 5.2a Open table and read snapshot (Postgres, SQLite, DuckDB)
  - [ ] 5.2b Time travel by version and timestamp
  - [ ] 5.2c Single-table commit
  - [ ] 5.2d Multi-table transaction
  - [ ] 5.2e Concurrency conflict handling
  - [ ] 5.2f Error cases (invalid URI, connection failure, etc.)
- [ ] 5.3 Run `mypy --strict` over all Python code and bindings
- [ ] 5.4 Run `pytest` with >80% code coverage

## 6. Documentation & CLI

- [x] 6.1 Add docstrings to all public APIs
- [x] 6.2 Create `README_PYTHON.md` with usage examples
- [ ] 6.3 Expose `dl import` CLI as a Python entry point (`python -m delkalakedb import ...`)
- [x] 6.4 Add type stub file for IDE autocomplete

## 7. Validation

- [ ] 7.1 Verify round-trip: write via Python, read via Rust, verify equality
- [ ] 7.2 Verify multi-table atomicity with concurrent test
- [ ] 7.3 Verify `_delta_log` mirroring after Python-initiated commits
- [ ] 7.4 Verify observability metrics emit during Python operations

## Dependencies

- Depends on all 12 active proposals (they provide the Rust core)
- No new external dependencies required (pyo3/maturin are build-time only)

