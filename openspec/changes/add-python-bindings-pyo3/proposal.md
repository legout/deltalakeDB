## Why

The Rust core (`crates/sql`, `crates/mirror`, etc.) is fully implemented, but Python users cannot access any of it. The `src/delkalakedb/` package is a stub with only a `hello()` function. This blocks:
- End-to-end integration tests
- User-facing API validation  
- Migration CLI accessibility from Python
- Multi-table transaction staging from Python
- Time-travel reads from Python applications

Per the PRD and project.md, Python 3.12 is the primary UX layer, with Rust providing the fast core via pyo3 bindings.

## What Changes

- **pyo3 integration** in `Cargo.toml` and crate manifests to expose Rust types to Python
- **High-level Python API** in `src/delkalakedb/` wrapping Rust:
  - `DeltaSQL` connection/table discovery
  - `Table` class for reading snapshots and time-traveling
  - `Transaction` and `MultiTableTransaction` for writes
  - `TransactionBuilder` for staging multi-table actions
- **Type hints** (PEP 484) with `mypy` strict validation
- **CLI entry points** accessible from Python (`dl import` etc.)
- **Integration tests** demonstrating end-to-end workflows

## Impact

- Affected specs: 
  - `python-api` (NEW)
  - `delta-read-path` (MODIFIED - add Python usage)
  - `delta-write-path` (MODIFIED - add Python usage)
  - `multi-table-acid` (MODIFIED - add Python usage)
  - `observability` (MODIFIED - Python metrics)
- Affected code: 
  - `Cargo.toml` (add pyo3 feature)
  - `crates/*` (minimal: export public API)
  - `src/delkalakedb/` (full rewrite)
  - `pyproject.toml` (update build backend to maturin)

## References

- PRD §6 Stack – Python 3.12 UX layer, Rust core via pyo3
- PRD §11 API & UX – Example URIs and Python usage
- Project.md §13-14 – Build/packaging via pyo3 + maturin
- Project.md §31 Extensibility – Engine adapters registered by scheme

