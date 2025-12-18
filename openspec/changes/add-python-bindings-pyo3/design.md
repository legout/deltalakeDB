## Context

All 12 OpenSpec proposals implement a complete Rust core for SQL-backed Delta metadata. However, the Python package is a stub, making the system inaccessible to Python users—the primary UX layer per the PRD.

The project targets Python 3.12 as the UX entry point, with Rust providing performance via async I/O and tight DB integrations. This proposal bridges that gap by adding pyo3 bindings and a high-level Python API.

## Goals

- **Primary**: Expose full Rust core to Python 3.12 users with type-safe, documented API
- **Secondary**: Enable end-to-end integration tests and CLI access from Python
- **Non-Goals**: Async Python API (start with blocking calls into Rust; can add AsyncIO layer later)
- **Non-Goals**: Custom Python query language (SQL adapters stay in Rust)

## Decisions

### 1. Build System: maturin over pyo3-build

**Decision**: Use `maturin` as the build backend in `pyproject.toml`.

**Rationale**:
- Maturin handles Rust compilation, wheel building, and PyPI distribution seamlessly
- Simpler than manual pyo3 setup and custom build scripts
- Standard tool in the PyO3 ecosystem
- Supports both local `pip install -e .` and publishing to PyPI

**Alternatives Considered**:
- Raw pyo3 with custom build.py: More control but higher maintenance
- Setuptools-rust: Older, less actively maintained than maturin

### 2. API Design: Blocking (Sync) First

**Decision**: Expose a synchronous Python API that internally calls async Rust code on a Tokio runtime.

**Rationale**:
- Simpler mental model for Python users initially
- Can wrap Rust async functions with `pyo3::prelude::block_on` or equivalent
- Easier to implement and test
- Can add AsyncIO support later if demand exists

**Trade-offs**:
- Python threads will not block Tokio runtime (acceptable for typical use cases)
- No true concurrency from Python; OK for MVP

**Future**: Add `async def` support with `pyo3_asyncio` or similar if needed

### 3. Error Handling: Rich Python Exceptions

**Decision**: Map Rust error types (via `thiserror`) to custom Python exception classes.

**Rationale**:
- Gives Python developers clear, actionable error messages
- Allows except clauses like `except ConcurrencyError as e`
- Preserves error context and diagnostic info from Rust

**Implementation**:
- Rust `enum Error { Concurrency(...), Connection(...), ...}`
- pyo3 bindings convert to Python `ConcurrencyError`, `ConnectionError` classes (subclass `Exception`)
- Include helpful messages with suggestions (e.g., "Got version X, expected Y; table may have been modified")

### 4. Type System: PEP 484 + Type Stubs

**Decision**: Add `.pyi` type stub files for all public APIs and ensure mypy strict compliance.

**Rationale**:
- Python IDE (PyCharm, VS Code) can provide autocomplete and type checking
- `mypy --strict` can validate user code without runtime overhead
- Bindings already carry type info; stubs make it visible to tooling

**Implementation**:
- Generate or hand-write `src/delkalakedb/__init__.pyi`
- Each class and function includes docstring and type hints
- Run `mypy --strict` on both bindings and test code

### 5. Connection Pooling & Lifecycle

**Decision**: `DeltaSQL` wraps a Rust connection pool managed by sqlx.

**Rationale**:
- Reuses connection management already in Rust adapters
- Thread-safe: Python GIL release during Rust calls allows concurrent Python threads
- User creates one `DeltaSQL` per database and reuses across operations

**Lifecycle**:
- `conn = DeltaSQL(uri)` → opens pool
- `table = conn.open_table(...)` → acquires connection from pool
- Pool cleanup on `__del__` or context manager exit

**Future**: Context manager support (`with DeltaSQL(...) as conn:`)

### 6. Multi-Table Transaction Staging

**Decision**: `TransactionBuilder` is a stateful Python class that stages Rust actions.

**Rationale**:
- Clear, imperative API: stage actions per table, then commit atomically
- Mirrors Rust `TransactionBuilder` design
- Allows inspection before commit

**Design**:
```python
builder = conn.transaction_builder()
builder.add_table_actions(table_id_a, [...actions...])
builder.add_table_actions(table_id_b, [...actions...])
result = builder.commit({table_a_id: expected_version_a, table_b_id: expected_version_b})
```

### 7. Time-Travel API

**Decision**: Method chaining with fluent API (`table.version(v).snapshot()`).

**Rationale**:
- Composable, readable: `table.at_timestamp("2024-12-18").snapshot()`
- Aligns with Rust builder patterns
- Easy to extend

**Alternatives Considered**:
- Separate `open_version(table, v)` functions: less ergonomic
- Kwargs: `table.snapshot(version=v)` - works but less composable

### 8. Observability Integration

**Decision**: Metrics and tracing automatically collected on Rust side; Python can optionally export via OpenTelemetry.

**Rationale**:
- Observability is built into Rust adapters (no extra work in Python)
- Python doesn't need to know about internals; just works
- Can integrate with standard Python telemetry libraries

**Future**: Add `delkalakedb.telemetry` module for exporting metrics to Prometheus, DataDog, etc.

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| GIL contention if Python threads wait on Rust I/O | Release GIL in pyo3 bindings; document threading model; add async API later if needed |
| Type stub drift from implementation | Enforce in CI: generate stubs from bindings or hand-maintain with tests |
| Wheel build complexity across platforms | Use maturin; test on CI (Linux, macOS, Windows) |
| Large binary size | pyo3 wheels are typically 5–20 MB; acceptable for data tools |
| Missing Rust API coverage | Define minimum viable API (DeltaSQL, Table, Transaction); expand incrementally |

## Migration Plan

1. **Week 1**: Add pyo3 to Cargo.toml, set up maturin in pyproject.toml, create binding scaffolds
2. **Week 2**: Implement core type bindings and error handling
3. **Week 3**: Implement DeltaSQL, Table, Snapshot classes
4. **Week 4**: Implement Transaction and TransactionBuilder
5. **Week 5**: Add tests, validate mypy, document
6. **Week 6**: Polish, CI integration, release

## Open Questions

1. Should we support async Python from day one, or defer?
   - **Answer**: Defer. Start with blocking; add pyo3_asyncio later if users request it.

2. Should TransactionBuilder support rollback?
   - **Answer**: No; Rust side already has idempotent commit semantics. Staging = dry-run; commit is final.

3. Should Python API be in a separate crate or the CLI crate?
   - **Answer**: Separate `crates/pyo3_bindings/` or new `crates/python/` to keep concerns clean.

4. CLI entry point or manual import?
   - **Answer**: Support both: `python -m delkalakedb import ...` (wrapper) and `python -c "import delkalakedb; ..."`

