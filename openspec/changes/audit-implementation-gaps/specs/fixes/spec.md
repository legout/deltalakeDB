# Implementation Gaps Fix Specification

## Overview

This specification documents the fixes required to align implementation with OpenSpec specifications. 8 issues across multiple modules need correction to achieve production readiness.

## Issue Categories

### 1. Python API Integration (Affects: add-multi-table-txn)

**Module**: `crates/core/src/python.rs`

**Current State**: Rust structures without pyo3 integration

**Fix**:
- Add `#[pyclass]` to `PyMultiTableTransaction`
- Add `#[pymethods]` with public methods
- Implement `__enter__` and `__exit__` for context manager
- Create Python module initialization
- Add `cdylib` target to Cargo.toml

**Result**: Python API becomes functional with context manager support

### 2. URI Routing (Affects: add-sql-uri-support)

**Module**: `crates/core/src/table.rs`

**Current State**: Routing returns `MockTxnLogReader` for all backends

**Fix**:
- Create `PostgresReaderAdapter` wrapping actual `PostgresReader`
- Create `SqliteReaderAdapter` for SQLite
- Create `DuckDbReaderAdapter` for DuckDB
- Wire adapters in `DeltaTable::open()` 
- Remove mock reader placeholders

**Result**: URI routing returns functional readers for all backends

### 3. Mirror Engine (Affects: add-multi-table-txn)

**Module**: `crates/sql-metadata-postgres/src/multi_table_writer.rs`

**Current State**: `mirror_spark()` and `mirror_duckdb()` return `Ok(())` without mirroring

**Fix**:
- Call actual mirror engine from `crates/mirror`
- Generate `_delta_log` entries per Delta protocol
- Write files to object store (S3)
- Handle serialization correctly
- Track mirror status in database

**Result**: Multi-table commits actually mirror to external systems

### 4. Schema Compliance (Affects: add-sql-schema-postgres)

**Module**: SQL migrations

**Current State**: `dl_remove_files` missing `data_change` boolean field

**Fix**:
- Create migration adding `data_change BOOLEAN NOT NULL DEFAULT TRUE`
- Update `PostgresWriter` to populate field
- Update `PostgresReader` to read field
- Add validation tests

**Result**: Schema fully complies with Delta protocol

### 5. Python Staging (Affects: add-multi-table-txn)

**Module**: `crates/core/src/python.rs`

**Current State**: `stage_table()` validates but doesn't stage

**Fix**:
- Accept DataFrame or actions as parameter
- Convert DataFrame to Actions
- Call `tx.stage_table()` on inner transaction
- Actually record actions

**Result**: Python staging works end-to-end

### 6. Test Fixtures (Affects: All test suites)

**Module**: `crates/sql-metadata-postgres/tests/fixtures/mod.rs`

**Current State**: Default connection string fails silently

**Fix**:
- Use `expect()` instead of `unwrap_or_else()`
- Provide clear error message
- Document test database setup

**Result**: Test failures are clear and actionable

### 7. Performance Assertions (Affects: add-sql-writer-postgres, add-sql-reader-postgres)

**Module**: `crates/sql-metadata-postgres/tests/` test files

**Current State**: Assertions don't match stated targets

**Fix**:
- Update assertions to actual targets
- Document any SLO changes
- Ensure targets are realistic

**Result**: Performance tests enforce actual SLOs

### 8. Error Types (Affects: All modules)

**Module**: Across codebase

**Current State**: Inconsistent error naming (`Error`, `TxnLogError`, `TransactionError`)

**Fix**:
- Define clear error hierarchy
- Use consistent naming convention
- Implement `From` trait for conversions
- Document error handling strategy

**Result**: Consistent, predictable error handling

## Testing Strategy

### For Each Fix
1. **Unit Tests**: Test fix in isolation
2. **Integration Tests**: Test with real database
3. **Conformance Tests**: Verify against Delta protocol
4. **Regression Tests**: Ensure no performance degradation

### New Test Files
- `python_integration_tests.rs` (or `.py`)
- `uri_routing_integration_tests.rs`
- `mirror_integration_tests.rs`
- `schema_conformance_tests.rs`

## Validation Checklist

- [ ] All 54 tasks completed
- [ ] 100% test pass rate
- [ ] No performance regression
- [ ] All specifications match implementation
- [ ] Code review approval
- [ ] Documentation updated
- [ ] Staging/production deployment tested

## Success Criteria

✅ Python API: `with deltalakedb.begin() as tx:` works  
✅ URI Routing: All backends functional  
✅ Mirroring: Actual files created  
✅ Schema: All Delta fields present  
✅ Tests: All assertions enforce targets  
✅ Documentation: All features documented  

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Python API breaking changes | HIGH | Comprehensive Python tests |
| Mirror format issues | HIGH | Reference existing tests |
| Schema migration on prod | MEDIUM | Staging test first |
| Performance regression | MEDIUM | Benchmark before/after |

## Timeline

- Phase 1: 2-3 days
- Phase 2: 2 days
- Phase 3: 1 day
- Phase 4: 0.5 days
- Testing: 1-2 days

**Total**: 6-7 days to full production readiness
