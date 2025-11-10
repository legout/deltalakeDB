# OpenSpec Proposal: Audit and Fix Implementation Gaps

## Executive Summary

Comprehensive code review comparing OpenSpec specifications against actual implementation found 8 significant discrepancies:

- **1 CRITICAL**: Python API (pyo3) not actually integrated - non-functional
- **2 HIGH**: Mirror placeholder implementation, URI routing returns mocks  
- **3 MEDIUM**: Python staging incomplete, schema field missing, test fixtures brittle
- **2 LOW**: Performance tests don't enforce SLOs, error types inconsistent

All issues are implementation gaps, not design flaws. Code architecture is sound.

---

## Background

The project delivered 7 complete OpenSpec proposals (175+ tasks, 11,000+ lines) in excellent time. However, final code review found gaps between what specifications promise and what code actually delivers. These gaps prevent production use in several areas.

**Goals of this proposal**:
1. Document all identified discrepancies
2. Create tasks to fix each issue
3. Add validation tests to prevent regression
4. Ensure specifications and implementation alignment

---

## Critical Issues

### Issue 1: Python pyo3 Bindings Not Integrated

**Spec Promise** (add-multi-table-txn Tasks 5.1-5.4):
- Expose transaction API through pyo3 bindings
- Implement context manager for `with deltars.begin()` pattern
- Provide clear error messages for Python users

**Current State**:
```rust
pub struct PyMultiTableTransaction {
    // No #[pyclass] attribute
    // No pyo3 integration
    // Cannot be called from Python
}
```

**Impact**: Users cannot use Python API at all. The module exists but is non-functional.

**Root Cause**: Python stubs were created but pyo3 integration was never done. Code is Rust types without Python bindings.

**Fix**: Implement actual pyo3 integration with proper attributes and module setup.

---

### Issue 2: Mirror Engine is Placeholder

**Spec Promise** (add-multi-table-txn Task 4.1):
- "Mirror each table's version to `_delta_log` after commit"
- Handle partial mirror failures without rolling back SQL
- Mark failed mirrors for reconciliation
- Emit metrics for cross-table mirror lag

**Current State**:
```rust
async fn mirror_spark(&self, location: &str, table_id: Uuid, version: i64) -> Result<(), String> {
    debug!("Mirroring table {} to Spark at {} (v{})", table_id, location, version);
    // For now, this is a successful no-op that can be tracked
    Ok(())  // ← Doesn't actually mirror
}
```

**Impact**: External readers (Spark, DuckDB) don't see any updates from multi-table transactions.

**Root Cause**: Mirror stubs created to satisfy compile but never implemented. Function returns success without doing anything.

**Fix**: Integrate with existing mirror engine implementation and actually write files.

---

### Issue 3: URI Routing Returns Mock Readers

**Spec Promise** (add-sql-uri-support Task 4.1-4.4):
- "Integration with DeltaTable API"
- URI parsing routes to correct backend
- Support PostgreSQL, SQLite, DuckDB

**Current State**:
```rust
// From crates/core/src/table.rs:230-244
match uri {
    DeltaSqlUri::Postgres(_) => {
        // TODO: Return PostgresSqlTxnLogReader once implementation is available
        Ok(Box::new(crate::mocks::MockTxnLogReader::new()) as Box<dyn TxnLogReader>)
    }
    DeltaSqlUri::Sqlite(_) => {
        // TODO: Return SqliteTxnLogReader once implementation is available
        Ok(Box::new(crate::mocks::MockTxnLogReader::new()) as Box<dyn TxnLogReader>)
    }
    DeltaSqlUri::DuckDb(_) => {
        // TODO: Return DuckDbTxnLogReader once implementation is available
        Ok(Box::new(crate::mocks::MockTxnLogReader::new()) as Box<dyn TxnLogReader>)
    }
}
```

**Impact**: Users can parse URIs but get mock readers instead of functional implementations. Code compiles but doesn't work.

**Root Cause**: URI parsing completed but never connected to actual reader implementations. Placeholder mocks left in place.

**Fix**: Create reader adapter implementations and wire up URI routing.

---

## High Priority Issues

### Issue 4: Missing Delta Protocol Field

**Spec Promise** (add-sql-schema-postgres):
- Faithful representation of Delta actions
- Support all Delta protocol fields

**Current State**: `dl_remove_files` table missing `data_change` boolean field required by Delta protocol.

**Schema Gap**:
```sql
CREATE TABLE dl_remove_files (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    file_path TEXT NOT NULL,
    deletion_timestamp BIGINT,
    -- MISSING: data_change BOOLEAN NOT NULL
    ...
)
```

**Impact**: Cannot correctly distinguish between data removal and compaction. Violates Delta protocol conformance.

**Fix**: Add schema migration for `data_change` field.

---

## Medium Priority Issues

### Issue 5: Python Staging Not Actually Staging

**Current State**:
```rust
pub async fn stage_table(&self, table_id: String, mode: String, action_count: usize) 
    -> Result<usize, String> {
    // Validation...
    
    // In a real implementation, we'd convert DataFrame to Actions
    // For now, return the count for validation
    Ok(action_count)  // ← Never actually stages!
}
```

**Impact**: Python staging appears to work but no actions are recorded.

---

### Issue 6: Test Database Credentials Handling

**Current State**: While fixed to avoid credentials in code, default connection string will fail silently:

```rust
.unwrap_or_else(|_| "postgresql://localhost/deltalakedb_test".to_string())
```

**Better Approach**: Fail fast with clear message.

---

### Issue 7: Performance Test Assertions Mismatched

**Issue**: Test says "target: > 10" but asserts "> 5":

```rust
assert!(single_cps > 5.0, "Single-file throughput below target: {:.2} commits/sec (target: > 10)", single_cps);
```

**Impact**: Tests pass even when performance targets aren't met.

---

## Low Priority Issues

### Issue 8: Error Type Inconsistency

Mix of `Error`, `TxnLogError`, `TransactionError` without clear hierarchy. Should standardize naming and conversion.

---

## Implementation Plan

### Phase 1: Critical Fixes (2-3 days)

**1.1 Python pyo3 Integration** (2 days)
- Add `pyo3 = "0.20"` to Cargo.toml
- Implement `#[pyclass]` and `#[pymethods]`
- Create Python module wrapper
- Add Python examples and documentation
- Update Cargo.toml for `cdylib` target

**1.2 URI Routing Implementation** (1.5 days)
- Create `PostgresReaderAdapter` wrapping actual reader
- Create `SqliteReaderAdapter`
- Create `DuckDbReaderAdapter`
- Wire up in `DeltaTable::open()`
- Add integration tests

### Phase 2: High Priority (2 days)

**2.1 Mirror Engine Integration** (1.5 days)
- Replace placeholder with actual mirror engine calls
- Generate `_delta_log` entries
- Write to object store
- Handle serialization

**2.2 Schema Migration** (0.5 days)
- Create migration for `data_change` field
- Update writer code
- Update reader code
- Add validation tests

### Phase 3: Medium Priority (1 day)

**3.1 Python Staging** (0.5 days)
- Accept DataFrame or actions parameter
- Implement actual staging

**3.2 Test Fixtures & Assertions** (0.5 days)
- Fix test database URL
- Update performance assertions

### Phase 4: Polish (0.5 day)

**4.1 Error Standardization** (0.5 days)
- Define error hierarchy
- Standardize names
- Document conversions

---

## Success Criteria

For each issue, verification will include:

✅ **Code**: Implementation matches specification  
✅ **Tests**: Comprehensive test coverage  
✅ **Docs**: Behavior documented  
✅ **Review**: Code review approval  
✅ **Integration**: Works with rest of system  

### Specific Milestones

- Python API: `with deltalakedb.begin() as tx: tx.write(table, df)` works end-to-end
- URI Routing: `DeltaTable::open("deltasql://postgres/...")` returns functional reader
- Mirroring: Multi-table commit creates actual `_delta_log` entries
- Schema: All Delta fields present and tested
- Performance: All assertions match stated targets

---

## Testing Strategy

### New Tests Required

1. **Python Integration Tests**
   - End-to-end multi-table transaction from Python
   - Context manager behavior
   - Error handling

2. **URI Routing Tests**
   - All backend types return functional readers
   - Actual data operations work

3. **Mirror Integration Tests**
   - Multi-table commit creates files
   - Partial failures handled correctly
   - Files are in correct format

4. **Schema Conformance**
   - All Delta protocol fields present
   - Remove actions have data_change field

5. **Performance Regression Tests**
   - Performance assertions actually enforce targets

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Python API changes | HIGH | Comprehensive Python tests before release |
| Mirror file format | HIGH | Use existing mirror engine tests as reference |
| Schema migration | MEDIUM | Test on staging database first, rollback plan |
| Performance regression | MEDIUM | Benchmark before/after each change |

---

## Timeline

**Total Duration**: 6-7 days

**Parallel Work**: Python and Mirror can be done independently

**Critical Path**: Python API (blocks users) → Mirror Integration (blocks external readers) → Schema fixes

**Validation**: 1-2 days for comprehensive testing

---

## Dependency Analysis

| Task | Dependencies | Blocks |
|------|-------------|--------|
| Python pyo3 | None | Python users |
| URI Routing | None | URI users |
| Mirror Integration | Mirror engine | External readers |
| Schema Migration | None | Delta conformance |
| Performance Tests | None | SLO validation |

---

## Documentation Updates Needed

1. **Python API Guide**: How to use context managers, examples
2. **URI Support**: Document routing behavior, examples
3. **Known Limitations**: Document any remaining placeholders
4. **Performance SLOs**: Clarify actual targets
5. **Schema**: Document all fields and their meaning

---

## Conclusion

The implementation delivered excellent architecture and comprehensive code, but has implementation gaps that prevent production use in critical areas:

**Blockers**:
- Python API non-functional
- URI routing returns mocks
- Mirror engine is placeholder

**Important but not blocking**:
- Schema fields missing
- Test assertions lenient
- Error types inconsistent

This proposal creates a clear path to fix these issues and ensure specifications match implementation, moving the project from "85% complete" to "production ready."
