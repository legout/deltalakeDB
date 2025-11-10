# OpenSpec Tasks: Audit and Fix Implementation Gaps

## Phase 1: Critical Fixes

### 1. Python pyo3 Integration (2 days)

- [ ] 1.1 Add pyo3 dependency to Cargo.toml with `cdylib` target
- [ ] 1.2 Implement #[pyclass] and #[pymethods] for PyMultiTableTransaction
- [ ] 1.3 Implement __enter__ and __exit__ for context manager support
- [ ] 1.4 Create Python module initialization and __init__.py
- [ ] 1.5 Add Python examples showing context manager usage
- [ ] 1.6 Add comprehensive Python integration tests
- [ ] 1.7 Document Python API in guides

### 2. URI Routing - Reader Adapters (1.5 days)

- [ ] 2.1 Create PostgresReaderAdapter wrapping actual PostgresReader
- [ ] 2.2 Create SqliteReaderAdapter for SQLite backend
- [ ] 2.3 Create DuckDbReaderAdapter for DuckDB backend
- [ ] 2.4 Wire up adapters in DeltaTable::open() based on URI
- [ ] 2.5 Remove TODO comments and mock reader placeholders
- [ ] 2.6 Add integration tests for URI routing to real readers
- [ ] 2.7 Verify with end-to-end test: parse URI → read table

## Phase 2: High Priority Fixes

### 3. Mirror Engine Integration (1.5 days)

- [ ] 3.1 Replace mirror_spark() placeholder with actual implementation
- [ ] 3.2 Replace mirror_duckdb() placeholder with actual implementation
- [ ] 3.3 Integrate with existing mirror engine from crates/mirror
- [ ] 3.4 Generate _delta_log entries after multi-table commit
- [ ] 3.5 Handle object store (S3) writes
- [ ] 3.6 Add mirror integration tests
- [ ] 3.7 Verify files created in correct format

### 4. Schema Migration - data_change Field (0.5 days)

- [ ] 4.1 Create new migration adding data_change field to dl_remove_files
- [ ] 4.2 Update PostgresWriter to set data_change on remove actions
- [ ] 4.3 Update PostgresReader to read data_change field
- [ ] 4.4 Add schema conformance tests for Delta protocol
- [ ] 4.5 Verify RemoveFile actions now have required field

## Phase 3: Medium Priority Fixes

### 5. Python Staging Implementation (0.5 days)

- [ ] 5.1 Update PyMultiTableTransaction::stage_table() to accept DataFrame or actions
- [ ] 5.2 Implement DataFrame → Actions conversion
- [ ] 5.3 Actually call tx.stage_table() instead of just validating
- [ ] 5.4 Add tests verifying staged actions are recorded

### 6. Test Fixtures - Fail Fast (0.5 days)

- [ ] 6.1 Update get_test_database_url() to use expect() instead of unwrap_or_else()
- [ ] 6.2 Add clear error message for missing TEST_DATABASE_URL
- [ ] 6.3 Add documentation for setting up test database

### 7. Performance Test Assertions (0.5 days)

- [ ] 7.1 Update test_commit_latency_single_file to assert < 50ms (not lenient)
- [ ] 7.2 Update test_throughput_targets to assert > 10 commits/sec (not > 5)
- [ ] 7.3 Update all performance tests to match stated SLOs
- [ ] 7.4 Document any SLO changes with rationale

## Phase 4: Polish

### 8. Error Type Standardization (0.5 days)

- [ ] 8.1 Define clear error hierarchy (TxnLogError as base)
- [ ] 8.2 Rename TransactionError → TxnLogTransactionError for consistency
- [ ] 8.3 Implement From trait for error conversions
- [ ] 8.4 Update all modules to use standardized types
- [ ] 8.5 Document error handling strategy

### 9. Documentation Updates (1 day)

- [ ] 9.1 Add Python API guide with context manager examples
- [ ] 9.2 Add URI routing documentation
- [ ] 9.3 Update schema documentation with all fields
- [ ] 9.4 Document known limitations (if any remain)
- [ ] 9.5 Add troubleshooting section for common issues

### 10. Comprehensive Testing (1-2 days)

- [ ] 10.1 End-to-end test: Python context manager with multi-table transaction
- [ ] 10.2 End-to-end test: URI routing for all backends
- [ ] 10.3 End-to-end test: Multi-table commit with mirroring
- [ ] 10.4 End-to-end test: Schema conformance with all Delta fields
- [ ] 10.5 Performance regression tests
- [ ] 10.6 Concurrency tests with all fixes in place
- [ ] 10.7 Error handling tests for all code paths

## Summary

- **Total Tasks**: 51 tasks
- **Total Effort**: 6-7 days
- **Critical Path**: Tasks 1-2 → 3-4 → 5-7 → 8-10
- **Parallelizable**: Tasks 1-2 can be done in parallel

## Success Metrics

✅ All 51 tasks completed  
✅ 100% test pass rate  
✅ No regressions in performance  
✅ All specifications match implementation  
✅ Production-ready code quality  
