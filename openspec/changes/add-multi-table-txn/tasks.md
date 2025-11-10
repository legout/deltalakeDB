## 1. Transaction API Design
- [x] 1.1 Create `MultiTableTransaction` struct managing staged actions
- [x] 1.2 Design API: `begin()`, `stage_table()`, `commit()`, `rollback()`
- [x] 1.3 Support multiple table writes within transaction scope
- [x] 1.4 Validate all table versions before commit

## 2. Staged Actions
- [x] 2.1 Implement staging for add/remove/metadata actions per table
- [x] 2.2 Store staged actions in memory with table_id mapping
- [x] 2.3 Validate each table's actions independently
- [x] 2.4 Prevent duplicate staging for same table

## 3. Atomic Commit
- [x] 3.1 Open single database transaction for all tables
- [x] 3.2 Validate versions for all tables with row locks
- [x] 3.3 Insert actions for all tables within transaction
- [x] 3.4 Update versions for all tables atomically
- [x] 3.5 Commit transaction or rollback on any failure

## 4. Post-Commit Mirroring
- [x] 4.1 Mirror each table's version to `_delta_log` after commit
- [x] 4.2 Handle partial mirror failures without rolling back SQL
- [x] 4.3 Mark failed mirrors for reconciliation
- [x] 4.4 Emit metrics for cross-table mirror lag

## 5. Python API
- [x] 5.1 Expose transaction API through pyo3 bindings
- [x] 5.2 Implement context manager for `with deltars.begin()` pattern
- [x] 5.3 Add `transaction.write(table, df, mode)` method
- [x] 5.4 Provide clear error messages for Python users

## 6. Testing
- [x] 6.1 Unit tests for transaction staging and validation
- [x] 6.2 Integration tests with 2+ tables and real database
- [x] 6.3 Test rollback on version conflict in second table
- [x] 6.4 Test partial mirror failure handling
- [x] 6.5 Test external reader consistency after multi-table commit
- [x] 6.6 Concurrent multi-table transaction isolation tests

## 7. Documentation
- [x] 7.1 Document multi-table transaction semantics
- [x] 7.2 Add examples for common use cases (feature store, dimension+fact)
- [x] 7.3 Explain consistency guarantees and caveats
- [x] 7.4 Document mirror lag behavior for cross-table commits
