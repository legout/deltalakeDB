## 1. Transaction API Design
- [ ] 1.1 Create `MultiTableTransaction` struct managing staged actions
- [ ] 1.2 Design API: `begin()`, `stage_table()`, `commit()`, `rollback()`
- [ ] 1.3 Support multiple table writes within transaction scope
- [ ] 1.4 Validate all table versions before commit

## 2. Staged Actions
- [ ] 2.1 Implement staging for add/remove/metadata actions per table
- [ ] 2.2 Store staged actions in memory with table_id mapping
- [ ] 2.3 Validate each table's actions independently
- [ ] 2.4 Prevent duplicate staging for same table

## 3. Atomic Commit
- [ ] 3.1 Open single database transaction for all tables
- [ ] 3.2 Validate versions for all tables with row locks
- [ ] 3.3 Insert actions for all tables within transaction
- [ ] 3.4 Update versions for all tables atomically
- [ ] 3.5 Commit transaction or rollback on any failure

## 4. Post-Commit Mirroring
- [ ] 4.1 Mirror each table's version to `_delta_log` after commit
- [ ] 4.2 Handle partial mirror failures without rolling back SQL
- [ ] 4.3 Mark failed mirrors for reconciliation
- [ ] 4.4 Emit metrics for cross-table mirror lag

## 5. Python API
- [ ] 5.1 Expose transaction API through pyo3 bindings
- [ ] 5.2 Implement context manager for `with deltars.begin()` pattern
- [ ] 5.3 Add `transaction.write(table, df, mode)` method
- [ ] 5.4 Provide clear error messages for Python users

## 6. Testing
- [ ] 6.1 Unit tests for transaction staging and validation
- [ ] 6.2 Integration tests with 2+ tables and real database
- [ ] 6.3 Test rollback on version conflict in second table
- [ ] 6.4 Test partial mirror failure handling
- [ ] 6.5 Test external reader consistency after multi-table commit
- [ ] 6.6 Concurrent multi-table transaction isolation tests

## 7. Documentation
- [ ] 7.1 Document multi-table transaction semantics
- [ ] 7.2 Add examples for common use cases (feature store, dimension+fact)
- [ ] 7.3 Explain consistency guarantees and caveats
- [ ] 7.4 Document mirror lag behavior for cross-table commits
