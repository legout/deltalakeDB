## 1. Writer Implementation
- [ ] 1.1 Create `PostgresWriter` struct with sqlx transaction support
- [ ] 1.2 Implement `begin_commit()` opening database transaction
- [ ] 1.3 Implement `validate_version()` checking current_version + 1 with row lock
- [ ] 1.4 Implement `write_actions()` inserting into action tables
- [ ] 1.5 Implement `finalize_commit()` committing transaction and updating version

## 2. Optimistic Concurrency Control
- [ ] 2.1 Use `SELECT ... FOR UPDATE` or CAS pattern on version column
- [ ] 2.2 Detect concurrent writer conflicts and return clear error
- [ ] 2.3 Ensure version increments by exactly one per commit
- [ ] 2.4 Test concurrent writer scenarios with isolation

## 3. Action Insertion
- [ ] 3.1 Insert add file actions into `dl_add_files` with stats
- [ ] 3.2 Insert remove file actions into `dl_remove_files`
- [ ] 3.3 Insert metadata updates into `dl_metadata_updates`
- [ ] 3.4 Insert protocol updates into `dl_protocol_updates`
- [ ] 3.5 Insert transaction actions into `dl_txn_actions`
- [ ] 3.6 Use batch inserts for multiple files in single commit

## 4. Idempotency and Safety
- [ ] 4.1 Ensure duplicate commit attempts are safely rejected
- [ ] 4.2 Validate action schemas before insertion
- [ ] 4.3 Handle rollback on validation failure
- [ ] 4.4 Prevent partial commits on errors

## 5. Testing
- [ ] 5.1 Unit tests for each writer method
- [ ] 5.2 Integration tests with real PostgreSQL
- [ ] 5.3 Concurrent writer conflict tests
- [ ] 5.4 Large commit tests (1000+ files)
- [ ] 5.5 Rollback and error recovery tests
- [ ] 5.6 Conformance tests ensuring SQL and file-based equivalence

## 6. Performance
- [ ] 6.1 Profile commit latency for various payload sizes
- [ ] 6.2 Optimize batch inserts with COPY or multi-row INSERT
- [ ] 6.3 Measure and document commit throughput
