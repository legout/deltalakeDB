## 1. Writer Implementation
- [x] 1.1 Create `PostgresWriter` struct with sqlx transaction support
- [x] 1.2 Implement `begin_commit()` opening database transaction
- [x] 1.3 Implement `validate_version()` checking current_version + 1 with row lock
- [x] 1.4 Implement `write_actions()` inserting into action tables
- [x] 1.5 Implement `finalize_commit()` committing transaction and updating version

## 2. Optimistic Concurrency Control
- [x] 2.1 Use `SELECT ... FOR UPDATE` or CAS pattern on version column
- [x] 2.2 Detect concurrent writer conflicts and return clear error
- [x] 2.3 Ensure version increments by exactly one per commit
- [ ] 2.4 Test concurrent writer scenarios with isolation (pending: integration test setup)

## 3. Action Insertion
- [x] 3.1 Insert add file actions into `dl_add_files` with stats
- [x] 3.2 Insert remove file actions into `dl_remove_files`
- [x] 3.3 Insert metadata updates into `dl_metadata_updates`
- [x] 3.4 Insert protocol updates into `dl_protocol_updates`
- [x] 3.5 Insert transaction actions into `dl_txn_actions`
- [x] 3.6 Use batch inserts for multiple files in single commit

## 4. Idempotency and Safety
- [x] 4.1 Ensure duplicate commit attempts are safely rejected (via PK constraint)
- [x] 4.2 Validate action schemas before insertion
- [x] 4.3 Handle rollback on validation failure
- [x] 4.4 Prevent partial commits on errors (transaction atomicity)

## 5. Testing
- [x] 5.1 Unit tests for each writer method
- [ ] 5.2 Integration tests with real PostgreSQL (pending: database setup)
- [ ] 5.3 Concurrent writer conflict tests (pending: database setup)
- [ ] 5.4 Large commit tests (1000+ files) (pending: database setup)
- [ ] 5.5 Rollback and error recovery tests (pending: database setup)
- [ ] 5.6 Conformance tests (pending: file-based writer)

## 6. Performance
- [ ] 6.1 Profile commit latency (pending: benchmark infrastructure)
- [ ] 6.2 Optimize batch inserts with COPY (pending: tokio-postgres integration)
- [ ] 6.3 Measure and document throughput (pending: benchmarks)
