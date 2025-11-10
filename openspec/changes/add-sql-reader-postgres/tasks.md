## 1. Reader Implementation
- [x] 1.1 Create `PostgresReader` struct with sqlx connection pool
- [x] 1.2 Implement `get_latest_version()` with single query to `dl_table_versions`
- [x] 1.3 Implement `read_snapshot()` reconstructing state from add/remove files
- [x] 1.4 Implement `get_files()` returning active files at a version
- [x] 1.5 Implement `get_schema()` retrieving latest metadata update
- [x] 1.6 Implement `get_protocol()` reading protocol requirements

## 2. Time Travel Support
- [x] 2.1 Implement `time_travel_by_version(version)` reading snapshot at specific version
- [x] 2.2 Implement `time_travel_by_timestamp(ts)` finding version closest to timestamp
- [x] 2.3 Add efficient query using indexed timestamp lookups

## 3. Query Optimization
- [x] 3.1 Use prepared statements with sqlx compile-time checking
- [x] 3.2 Implement batch loading for large file sets
- [x] 3.3 Add connection pooling configuration
- [x] 3.4 Profile queries and optimize with EXPLAIN ANALYZE (documented in guide)

## 4. Testing
- [x] 4.1 Unit tests for each reader method
- [x] 4.2 Integration tests with real PostgreSQL via testcontainers (reader_tests.rs)
- [ ] 4.3 Performance tests measuring p95 latency on 100k files (pending: benchmark infrastructure)
- [ ] 4.4 Test time travel accuracy against file-based implementation (pending: file-based reader)
- [ ] 4.5 Run conformance tests comparing SQL vs file-based snapshots (pending: file-based reader)

## 5. Error Handling
- [x] 5.1 Handle connection failures with retry logic (documented in guide)
- [x] 5.2 Return clear errors for missing tables or versions
- [x] 5.3 Wrap database errors with context
