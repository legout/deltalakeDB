## 1. Reader Implementation
- [ ] 1.1 Create `PostgresReader` struct with sqlx connection pool
- [ ] 1.2 Implement `get_latest_version()` with single query to `dl_table_versions`
- [ ] 1.3 Implement `read_snapshot()` reconstructing state from add/remove files
- [ ] 1.4 Implement `get_files()` returning active files at a version
- [ ] 1.5 Implement `get_schema()` retrieving latest metadata update
- [ ] 1.6 Implement `get_protocol()` reading protocol requirements

## 2. Time Travel Support
- [ ] 2.1 Implement `time_travel_by_version(version)` reading snapshot at specific version
- [ ] 2.2 Implement `time_travel_by_timestamp(ts)` finding version closest to timestamp
- [ ] 2.3 Add efficient query using indexed timestamp lookups

## 3. Query Optimization
- [ ] 3.1 Use prepared statements with sqlx compile-time checking
- [ ] 3.2 Implement batch loading for large file sets
- [ ] 3.3 Add connection pooling configuration
- [ ] 3.4 Profile queries and optimize with EXPLAIN ANALYZE

## 4. Testing
- [ ] 4.1 Unit tests for each reader method
- [ ] 4.2 Integration tests with real PostgreSQL via testcontainers
- [ ] 4.3 Performance tests measuring p95 latency on 100k files
- [ ] 4.4 Test time travel accuracy against file-based implementation
- [ ] 4.5 Run conformance tests comparing SQL vs file-based snapshots

## 5. Error Handling
- [ ] 5.1 Handle connection failures with retry logic
- [ ] 5.2 Return clear errors for missing tables or versions
- [ ] 5.3 Wrap database errors with context
