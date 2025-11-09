## 1. SQL Schema Implementation
- [ ] 1.1 Create DDL for Postgres (JSONB, arrays)
- [ ] 1.2 Create DDL for SQLite (TEXT/JSON, no arrays)
- [ ] 1.3 Create DDL for DuckDB (JSON, lists)
- [ ] 1.4 Add database-specific indexes and constraints
- [ ] 1.5 Add schema migration utilities

## 2. SqlTxnLogReader Implementation
- [ ] 2.1 Implement database connection management
- [ ] 2.2 Add query builders for active files calculation
- [ ] 2.3 Implement latest snapshot reading
- [ ] 2.4 Add time travel by version support
- [ ] 2.5 Add time travel by timestamp support
- [ ] 2.6 Implement table discovery API

## 3. Testing and Validation
- [ ] 3.1 Add unit tests for query builders
- [ ] 3.2 Add integration tests for each database engine
- [ ] 3.3 Add performance benchmarks vs file-based reads
- [ ] 3.4 Add conformance tests against Delta protocol
