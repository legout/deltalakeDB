## 1. Implementation
- [x] 1.1 Add Postgres adapter implementing `SqlTxnLogReader`
- [x] 1.2 Implement latest snapshot query (adds - removes up to version)
- [x] 1.3 Implement time travel by version and by timestamp
- [x] 1.4 Expose metadata/protocol properties

## 2. Validation
- [x] 2.1 Open table returns latest version and active files
- [x] 2.2 Time travel by version returns consistent snapshot
- [x] 2.3 Time travel by timestamp resolves to expected version

## 3. Dependencies
- Depends on `add-txnlog-abstractions` and `add-sql-schema-core`
