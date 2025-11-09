## 1. Implementation
- [ ] 1.1 Add Postgres adapter implementing `SqlTxnLogReader`
- [ ] 1.2 Implement latest snapshot query (adds - removes up to version)
- [ ] 1.3 Implement time travel by version and by timestamp
- [ ] 1.4 Expose metadata/protocol properties

## 2. Validation
- [ ] 2.1 Open table returns latest version and active files
- [ ] 2.2 Time travel by version returns consistent snapshot
- [ ] 2.3 Time travel by timestamp resolves to expected version

## 3. Dependencies
- Depends on `add-txnlog-abstractions` and `add-sql-schema-core`

