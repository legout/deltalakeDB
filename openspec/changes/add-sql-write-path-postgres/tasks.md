## 1. Implementation
- [x] 1.1 Add Postgres `SqlTxnLogWriter`
- [x] 1.2 Lock/CAS: `UPDATE dl_table_heads SET current_version = $new WHERE table_id=$id AND current_version=$expected`
- [x] 1.3 Insert actions into `dl_add_files`, `dl_remove_files`, `dl_metadata_updates`, `dl_protocol_updates`, and a row in `dl_table_versions`
- [x] 1.4 Enqueue `(table_id, version)` in `dl_mirror_status` as PENDING

## 2. Validation
- [x] 2.1 Successful commit persists all rows atomically
- [x] 2.2 CAS violation returns Concurrency error
- [x] 2.3 Re-commit with same `app_id`/txn treated as idempotent (no duplicate rows)

## 3. Dependencies
- Depends on `add-txnlog-abstractions`, `add-sql-schema-core`
