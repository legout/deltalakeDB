## 1. Implementation
- [ ] 1.1 Add Postgres `SqlTxnLogWriter`
- [ ] 1.2 Lock/CAS: `UPDATE dl_table_heads SET current_version = $new WHERE table_id=$id AND current_version=$expected`
- [ ] 1.3 Insert actions into `dl_add_files`, `dl_remove_files`, `dl_metadata_updates`, `dl_protocol_updates`, and a row in `dl_table_versions`
- [ ] 1.4 Enqueue `(table_id, version)` in `dl_mirror_status` as PENDING

## 2. Validation
- [ ] 2.1 Successful commit persists all rows atomically
- [ ] 2.2 CAS violation returns Concurrency error
- [ ] 2.3 Re-commit with same `app_id`/txn treated as idempotent (no duplicate rows)

## 3. Dependencies
- Depends on `add-txnlog-abstractions`, `add-sql-schema-core`

