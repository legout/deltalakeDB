## 1. Implementation
- [x] 1.1 Add `TransactionBuilder` that stages actions per table_id
- [x] 1.2 On commit: open DB tx, verify expected heads, insert actions and versions for each table, update heads, commit
- [x] 1.3 Enqueue mirror status per `(table_id, version)`

## 2. Validation
- [x] 2.1 Two-table commit succeeds and is atomic (either both or neither)
- [x] 2.2 Concurrency conflict on any table aborts the whole transaction

## 3. Dependencies
- Depends on `add-sql-write-path-postgres`, `add-mirror-json-after-commit`
