## 1. Implementation
- [x] 1.1 Add URI parser for `deltasql://`
- [x] 1.2 Support engines: Postgres, SQLite, DuckDB
- [x] 1.3 Map parsed URI to engine adapter config (DSN/path + table)

## 2. Validation
- [x] 2.1 Example URIs parse successfully
- [x] 2.2 Unknown scheme or missing table yields a clear error

## 3. Dependencies
- Depends on `TxnLogReader`/`Writer` traits existing for routing (from `add-txnlog-abstractions`)
