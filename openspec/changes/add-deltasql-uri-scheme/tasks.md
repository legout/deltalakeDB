## 1. Implementation
- [ ] 1.1 Add URI parser for `deltasql://`
- [ ] 1.2 Support engines: Postgres, SQLite, DuckDB
- [ ] 1.3 Map parsed URI to engine adapter config (DSN/path + table)

## 2. Validation
- [ ] 2.1 Example URIs parse successfully
- [ ] 2.2 Unknown scheme or missing table yields a clear error

## 3. Dependencies
- Depends on `TxnLogReader`/`Writer` traits existing for routing (from `add-txnlog-abstractions`)

