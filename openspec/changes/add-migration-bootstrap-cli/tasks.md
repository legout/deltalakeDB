## 1. Implementation
- [x] 1.1 Add CLI subcommand `dl import <table_path> --dsn <db> [--schema <s>] [--table <t>]`
- [x] 1.2 Read latest checkpoint if present; otherwise start from version 0
- [x] 1.3 Replay JSON commits into `dl_*` tables

## 2. Validation
- [x] 2.1 Import completes for a sample `_delta_log/`
- [x] 2.2 SQL snapshot equals `_delta_log` snapshot by diff tool

## 3. Dependencies
- Depends on `add-sql-schema-core`
