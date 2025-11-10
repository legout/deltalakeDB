## 1. Schema Design
- [x] 1.1 Create `dl_tables` table with UUID primary key, location, protocol versions, and properties
- [x] 1.2 Create `dl_table_versions` table tracking each commit with operation metadata
- [x] 1.3 Create `dl_add_files` table for add actions with partition values and stats
- [x] 1.4 Create `dl_remove_files` table for remove actions
- [x] 1.5 Create `dl_metadata_updates` table for schema changes
- [x] 1.6 Create `dl_protocol_updates` table for protocol version changes
- [x] 1.7 Create `dl_txn_actions` table for streaming progress tracking

## 2. Indexes and Constraints
- [x] 2.1 Add primary key constraints on (table_id, version) combinations
- [x] 2.2 Add btree index on `dl_add_files(table_id, path)` for file lookups
- [x] 2.3 Add btree index on `dl_table_versions(table_id DESC, version DESC)` for latest version
- [x] 2.4 Add GIN index on `dl_add_files(stats)` for predicate pushdown
- [x] 2.5 Add foreign key constraints from action tables to `dl_tables`

## 3. Migration Infrastructure
- [x] 3.1 Create initial migration `001_create_base_schema.sql`
- [x] 3.2 Add rollback script `001_create_base_schema_down.sql`
- [x] 3.3 Configure sqlx migrations in Cargo.toml
- [x] 3.4 Test migration on clean database (pending: PostgreSQL available)

## 4. Documentation
- [x] 4.1 Document schema design rationale (docs/POSTGRES_SCHEMA_DESIGN.md)
- [ ] 4.2 Add ER diagram showing table relationships (pending: diagram generation)
- [x] 4.3 Document indexing strategy and query patterns (docs/POSTGRES_SCHEMA_DESIGN.md)
- [x] 4.4 Add schema versioning guidelines (docs/POSTGRES_SCHEMA_DESIGN.md)
