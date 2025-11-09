## 1. Schema Design
- [ ] 1.1 Create `dl_tables` table with UUID primary key, location, protocol versions, and properties
- [ ] 1.2 Create `dl_table_versions` table tracking each commit with operation metadata
- [ ] 1.3 Create `dl_add_files` table for add actions with partition values and stats
- [ ] 1.4 Create `dl_remove_files` table for remove actions
- [ ] 1.5 Create `dl_metadata_updates` table for schema changes
- [ ] 1.6 Create `dl_protocol_updates` table for protocol version changes
- [ ] 1.7 Create `dl_txn_actions` table for streaming progress tracking

## 2. Indexes and Constraints
- [ ] 2.1 Add primary key constraints on (table_id, version) combinations
- [ ] 2.2 Add btree index on `dl_add_files(table_id, path)` for file lookups
- [ ] 2.3 Add btree index on `dl_table_versions(table_id DESC, version DESC)` for latest version
- [ ] 2.4 Add GIN index on `dl_add_files(stats)` for predicate pushdown
- [ ] 2.5 Add foreign key constraints from action tables to `dl_tables`

## 3. Migration Infrastructure
- [ ] 3.1 Create initial migration `001_create_base_schema.sql`
- [ ] 3.2 Add rollback script `001_create_base_schema_down.sql`
- [ ] 3.3 Configure sqlx migrations in Cargo.toml
- [ ] 3.4 Test migration on clean database

## 4. Documentation
- [ ] 4.1 Document schema design rationale
- [ ] 4.2 Add ER diagram showing table relationships
- [ ] 4.3 Document indexing strategy and query patterns
- [ ] 4.4 Add schema versioning guidelines
