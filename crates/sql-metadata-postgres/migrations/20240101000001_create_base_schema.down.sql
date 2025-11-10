-- Rollback: Remove base Delta Lake metadata schema

-- Drop views first (depend on tables)
DROP VIEW IF EXISTS v_active_files CASCADE;

-- Drop tables in dependency order (foreign key constraints)
DROP TABLE IF EXISTS dl_txn_actions CASCADE;
DROP TABLE IF EXISTS dl_protocol_updates CASCADE;
DROP TABLE IF EXISTS dl_metadata_updates CASCADE;
DROP TABLE IF EXISTS dl_remove_files CASCADE;
DROP TABLE IF EXISTS dl_add_files CASCADE;
DROP TABLE IF EXISTS dl_table_versions CASCADE;
DROP TABLE IF EXISTS dl_tables CASCADE;

-- Note: uuid-ossp extension is left in place as it may be used by other parts of the system
-- If needed, remove with: DROP EXTENSION IF EXISTS "uuid-ossp";
