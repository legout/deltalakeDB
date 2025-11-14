## Why
One of the key advantages of SQL-backed metadata is enabling true multi-table ACID transactions that span multiple Delta tables within a single database transaction, which is impossible with independent file-based logs.

## What Changes
- Add multi-table transaction API that stages actions across multiple tables
- Implement atomic commit across multiple table_ids in single SQL transaction
- Add ordered mirroring for each table after successful SQL commit
- Create transaction context and rollback handling
- Add cross-table consistency validation

## Impact
- Affected specs: rust-workspace, sql
- Affected code: Extensions to sql crate with multi-table transaction support
- Enables atomic dimension + fact table rewrites, feature store updates
- Key differentiator vs standard Delta Lake implementations
