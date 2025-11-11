## Why
The SQL-Backed Delta Metadata system must support multi-table ACID transactions to enable atomic commits across multiple Delta tables. This is critical for use cases like dimension + fact table updates, feature store deployments, and complex data pipelines that require consistency across related tables. The transaction system must handle optimistic concurrency, conflict resolution, and proper rollback semantics.

## What Changes
- Add multi-table transaction coordination with SQL database transactions
- Implement optimistic concurrency control with CAS operations
- Add conflict resolution and retry mechanisms
- Include transaction API for staging and committing across multiple tables

## Impact
- Affected specs: rust-workspace
- Affected code: crates/core/src/lib.rs and crates/sql/src/lib.rs (transaction coordination)
- Dependencies: sqlx (for transaction handling), uuid (for transaction IDs), tokio (async coordination)