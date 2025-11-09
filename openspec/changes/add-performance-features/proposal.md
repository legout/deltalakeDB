## Why
The SQL-Backed Delta Metadata system needs performance optimizations to achieve the 3-10× metadata read speed improvements outlined in the PRD. This includes materialized views, query optimization, connection pooling, caching, and background maintenance to ensure the system can handle large-scale production workloads with millions of files and frequent metadata access patterns.

## What Changes
- Add materialized views and fast path metadata reads for common queries
- Implement query optimization for metadata access patterns
- Add connection pooling and caching layers
- Include background index maintenance and performance monitoring

## Impact
- Affected specs: rust-workspace, sql-adapters
- Affected code: crates/sql/src/lib.rs and crates/core/src/lib.rs (performance optimizations)
- Dependencies: Additional performance monitoring and caching libraries