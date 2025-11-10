## Context
The SQL adapters layer provides the bridge between Delta Lake domain models and SQL database storage. This layer must abstract database differences while leveraging database-specific features for optimal performance. It implements the core transaction log reading/writing capabilities that make SQL metadata queries fast and reliable.

## Goals / Non-Goals
- Goals:
  - Unified interface for reading/writing Delta metadata across SQL databases
  - Database-specific optimizations for performance
  - Robust connection management and error handling
  - Schema migration and versioning support
- Non-Goals:
  - Delta log file format handling (handled by mirroring layer)
  - Multi-database transactions (handled at higher layer)
  - Database administration operations (focus on Delta metadata only)

## Decisions
- Decision: Use sqlx for database operations with compile-time query checking
  - Rationale: Type safety, async support, compile-time verification, multi-database support
  - Alternatives considered: diesel (more complex, less async-focused), raw drivers (more boilerplate)

- Decision: Implement adapter pattern for database-specific behavior
  - Rationale: Clean separation of concerns, easy to add new databases, database-specific optimizations
  - Alternatives considered: single implementation with feature flags (harder to maintain), macros (less readable)

- Decision: Use normalized schema design for Delta metadata
  - Rationale: Query flexibility, data integrity, easier indexing and optimization
  - Alternatives considered: document storage (less query flexibility), hybrid approach (more complexity)

- Decision: Include migration system for schema evolution
  - Rationale: Support for schema changes over time, upgrade paths, development convenience
  - Alternatives considered: manual schema management (error-prone), external migration tools (more dependencies)

## Risks / Trade-offs
- Risk: SQL query performance variations across databases
  - Mitigation: Database-specific optimizations, proper indexing, query profiling
- Trade-off: Normalized schema vs. query performance
  - Decision: Favor normalized design with proper indexing for flexibility and integrity
- Risk: Connection pool management complexity
  - Mitigation: Use proven connection pooling libraries, comprehensive health checking
- Risk: SQL injection vulnerabilities
  - Mitigation: Use parameterized queries exclusively, compile-time query checking with sqlx

## Migration Plan
- No database migration required - this is new functionality
- Existing placeholder code in crates/sql will be replaced
- Schema migration system will handle future database schema changes

## Open Questions
- Should we support read replicas for query scaling?
- How should we handle database connection failures during transactions?
- What level of database-specific query optimization should we expose?