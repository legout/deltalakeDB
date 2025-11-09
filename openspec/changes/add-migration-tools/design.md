## Context

Adopting SQL-backed metadata for existing Delta tables requires robust migration tooling to bootstrap the SQL catalog from existing `_delta_log/` files. This involves replaying potentially millions of JSON commits and large Parquet checkpoints, validating the results, and providing rollback capabilities.

The challenge is handling the scale and complexity of real-world Delta tables while ensuring data integrity, providing progress visibility, and supporting various failure scenarios. Migration must be reliable for production use cases where data loss or corruption is unacceptable.

Key constraints:
- Must handle tables with millions of files and versions
- Migration must be resumable after interruptions
- Validation must ensure byte-for-byte equivalence
- Rollback must be possible without data loss
- Performance must be acceptable for large production tables

## Goals / Non-Goals

### Goals
- Provide reliable migration from file-based to SQL-backed metadata
- Support resumable migrations for large tables
- Ensure data integrity through comprehensive validation
- Enable rollback without data loss
- Support all three database engines (Postgres, SQLite, DuckDB)
- Provide clear progress reporting and status tracking

### Non-Goals
- Zero-downtime migrations (require maintenance windows)
- Automatic schema migration (users must handle schema changes)
- Cross-database migrations (same database engine only)
- Real-time synchronization during migration

## Decisions

### Decision: Checkpoint-First Migration Strategy
**What**: Read latest checkpoint first, then replay only JSON commits after checkpoint version.
**Why**: Dramatically reduces the number of JSON files to process and leverages Delta's built-in optimization.
**Alternatives considered**:
- JSON-only replay: Would be much slower for large tables
- Incremental migration: More complex, harder to validate

### Decision: Bulk Insert with Batch Processing
**What**: Use database-specific bulk insert operations with configurable batch sizes.
**Why**: Optimizes performance for large datasets while managing memory usage.
**Alternatives considered**:
- Row-by-row inserts: Too slow for large tables
- Single massive insert: Could exceed memory limits and database constraints

### Decision: Progress Tracking with Checkpointing
**What**: Track migration progress in a separate table and support resumable migrations.
**Why**: Enables recovery from failures without restarting entire migration.
**Alternatives considered**:
- No progress tracking: Would require full restart on failure
- File-based progress: Less reliable and harder to manage

### Decision: Comprehensive Validation Strategy
**What**: Validate SQL-derived snapshots against file-based snapshots at multiple levels.
**Why**: Ensures data integrity and builds confidence in migration process.
**Alternatives considered**:
- No validation: Too risky for production data
- Sample validation only: Might miss subtle inconsistencies

## Risks / Trade-offs

### Risk: Memory Exhaustion on Large Tables
**Mitigation**: Implement streaming processing, configurable batch sizes, memory monitoring

### Risk: Database Performance Degradation
**Mitigation**: Use bulk operations, implement rate limiting, schedule during maintenance windows

### Risk: Migration Interruption Leaving Inconsistent State
**Mitigation**: Transactional batches, rollback procedures, validation before finalization

### Risk: Validation False Positives/Negatives
**Mitigation**: Multiple validation levels, checksum verification, manual review processes

### Trade-off: Migration Time vs. Validation Thoroughness
More comprehensive validation takes longer but provides higher confidence in data integrity.

### Trade-off: Batch Size vs. Memory Usage
Larger batches improve performance but increase memory requirements and failure impact.

## Migration Plan

### Phase 1: Core Migration Engine
1. Implement checkpoint reading and parsing
2. Add JSON log replay functionality
3. Create bulk insert operations for each database engine
4. Add basic progress tracking

### Phase 2: Validation and Reliability
1. Implement snapshot comparison validation
2. Add drift detection algorithms
3. Create rollback procedures
4. Add resumable migration support

### Phase 3: Production Features
1. Add comprehensive progress reporting
2. Implement batch migration support
3. Create performance optimization features
4. Add operational tooling and monitoring

### Phase 4: Advanced Features
1. Add parallel processing for multiple tables
2. Implement incremental migration capabilities
3. Create migration scheduling and automation
4. Add advanced error recovery procedures

### Rollback Procedures
1. **Before Finalization**: Simply drop SQL tables and restart migration
2. **After Finalization**: Keep original `_delta_log/` as backup, switch writers back to file-based mode
3. **Partial Rollback**: Use transaction logs to undo specific batches if needed

## Open Questions

1. What is the optimal batch size for different database engines and table sizes?
2. How should we handle schema evolution during migration?
3. Should we support parallel migration of multiple tables?
4. What level of validation is sufficient for different use cases?
5. How should we handle very large checkpoints that exceed memory limits?
6. Should we implement dry-run mode for migration planning?
7. How should we estimate migration time for large production tables?
8. What monitoring and alerting should be in place during migration?
