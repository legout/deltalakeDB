## Context

The SQL-backed metadata system must maintain Delta Lake compatibility by mirroring SQL commits to canonical Delta artifacts in `_delta_log/`. This creates a dual-write system where SQL is the source of truth but object storage remains the public interface for external engines. The challenge is ensuring exactly-once semantics, handling failures gracefully, and maintaining performance while writing to both systems.

Key constraints:
- SQL commit must succeed before any mirroring begins
- Mirror failures must not affect SQL state consistency
- External engines must see correct Delta artifacts eventually
- Mirror lag must be bounded and monitored

## Goals / Non-Goals

### Goals
- Provide exactly-once mirroring semantics for Delta JSON and Parquet artifacts
- Handle mirror failures with automatic retry and recovery
- Maintain sub-second mirror latency under normal conditions
- Support all three database engines (Postgres, SQLite, DuckDB)
- Provide observability into mirror status and lag

### Non-Goals
- Real-time synchronization (mirroring is asynchronous)
- Custom Delta protocol extensions (must be byte-for-byte compatible)
- Performance optimization of object storage writes beyond standard practices
- Support for non-Delta external formats

## Decisions

### Decision: SQL-First Write Ordering
**What**: All commits are written to SQL first, then mirrored to object storage.
**Why**: Ensures SQL is always the authoritative source of truth and allows for atomic multi-table transactions.
**Alternatives considered**: 
- Object-store-first: Would break multi-table ACID guarantees
- Simultaneous writes: Complex coordination, higher failure surface

### Decision: Asynchronous Mirroring with Status Tracking
**What**: Mirror operations run asynchronously after SQL commit, with status tracked in `dl_mirror_status` table.
**Why**: Decouples commit latency from object storage performance and allows for retry logic.
**Alternatives considered**:
- Synchronous mirroring: Would increase commit latency and reduce availability
- Event-driven mirroring: More complex, requires additional infrastructure

### Decision: Deterministic Serialization
**What**: Mirror engine generates canonical Delta JSON and Parquet from SQL actions using deterministic ordering.
**Why**: Ensures reproducible artifacts and simplifies validation/debugging.
**Alternatives considered**:
- Direct action forwarding: Would lose ordering guarantees and validation opportunities

### Decision: Retry with Exponential Backoff
**What**: Failed mirror operations are retried with exponential backoff up to a maximum limit.
**Why**: Handles transient object storage issues while avoiding thundering herd problems.
**Alternatives considered**:
- Immediate retry: Could overwhelm object storage during outages
- Fixed delay: Less adaptive to varying failure conditions

## Risks / Trade-offs

### Risk: Mirror Lag Leading to Stale External Reads
**Mitigation**: Monitor mirror lag with SLOs, alert on excessive lag, provide mirror status API for consumers

### Risk: Partial Mirror Writes Causing Inconsistent State
**Mitigation**: Use atomic rename operations, validate artifacts before publishing, implement checksum verification

### Risk: Object Storage Outages Blocking Mirroring
**Mitigation**: Persistent retry queue, dead letter queue for failed artifacts, manual override procedures

### Trade-off: Eventual Consistency for External Readers
External engines may see slightly stale data during mirror lag, but this is acceptable for most analytics workloads and provides better availability.

### Trade-off: Increased Storage Requirements
Both SQL and object storage store the same metadata, doubling storage requirements but providing compatibility and performance benefits.

## Migration Plan

### Phase 1: Core Mirror Engine
1. Implement deterministic JSON serialization from SQL actions
2. Add basic object storage write operations with atomic rename
3. Create mirror status tracking table
4. Add simple retry logic

### Phase 2: Advanced Features
1. Add Parquet checkpoint generation
2. Implement exponential backoff retry
3. Add mirror lag monitoring and alerting
4. Create background reconciler process

### Phase 3: Production Hardening
1. Add comprehensive error handling and recovery
2. Implement performance optimizations
3. Add operational tooling and dashboards
4. Create failure scenario testing

### Rollback
If mirroring fails catastrophically:
1. Stop new writes to SQL-backed tables
2. Switch writers back to file-based mode
3. SQL state remains consistent and can be used for recovery
4. External engines continue reading from existing `_delta_log/`

## Open Questions

1. What is the optimal checkpoint cadence for mirroring? Should it match Delta defaults or be configurable?
2. How should we handle very large commits that exceed object storage limits?
3. Should we implement priority queuing for critical tables vs. less important ones?
4. What level of mirror lag is acceptable for different use cases?
5. How should we handle schema evolution during mirror failures?
