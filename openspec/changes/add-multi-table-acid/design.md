## Context

Multi-table ACID transactions are a key advantage of SQL-backed metadata, enabling atomic operations across multiple Delta tables that are impossible with independent file-based logs. This requires coordinating commits across multiple table_ids within a single SQL transaction, then handling the complex mirroring semantics for external Delta compatibility.

The challenge is maintaining consistency guarantees across both the SQL world (where atomicity is natural) and the Delta ecosystem (where each table has independent logs). External engines may temporarily see inconsistent states during mirroring, which must be documented and managed.

Key constraints:
- All tables must be updated atomically within a single SQL transaction
- Mirroring must occur per-table after successful SQL commit
- External consistency is eventually consistent during mirror lag
- Transaction isolation must prevent anomalies across tables

## Goals / Non-Goals

### Goals
- Provide true ACID semantics across multiple Delta tables
- Support atomic dimension + fact table rewrites
- Enable feature store + label snapshot atomicity
- Maintain Delta compatibility through ordered mirroring
- Handle partial mirror failures gracefully

### Non-Goals
- Real-time consistency across external engines (eventual consistency only)
- Distributed transactions across multiple databases (single DB instance only)
- Custom conflict resolution beyond standard SQL semantics
- Support for cross-database transactions

## Decisions

### Decision: Single-Database Transaction Scope
**What**: Multi-table transactions are scoped to a single database instance.
**Why**: Leverages native ACID guarantees and avoids distributed transaction complexity.
**Alternatives considered**:
- Cross-database transactions: Would require two-phase commit, significantly more complex
- Saga pattern: More complex, doesn't provide true ACID guarantees

### Decision: SQL-First with Ordered Mirroring
**What**: SQL commit succeeds atomically, then each table is mirrored sequentially in version order.
**Why**: Ensures SQL consistency while maintaining Delta protocol compatibility.
**Alternatives considered**:
- Parallel mirroring: Could cause version ordering issues for external readers
- Batch mirroring: Would delay visibility of all changes

### Decision: Transaction Context with Staged Actions
**What**: Actions are staged in a transaction context before atomic commit.
**Why**: Allows for validation, rollback, and consistent error handling.
**Alternatives considered**:
- Direct execution: Would make rollback and validation difficult
- Optimistic locking: More complex, doesn't provide the same guarantees

### Decision: Per-Table Version Management
**What**: Each table maintains its own version sequence within the multi-table transaction.
**Why**: Preserves Delta protocol semantics and enables independent time travel.
**Alternatives considered**:
- Global version: Would break Delta compatibility and time travel semantics

## Risks / Trade-offs

### Risk: Deadlocks in Multi-Table Transactions
**Mitigation**: Implement consistent table ordering, use deadlock detection, provide retry logic with exponential backoff

### Risk: Partial Mirror Failure Causing External Inconsistency
**Mitigation**: Track mirror status per table, provide reconciliation tools, document eventual consistency model

### Risk: Long-Running Transactions Blocking Concurrency
**Mitigation**: Implement transaction timeouts, provide batch operation APIs, optimize for common patterns

### Risk: Cross-Table Constraint Violations
**Mitigation**: Add validation phase before commit, provide constraint checking APIs, document limitations

### Trade-off: Eventual Consistency for External Readers
External engines may see tables at different versions during mirror lag, but this enables true multi-table ACID which is impossible with file-based Delta.

### Trade-off: Increased Transaction Complexity
Multi-table transactions are more complex than single-table operations but provide significant value for data engineering workflows.

## Migration Plan

### Phase 1: Core Transaction Framework
1. Implement transaction context and action staging
2. Add atomic commit across multiple table_ids
3. Create basic rollback handling
4. Add simple validation logic

### Phase 2: Advanced Features
1. Implement deadlock detection and resolution
2. Add transaction isolation levels
3. Create cross-table consistency validation
4. Add ordered mirroring with status tracking

### Phase 3: Production Features
1. Add transaction monitoring and observability
2. Implement performance optimizations
3. Create batch operation APIs
4. Add comprehensive error handling

### Rollback
If multi-table transactions fail:
1. Fall back to single-table operations
2. Existing single-table transactions continue to work
3. SQL state remains consistent
4. No impact on external Delta compatibility

## Open Questions

1. What transaction isolation level should be the default? (READ COMMITTED vs. SERIALIZABLE)
2. How should we handle very large multi-table transactions that exceed memory limits?
3. Should we implement transaction priority or fairness policies?
4. What level of cross-table validation should be automatic vs. user-specified?
5. How should we handle schema changes during active multi-table transactions?
6. Should we provide transaction nesting or savepoint support?
