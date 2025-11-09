## Context

The current `delta-rs` codebase assumes file-based transaction logs stored in object storage. To support SQL-backed metadata as an alternative while maintaining the existing file-based approach, we need an abstraction layer that decouples the transaction log interface from its implementation.

This is a foundational change that will be used by all subsequent SQL metadata work and must integrate cleanly with existing `delta-rs` code without breaking changes.

## Goals / Non-Goals

**Goals:**
- Define clear trait boundaries for reading and writing Delta transaction logs
- Support async operations with Tokio runtime
- Enable multiple implementations (file-based, SQL-based) behind same interface
- Maintain zero-cost abstractions where possible
- Preserve all existing file-based functionality

**Non-Goals:**
- Implementing SQL-backed versions (that's subsequent proposals)
- Changing Delta protocol or table format
- Optimizing file-based implementation performance
- Supporting sync (non-async) APIs

## Decisions

### Trait Design: Separate Reader and Writer

**Decision:** Define `TxnLogReader` and `TxnLogWriter` as separate traits rather than a single `TxnLog` trait.

**Rationale:**
- Many operations only need read access (queries, time travel)
- Allows different ownership patterns (e.g., shared readers, exclusive writers)
- Mirrors common database patterns (read replicas vs primary)
- Simplifies testing with read-only mocks

**Alternatives considered:**
- Single trait with all operations: rejected due to unnecessary coupling
- Builder pattern: rejected as overly complex for this use case

### Async-First API

**Decision:** All trait methods are async and return `Future` types.

**Rationale:**
- SQL databases require async IO (sqlx, tokio-postgres are async)
- Object storage operations are already async in delta-rs
- Matches Rust ecosystem direction (tokio, async-std)
- Allows efficient connection pooling and concurrency

**Alternatives considered:**
- Sync API with blocking calls: rejected as incompatible with modern Rust DB drivers
- Separate sync and async traits: rejected as doubling maintenance burden

### Error Handling Strategy

**Decision:** Define custom error types `TxnLogError` with variants for common failures (version conflict, IO error, not found, etc.)

**Rationale:**
- Allows callers to handle specific error cases programmatically
- Preserves error context across trait boundary
- Supports `anyhow` for error propagation with context

**Implementation:**
```rust
pub enum TxnLogError {
    VersionConflict { expected: i64, actual: i64 },
    TableNotFound(String),
    IoError(std::io::Error),
    // ...
}
```

### Integration with delta-rs

**Decision:** Define traits in new crate `sql-metadata-core`, implement for existing file-based code via adapter pattern.

**Rationale:**
- Avoids modifying delta-rs internals initially
- Allows gradual migration
- Clear separation of concerns
- Can upstream to delta-rs later if desired

**Migration path:**
- Phase 1: Traits in separate crate with file-based adapter
- Phase 2: Prove SQL implementation works
- Phase 3: Propose upstream integration to delta-rs

### Method Signatures

**Decision:** Key trait methods:

```rust
#[async_trait]
pub trait TxnLogReader: Send + Sync {
    async fn get_latest_version(&self) -> Result<i64, TxnLogError>;
    async fn read_snapshot(&self, version: Option<i64>) -> Result<Snapshot, TxnLogError>;
    async fn get_files(&self, version: i64) -> Result<Vec<AddFile>, TxnLogError>;
    async fn time_travel(&self, timestamp: i64) -> Result<Snapshot, TxnLogError>;
}

#[async_trait]
pub trait TxnLogWriter: Send + Sync {
    async fn begin_commit(&mut self, table_id: &str) -> Result<CommitHandle, TxnLogError>;
    async fn write_actions(&mut self, handle: &CommitHandle, actions: Vec<Action>) -> Result<(), TxnLogError>;
    async fn finalize_commit(&mut self, handle: CommitHandle) -> Result<i64, TxnLogError>;
}
```

**Rationale:**
- `Send + Sync` bounds enable use across thread boundaries
- `CommitHandle` represents in-progress transaction state
- Return types use domain types (Snapshot, AddFile, Action) from delta-rs

## Risks / Trade-offs

**Risk: Trait overhead**
- Mitigation: Use `async_trait` macro to minimize boilerplate; profile to ensure no unexpected overhead

**Risk: Breaking changes to trait later**
- Mitigation: Start with minimal interface, extend carefully; use sealed trait pattern if needed

**Trade-off: Abstraction vs simplicity**
- Cost: Additional indirection layer
- Benefit: Enables SQL implementation without forking delta-rs

**Risk: Integration friction with delta-rs**
- Mitigation: Design traits to match existing patterns in delta-rs; engage with upstream early

## Migration Plan

**Phase 1: Trait Definition (this proposal)**
1. Create `sql-metadata-core` crate with trait definitions
2. Add comprehensive trait documentation
3. Create mock implementations for testing

**Phase 2: File-Based Adapter (follow-on work)**
1. Implement `FileTxnLogReader` and `FileTxnLogWriter` wrapping existing delta-rs code
2. Prove traits work with existing functionality
3. Add integration tests comparing trait-based and direct usage

**Phase 3: SQL Implementation (subsequent proposals)**
1. Implement `SqlTxnLogReader` and `SqlTxnLogWriter` for PostgreSQL
2. Extend to SQLite and DuckDB

**Rollback:** If traits prove inadequate, they remain internal to our crate without upstream impact.

## Open Questions

1. **Should traits support batched operations?**
   - Leaning: Add later if needed; YAGNI principle
   
2. **How to handle table registration/discovery?**
   - Leaning: Separate concern, addressed in URI support proposal

3. **Should CommitHandle be opaque or expose internal state?**
   - Leaning: Opaque (newtype) to allow implementation flexibility

4. **Version type: i64 vs u64 vs custom type?**
   - Decision: Use i64 to match Delta spec and avoid conversions; versions are always non-negative in practice
