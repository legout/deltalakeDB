## Implementation Plan for add-txnlog-traits

### Overview
Define foundational `TxnLogReader` and `TxnLogWriter` traits in the existing `deltalakedb-core` crate to enable multiple transaction log implementations (file-based and SQL-backed).

### Phase 1: Dependencies & Setup
1. **Update `crates/core/Cargo.toml`** to uncomment and enable:
   - `serde` + `serde_json` (for serialization)
   - `thiserror` (for error types)
   - `tokio` with async runtime
   - `async-trait` (for async trait methods)
   - `anyhow` (for error context)

2. **Verify Rust toolchain** - ensure rustfmt, clippy, and cargo test work

### Phase 2: Define Core Error Types
Create `crates/core/src/error.rs`:
- `TxnLogError` enum with variants:
  - `VersionConflict { expected: i64, actual: i64 }`
  - `TableNotFound(String)`
  - `IoError(std::io::Error)` 
  - `SerializationError(String)`
  - `Other(String)`
- Implement `From<std::io::Error>` and `Display`
- Use `#[derive(thiserror::Error)]`

### Phase 3: Define Domain Types
Create `crates/core/src/types.rs`:
- `AddFile` struct (path, size, modification_time, data_change_version, stats, etc.)
- `RemoveFile` struct (path, deletion_timestamp, etc.)
- `Action` enum (Add/Remove/Metadata/Protocol/Txn variants)
- `Snapshot` struct (version, timestamp, schema, files, metadata)
- `CommitHandle` as newtype wrapping transaction ID

### Phase 4: Define Traits
Create `crates/core/src/traits.rs`:

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
    async fn validate_version(&self, expected: i64) -> Result<(), TxnLogError>;
}
```

### Phase 5: Update Module Structure
- Update `crates/core/src/lib.rs`:
  - Remove placeholder module
  - Add `mod error`, `mod types`, `mod traits`
  - Export public items: `pub use error::*`, `pub use types::*`, `pub use traits::*`
  - Add crate-level doc comments

### Phase 6: Add Mock Implementations
Create `crates/core/src/mocks.rs`:
- `MockTxnLogReader` for testing reader implementations
- `MockTxnLogWriter` for testing writer implementations
- Mock data generators

### Phase 7: Documentation
- Add module-level docs to each file
- Add trait method documentation with usage examples
- Document error scenarios and handling patterns
- Add doc-comments with scenarios

### Phase 8: Testing
- Unit tests for error types
- Tests for mock implementations
- Async runtime tests using `tokio::test`
- Verify no existing tests break (`cargo test --all-features`)

### Phase 9: Linting & Formatting
- Run `cargo fmt` on all code
- Run `cargo clippy --all-features` and fix warnings
- Verify all tests pass: `cargo test --all-features`

### Success Criteria
✓ All 4 task.md items completed and marked `[x]`
✓ `cargo test --all-features` passes
✓ `cargo clippy --all-features` has no warnings
✓ `cargo fmt` produces no changes
✓ Traits support all scenarios from spec.md
✓ Error handling covers all defined variants