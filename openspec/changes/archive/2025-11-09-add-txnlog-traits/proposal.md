## Why
The current file-based Delta transaction log implementation is tightly coupled to object storage. To support SQL-backed metadata while preserving the existing file-based approach, we need abstract interfaces that allow multiple implementations.

## What Changes
- Define `TxnLogReader` and `TxnLogWriter` traits in Rust
- Extract common behaviors for reading/writing transaction logs
- Ensure existing file-based implementation continues to work unchanged
- Establish foundation for future SQL-backed implementations

## Impact
- Affected specs: new capability `sql-metadata-core`
- Affected code: `delta-rs` integration layer, new `crates/sql-metadata-core/src/traits.rs`
- Breaking: None - this is additive only
- Dependencies: Foundation for all subsequent SQL-backed metadata work (M0 milestone)
