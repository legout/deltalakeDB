## Why
The current delta-rs implementation tightly couples transaction log reading/writing to file-based operations. To support SQL-backed metadata while maintaining Delta compatibility, we need pluggable abstractions that allow both file-based and SQL-based implementations.

## What Changes
- Add `TxnLogReader` and `TxnLogWriter` Rust traits to abstract transaction log operations
- Implement `FileTxnLogReader` and `FileTxnLogWriter` for existing file-based behavior
- Define core data structures for Delta actions (add/remove/metadata/protocol/txn)
- Add engine-agnostic error types and result handling

## Impact
- Affected specs: rust-workspace
- Affected code: New core crate under `crates/core/src/lib.rs`
- Foundation for all subsequent SQL-backed metadata features
