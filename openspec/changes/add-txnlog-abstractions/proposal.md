## Why
Introduce clear reader/writer abstractions so file-backed and SQL-backed metadata paths can coexist behind stable traits. This enables incremental adoption (no behavior change) and sets the foundation for later SQL features.

## What Changes
- Add Rust traits `TxnLogReader` and `TxnLogWriter` in the core library.
- Provide `FileTxnLogReader` and `FileTxnLogWriter` implementations preserving existing behavior.
- Route all metadata reads/writes through these traits.

## Impact
- Affected specs: delta-txnlog
- Affected code: Rust core crate (traits + file-backed impl), existing file-backed code moved under trait impls.

## References
- PRD §6.2 Components – TxnLogReader/Writer abstractions
- PRD §6.1 High-Level – Dual path with SQL authority and file mirroring
- PRD §15 M0 – Abstractions landed
