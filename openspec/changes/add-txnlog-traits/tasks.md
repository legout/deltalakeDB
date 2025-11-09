## 1. Design Traits
- [ ] 1.1 Define `TxnLogReader` trait with methods: `get_latest_version()`, `read_snapshot()`, `get_files()`, `time_travel()`
- [ ] 1.2 Define `TxnLogWriter` trait with methods: `begin_commit()`, `validate_version()`, `write_actions()`, `finalize_commit()`
- [ ] 1.3 Define error types for transaction log operations
- [ ] 1.4 Add comprehensive trait documentation with usage examples

## 2. Implementation
- [ ] 2.1 Create `crates/sql-metadata-core` crate
- [ ] 2.2 Implement traits in `crates/sql-metadata-core/src/traits.rs`
- [ ] 2.3 Add trait bounds and type aliases for common patterns
- [ ] 2.4 Ensure traits support async operations via Tokio

## 3. Testing
- [ ] 3.1 Add unit tests for trait definitions
- [ ] 3.2 Create mock implementations for testing
- [ ] 3.3 Verify existing file-based tests still pass
- [ ] 3.4 Run `cargo test --all-features`

## 4. Documentation
- [ ] 4.1 Add module-level documentation
- [ ] 4.2 Document trait contract and requirements
- [ ] 4.3 Add examples of trait implementation patterns
