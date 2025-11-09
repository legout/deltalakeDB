## 1. Implementation
- [ ] 1.1 Define `TxnLogReader` and `TxnLogWriter` traits in Rust core
- [ ] 1.2 Implement `FileTxnLogReader` using existing file-backed read path
- [ ] 1.3 Implement `FileTxnLogWriter` using existing file-backed write path
- [ ] 1.4 Wire existing code to call through traits

## 2. Validation
- [ ] 2.1 All existing file-backed tests pass unchanged
- [ ] 2.2 Backward compatibility: opening and writing a file-backed table works

## 3. Dependencies
- None (no SQL dependencies yet)

