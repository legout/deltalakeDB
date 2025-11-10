## 1. Implementation
- [x] 1.1 Add reconciler loop polling `dl_mirror_status`
- [x] 1.2 Implement per-table ordering (do not mirror V+1 before V)
- [x] 1.3 Record success/failure with attempt count, last_error, updated_at
- [x] 1.4 Expose metrics and simple lag alert thresholds

## 2. Validation
- [x] 2.1 Inject failures and confirm retries until success
- [x] 2.2 Lag > threshold triggers alert event

## 3. Dependencies
- Depends on `add-mirror-json-after-commit`
