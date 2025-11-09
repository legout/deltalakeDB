## 1. Mirror Engine Core
- [ ] 1.1 Create `MirrorEngine` struct with object store client
- [ ] 1.2 Implement action-to-JSON serialization following Delta spec
- [ ] 1.3 Implement JSON commit file writer (NNNNNNNNNN.json)
- [ ] 1.4 Ensure newline-delimited JSON format per Delta protocol
- [ ] 1.5 Add deterministic ordering of actions in JSON output

## 2. Checkpoint Generation
- [ ] 2.1 Implement Parquet checkpoint writer
- [ ] 2.2 Generate checkpoints at configurable intervals (default: every 10 commits)
- [ ] 2.3 Follow Delta checkpoint schema and compression settings
- [ ] 2.4 Support incremental checkpoint optimization

## 3. Mirror Coordination
- [ ] 3.1 Hook mirror engine into writer post-commit phase
- [ ] 3.2 Ensure mirroring only occurs after SQL transaction commits
- [ ] 3.3 Implement idempotent writes (retry-safe)
- [ ] 3.4 Add atomic rename or put-if-absent for conflict detection

## 4. Status Tracking
- [ ] 4.1 Create `dl_mirror_status` table for tracking mirror state
- [ ] 4.2 Record success/failure/pending for each (table_id, version)
- [ ] 4.3 Track timestamp and retry count
- [ ] 4.4 Expose mirror lag metrics

## 5. Reconciliation
- [ ] 5.1 Implement background reconciler that finds gaps
- [ ] 5.2 Retry failed mirrors with exponential backoff
- [ ] 5.3 Alert on mirror lag > 1 minute (configurable)
- [ ] 5.4 Provide manual repair command for stuck mirrors

## 6. Testing
- [ ] 6.1 Unit tests for JSON serialization against Delta spec examples
- [ ] 6.2 Unit tests for Parquet checkpoint generation
- [ ] 6.3 Integration tests with S3-compatible storage (MinIO)
- [ ] 6.4 Conformance tests comparing mirrored vs native Delta artifacts
- [ ] 6.5 Test mirror retry on transient failures
- [ ] 6.6 Test reconciler behavior with simulated lag

## 7. Validation
- [ ] 7.1 Property tests for serialization determinism
- [ ] 7.2 Cross-validate mirrored tables with Spark/DuckDB readers
- [ ] 7.3 Verify checkpoints are readable by Delta reference reader
