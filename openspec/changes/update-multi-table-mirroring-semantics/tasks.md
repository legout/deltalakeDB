## 1. Spec Update
- [x] 1.1 Modify `transactions/spec.md` to reflect per-table mirroring after DB commit
- [x] 1.2 Add scenarios for mirror status, lag SLOs, and reconciliation
- [x] 1.3 Validate formatting with `openspec validate --strict`

## 2. Docs & Observability
- [x] 2.1 Document temporary external inconsistency window
- [x] 2.2 Define metrics for mirror lag and status per table/version

## 3. Follow-ups (Non-blocking)
- [x] 3.1 Review tests to align with new mirroring semantics
- [x] 3.2 Ensure alerting thresholds match SLOs
