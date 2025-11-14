## 1. Tooling
- [x] 1.1 Implement `dl import` to read latest checkpoint + JSON commits and populate `dl_*` tables
- [x] 1.2 Implement snapshot diff: SQL-derived vs `_delta_log`-derived active files and metadata
- [x] 1.3 Add drift reporting with exit codes for CI and human-readable summaries

## 2. Cutover & Rollback
- [x] 2.1 Document writer cutover to `deltasql://` URIs
- [x] 2.2 Provide rollback procedure to file-backed writers
- [x] 2.3 Provide post-cutover parity validation commands

## 3. Alerts & SLOs
- [x] 3.1 Integrate drift alerts and thresholds
- [x] 3.2 Expose mirror lag and parity metrics for dashboards

## 4. Validation
- [x] 4.1 `openspec validate add-migration-tooling --strict`
