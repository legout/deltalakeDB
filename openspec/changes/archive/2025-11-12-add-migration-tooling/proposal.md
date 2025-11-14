## Why
Provide end-to-end migration tooling to bootstrap SQL metadata from existing Delta `_delta_log/`, validate parity, cut over writers to SQL-backed mode, and enable safe rollback. This implements PRD §12 (Migration Plan) with operational guardrails.

## What Changes
- Add migration CLI to import from checkpoints and JSON logs into normalized SQL tables
- Add validation tooling to compare SQL-derived snapshots against `_delta_log` snapshots (parity/diff)
- Define cutover procedure and rollback steps; document operational runbook
- Add drift detection and alerting integration

## Impact
- Affected specs: migration
- Affected code: migration CLI, snapshot diff/validation utilities, docs/runbooks
- Dependencies: object_store, parquet, serde_json, database adapters

