## Why
Provide a minimal CLI to bootstrap SQL metadata from an existing Delta table’s `_delta_log/` so teams can adopt SQL-backed reads without rewriting data.

## What Changes
- Add `dl import` (or equivalent) command that reads latest checkpoint (if present) and replays JSON to populate `dl_*` tables.
- Initialize `current_version` to the latest imported version.

## Impact
- Affected specs: migration-tooling
- Affected code: Rust CLI (pyo3 binding optional), readers for `_delta_log` JSON + checkpoint.

## References
- PRD §12 Migration Plan – Bootstrap/import from existing `_delta_log`
- PRD §13 Rollout Strategy – Alpha adoption path
