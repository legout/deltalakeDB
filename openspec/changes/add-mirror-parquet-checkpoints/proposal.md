## Why
Reduce replay cost for large tables by emitting periodic Parquet checkpoints in `_delta_log/`, complementing JSON mirroring.

## What Changes
- Add checkpoint serializer producing canonical Parquet checkpoints per configured interval (e.g., every 10 versions).
- Write checkpoints after the corresponding JSON commit is present.

## Impact
- Affected specs: delta-mirror
- Affected code: Mirror crate adds Parquet writer and interval policy.

## References
- PRD §17 Appendix – Checkpoint Parquet schema and rules
- PRD §4.2 Write Path – Mirror and checkpoint cadence
- PRD §5 Non-Functional – Replay/performance targets
