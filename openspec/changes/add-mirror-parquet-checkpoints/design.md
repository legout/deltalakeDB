## Context
Add Parquet checkpoint mirroring to reduce replay time, complementing JSON mirroring. Checkpoints must match the Delta protocol schema and be emitted at a configurable cadence (PRD §11 config, §17 rules).

## Goals / Non-Goals
- Goals
  - Serialize checkpoints deterministically for a given version `V`.
  - Only emit after JSON `V.json` exists.
  - Ensure replay correctness: checkpoint at `V` + JSON `>V` equals full replay.
- Non-Goals
  - Checkpoint compaction or rewriting policies (future work).

## Decisions
- Triggering
  - Emit when `V % checkpoint_interval == 0` (default 10); allow size-based override later.
- Input
  - Build checkpoint from the authoritative SQL snapshot at `V` to avoid drift.
- Schema & encoding
  - Match Delta’s checkpoint schema for the active protocol; ZSTD compression default; target row groups ~128MB for large tables.
- Write strategy
  - Write to a temp object and atomically rename; if rename unsupported, use conditional put or verify digest.
- Validation
  - Golden tests: replay from checkpoint + JSON must yield identical active file list and metadata.

## Risks / Trade-offs
- Parquet writer version/encoding differences may affect byte-level equality; rely on logical equivalence and schema conformance.
- Large checkpoints can be slow to write; consider parallelization of row groups later.

## Open Questions
- Should we support multiple concurrent checkpoint intervals per table (e.g., 10 and 1000) for very large logs?

