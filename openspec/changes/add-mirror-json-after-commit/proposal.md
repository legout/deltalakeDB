## Why
Preserve full Delta compatibility by mirroring committed actions to canonical `_delta_log/NNNNNNNNNN.json` files after a successful DB commit. Start with JSON only; checkpoints can come later.

## What Changes
- Add a mirror engine that serializes committed actions to canonical Delta JSON.
- Write JSON to object storage only after DB commit succeeds.
- Record mirror status and retry on failures.

## Impact
- Affected specs: delta-mirror
- Affected code: Rust `mirror` crate and object-store IO integration.

## References
- PRD §4.2 Write Path – Mirror after DB commit
- PRD §8 Failure Modes & Recovery – Mirror lag and retries
- PRD §17 Appendix – Mirror serialization rules (JSON/Checkpoint)
- PRD §15 M2 – JSON mirror in write path
