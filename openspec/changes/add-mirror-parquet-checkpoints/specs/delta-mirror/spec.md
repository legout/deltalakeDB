## ADDED Requirements
### Requirement: Parquet Checkpoint Mirroring
The system SHALL emit Parquet checkpoints at a configurable interval to reduce replay time for large logs.

#### Scenario: Interval-based checkpoint
- **WHEN** version `V` is committed and `V % checkpoint_interval == 0`
- **THEN** a `_delta_log/V.checkpoint.parquet` file exists containing the canonical snapshot at `V`

#### Scenario: Replay from checkpoint
- **WHEN** an external reader loads the table using the checkpoint at `V`
- **THEN** replaying JSON files `> V` reconstructs the latest snapshot equivalently to full JSON replay
