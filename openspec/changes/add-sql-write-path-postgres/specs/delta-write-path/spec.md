## ADDED Requirements
### Requirement: SQL Write Path (Postgres)
The system SHALL provide a Postgres-backed `SqlTxnLogWriter` that records a commit’s actions within a single SQL transaction and advances the table version atomically.

#### Scenario: Optimistic concurrency via CAS
- **WHEN** committing expected version `N+1`
- **THEN** the writer updates `dl_table_heads.current_version` from `N` to `N+1` using a conditional update; otherwise returns a Concurrency error

#### Scenario: Atomic action persistence
- **WHEN** a commit succeeds
- **THEN** rows for actions exist in `dl_add_files`/`dl_remove_files`/`dl_metadata_updates`/`dl_protocol_updates` and a ledger row in `dl_table_versions`

#### Scenario: Idempotent commit by app_id
- **WHEN** the same logical commit is retried with identical `app_id`/txn markers
- **THEN** the writer treats it as idempotent and does not create duplicate action rows

