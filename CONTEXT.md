# deltalakeDB

A SQL-backed metadata plane for Delta Lake: a relational database is the authoritative store for table metadata, while canonical `_delta_log/` artifacts are mirrored out for external engines.

## Language

### Metadata plane

**Catalog**:
The set of `dl_*` tables in the backing database holding table versions, metadata updates, protocol updates, and add/remove file actions. The catalog is authoritative for our stack.
_Avoid_: database, metadata store, registry

**Engine**:
A supported SQL backend for the catalog: Postgres, SQLite, or DuckDB.
_Avoid_: database (that's the catalog's home), driver

**Txn log**:
The versioned sequence of commits for a table, abstract over its backing store — readable and writable the same way whether backed by the catalog or by files.
_Avoid_: WAL, changelog, journal

### Delta compatibility

**Version**:
The strictly increasing ordinal assigned to each commit of a table. Optimistic concurrency relies on it.
_Avoid_: sequence number, revision

**Commit**:
The set of actions (adds, removes, metadata, protocol) applied atomically to a table at one version.
_Avoid_: transaction (that's the database mechanism), update

**Active files**:
The set of files belonging to a table at a version — adds minus removes, latest action per path wins.
_Avoid_: live files, current files

**Snapshot**:
A consistent read view of a table at a version or timestamp: metadata, protocol, and active files together.
_Avoid_: checkpoint (an artifact), state, view

**Mirror**:
The post-commit process that emits canonical `_delta_log/` artifacts — JSON commit files and Parquet checkpoints — from the catalog for external engines.
_Avoid_: exporter, syncer, replicator

**Checkpoint**:
A Parquet artifact in `_delta_log/` summarizing the txn log up to a version, so readers don't replay every JSON commit.
_Avoid_: snapshot (that's the read view)
