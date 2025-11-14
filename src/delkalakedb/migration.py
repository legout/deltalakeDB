"""Migration tooling primitives for the `dl` CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

try:  # Optional dependency used for checkpoint reads.
    import pyarrow.parquet as pq  # type: ignore
except ImportError:  # pragma: no cover - optional path
    pq = None  # type: ignore


LOGGER = logging.getLogger(__name__)


class MigrationError(Exception):
    """Raised when migration tooling fails."""


def _ms_to_iso(ts_ms: Optional[float]) -> Optional[str]:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True)


def _json_dump_list(value: Sequence[Any] | None) -> str:
    return json.dumps(list(value or []), separators=(",", ":"))


def _normalize_metadata(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    schema = payload.get("schemaString") or payload.get("schema_json")
    partitions = payload.get("partitionColumns") or payload.get("partition_columns") or []
    config = payload.get("configuration") or payload.get("table_properties") or {}
    return {
        "schema": schema,
        "partitionColumns": list(partitions),
        "configuration": config,
    }


def _normalize_protocol(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    return {
        "minReaderVersion": payload.get("minReaderVersion")
        or payload.get("min_reader_version"),
        "minWriterVersion": payload.get("minWriterVersion")
        or payload.get("min_writer_version"),
    }


@dataclass
class CheckpointInfo:
    version: int
    parts: int = 1

    def file_candidates(self, log_dir: Path) -> List[Path]:
        base = log_dir / f"{self.version:020}.checkpoint.parquet"
        if base.exists():
            return [base]
        candidates = sorted(log_dir.glob(f"{self.version:020}.checkpoint.*.parquet"))
        return candidates


@dataclass
class CommitBatch:
    version: int
    commit_info: Optional[Dict[str, Any]] = None
    adds: List[Dict[str, Any]] = field(default_factory=list)
    removes: List[Dict[str, Any]] = field(default_factory=list)
    metadata: List[Dict[str, Any]] = field(default_factory=list)
    protocol: List[Dict[str, Any]] = field(default_factory=list)
    txns: List[Dict[str, Any]] = field(default_factory=list)

    def latest_metadata(self) -> Optional[Dict[str, Any]]:
        return self.metadata[-1] if self.metadata else None

    def latest_protocol(self) -> Optional[Dict[str, Any]]:
        return self.protocol[-1] if self.protocol else None


@dataclass
class ImportConfig:
    database_path: Path
    log_dir: Path
    table_id: str
    table_name: Optional[str]
    table_location: str
    mode: str = "fast"  # fast uses checkpoint as baseline; full-history replays JSON.


@dataclass
class ImportResult:
    table_id: str
    versions_imported: int
    latest_version: Optional[int]
    checkpoint_version: Optional[int]
    duration_seconds: float
    summary: str


@dataclass
class TableSnapshot:
    version: Optional[int]
    committed_at: Optional[str]
    metadata: Optional[Dict[str, Any]]
    protocol: Optional[Dict[str, Any]]
    active_files: Dict[str, Dict[str, Any]]


@dataclass
class DriftThresholds:
    max_lag_seconds: Optional[float] = None
    max_file_drift: Optional[int] = None


@dataclass
class DriftReport:
    status: str
    file_missing_in_db: List[str]
    file_missing_in_log: List[str]
    metadata_matches: bool
    protocol_matches: bool
    version_delta: int
    lag_seconds: Optional[float]

    def metrics(self) -> Dict[str, Any]:
        file_drift = len(self.file_missing_in_db) + len(self.file_missing_in_log)
        return {
            "mirror_lag_seconds": self.lag_seconds or 0.0,
            "file_drift_count": file_drift,
            "metadata_drift": 0 if self.metadata_matches else 1,
            "protocol_drift": 0 if self.protocol_matches else 1,
            "version_delta": self.version_delta,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "file_missing_in_db": self.file_missing_in_db,
            "file_missing_in_log": self.file_missing_in_log,
            "metadata_matches": self.metadata_matches,
            "protocol_matches": self.protocol_matches,
            "version_delta": self.version_delta,
            "lag_seconds": self.lag_seconds,
            "metrics": self.metrics(),
        }


class DeltaLogReader:
    """Utility to read Delta JSON logs and optional checkpoints."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        if not self.log_dir.exists():
            raise MigrationError(f"Delta log dir not found: {log_dir}")

    def list_versions(self) -> List[int]:
        versions: List[int] = []
        for path in self.log_dir.glob("*.json"):
            version = self._version_from_path(path)
            if version is not None:
                versions.append(version)
        return sorted(set(versions))

    def iter_commits(self, start_version: int = 0) -> Iterator[CommitBatch]:
        for version in self.list_versions():
            if version < start_version:
                continue
            yield self._parse_commit_file(version)

    def _parse_commit_file(self, version: int) -> CommitBatch:
        path = self.log_dir / f"{version:020}.json"
        if not path.exists():
            raise MigrationError(f"Missing commit file for version {version}")
        batch = CommitBatch(version=version)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if "add" in payload:
                    batch.adds.append(payload["add"])
                elif "remove" in payload:
                    batch.removes.append(payload["remove"])
                elif "metaData" in payload:
                    batch.metadata.append(payload["metaData"])
                elif "protocol" in payload:
                    batch.protocol.append(payload["protocol"])
                elif "txn" in payload:
                    batch.txns.append(payload["txn"])
                elif "commitInfo" in payload:
                    batch.commit_info = payload["commitInfo"]
        return batch

    def read_checkpoint(self) -> tuple[Optional[CheckpointInfo], Optional[CommitBatch]]:
        meta_path = self.log_dir / "_last_checkpoint"
        if not meta_path.exists():
            return None, None
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        info = CheckpointInfo(version=int(data["version"]), parts=int(data.get("parts", 1)))
        if pq is None:
            LOGGER.info("pyarrow not available; skipping checkpoint replay")
            return info, None
        files = info.file_candidates(self.log_dir)
        if not files:
            LOGGER.warning("No checkpoint Parquet files found for version %s", info.version)
            return info, None
        batch = CommitBatch(version=info.version)
        for file in files:
            table = pq.read_table(file)
            for row in table.to_pylist():
                if not row:
                    continue
                if row.get("add"):
                    batch.adds.append(row["add"])
                if row.get("remove"):
                    batch.removes.append(row["remove"])
                if row.get("metaData"):
                    batch.metadata.append(row["metaData"])
                if row.get("protocol"):
                    batch.protocol.append(row["protocol"])
                if row.get("txn"):
                    batch.txns.append(row["txn"])
        if batch.adds or batch.metadata or batch.protocol:
            checkpoint_info = {
                "operation": "CHECKPOINT_IMPORT",
                "timestamp": int(time.time() * 1000),
                "operationParameters": {"source": "checkpoint", "parts": info.parts},
            }
            batch.commit_info = checkpoint_info
        return info, batch

    @staticmethod
    def _version_from_path(path: Path) -> Optional[int]:
        name = path.name
        if not name.endswith(".json"):
            return None
        stem = name.split(".")[0]
        if len(stem) != 20 or not stem.isdigit():
            return None
        return int(stem)


class MigrationDatabase:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dl_tables (
                table_id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                protocol_min_reader INTEGER NOT NULL DEFAULT 1,
                protocol_min_writer INTEGER NOT NULL DEFAULT 1,
                properties TEXT NOT NULL DEFAULT '{}',
                import_metadata TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS dl_table_versions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                committed_at TEXT NOT NULL,
                committer TEXT,
                operation TEXT,
                operation_params TEXT,
                PRIMARY KEY (table_id, version)
            );

            CREATE TABLE IF NOT EXISTS dl_add_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                size_bytes INTEGER,
                partition_values TEXT,
                stats TEXT,
                data_change INTEGER DEFAULT 1,
                modification_time INTEGER,
                PRIMARY KEY (table_id, version, path)
            );

            CREATE TABLE IF NOT EXISTS dl_remove_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                deletion_timestamp INTEGER,
                data_change INTEGER DEFAULT 1,
                PRIMARY KEY (table_id, version, path)
            );

            CREATE TABLE IF NOT EXISTS dl_metadata_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                schema_json TEXT NOT NULL,
                partition_columns TEXT,
                table_properties TEXT,
                PRIMARY KEY (table_id, version)
            );

            CREATE TABLE IF NOT EXISTS dl_protocol_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                PRIMARY KEY (table_id, version)
            );

            CREATE TABLE IF NOT EXISTS dl_txn_actions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                app_id TEXT NOT NULL,
                last_update INTEGER NOT NULL,
                PRIMARY KEY (table_id, version, app_id)
            );

            CREATE TABLE IF NOT EXISTS dl_import_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                checkpoint_version INTEGER,
                start_version INTEGER,
                end_version INTEGER,
                status TEXT NOT NULL,
                summary TEXT
            );
            """
        )

    def ensure_table_record(self, table_id: str, name: Optional[str], location: str) -> None:
        existing = self.conn.execute(
            "SELECT table_id FROM dl_tables WHERE table_id = ?", (table_id,)
        ).fetchone()
        if existing:
            return
        self.conn.execute(
            """
            INSERT INTO dl_tables (table_id, name, location, properties, import_metadata)
            VALUES (?, ?, ?, '{}', '{}')
            """,
            (table_id, name, location),
        )

    def max_imported_version(self, table_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(version) AS version FROM dl_table_versions WHERE table_id = ?",
            (table_id,),
        ).fetchone()
        return int(row["version"]) if row and row["version"] is not None else None

    def import_table(self, config: ImportConfig, reader: DeltaLogReader) -> ImportResult:
        self.ensure_schema()
        self.ensure_table_record(config.table_id, config.table_name, config.table_location)
        start = time.time()
        checkpoint_info, checkpoint_batch = reader.read_checkpoint()
        existing_version = self.max_imported_version(config.table_id)
        run_id = self._start_import_run(
            table_id=config.table_id,
            checkpoint_version=checkpoint_info.version if checkpoint_info else None,
            start_version=existing_version + 1 if existing_version is not None else 0,
        )
        versions_imported = 0
        latest_version = existing_version
        try:
            if (
                existing_version is None
                and config.mode == "fast"
                and checkpoint_batch is not None
            ):
                self._apply_commit(config.table_id, checkpoint_batch)
                versions_imported += 1
                latest_version = checkpoint_batch.version
            start_version = (latest_version + 1) if latest_version is not None else 0
            for commit in reader.iter_commits(start_version=start_version):
                self._apply_commit(config.table_id, commit)
                versions_imported += 1
                latest_version = commit.version
            summary = (
                f"Imported {versions_imported} version(s); latest version="
                f" {latest_version if latest_version is not None else 'n/a'}"
            )
            self._finish_import_run(run_id, "succeeded", latest_version, summary)
            duration = time.time() - start
            return ImportResult(
                table_id=config.table_id,
                versions_imported=versions_imported,
                latest_version=latest_version,
                checkpoint_version=checkpoint_info.version if checkpoint_info else None,
                duration_seconds=duration,
                summary=summary,
            )
        except Exception as exc:  # pragma: no cover - error path
            self._finish_import_run(run_id, "failed", latest_version, str(exc))
            raise

    def _start_import_run(
        self,
        table_id: str,
        checkpoint_version: Optional[int],
        start_version: int,
    ) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO dl_import_runs (
                table_id, started_at, checkpoint_version, start_version, status
            ) VALUES (?, ?, ?, ?, 'running')
            """,
            (table_id, _utcnow_iso(), checkpoint_version, start_version),
        )
        return int(cursor.lastrowid)

    def _finish_import_run(
        self,
        run_id: int,
        status: str,
        end_version: Optional[int],
        summary: str,
    ) -> None:
        self.conn.execute(
            """
            UPDATE dl_import_runs
            SET finished_at = ?, status = ?, end_version = ?, summary = ?
            WHERE id = ?
            """,
            (_utcnow_iso(), status, end_version, summary, run_id),
        )

    def _apply_commit(self, table_id: str, commit: CommitBatch) -> None:
        with self.conn:  # ensures atomicity and idempotence per version
            self.conn.execute(
                "DELETE FROM dl_table_versions WHERE table_id = ? AND version = ?",
                (table_id, commit.version),
            )
            self.conn.execute(
                """
                INSERT INTO dl_table_versions (
                    table_id, version, committed_at, committer, operation, operation_params
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    table_id,
                    commit.version,
                    _ms_to_iso(
                        (commit.commit_info or {}).get("timestamp")
                    )
                    or _utcnow_iso(),
                    (commit.commit_info or {}).get("userName"),
                    (commit.commit_info or {}).get("operation", "UNKNOWN"),
                    _json_dumps((commit.commit_info or {}).get("operationParameters")),
                ),
            )

            # Files
            self.conn.execute(
                "DELETE FROM dl_add_files WHERE table_id = ? AND version = ?",
                (table_id, commit.version),
            )
            self.conn.execute(
                "DELETE FROM dl_remove_files WHERE table_id = ? AND version = ?",
                (table_id, commit.version),
            )
            for add in commit.adds:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO dl_add_files (
                        table_id, version, path, size_bytes, partition_values, stats,
                        data_change, modification_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        commit.version,
                        add.get("path"),
                        add.get("size"),
                        _json_dumps(add.get("partitionValues")),
                        add.get("stats"),
                        1 if add.get("dataChange", True) else 0,
                        add.get("modificationTime"),
                    ),
                )
            for remove in commit.removes:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO dl_remove_files (
                        table_id, version, path, deletion_timestamp, data_change
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        commit.version,
                        remove.get("path"),
                        remove.get("deletionTimestamp"),
                        1 if remove.get("dataChange", True) else 0,
                    ),
                )

            if commit.metadata:
                meta = commit.latest_metadata()
                assert meta is not None  # for type checkers
                self.conn.execute(
                    "DELETE FROM dl_metadata_updates WHERE table_id = ? AND version = ?",
                    (table_id, commit.version),
                )
                self.conn.execute(
                    """
                    INSERT INTO dl_metadata_updates (
                        table_id, version, schema_json, partition_columns, table_properties
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        commit.version,
                        meta.get("schemaString", ""),
                        _json_dump_list(meta.get("partitionColumns")),
                        _json_dumps(meta.get("configuration")),
                    ),
                )
                self.conn.execute(
                    """
                    UPDATE dl_tables
                    SET name = COALESCE(?, name), properties = ?
                    WHERE table_id = ?
                    """,
                    (
                        meta.get("name"),
                        _json_dumps(meta.get("configuration")),
                        table_id,
                    ),
                )

            if commit.protocol:
                proto = commit.latest_protocol()
                assert proto is not None
                self.conn.execute(
                    "DELETE FROM dl_protocol_updates WHERE table_id = ? AND version = ?",
                    (table_id, commit.version),
                )
                self.conn.execute(
                    """
                    INSERT INTO dl_protocol_updates (
                        table_id, version, min_reader_version, min_writer_version
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        commit.version,
                        proto.get("minReaderVersion"),
                        proto.get("minWriterVersion"),
                    ),
                )
                self.conn.execute(
                    """
                    UPDATE dl_tables
                    SET protocol_min_reader = ?, protocol_min_writer = ?
                    WHERE table_id = ?
                    """,
                    (
                        proto.get("minReaderVersion"),
                        proto.get("minWriterVersion"),
                        table_id,
                    ),
                )

            for txn in commit.txns:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO dl_txn_actions (
                        table_id, version, app_id, last_update
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        commit.version,
                        txn.get("appId"),
                        txn.get("lastUpdated"),
                    ),
                )

    def snapshot_from_db(self, table_id: str) -> TableSnapshot:
        row = self.conn.execute(
            "SELECT MAX(version) AS version FROM dl_table_versions WHERE table_id = ?",
            (table_id,),
        ).fetchone()
        latest_version = int(row["version"]) if row and row["version"] is not None else None
        committed_at = None
        if latest_version is not None:
            committed_row = self.conn.execute(
                """
                SELECT committed_at FROM dl_table_versions
                WHERE table_id = ? AND version = ?
                """,
                (table_id, latest_version),
            ).fetchone()
            committed_at = committed_row["committed_at"] if committed_row else None

        meta_row = self.conn.execute(
            """
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (table_id,),
        ).fetchone()
        metadata = None
        if meta_row:
            metadata = {
                "schema_json": meta_row["schema_json"],
                "partition_columns": json.loads(meta_row["partition_columns"] or "[]"),
                "table_properties": json.loads(meta_row["table_properties"] or "{}"),
            }

        proto_row = self.conn.execute(
            """
            SELECT min_reader_version, min_writer_version
            FROM dl_protocol_updates
            WHERE table_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (table_id,),
        ).fetchone()
        protocol = None
        if proto_row:
            protocol = {
                "min_reader_version": proto_row["min_reader_version"],
                "min_writer_version": proto_row["min_writer_version"],
            }

        active: Dict[str, Dict[str, Any]] = {}
        for add in self.conn.execute(
            """
            SELECT path, size_bytes, partition_values, stats, data_change, modification_time
            FROM dl_add_files
            WHERE table_id = ?
            ORDER BY version ASC
            """,
            (table_id,),
        ):
            active[add["path"]] = {
                "size": add["size_bytes"],
                "partitionValues": json.loads(add["partition_values"] or "{}"),
                "stats": add["stats"],
                "modificationTime": add["modification_time"],
                "dataChange": bool(add["data_change"]),
            }
        for remove in self.conn.execute(
            """
            SELECT path FROM dl_remove_files
            WHERE table_id = ?
            ORDER BY version ASC
            """,
            (table_id,),
        ):
            active.pop(remove["path"], None)

        return TableSnapshot(
            version=latest_version,
            committed_at=committed_at,
            metadata=metadata,
            protocol=protocol,
            active_files=active,
        )


class SnapshotBuilder:
    def __init__(self) -> None:
        self.version: Optional[int] = None
        self.committed_at: Optional[str] = None
        self.metadata: Optional[Dict[str, Any]] = None
        self.protocol: Optional[Dict[str, Any]] = None
        self.active: Dict[str, Dict[str, Any]] = {}

    def apply(self, commit: CommitBatch) -> None:
        self.version = commit.version
        if commit.commit_info and commit.commit_info.get("timestamp"):
            self.committed_at = _ms_to_iso(commit.commit_info.get("timestamp"))
        if commit.metadata:
            self.metadata = commit.latest_metadata()
        if commit.protocol:
            self.protocol = commit.latest_protocol()
        for add in commit.adds:
            self.active[add.get("path")] = add
        for remove in commit.removes:
            path = remove.get("path")
            if path in self.active:
                del self.active[path]

    def to_snapshot(self) -> TableSnapshot:
        return TableSnapshot(
            version=self.version,
            committed_at=self.committed_at,
            metadata=self.metadata,
            protocol=self.protocol,
            active_files=self.active,
        )


def build_log_snapshot(reader: DeltaLogReader) -> TableSnapshot:
    builder = SnapshotBuilder()
    for commit in reader.iter_commits():
        builder.apply(commit)
    return builder.to_snapshot()


def diff_snapshots(
    db_snapshot: TableSnapshot,
    log_snapshot: TableSnapshot,
    thresholds: DriftThresholds,
) -> DriftReport:
    db_files = set(db_snapshot.active_files.keys())
    log_files = set(log_snapshot.active_files.keys())
    missing_in_db = sorted(log_files - db_files)
    missing_in_log = sorted(db_files - log_files)
    metadata_matches = _normalize_metadata(db_snapshot.metadata) == _normalize_metadata(
        log_snapshot.metadata
    )
    protocol_matches = _normalize_protocol(db_snapshot.protocol) == _normalize_protocol(
        log_snapshot.protocol
    )
    version_delta = (db_snapshot.version or 0) - (log_snapshot.version or 0)

    lag_seconds = None
    if db_snapshot.committed_at and log_snapshot.committed_at:
        lag_seconds = (
            datetime.fromisoformat(db_snapshot.committed_at).timestamp()
            - datetime.fromisoformat(log_snapshot.committed_at).timestamp()
        )
        lag_seconds = round(lag_seconds, 3)

    file_drift = len(missing_in_db) + len(missing_in_log)
    allowed_file_drift = thresholds.max_file_drift if thresholds.max_file_drift is not None else 0
    file_violation = file_drift > allowed_file_drift

    lag_breach = (
        thresholds.max_lag_seconds is not None
        and lag_seconds is not None
        and lag_seconds > thresholds.max_lag_seconds
    )

    status = "ok"
    if file_violation or not metadata_matches or not protocol_matches:
        status = "drift"
    elif lag_breach:
        status = "lagging"

    return DriftReport(
        status=status,
        file_missing_in_db=missing_in_db,
        file_missing_in_log=missing_in_log,
        metadata_matches=metadata_matches,
        protocol_matches=protocol_matches,
        version_delta=version_delta,
        lag_seconds=lag_seconds,
    )
