"""Migration management for Delta tables to SQL-backed metadata."""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
# from tqdm import tqdm  # Commented out to avoid dependency

import pyarrow as pa
import pyarrow.parquet as pq

from .database import DatabaseManager


@dataclass
class MigrationConfig:
    """Configuration for migration operations."""

    database_url: str
    table_path: Path
    table_id: Optional[str] = None
    batch_size: int = 1000
    resume: bool = False
    dry_run: bool = False
    verbose: bool = False


@dataclass
class MigrationResult:
    """Result of a migration operation."""

    success: bool
    table_id: str
    version: int
    files_processed: int
    duration: float
    error: Optional[str] = None


@dataclass
class MigrationStatus:
    """Status of a migration operation."""

    table_id: str
    status: str  # "pending", "running", "completed", "failed"
    version: Optional[int] = None
    files_processed: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class MigrationManager:
    """Manages migration of Delta tables to SQL-backed metadata."""

    def __init__(self, config: MigrationConfig):
        self.config = config
        self.table_id = config.table_id or config.table_path.name
        self.db_manager = DatabaseManager(config.database_url)

    async def migrate(self) -> MigrationResult:
        """Migrate a Delta table to SQL-backed metadata."""
        start_time = time.time()

        try:
            if self.config.verbose:
                print(f"Starting migration for table: {self.table_id}")
                print(f"Source path: {self.config.table_path}")
                print(f"Database URL: {self.config.database_url}")

            # Connect to database and ensure schema
            await self.db_manager.connect()
            await self.db_manager.ensure_schema()

            # Check for existing migration if resume is enabled
            resume_from_version = None
            if self.config.resume:
                existing_status = await self.db_manager.get_migration_status(
                    self.table_id
                )
                if existing_status and existing_status["status"] in [
                    "running",
                    "failed",
                ]:
                    resume_from_version = existing_status.get("version")
                    files_processed = existing_status.get("files_processed", 0)
                    if self.config.verbose:
                        print(f"Resuming migration from version {resume_from_version}")
                        print(f"Already processed {files_processed} files")

            # Update status to running
            await self.db_manager.update_migration_status(self.table_id, "running")

            # Discover table version and find latest checkpoint
            version, checkpoint_path = await self._find_latest_checkpoint()

            if self.config.verbose:
                print(f"Table version: {version}")
                print(f"Latest checkpoint: {checkpoint_path}")

            # Process checkpoint if available (only if not resuming or checkpoint is newer)
            files_processed = 0
            if checkpoint_path and (
                resume_from_version is None or resume_from_version < version
            ):
                checkpoint_files = await self._process_checkpoint(checkpoint_path)
                files_processed += len(checkpoint_files)

                if self.config.verbose:
                    print(f"Processed checkpoint with {len(checkpoint_files)} files")

                if not self.config.dry_run:
                    await self.db_manager.bulk_insert_files(
                        self.table_id, checkpoint_files
                    )
                    await self.db_manager.update_migration_status(
                        self.table_id,
                        "running",
                        version=version,
                        files_processed=files_processed,
                    )

            # Process JSON log files after checkpoint (or from resume point)
            json_files = await self._get_json_files_after_checkpoint(
                resume_from_version if resume_from_version is not None else version
            )

            total_json_files = len(json_files)
            for i, json_file in enumerate(json_files):
                if self.config.verbose:
                    print(
                        f"Processing JSON file {i + 1}/{total_json_files}: {json_file.name}"
                    )

                actions = await self._process_json_file(json_file)
                files_processed += len(actions)

                if self.config.verbose and actions:
                    print(f"  Found {len(actions)} actions")

                if not self.config.dry_run:
                    await self.db_manager.bulk_insert_files(self.table_id, actions)
                    await self.db_manager.update_migration_status(
                        self.table_id,
                        "running",
                        version=version,
                        files_processed=files_processed,
                    )

            # Discover table version and find latest checkpoint
            version, checkpoint_path = await self._find_latest_checkpoint()

            if self.config.verbose:
                print(f"Table version: {version}")
                print(f"Latest checkpoint: {checkpoint_path}")

            # Process checkpoint if available
            files_processed = 0
            if checkpoint_path:
                checkpoint_files = await self._process_checkpoint(checkpoint_path)
                files_processed += len(checkpoint_files)

                if not self.config.dry_run:
                    await self.db_manager.bulk_insert_files(
                        self.table_id, checkpoint_files
                    )
                    await self.db_manager.update_migration_status(
                        self.table_id,
                        "running",
                        version=version,
                        files_processed=files_processed,
                    )

            # Process JSON log files after checkpoint
            json_files = await self._get_json_files_after_checkpoint(version)
            for json_file in json_files:
                actions = await self._process_json_file(json_file)
                files_processed += len(actions)

                if not self.config.dry_run:
                    await self.db_manager.bulk_insert_files(self.table_id, actions)
                    await self.db_manager.update_migration_status(
                        self.table_id,
                        "running",
                        version=version,
                        files_processed=files_processed,
                    )

            # Update status to completed
            await self.db_manager.update_migration_status(
                self.table_id,
                "completed",
                version=version,
                files_processed=files_processed,
            )

            await self.db_manager.disconnect()

            duration = time.time() - start_time

            return MigrationResult(
                success=True,
                table_id=self.table_id,
                version=version,
                files_processed=files_processed,
                duration=duration,
            )

        except Exception as e:
            try:
                await self.db_manager.update_migration_status(
                    self.table_id, "failed", error_message=str(e)
                )
                await self.db_manager.disconnect()
            except:
                pass

            duration = time.time() - start_time
            return MigrationResult(
                success=False,
                table_id=self.table_id,
                version=0,
                files_processed=0,
                duration=duration,
                error=str(e),
            )

    async def _find_latest_checkpoint(self) -> tuple[int, Optional[Path]]:
        """Find the latest checkpoint and table version."""
        delta_log = self.config.table_path / "_delta_log"

        if not delta_log.exists():
            raise ValueError(
                f"_delta_log directory not found in {self.config.table_path}"
            )

        # Find checkpoint files
        checkpoint_files = list(delta_log.glob("*.checkpoint.parquet"))
        if not checkpoint_files:
            # No checkpoint found, find latest JSON file
            json_files = list(delta_log.glob("*.json"))
            if not json_files:
                raise ValueError("No checkpoint or JSON files found")

            latest_json = max(json_files, key=lambda f: int(f.stem.split("_")[0]))
            version = int(latest_json.stem.split("_")[0])
            return version, None

        # Find latest checkpoint
        latest_checkpoint = max(
            checkpoint_files, key=lambda f: int(f.stem.split(".")[0].split("_")[0])
        )
        version = int(latest_checkpoint.stem.split(".")[0].split("_")[0])
        return version, latest_checkpoint

    async def _process_checkpoint(self, checkpoint_path: Path) -> List[Dict[str, Any]]:
        """Process a checkpoint file and extract file actions."""
        table = pq.read_table(checkpoint_path)

        files = []
        for batch in table.to_batches():
            df = batch.to_pandas()

            # Extract add files
            add_files = df[df["add"] == True]
            for _, row in add_files.iterrows():
                files.append(
                    {
                        "path": row["path"],
                        "size": row.get("size", 0),
                        "modificationTime": row.get("modificationTime", 0),
                        "dataChange": row.get("dataChange", True),
                        "stats": row.get("stats", ""),
                        "tags": row.get("tags", "{}"),
                        "partitionValues": row.get("partitionValues", "{}"),
                        "action_type": "add",
                        "version": self._extract_version_from_path(checkpoint_path),
                    }
                )

            # Extract remove files
            remove_files = df[df["remove"] == True]
            for _, row in remove_files.iterrows():
                files.append(
                    {
                        "path": row["path"],
                        "deletionTimestamp": row.get("deletionTimestamp", 0),
                        "dataChange": row.get("dataChange", True),
                        "extendedFileMetadata": row.get("extendedFileMetadata", False),
                        "action_type": "remove",
                        "version": self._extract_version_from_path(checkpoint_path),
                    }
                )

        return files

    async def _get_json_files_after_checkpoint(
        self, checkpoint_version: int
    ) -> List[Path]:
        """Get JSON files after the checkpoint version."""
        delta_log = self.config.table_path / "_delta_log"
        json_files = []

        for json_file in delta_log.glob("*.json"):
            version = int(json_file.stem.split("_")[0])
            if version > checkpoint_version:
                json_files.append(json_file)

        return sorted(json_files, key=lambda f: int(f.stem.split("_")[0]))

    async def _process_json_file(self, json_file: Path) -> List[Dict[str, Any]]:
        """Process a JSON log file and extract actions."""
        files = []
        version = int(json_file.stem.split("_")[0])

        with open(json_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    action = json.loads(line)

                    if action.get("add"):
                        add_action = action["add"]
                        files.append(
                            {
                                "path": add_action["path"],
                                "size": add_action.get("size", 0),
                                "modificationTime": add_action.get(
                                    "modificationTime", 0
                                ),
                                "dataChange": add_action.get("dataChange", True),
                                "stats": add_action.get("stats", ""),
                                "tags": json.dumps(add_action.get("tags", {})),
                                "partitionValues": json.dumps(
                                    add_action.get("partitionValues", {})
                                ),
                                "action_type": "add",
                                "version": version,
                            }
                        )

                    elif action.get("remove"):
                        remove_action = action["remove"]
                        files.append(
                            {
                                "path": remove_action["path"],
                                "deletionTimestamp": remove_action.get(
                                    "deletionTimestamp", 0
                                ),
                                "dataChange": remove_action.get("dataChange", True),
                                "extendedFileMetadata": remove_action.get(
                                    "extendedFileMetadata", False
                                ),
                                "action_type": "remove",
                                "version": version,
                            }
                        )

                except json.JSONDecodeError as e:
                    if self.config.verbose:
                        print(f"Warning: Failed to parse JSON line in {json_file}: {e}")
                    continue

        return files

    async def _bulk_insert_files(self, files: List[Dict[str, Any]]) -> None:
        """Bulk insert file actions into SQL database."""
        if not files:
            return

        if self.config.verbose:
            print(f"Bulk inserting {len(files)} file actions")

        await self.db_manager.bulk_insert_files(self.table_id, files)

    def _extract_version_from_path(self, path: Path) -> int:
        """Extract version number from checkpoint or JSON file path."""
        stem = path.stem
        if ".checkpoint" in stem:
            version_str = stem.split(".")[0].split("_")[0]
        else:
            version_str = stem.split("_")[0]
        return int(version_str)

    async def get_table_status(self, table_id: str) -> Optional[MigrationStatus]:
        """Get migration status for a specific table."""
        await self.db_manager.connect()
        status_data = await self.db_manager.get_migration_status(table_id)
        await self.db_manager.disconnect()

        if status_data:
            return MigrationStatus(
                table_id=status_data["table_id"],
                status=status_data["status"],
                version=status_data.get("version"),
                files_processed=status_data.get("files_processed", 0),
                started_at=status_data.get("started_at"),
                completed_at=status_data.get("completed_at"),
                error_message=status_data.get("error_message"),
            )
        return None

    async def get_all_migration_status(self) -> List[MigrationStatus]:
        """Get migration status for all tables."""
        await self.db_manager.connect()
        status_list = await self.db_manager.get_all_migration_status()
        await self.db_manager.disconnect()

        return [
            MigrationStatus(
                table_id=status["table_id"],
                status=status["status"],
                version=status.get("version"),
                files_processed=status.get("files_processed", 0),
                started_at=status.get("started_at"),
                completed_at=status.get("completed_at"),
                error_message=status.get("error_message"),
            )
            for status in status_list
        ]
