"""Table discovery utilities for Delta tables."""

import json
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class DeltaTable:
    """Information about a discovered Delta table."""

    path: Path
    table_id: str
    version: int
    num_files: int
    size_bytes: int
    last_modified: Optional[float] = None


class TableDiscovery:
    """Discovers Delta tables in filesystem paths."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def discover_tables(self, path: Path, recursive: bool = False) -> List[DeltaTable]:
        """Discover Delta tables in the given path."""
        tables = []

        if recursive:
            # Search recursively for _delta_log directories
            for delta_log in path.rglob("_delta_log"):
                if delta_log.is_dir():
                    table_path = delta_log.parent
                    table = self._analyze_table(table_path)
                    if table:
                        tables.append(table)
        else:
            # Search only in immediate subdirectories
            for item in path.iterdir():
                if item.is_dir():
                    delta_log = item / "_delta_log"
                    if delta_log.exists() and delta_log.is_dir():
                        table = self._analyze_table(item)
                        if table:
                            tables.append(table)

        return tables

    def _analyze_table(self, table_path: Path) -> Optional[DeltaTable]:
        """Analyze a Delta table and return its information."""
        try:
            delta_log = table_path / "_delta_log"

            if not delta_log.exists() or not delta_log.is_dir():
                return None

            # Find latest version
            version = self._get_latest_version(delta_log)
            if version is None:
                return None

            # Count files and estimate size
            num_files, size_bytes = self._count_files(delta_log, version)

            # Get last modified time
            last_modified = self._get_last_modified(delta_log)

            table_id = table_path.name

            return DeltaTable(
                path=table_path,
                table_id=table_id,
                version=version,
                num_files=num_files,
                size_bytes=size_bytes,
                last_modified=last_modified,
            )

        except Exception as e:
            if self.verbose:
                print(f"Warning: Failed to analyze table at {table_path}: {e}")
            return None

    def _get_latest_version(self, delta_log: Path) -> Optional[int]:
        """Get the latest version from Delta log."""
        latest_version = None

        # Check checkpoint files first
        checkpoint_files = list(delta_log.glob("*.checkpoint.parquet"))
        if checkpoint_files:
            for checkpoint_file in checkpoint_files:
                try:
                    version = int(checkpoint_file.stem.split(".")[0].split("_")[0])
                    if latest_version is None or version > latest_version:
                        latest_version = version
                except (ValueError, IndexError):
                    continue

        # Check JSON files
        json_files = list(delta_log.glob("*.json"))
        for json_file in json_files:
            try:
                version = int(json_file.stem.split("_")[0])
                if latest_version is None or version > latest_version:
                    latest_version = version
            except (ValueError, IndexError):
                continue

        return latest_version

    def _count_files(self, delta_log: Path, version: int) -> tuple[int, int]:
        """Count files and estimate size for a given version."""
        num_files = 0
        size_bytes = 0

        # Try to read from latest checkpoint first
        checkpoint_files = list(delta_log.glob("*.checkpoint.parquet"))
        if checkpoint_files:
            latest_checkpoint = max(
                checkpoint_files, key=lambda f: int(f.stem.split(".")[0].split("_")[0])
            )

            try:
                import pyarrow.parquet as pq

                table = pq.read_table(latest_checkpoint)
                df = table.to_pandas()

                # Count add files
                add_files = df[df["add"] == True]
                num_files = len(add_files)
                size_bytes = add_files["size"].fillna(0).sum()

                return num_files, int(size_bytes)

            except Exception:
                # Fallback to JSON processing
                pass

        # Fallback: process JSON files up to the version
        json_files = []
        for json_file in delta_log.glob("*.json"):
            try:
                file_version = int(json_file.stem.split("_")[0])
                if file_version <= version:
                    json_files.append((json_file, file_version))
            except (ValueError, IndexError):
                continue

        json_files.sort(key=lambda x: x[1])

        # Track current file state
        current_files = {}

        for json_file, file_version in json_files:
            try:
                with open(json_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            action = json.loads(line)

                            if action.get("add"):
                                add_action = action["add"]
                                path = add_action["path"]
                                current_files[path] = add_action.get("size", 0)

                            elif action.get("remove"):
                                remove_action = action["remove"]
                                path = remove_action["path"]
                                current_files.pop(path, None)

                        except json.JSONDecodeError:
                            continue

            except Exception:
                continue

        num_files = len(current_files)
        size_bytes = sum(current_files.values())

        return num_files, size_bytes

    def _get_last_modified(self, delta_log: Path) -> Optional[float]:
        """Get the last modified time of the Delta log."""
        try:
            latest_time = None

            for item in delta_log.iterdir():
                if item.is_file():
                    mtime = item.stat().st_mtime
                    if latest_time is None or mtime > latest_time:
                        latest_time = mtime

            return latest_time

        except Exception:
            return None
