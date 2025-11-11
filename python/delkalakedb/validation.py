"""Validation tools for migrated Delta tables."""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from .database import DatabaseManager


@dataclass
class ValidationResult:
    """Result of table validation."""

    is_valid: bool
    table_id: str
    issues: List[str]
    warnings: List[str]
    files_checked: int
    mismatches: int


@dataclass
class RollbackResult:
    """Result of rollback verification."""

    can_rollback: bool
    table_id: str
    rollback_method: str
    steps: List[str]
    warnings: List[str]
    estimated_time: float  # in seconds


@dataclass
class FileSnapshot:
    """Snapshot of file state at a version."""

    path: str
    size: int
    modification_time: int
    version: int
    action_type: str  # 'add' or 'remove'
    checksum: Optional[str] = None


class ValidationManager:
    """Manages validation of migrated Delta tables."""

    def __init__(self, database_url: str, verbose: bool = False):
        self.database_url = database_url
        self.verbose = verbose
        self.db_manager = DatabaseManager(database_url)

    async def validate_table(self, table_path: Path, table_id: str) -> ValidationResult:
        """Validate a migrated table against file-based metadata."""
        issues = []
        warnings = []
        files_checked = 0
        mismatches = 0

        try:
            if self.verbose:
                print(f"Validating table {table_id} at {table_path}")

            # Connect to database
            await self.db_manager.connect()

            # Get file-based snapshot
            file_snapshot = await self._get_file_based_snapshot(table_path)

            # Get SQL-based snapshot
            sql_snapshot = await self._get_sql_based_snapshot(table_id)

            # Compare snapshots
            mismatches = await self._compare_snapshots(
                file_snapshot, sql_snapshot, issues, warnings
            )

            files_checked = len(file_snapshot)

            await self.db_manager.disconnect()

            is_valid = len(issues) == 0

            return ValidationResult(
                is_valid=is_valid,
                table_id=table_id,
                issues=issues,
                warnings=warnings,
                files_checked=files_checked,
                mismatches=mismatches,
            )

        except Exception as e:
            try:
                await self.db_manager.disconnect()
            except:
                pass

            issues.append(f"Validation failed with error: {str(e)}")
            return ValidationResult(
                is_valid=False,
                table_id=table_id,
                issues=issues,
                warnings=warnings,
                files_checked=files_checked,
                mismatches=mismatches,
            )

    async def _get_file_based_snapshot(self, table_path: Path) -> List[FileSnapshot]:
        """Get file snapshot from Delta log files."""
        delta_log = table_path / "_delta_log"
        snapshot = []

        # Find latest checkpoint
        checkpoint_files = list(delta_log.glob("*.checkpoint.parquet"))
        if checkpoint_files:
            latest_checkpoint = max(
                checkpoint_files, key=lambda f: int(f.stem.split(".")[0].split("_")[0])
            )
            checkpoint_version = int(latest_checkpoint.stem.split(".")[0].split("_")[0])

            # Process checkpoint
            checkpoint_snapshot = await self._process_checkpoint_for_validation(
                latest_checkpoint, checkpoint_version
            )
            snapshot.extend(checkpoint_snapshot)

            # Process JSON files after checkpoint
            json_files = []
            for json_file in delta_log.glob("*.json"):
                version = int(json_file.stem.split("_")[0])
                if version > checkpoint_version:
                    json_files.append((json_file, version))

            json_files.sort(key=lambda x: x[1])

            for json_file, version in json_files:
                json_snapshot = await self._process_json_for_validation(
                    json_file, version
                )
                snapshot.extend(json_snapshot)
        else:
            # No checkpoint, process all JSON files
            json_files = []
            for json_file in delta_log.glob("*.json"):
                version = int(json_file.stem.split("_")[0])
                json_files.append((json_file, version))

            json_files.sort(key=lambda x: x[1])

            for json_file, version in json_files:
                json_snapshot = await self._process_json_for_validation(
                    json_file, version
                )
                snapshot.extend(json_snapshot)

        return snapshot

    async def _process_checkpoint_for_validation(
        self, checkpoint_path: Path, version: int
    ) -> List[FileSnapshot]:
        """Process checkpoint file for validation."""
        snapshot = []

        try:
            table = pq.read_table(checkpoint_path)

            for batch in table.to_batches():
                df = batch.to_pandas()

                # Process add files
                add_files = df[df["add"] == True]
                for _, row in add_files.iterrows():
                    snapshot.append(
                        FileSnapshot(
                            path=row["path"],
                            size=row.get("size", 0),
                            modification_time=row.get("modificationTime", 0),
                            version=version,
                            action_type="add",
                            checksum=row.get(
                                "stats", ""
                            ),  # Use stats as checksum proxy
                        )
                    )

                # Process remove files
                remove_files = df[df["remove"] == True]
                for _, row in remove_files.iterrows():
                    snapshot.append(
                        FileSnapshot(
                            path=row["path"],
                            size=0,  # Remove files don't have size
                            modification_time=row.get("deletionTimestamp", 0),
                            version=version,
                            action_type="remove",
                            checksum=None,
                        )
                    )

        except Exception as e:
            if self.verbose:
                print(f"Warning: Failed to process checkpoint {checkpoint_path}: {e}")

        return snapshot

    async def _process_json_for_validation(
        self, json_file: Path, version: int
    ) -> List[FileSnapshot]:
        """Process JSON file for validation."""
        snapshot = []

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
                            snapshot.append(
                                FileSnapshot(
                                    path=add_action["path"],
                                    size=add_action.get("size", 0),
                                    modification_time=add_action.get(
                                        "modificationTime", 0
                                    ),
                                    version=version,
                                    action_type="add",
                                    checksum=add_action.get("stats", ""),
                                )
                            )

                        elif action.get("remove"):
                            remove_action = action["remove"]
                            snapshot.append(
                                FileSnapshot(
                                    path=remove_action["path"],
                                    size=0,
                                    modification_time=remove_action.get(
                                        "deletionTimestamp", 0
                                    ),
                                    version=version,
                                    action_type="remove",
                                    checksum=None,
                                )
                            )

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            if self.verbose:
                print(f"Warning: Failed to process JSON file {json_file}: {e}")

        return snapshot

    async def _get_sql_based_snapshot(self, table_id: str) -> List[FileSnapshot]:
        """Get file snapshot from SQL database."""
        sql_files = await self.db_manager.get_file_snapshot(table_id)

        snapshot = []
        for file_data in sql_files:
            snapshot.append(
                FileSnapshot(
                    path=file_data["path"],
                    size=file_data.get("size", 0),
                    modification_time=file_data.get("modification_time", 0),
                    version=file_data["version"],
                    action_type=file_data["action_type"],
                    checksum=file_data.get("stats", ""),
                )
            )

        return snapshot

    async def _compare_snapshots(
        self,
        file_snapshot: List[FileSnapshot],
        sql_snapshot: List[FileSnapshot],
        issues: List[str],
        warnings: List[str],
    ) -> int:
        """Compare file-based and SQL-based snapshots."""
        mismatches = 0

        # Create dictionaries for easier comparison
        file_dict = {f.path: f for f in file_snapshot}
        sql_dict = {f.path: f for f in sql_snapshot}

        # Check for missing files in SQL
        for path, file_info in file_dict.items():
            if path not in sql_dict:
                issues.append(f"File {path} found in Delta log but missing in SQL")
                mismatches += 1
            else:
                sql_info = sql_dict[path]

                # Compare file attributes
                if file_info.size != sql_info.size:
                    issues.append(
                        f"File {path} size mismatch: file={file_info.size}, sql={sql_info.size}"
                    )
                    mismatches += 1

                if file_info.action_type != sql_info.action_type:
                    issues.append(
                        f"File {path} action type mismatch: file={file_info.action_type}, sql={sql_info.action_type}"
                    )
                    mismatches += 1

        # Check for extra files in SQL
        for path in sql_dict:
            if path not in file_dict:
                issues.append(f"File {path} found in SQL but missing in Delta log")
                mismatches += 1

        return mismatches

    async def verify_rollback(self, table_id: str) -> RollbackResult:
        """Verify rollback capabilities for a migrated table."""
        steps = []
        warnings = []
        can_rollback = True
        rollback_method = "sql_cleanup"
        estimated_time = 0.0

        try:
            if self.verbose:
                print(f"Verifying rollback for table {table_id}")

            await self.db_manager.connect()

            # Check if table exists in SQL
            status = await self.db_manager.get_migration_status(table_id)
            table_exists = status is not None
            if not table_exists:
                can_rollback = False
                steps.append("❌ Table not found in SQL database")
                return RollbackResult(
                    can_rollback=False,
                    table_id=table_id,
                    rollback_method="none",
                    steps=steps,
                    warnings=warnings,
                    estimated_time=0.0,
                )

            # Get migration status
            status = await self.db_manager.get_migration_status(table_id)
            if not status:
                warnings.append(
                    "No migration status found - rollback may be incomplete"
                )

            # Check for dependent objects
            dependencies = await self._check_dependencies(table_id)
            if dependencies:
                warnings.append(f"Found {len(dependencies)} dependent objects")
                rollback_method = "cascade_cleanup"
                steps.extend([f"  - Handle dependency: {dep}" for dep in dependencies])

            # Estimate rollback time based on table size
            file_snapshot = await self.db_manager.get_file_snapshot(table_id)
            file_count = len(file_snapshot)
            estimated_time = max(30.0, file_count * 0.001)  # Base 30s + 1ms per file

            # Build rollback steps
            steps = [
                "1. Backup current SQL metadata (if needed)",
                "2. Remove SQL table entries for migrated files",
                "3. Clean up migration status records",
                "4. Verify file-based Delta log is intact",
                "5. Confirm table can be accessed using original Delta log",
            ]

            if rollback_method == "cascade_cleanup":
                steps.insert(2, "2b. Handle dependent objects (views, indexes, etc.)")
                steps.insert(6, "6. Recreate dependent objects if needed")

            # Verify Delta log is still accessible
            delta_log_accessible = await self._verify_delta_log_accessible(table_id)
            if not delta_log_accessible:
                can_rollback = False
                warnings.append("Delta log may not be accessible - rollback risky")
                steps.append("❌ Delta log verification failed")

            await self.db_manager.disconnect()

            return RollbackResult(
                can_rollback=can_rollback,
                table_id=table_id,
                rollback_method=rollback_method,
                steps=steps,
                warnings=warnings,
                estimated_time=estimated_time,
            )

        except Exception as e:
            try:
                await self.db_manager.disconnect()
            except:
                pass

            steps.append(f"❌ Rollback verification failed: {str(e)}")
            return RollbackResult(
                can_rollback=False,
                table_id=table_id,
                rollback_method="error",
                steps=steps,
                warnings=warnings + [f"Verification error: {str(e)}"],
                estimated_time=0.0,
            )

    async def _check_dependencies(self, table_id: str) -> List[str]:
        """Check for database objects that depend on the migrated table."""
        dependencies = []

        try:
            # This would need to be implemented based on the specific database
            # For now, return empty list as placeholder
            # In a real implementation, you'd query information_schema or equivalent
            pass
        except Exception as e:
            if self.verbose:
                print(f"Warning: Could not check dependencies: {e}")

        return dependencies

    async def _verify_delta_log_accessible(self, table_id: str) -> bool:
        """Verify that the Delta log is still accessible."""
        try:
            # This would need the table path to verify
            # For now, return True as placeholder
            # In a real implementation, you'd check if _delta_log directory exists and has files
            return True
        except Exception:
            return False

    async def compare_performance(
        self, table_path: Optional[Path], table_id: str
    ) -> Dict[str, Any]:
        """Compare performance between file-based and SQL-based metadata access."""
        results = {
            "file_based": {"read_time": 0.0, "file_count": 0},
            "sql_based": {"read_time": 0.0, "file_count": 0},
            "comparison": {"speedup": 0.0, "recommendation": ""},
            "warnings": [],
        }

        try:
            if self.verbose:
                print(f"Comparing performance for table {table_id}")

            await self.db_manager.connect()

            # Measure file-based performance
            if table_path:
                start_time = time.time()
                file_snapshot = await self._get_file_based_snapshot(table_path)
                file_time = time.time() - start_time
            else:
                file_snapshot = []
                file_time = 0.0
                results["warnings"] = [
                    "No table path provided for file-based comparison"
                ]

            # Measure SQL-based performance
            start_time = time.time()
            sql_snapshot = await self._get_sql_based_snapshot(table_id)
            sql_time = time.time() - start_time

            results["file_based"] = {
                "read_time": file_time,
                "file_count": len(file_snapshot),
            }
            results["sql_based"] = {
                "read_time": sql_time,
                "file_count": len(sql_snapshot),
            }

            # Calculate comparison
            if file_time > 0:
                speedup = file_time / sql_time if sql_time > 0 else float("inf")
                results["comparison"]["speedup"] = speedup

                if speedup > 2.0:
                    results["comparison"]["recommendation"] = (
                        "SQL-based shows significant performance improvement"
                    )
                elif speedup > 1.2:
                    results["comparison"]["recommendation"] = (
                        "SQL-based shows moderate performance improvement"
                    )
                elif speedup < 0.8:
                    results["comparison"]["recommendation"] = (
                        "File-based may be faster for this table"
                    )
                else:
                    results["comparison"]["recommendation"] = (
                        "Performance is comparable between methods"
                    )

            await self.db_manager.disconnect()

        except Exception as e:
            try:
                await self.db_manager.disconnect()
            except:
                pass

            results["error"] = {"message": str(e)}

        return results
