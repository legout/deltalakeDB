"""Failure scenario tests for migration tools."""

import pytest
import tempfile
import shutil
import json
import asyncio
from pathlib import Path
from unittest.mock import patch, AsyncMock

from delkalakedb.migration import MigrationManager, MigrationConfig
from delkalakedb.validation import ValidationManager
from delkalakedb.database import DatabaseManager


class TestMigrationFailureScenarios:
    """Test migration behavior under various failure conditions."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sqlite_db_url(self, temp_dir):
        """Create a SQLite database URL for testing."""
        db_path = temp_dir / "test.db"
        return f"sqlite:///{db_path}"

    @pytest.fixture
    def corrupted_delta_table(self, temp_dir):
        """Create a Delta table with corrupted log files."""
        table_path = temp_dir / "corrupted_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a valid log file
        version = 0
        log_entry = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": '{"numRecords": 100}',
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        # Create a corrupted log file
        version = 1
        corrupted_file = delta_log / f"{version:020d}.json"
        with open(corrupted_file, "w") as f:
            f.write("invalid json content {")

        return table_path

    @pytest.fixture
    def incomplete_delta_table(self, temp_dir):
        """Create a Delta table with incomplete log files."""
        table_path = temp_dir / "incomplete_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create an incomplete log file
        version = 0
        incomplete_file = delta_log / f"{version:020d}.json"
        with open(incomplete_file, "w") as f:
            f.write('{"add": {"path": "part-00000.parquet"')  # Incomplete JSON

        return table_path

    @pytest.mark.asyncio
    async def test_corrupted_log_handling(self, corrupted_delta_table, sqlite_db_url):
        """Test migration with corrupted log files."""
        table_id = "corrupted_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=corrupted_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should succeed despite corrupted file (skip it)
        assert result.success
        assert result.files_processed >= 1  # At least the valid file

    @pytest.mark.asyncio
    async def test_incomplete_log_handling(self, incomplete_delta_table, sqlite_db_url):
        """Test migration with incomplete log files."""
        table_id = "incomplete_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=incomplete_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should handle incomplete files gracefully
        assert result.success or result.error is not None

    @pytest.mark.asyncio
    async def test_database_connection_failure(self, temp_dir):
        """Test migration with database connection failure."""
        table_path = temp_dir / "test_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a simple log file
        version = 0
        log_entry = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": '{"numRecords": 100}',
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        # Use invalid database URL
        invalid_db_url = "postgresql://invalid:invalid@localhost:9999/invalid"

        config = MigrationConfig(
            database_url=invalid_db_url,
            table_path=table_path,
            table_id="test_table",
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should fail gracefully
        assert not result.success
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_missing_delta_log(self, temp_dir, sqlite_db_url):
        """Test migration with missing _delta_log directory."""
        table_path = temp_dir / "no_log_table"
        table_path.mkdir()
        # Don't create _delta_log directory

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id="no_log_table",
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should fail with clear error message
        assert not result.success
        assert "_delta_log" in result.error.lower()

    @pytest.mark.asyncio
    async def test_empty_delta_log(self, temp_dir, sqlite_db_url):
        """Test migration with empty _delta_log directory."""
        table_path = temp_dir / "empty_log_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()
        # Don't add any files

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id="empty_log_table",
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should fail with clear error message
        assert not result.success
        assert (
            "no checkpoint" in result.error.lower() or "no json" in result.error.lower()
        )

    @pytest.mark.asyncio
    async def test_permission_denied(self, temp_dir, sqlite_db_url):
        """Test migration with permission issues."""
        table_path = temp_dir / "permission_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a log file
        version = 0
        log_entry = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": '{"numRecords": 100}',
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        # Mock permission error
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            config = MigrationConfig(
                database_url=sqlite_db_url,
                table_path=table_path,
                table_id="permission_table",
                verbose=False,
            )

            manager = MigrationManager(config)
            result = await manager.migrate()

            # Should fail gracefully
            assert not result.success
            assert "permission" in result.error.lower()

    @pytest.mark.asyncio
    async def test_disk_space_exhausted(self, temp_dir, sqlite_db_url):
        """Test migration when disk space is exhausted."""
        table_path = temp_dir / "disk_full_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a log file
        version = 0
        log_entry = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": '{"numRecords": 100}',
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        # Mock disk space error
        with patch(
            "delkalakedb.database.DatabaseManager.bulk_insert_files",
            side_effect=OSError("No space left on device"),
        ):
            config = MigrationConfig(
                database_url=sqlite_db_url,
                table_path=table_path,
                table_id="disk_full_table",
                verbose=False,
            )

            manager = MigrationManager(config)
            result = await manager.migrate()

            # Should fail gracefully
            assert not result.success
            assert "space" in result.error.lower()

    @pytest.mark.asyncio
    async def test_network_interruption(self, temp_dir, sqlite_db_url):
        """Test migration with network interruptions."""
        table_path = temp_dir / "network_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a log file
        version = 0
        log_entry = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": '{"numRecords": 100}',
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        # Mock network error
        with patch(
            "delkalakedb.database.DatabaseManager.connect",
            side_effect=ConnectionError("Network unreachable"),
        ):
            config = MigrationConfig(
                database_url=sqlite_db_url,
                table_path=table_path,
                table_id="network_table",
                verbose=False,
            )

            manager = MigrationManager(config)
            result = await manager.migrate()

            # Should fail gracefully
            assert not result.success
            assert "network" in result.error.lower()

    @pytest.mark.asyncio
    async def test_resume_after_failure(self, temp_dir, sqlite_db_url):
        """Test resume functionality after migration failure."""
        table_path = temp_dir / "resume_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create multiple log files
        for i in range(5):
            version = i
            log_entry = {
                "add": {
                    "path": f"part-{i:05d}.parquet",
                    "size": 1024,
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {100 + i}}}',
                    "partitionValues": {},
                    "tags": {},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        table_id = "resume_table"

        # First migration - simulate failure after processing some files
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)

        # Mock a failure during bulk insert
        original_bulk_insert = manager.db_manager.bulk_insert_files
        call_count = 0

        async def failing_bulk_insert(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Fail on second call
                raise Exception("Simulated failure")
            return await original_bulk_insert(*args, **kwargs)

        with patch.object(
            manager.db_manager, "bulk_insert_files", side_effect=failing_bulk_insert
        ):
            result = await manager.migrate()
            assert not result.success

        # Resume migration
        config.resume = True
        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should succeed on resume
        assert result.success
        assert result.files_processed == 5

    @pytest.mark.asyncio
    async def test_validation_with_missing_files(self, temp_dir, sqlite_db_url):
        """Test validation when files are missing from SQL."""
        table_path = temp_dir / "validation_test_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create log files
        for i in range(3):
            version = i
            log_entry = {
                "add": {
                    "path": f"part-{i:05d}.parquet",
                    "size": 1024,
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {100 + i}}}',
                    "partitionValues": {},
                    "tags": {},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        table_id = "validation_test_table"

        # Migrate only some files (simulate partial migration)
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)

        # Mock to only insert first 2 files
        original_bulk_insert = manager.db_manager.bulk_insert_files

        async def partial_bulk_insert(table_id, files):
            # Only insert first 2 files
            return await original_bulk_insert(table_id, files[:2])

        with patch.object(
            manager.db_manager, "bulk_insert_files", side_effect=partial_bulk_insert
        ):
            result = await manager.migrate()
            assert result.success

        # Validation should detect missing files
        validator = ValidationManager(sqlite_db_url, verbose=False)
        validation_result = await validator.validate_table(table_path, table_id)

        assert not validation_result.is_valid
        assert len(validation_result.issues) > 0
        assert any("missing in SQL" in issue for issue in validation_result.issues)

    @pytest.mark.asyncio
    async def test_rollback_verification_with_no_migration(self, sqlite_db_url):
        """Test rollback verification for non-existent migration."""
        table_id = "non_existent_table"

        validator = ValidationManager(sqlite_db_url, verbose=False)
        rollback_result = await validator.verify_rollback(table_id)

        assert not rollback_result.can_rollback
        assert "not found" in " ".join(rollback_result.steps).lower()

    @pytest.mark.asyncio
    async def test_large_file_handling(self, temp_dir, sqlite_db_url):
        """Test handling of very large file entries."""
        table_path = temp_dir / "large_file_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a log entry with very large stats
        large_stats = '{"numRecords": ' + "9" * 1000000 + "}"  # Very large JSON

        version = 0
        log_entry = {
            "add": {
                "path": "large-part.parquet",
                "size": 1024 * 1024 * 1024,  # 1GB
                "modificationTime": 1609459200000,
                "dataChange": True,
                "stats": large_stats,
                "partitionValues": {},
                "tags": {},
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        table_id = "large_file_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        # Should handle large files without memory issues
        assert result.success
        assert result.files_processed == 1


if __name__ == "__main__":
    pytest.main([__file__])
