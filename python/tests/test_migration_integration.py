"""Integration tests for migration tools with real Delta tables."""

import pytest
import tempfile
import shutil
import json
import asyncio
from pathlib import Path
from datetime import datetime

from delkalakedb.migration import MigrationManager, MigrationConfig
from delkalakedb.validation import ValidationManager
from delkalakedb.discovery import TableDiscovery


class TestMigrationIntegration:
    """Integration tests for migration functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_delta_table(self, temp_dir):
        """Create a sample Delta table for testing."""
        table_path = temp_dir / "test_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a simple Delta log
        version = 0

        # Add file action
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

        return table_path

    @pytest.fixture
    def sqlite_db_url(self, temp_dir):
        """Create a SQLite database URL for testing."""
        db_path = temp_dir / "test.db"
        return f"sqlite:///{db_path}"

    @pytest.mark.asyncio
    async def test_full_migration_cycle(self, sample_delta_table, sqlite_db_url):
        """Test complete migration and validation cycle."""
        table_id = "test_table"

        # Test migration
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=sample_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success
        assert result.table_id == table_id
        assert result.version == 0
        assert result.files_processed == 1

        # Test validation
        validator = ValidationManager(sqlite_db_url, verbose=False)
        validation_result = await validator.validate_table(sample_delta_table, table_id)

        assert validation_result.is_valid
        assert len(validation_result.issues) == 0
        assert validation_result.files_checked == 1

    @pytest.mark.asyncio
    async def test_rollback_verification(self, sample_delta_table, sqlite_db_url):
        """Test rollback verification functionality."""
        table_id = "test_table"

        # First migrate the table
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=sample_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        await manager.migrate()

        # Test rollback verification
        validator = ValidationManager(sqlite_db_url, verbose=False)
        rollback_result = await validator.verify_rollback(table_id)

        assert rollback_result.can_rollback
        assert rollback_result.table_id == table_id
        assert len(rollback_result.steps) > 0
        assert rollback_result.estimated_time > 0

    @pytest.mark.asyncio
    async def test_performance_comparison(self, sample_delta_table, sqlite_db_url):
        """Test performance comparison functionality."""
        table_id = "test_table"

        # First migrate the table
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=sample_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        await manager.migrate()

        # Test performance comparison
        validator = ValidationManager(sqlite_db_url, verbose=False)
        perf_result = await validator.compare_performance(sample_delta_table, table_id)

        assert "file_based" in perf_result
        assert "sql_based" in perf_result
        assert "comparison" in perf_result
        assert perf_result["file_based"]["file_count"] == 1
        assert perf_result["sql_based"]["file_count"] == 1

    def test_table_discovery(self, temp_dir, sample_delta_table):
        """Test table discovery functionality."""
        discovery = TableDiscovery(verbose=False)

        # Test single table discovery
        tables = discovery.discover_tables(temp_dir, recursive=False)
        assert len(tables) == 1
        assert tables[0].table_id == "test_table"
        assert tables[0].path == sample_delta_table
        assert tables[0].version == 0

        # Test recursive discovery
        tables = discovery.discover_tables(temp_dir, recursive=True)
        assert len(tables) == 1

    @pytest.mark.asyncio
    async def test_migration_resume(self, sample_delta_table, sqlite_db_url):
        """Test migration resume functionality."""
        table_id = "test_table"

        # Start migration
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=sample_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success

        # Try to resume (should complete quickly)
        config.resume = True
        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success

    @pytest.mark.asyncio
    async def test_dry_run_migration(self, sample_delta_table, sqlite_db_url):
        """Test dry run migration functionality."""
        table_id = "test_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=sample_delta_table,
            table_id=table_id,
            dry_run=True,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success

        # Verify no data was actually inserted
        validator = ValidationManager(sqlite_db_url, verbose=False)
        validation_result = await validator.validate_table(sample_delta_table, table_id)

        # Should fail since no data was inserted in dry run
        assert not validation_result.is_valid

    @pytest.mark.asyncio
    async def test_multiple_file_delta_table(self, temp_dir, sqlite_db_url):
        """Test migration with multiple files in Delta table."""
        table_path = temp_dir / "multi_file_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create multiple log entries
        for i in range(3):
            version = i
            log_entry = {
                "add": {
                    "path": f"part-{i:05d}.parquet",
                    "size": 1024 * (i + 1),
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {100 * (i + 1)}}}',
                    "partitionValues": {},
                    "tags": {},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        table_id = "multi_file_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success
        assert result.version == 2  # Last version
        assert result.files_processed == 3

        # Validate
        validator = ValidationManager(sqlite_db_url, verbose=False)
        validation_result = await validator.validate_table(table_path, table_id)

        assert validation_result.is_valid
        assert validation_result.files_checked == 3

    @pytest.mark.asyncio
    async def test_remove_actions(self, temp_dir, sqlite_db_url):
        """Test migration with remove actions."""
        table_path = temp_dir / "remove_test_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Add a file
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

        # Remove the file
        version = 1
        log_entry = {
            "remove": {
                "path": "part-00000.parquet",
                "deletionTimestamp": 1609459201000,
                "dataChange": True,
            }
        }

        log_file = delta_log / f"{version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(log_entry, f)

        table_id = "remove_test_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        assert result.success
        assert result.version == 1
        assert result.files_processed == 2  # One add, one remove

        # Validate
        validator = ValidationManager(sqlite_db_url, verbose=False)
        validation_result = await validator.validate_table(table_path, table_id)

        assert validation_result.is_valid
        assert validation_result.files_checked == 2


if __name__ == "__main__":
    pytest.main([__file__])
