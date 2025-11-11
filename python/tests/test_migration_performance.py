"""Performance tests for migration tools."""

import pytest
import tempfile
import shutil
import json
import time
import asyncio
from pathlib import Path

from delkalakedb.migration import MigrationManager, MigrationConfig
from delkalakedb.validation import ValidationManager


class TestMigrationPerformance:
    """Performance tests for migration functionality."""

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
    def large_delta_table(self, temp_dir):
        """Create a large Delta table for performance testing."""
        table_path = temp_dir / "large_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create many files to simulate a large table
        num_files = 1000
        for i in range(num_files):
            version = i
            log_entry = {
                "add": {
                    "path": f"part-{i:05d}.parquet",
                    "size": 1024 * 1024,  # 1MB per file
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {1000 + i}}}',
                    "partitionValues": {"year": "2023", "month": f"{(i % 12) + 1:02d}"},
                    "tags": {"batch_id": str(i // 100)},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        return table_path

    @pytest.mark.asyncio
    async def test_large_table_migration_performance(
        self, large_delta_table, sqlite_db_url
    ):
        """Test migration performance with a large table."""
        table_id = "large_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=large_delta_table,
            table_id=table_id,
            batch_size=100,  # Smaller batch size for testing
            verbose=False,
        )

        manager = MigrationManager(config)

        # Measure migration time
        start_time = time.time()
        result = await manager.migrate()
        migration_time = time.time() - start_time

        assert result.success
        assert result.files_processed == 1000

        # Performance assertions (adjust based on expected performance)
        assert migration_time < 60.0  # Should complete within 60 seconds
        assert (
            result.files_processed / migration_time > 10
        )  # At least 10 files per second

    @pytest.mark.asyncio
    async def test_validation_performance(self, large_delta_table, sqlite_db_url):
        """Test validation performance with a large table."""
        table_id = "large_table"

        # First migrate the table
        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=large_delta_table,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)
        await manager.migrate()

        # Measure validation time
        validator = ValidationManager(sqlite_db_url, verbose=False)

        start_time = time.time()
        result = await validator.validate_table(large_delta_table, table_id)
        validation_time = time.time() - start_time

        assert result.is_valid
        assert result.files_checked == 1000

        # Performance assertions
        assert validation_time < 30.0  # Should complete within 30 seconds
        assert (
            result.files_checked / validation_time > 30
        )  # At least 30 files per second

    @pytest.mark.asyncio
    async def test_batch_size_performance(self, temp_dir, sqlite_db_url):
        """Test performance with different batch sizes."""
        # Create a medium-sized table
        table_path = temp_dir / "batch_test_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        num_files = 500
        for i in range(num_files):
            version = i
            log_entry = {
                "add": {
                    "path": f"part-{i:05d}.parquet",
                    "size": 1024 * 512,  # 512KB per file
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {500 + i}}}',
                    "partitionValues": {},
                    "tags": {},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        # Test different batch sizes
        batch_sizes = [10, 50, 100, 500, 1000]
        performance_results = {}

        for batch_size in batch_sizes:
            table_id = f"batch_test_{batch_size}"

            config = MigrationConfig(
                database_url=sqlite_db_url,
                table_path=table_path,
                table_id=table_id,
                batch_size=batch_size,
                verbose=False,
            )

            manager = MigrationManager(config)

            start_time = time.time()
            result = await manager.migrate()
            migration_time = time.time() - start_time

            performance_results[batch_size] = {
                "time": migration_time,
                "files_per_second": result.files_processed / migration_time,
            }

            assert result.success
            assert result.files_processed == num_files

        # Find optimal batch size
        optimal_batch_size = max(
            performance_results.keys(),
            key=lambda bs: performance_results[bs]["files_per_second"],
        )

        # Performance should be reasonable for all batch sizes
        for batch_size, metrics in performance_results.items():
            assert metrics["files_per_second"] > 5  # At least 5 files per second

    @pytest.mark.asyncio
    async def test_concurrent_migrations(self, temp_dir, sqlite_db_url):
        """Test performance with concurrent migrations."""
        # Create multiple tables
        tables = []
        for table_idx in range(5):
            table_path = temp_dir / f"concurrent_table_{table_idx}"
            table_path.mkdir()
            delta_log = table_path / "_delta_log"
            delta_log.mkdir()

            # Create 200 files per table
            for i in range(200):
                version = i
                log_entry = {
                    "add": {
                        "path": f"part-{i:05d}.parquet",
                        "size": 1024 * 256,  # 256KB per file
                        "modificationTime": 1609459200000 + i * 1000,
                        "dataChange": True,
                        "stats": f'{{"numRecords": {200 + i}}}',
                        "partitionValues": {},
                        "tags": {},
                    }
                }

                log_file = delta_log / f"{version:020d}.json"
                with open(log_file, "w") as f:
                    json.dump(log_entry, f)

            tables.append(table_path)

        # Migrate tables concurrently
        async def migrate_table(table_path, table_id):
            config = MigrationConfig(
                database_url=sqlite_db_url,
                table_path=table_path,
                table_id=table_id,
                verbose=False,
            )

            manager = MigrationManager(config)
            return await manager.migrate()

        start_time = time.time()

        tasks = [
            migrate_table(table_path, f"concurrent_table_{i}")
            for i, table_path in enumerate(tables)
        ]

        results = await asyncio.gather(*tasks)
        concurrent_time = time.time() - start_time

        # All migrations should succeed
        for result in results:
            assert result.success
            assert result.files_processed == 200

        # Concurrent migration should be faster than sequential
        total_files = sum(result.files_processed for result in results)
        files_per_second = total_files / concurrent_time

        assert (
            files_per_second > 20
        )  # Should process at least 20 files per second concurrently

    @pytest.mark.asyncio
    async def test_memory_usage_large_table(self, large_delta_table, sqlite_db_url):
        """Test memory usage with large tables."""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        table_id = "memory_test_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=large_delta_table,
            table_id=table_id,
            batch_size=50,  # Small batch size to test memory efficiency
            verbose=False,
        )

        manager = MigrationManager(config)
        result = await manager.migrate()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        assert result.success
        assert result.files_processed == 1000

        # Memory usage should be reasonable (less than 100MB increase for 1000 files)
        assert memory_increase < 100

    @pytest.mark.asyncio
    async def test_checkpoint_performance(self, temp_dir, sqlite_db_url):
        """Test performance with checkpoint files."""
        table_path = temp_dir / "checkpoint_table"
        table_path.mkdir()
        delta_log = table_path / "_delta_log"
        delta_log.mkdir()

        # Create a checkpoint file (simulated)
        # In real scenarios, this would be a Parquet file
        # For testing, we'll just create JSON files
        num_files = 100
        for i in range(num_files):
            version = i
            log_entry = {
                "add": {
                    "path": f"checkpoint-part-{i:05d}.parquet",
                    "size": 1024 * 2048,  # 2MB per file
                    "modificationTime": 1609459200000 + i * 1000,
                    "dataChange": True,
                    "stats": f'{{"numRecords": {2000 + i}}}',
                    "partitionValues": {"year": "2023"},
                    "tags": {"checkpoint": "true"},
                }
            }

            log_file = delta_log / f"{version:020d}.json"
            with open(log_file, "w") as f:
                json.dump(log_entry, f)

        table_id = "checkpoint_table"

        config = MigrationConfig(
            database_url=sqlite_db_url,
            table_path=table_path,
            table_id=table_id,
            verbose=False,
        )

        manager = MigrationManager(config)

        start_time = time.time()
        result = await manager.migrate()
        migration_time = time.time() - start_time

        assert result.success
        assert result.files_processed == num_files

        # Should handle large files efficiently
        assert migration_time < 30.0


if __name__ == "__main__":
    pytest.main([__file__])
