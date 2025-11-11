"""Basic tests for migration functionality."""

import pytest
import tempfile
import json
from pathlib import Path
from delkalakedb.migration import MigrationManager, MigrationConfig
from delkalakedb.validation import ValidationManager
from delkalakedb.discovery import TableDiscovery


@pytest.fixture
def temp_delta_table():
    """Create a temporary Delta table for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = Path(temp_dir) / "test_table"
        delta_log = table_path / "_delta_log"
        delta_log.mkdir(parents=True)

        # Create a simple JSON commit
        commit = {
            "commitInfo": {
                "timestamp": 1234567890000,
                "operation": "WRITE",
                "operationParameters": {},
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
                "changeDataFeed": False,
            }
        }

        add_file = {
            "add": {
                "path": "part-00000.parquet",
                "size": 1024,
                "modificationTime": 1234567890000,
                "dataChange": True,
                "stats": '{"numRecords": 10}',
            }
        }

        # Write commit file
        commit_file = delta_log / "00000000000000000000.json"
        with open(commit_file, "w") as f:
            f.write(json.dumps(commit) + "\n")
            f.write(json.dumps(add_file) + "\n")

        yield table_path


def test_table_discovery(temp_delta_table):
    """Test table discovery functionality."""
    discovery = TableDiscovery()
    tables = discovery.discover_tables(temp_delta_table.parent, recursive=False)

    assert len(tables) == 1
    assert tables[0].table_id == "test_table"
    assert tables[0].version == 0
    assert tables[0].num_files == 1


def test_migration_config():
    """Test migration configuration."""
    config = MigrationConfig(
        database_url="sqlite:///test.db",
        table_path=Path("/test/table"),
        table_id="test_table",
        batch_size=500,
        dry_run=True,
    )

    assert config.database_url == "sqlite:///test.db"
    assert config.table_path == Path("/test/table")
    assert config.table_id == "test_table"
    assert config.batch_size == 500
    assert config.dry_run is True


def test_migration_manager_creation():
    """Test migration manager creation."""
    config = MigrationConfig(
        database_url="sqlite:///test.db",
        table_path=Path("/test/table"),
    )

    manager = MigrationManager(config)
    assert manager.config == config
    assert manager.table_id == "table"


@pytest.mark.asyncio
async def test_migration_dry_run(temp_delta_table):
    """Test migration in dry run mode."""
    config = MigrationConfig(
        database_url="sqlite:///test.db",
        table_path=temp_delta_table,
        dry_run=True,
        verbose=True,
    )

    manager = MigrationManager(config)
    result = await manager.migrate()

    assert result.success is True
    assert result.table_id == "test_table"
    assert result.version == 0
    assert result.files_processed == 1
    assert result.duration > 0


@pytest.mark.asyncio
async def test_validation_manager_creation():
    """Test validation manager creation."""
    validator = ValidationManager("sqlite:///test.db", verbose=True)
    assert validator.database_url == "sqlite:///test.db"
    assert validator.verbose is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
