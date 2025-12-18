"""pytest configuration and fixtures for delkalakedb tests."""

import pytest


@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory: pytest.TempPathFactory):
    """Create a temporary directory for test data."""
    return tmp_path_factory.mktemp("test_data")


@pytest.fixture
def deltasql_sqlite_uri(tmp_path):
    """Create a temporary SQLite database URI for testing."""
    db_path = tmp_path / "test.db"
    return f"deltasql://sqlite:///{db_path}?table=test"
