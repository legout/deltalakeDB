"""Tests for the Python API.

Note: These tests are stubs that verify the API surface exists.
Full integration tests will be implemented in Phase 5 once pyo3 bindings are active.
"""

import pytest


class TestImports:
    """Test that all public types can be imported."""

    def test_import_main_classes(self) -> None:
        """Import main API classes."""
        from delkalakedb import DeltaSQL, Table, Transaction, TransactionBuilder

        assert DeltaSQL is not None
        assert Table is not None
        assert Transaction is not None
        assert TransactionBuilder is not None

    def test_import_types(self) -> None:
        """Import type definitions."""
        from delkalakedb import (
            ActiveFile,
            Protocol,
            RemovedFile,
            Snapshot,
            TableMetadata,
        )

        assert Snapshot is not None
        assert ActiveFile is not None
        assert RemovedFile is not None
        assert Protocol is not None
        assert TableMetadata is not None

    def test_import_errors(self) -> None:
        """Import error types."""
        from delkalakedb import (
            ConcurrencyError,
            ConnectionError,
            NotFoundError,
            ValidationError,
        )

        assert ConcurrencyError is not None
        assert ConnectionError is not None
        assert ValidationError is not None
        assert NotFoundError is not None


class TestAPIStubs:
    """Test that API stubs are in place."""

    def test_deltasql_constructor(self) -> None:
        """Test DeltaSQL constructor."""
        from delkalakedb import DeltaSQL

        # Valid URI
        conn = DeltaSQL("deltasql://postgres://localhost/db?table=t")
        assert conn is not None

    def test_deltasql_invalid_uri(self) -> None:
        """Test DeltaSQL rejects invalid URIs."""
        from delkalakedb import DeltaSQL

        with pytest.raises(ValueError):
            DeltaSQL("invalid://uri")

    def test_table_time_travel(self) -> None:
        """Test Table time-travel methods."""
        from delkalakedb import DeltaSQL

        conn = DeltaSQL("deltasql://sqlite:///db.sqlite?table=t")
        table = conn.open_table("t")

        # These should work (stubs return NotImplementedError on actual use)
        table_v5 = table.version(5)
        assert table_v5 is not None

        table_past = table.at_timestamp("2024-12-18T10:00:00Z")
        assert table_past is not None

    def test_transaction_staging(self) -> None:
        """Test Transaction action staging."""
        from delkalakedb import DeltaSQL

        conn = DeltaSQL("deltasql://sqlite:///db.sqlite?table=t")
        txn = conn.begin_transaction("t")

        # These should accept arguments but not actually execute
        txn.add_file("/data/file.parquet", 1024, 1702905600000)
        txn.remove_file("/data/old_file.parquet")
        assert txn is not None

    def test_transaction_builder_staging(self) -> None:
        """Test TransactionBuilder multi-table staging."""
        from delkalakedb import DeltaSQL

        conn = DeltaSQL("deltasql://sqlite:///db.sqlite?table=t")
        builder = conn.transaction_builder()

        builder.add_table_actions("table_a", ["action1", "action2"])
        builder.add_table_actions("table_b", ["action3"])

        tables = builder.staged_tables()
        assert "table_a" in tables
        assert "table_b" in tables


class TestTypeHints:
    """Test that type hints are available."""

    def test_type_stub_file_exists(self) -> None:
        """Verify __init__.pyi file is present for IDE support."""
        from pathlib import Path

        pyi_file = Path(__file__).parent.parent / "src" / "delkalakedb" / "__init__.pyi"
        assert pyi_file.exists(), "Type stub file __init__.pyi should exist"

        content = pyi_file.read_text()
        assert "class DeltaSQL" in content
        assert "class Table" in content
        assert "def snapshot" in content

    def test_docstrings_present(self) -> None:
        """Verify public API has docstrings."""
        from delkalakedb import DeltaSQL

        assert DeltaSQL.__doc__ is not None
        assert "connection" in DeltaSQL.__doc__.lower()


class TestErrorMessages:
    """Test error handling and messages."""

    def test_concurrency_error_creation(self) -> None:
        """Test ConcurrencyError can be created and raised."""
        from delkalakedb import ConcurrencyError

        err = ConcurrencyError()
        assert isinstance(err, Exception)

    def test_connection_error_creation(self) -> None:
        """Test ConnectionError can be created and raised."""
        from delkalakedb import ConnectionError

        err = ConnectionError()
        assert isinstance(err, Exception)

    def test_validation_error_creation(self) -> None:
        """Test ValidationError can be created and raised."""
        from delkalakedb import ValidationError

        err = ValidationError()
        assert isinstance(err, Exception)


# Integration tests (Phase 5+)
#
# Once pyo3 bindings are active, add tests like:
#
# def test_postgres_read_snapshot():
#     """Test reading snapshot from Postgres backend."""
#     conn = DeltaSQL("deltasql://postgres://localhost/testdb?table=test")
#     table = conn.open_table("test")
#     snapshot = table.snapshot()
#     assert snapshot.version >= 0
#
# def test_sqlite_read_snapshot():
#     """Test reading snapshot from SQLite backend."""
#     conn = DeltaSQL("deltasql://sqlite:///test.db?table=test")
#     table = conn.open_table("test")
#     snapshot = table.snapshot()
#     assert snapshot.version >= 0
#
# def test_transaction_commit():
#     """Test committing a transaction."""
#     conn = DeltaSQL("deltasql://sqlite:///test.db?table=test")
#     txn = conn.begin_transaction("test")
#     txn.add_file("/data/new.parquet", 1024, 1702905600000)
#     version = txn.commit()
#     assert version > 0
#
# def test_time_travel_consistency():
#     """Test time-travel returns consistent snapshots."""
#     conn = DeltaSQL("deltasql://sqlite:///test.db?table=test")
#     table = conn.open_table("test")
#     
#     snap_current = table.snapshot()
#     snap_same = table.snapshot()
#     assert snap_current.version == snap_same.version
#
# def test_multi_table_transaction():
#     """Test atomic multi-table commit."""
#     conn = DeltaSQL("deltasql://sqlite:///test.db?table=test")
#     builder = conn.transaction_builder()
#     
#     builder.add_table_actions("table_a", ["add_file(...)"])
#     builder.add_table_actions("table_b", ["add_file(...)"])
#     
#     result = builder.commit()
#     assert "table_a" in result
#     assert "table_b" in result
