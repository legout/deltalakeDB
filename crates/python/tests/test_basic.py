"""Basic tests for DeltaLakeDB Python bindings."""

import pytest
import sys

# Check if the Rust extension is available
try:
    import deltalakedb as dl
    from deltalakedb import (
        DeltaSqlUri, SqlConnection, SqlConfig, Table,
        DeltaDataType, SchemaField, TableSchema
    )
    RUST_EXTENSION_AVAILABLE = True
except ImportError:
    RUST_EXTENSION_AVAILABLE = False
    deltalakedb = None


@pytest.mark.skipif(not RUST_EXTENSION_AVAILABLE, reason="Rust extension not available")
class TestDeltaSqlUri:
    """Test DeltaSQL URI parsing and validation."""

    def test_parse_postgres_uri(self):
        """Test parsing a valid PostgreSQL URI."""
        uri = DeltaSqlUri.parse("deltasql://postgres://localhost:5432/testdb#mytable")
        assert uri.scheme == "deltasql"
        assert uri.database_type == "postgres"
        assert uri.connection_string == "localhost:5432/testdb"
        assert uri.table_name == "mytable"
        assert uri.is_valid

    def test_parse_uri_with_parameters(self):
        """Test parsing URI with query parameters."""
        uri = DeltaSqlUri.parse("deltasql://mysql://localhost:3306/testdb?ssl=true&timeout=30")
        assert uri.database_type == "mysql"
        assert uri.parameters.get("ssl") == "true"
        assert uri.parameters.get("timeout") == "30"
        assert uri.is_valid

    def test_invalid_scheme(self):
        """Test parsing URI with invalid scheme."""
        with pytest.raises(Exception):  # Should raise DeltaLakeError
            DeltaSqlUri.parse("http://postgres://localhost/test")

    def test_unsupported_database_type(self):
        """Test parsing URI with unsupported database type."""
        uri = DeltaSqlUri.parse("deltasql://unsupported://localhost/test")
        assert not uri.is_valid
        assert any("Unsupported database type" in error for error in uri.validation_errors)

    def test_build_uri(self):
        """Test building URI from components."""
        uri = DeltaSqlUri(
            scheme="deltasql",
            database_type="postgres",
            connection_string="localhost:5432/testdb",
            table_name="mytable",
            parameters={}
        )
        built = uri.build_uri()
        assert built == "deltasql://postgres/localhost:5432/testdb#mytable"

    def test_supported_database_types(self):
        """Test getting supported database types."""
        types = DeltaSqlUri.get_supported_database_types()
        assert "postgres" in types
        assert "mysql" in types
        assert "sqlite" in types
        assert "duckdb" in types


@pytest.mark.skipif(not RUST_EXTENSION_AVAILABLE, reason="Rust extension not available")
class TestSqlConnection:
    """Test SQL connection management."""

    def test_connection_creation(self):
        """Test creating a SQL connection."""
        conn = SqlConnection("deltasql://postgres://localhost:5432/testdb#mytable")
        assert conn.database_type == "postgres"
        assert conn.is_connected
        assert conn.created_at is not None

    def test_connection_from_components(self):
        """Test creating connection from components."""
        config = SqlConfig(database_type="sqlite", pool_size=5)
        conn = SqlConnection.from_components(
            database_type="sqlite",
            connection_string=":memory:",
            config=config
        )
        assert conn.database_type == "sqlite"
        assert conn.connection_string == ":memory:"

    def test_connection_status(self):
        """Test getting connection status."""
        conn = SqlConnection("deltasql://sqlite:///:memory:")
        status = conn.get_status()
        assert status["database_type"] == "sqlite"
        assert status["is_connected"] == "true"
        assert "created_at" in status

    def test_connection_health(self):
        """Test connection health check."""
        conn = SqlConnection("deltasql://sqlite:///:memory:")
        health = conn.test_connection()
        assert health is True

    def test_uri_methods(self):
        """Test URI-related methods on connection."""
        conn = SqlConnection("deltasql://postgres://localhost:5432/testdb#mytable")

        # Test parsed URI
        parsed_uri = conn.get_parsed_uri()
        assert parsed_uri.database_type == "postgres"
        assert parsed_uri.table_name == "mytable"

        # Test table name extraction
        table_name = conn.get_uri_table_name()
        assert table_name == "mytable"

        # Test base URI
        base_uri = conn.get_base_uri()
        assert base_uri == "deltasql://postgres://localhost:5432/testdb"

    def test_for_table(self):
        """Test creating connection for different table."""
        conn = SqlConnection("deltasql://postgres://localhost:5432/testdb")
        table_conn = conn.for_table("new_table")
        assert table_conn.uri.endswith("#new_table")
        assert table_conn.database_type == conn.database_type


@pytest.mark.skipif(not RUST_EXTENSION_AVAILABLE, reason="Rust extension not available")
class TestTypeSystem:
    """Test type system integration."""

    def test_primitive_data_type(self):
        """Test primitive data type creation."""
        dtype = DeltaDataType.Primitive("string")
        assert dtype.__str__() == "string"
        assert dtype.is_primitive()
        assert not dtype.is_complex()
        assert dtype.get_primitive_name() == "string"

    def test_struct_data_type(self):
        """Test struct data type creation."""
        # This would need to be implemented in the Rust code
        # For now, we'll test the basic structure
        pass

    def test_schema_field(self):
        """Test schema field creation."""
        field = SchemaField(
            name="test_field",
            data_type=DeltaDataType.Primitive("string"),
            nullable=True,
            metadata={}
        )
        assert field.name == "test_field"
        assert field.nullable is True
        assert field.metadata == {}

    def test_table_schema(self):
        """Test table schema creation."""
        fields = [
            SchemaField(
                name="id",
                data_type=DeltaDataType.Primitive("long"),
                nullable=False,
                metadata={}
            ),
            SchemaField(
                name="name",
                data_type=DeltaDataType.Primitive("string"),
                nullable=True,
                metadata={}
            )
        ]
        schema = TableSchema(
            fields=fields,
            partition_columns=["date"],
            schema_id="test-schema"
        )
        assert len(schema.fields) == 2
        assert len(schema.partition_columns) == 1
        assert schema.schema_id == "test-schema"

    def test_schema_validation(self):
        """Test schema validation."""
        # Valid schema
        fields = [
            SchemaField(
                name="id",
                data_type=DeltaDataType.Primitive("long"),
                nullable=False,
                metadata={}
            )
        ]
        schema = TableSchema(fields=fields, partition_columns=[], schema_id=None)
        assert schema.validate() is True
        assert len(schema.get_validation_errors()) == 0

        # Invalid schema with duplicate field names
        fields = [
            SchemaField(
                name="id",
                data_type=DeltaDataType.Primitive("long"),
                nullable=False,
                metadata={}
            ),
            SchemaField(
                name="id",
                data_type=DeltaDataType.Primitive("string"),
                nullable=True,
                metadata={}
            )
        ]
        schema = TableSchema(fields=fields, partition_columns=[], schema_id=None)
        assert schema.validate() is False
        assert len(schema.get_validation_errors()) > 0


@pytest.mark.skipif(not RUST_EXTENSION_AVAILABLE, reason="Rust extension not available")
class TestHighLevelAPI:
    """Test high-level API functions."""

    def test_connect_function(self):
        """Test connect function."""
        # This would test the connect function with a real database
        # For now, we'll test with a mock connection
        pass

    def test_create_function(self):
        """Test create function."""
        # This would test the create function with a real database
        # For now, we'll test with a mock connection
        pass

    def test_list_function(self):
        """Test list function."""
        # This would test the list function with a real database
        # For now, we'll test with a mock connection
        pass


class TestFallbackBehavior:
    """Test behavior when Rust extension is not available."""

    @pytest.mark.skipif(RUST_EXTENSION_AVAILABLE, reason="Rust extension is available")
    def test_fallback_imports(self):
        """Test that fallback implementations are provided."""
        import deltalakedb as dl

        # Should not raise ImportError
        assert dl.__version__ == "0.1.0-dev"

        # Should raise NotImplementedError when used
        with pytest.raises(NotImplementedError):
            dl.Table()

        with pytest.raises(NotImplementedError):
            dl.connect("test://uri")

    @pytest.mark.skipif(RUST_EXTENSION_AVAILABLE, reason="Rust extension is available")
    def test_warning_on_import(self):
        """Test that warning is issued when Rust extension is not available."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import deltalakedb as dl

            # Should have issued a warning about missing Rust extension
            assert len(w) > 0
            assert any("Failed to import Rust extension" in str(warning.message) for warning in w)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])