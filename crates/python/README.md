# delkalakedb Python Bindings

This directory contains the Python bindings for the deltalakedb Rust library.

## Building the Python Extension

To build the Python extension with the Rust bindings:

```bash
# Install development dependencies
pip install maturin

# Build the extension in development mode
pip install -e .

# Or build and install with maturin directly
maturin develop
```

## Usage

Once the extension is built, you can use it like this:

```python
import asyncio
import tempfile
import delkalakedb

# Check if Rust extension is available
if delkalakedb.is_rust_extension_available():
    # Create a SQLite database configuration
    with tempfile.NamedTemporaryFile(suffix='.db') as f:
        config = delkalakedb.create_sqlite_config(f.name)
        
        # Create multi-table configuration
        mt_config = delkalakedb.get_multi_table_config()()
        
        # Create client
        client = delkalakedb.MultiTableClient(config, mt_config)
        
        # Create a transaction
        tx = client.create_transaction()
        
        # Create table actions
        AddFile = delkalakedb.get_add_file()
        builder = delkalakedb.TableActionsBuilder("users", 0)
        builder.add_files([AddFile(
            path="users_1.parquet",
            size=1000,
            data_change=True
        )])
        
        actions = builder.build()
        
        # Add actions to transaction
        tx.add_actions(actions)
        
        # Commit transaction (async)
        async def commit():
            result = await client.commit_transaction(tx)
            print(f"Committed transaction: {result.transaction.transaction_id()}")
        
        asyncio.run(commit())
else:
    print("Rust extension not available")
```

## Architecture

The Python bindings provide:

1. **Low-level Rust bindings** - Direct access to Rust structs and functions
2. **High-level Python client** - Easier to use Python API
3. **Builder patterns** - For constructing complex objects
4. **Async support** - Full async/await support
5. **Type safety** - Proper type annotations

## Components

### Core Classes

- `MultiTableClient` - High-level client for multi-table operations
- `TableActionsBuilder` - Builder for creating table actions
- `MultiTableTransaction` - Represents a multi-table transaction

### Data Classes

- `DatabaseConfig` - Database connection configuration
- `MultiTableConfig` - Multi-table operation configuration
- `TableActions` - Actions for a single table
- `AddFile` / `RemoveFile` - File operations
- `Metadata` / `Format` / `Protocol` - Table metadata

### Results

- `MultiTableCommitResult` - Result of committing a transaction
- `TransactionSummary` - Summary of transaction
- `TableCommitResult` - Result for individual table
- `MirroringResult` - Result of mirroring operation

## Database Support

The Python bindings support the same databases as the Rust library:

- SQLite (for testing and development)
- PostgreSQL (for production)
- MySQL (for production)

## Error Handling

The Python bindings provide comprehensive error handling:

- Rust errors are converted to Python exceptions
- Validation errors are clearly reported
- Database connection errors are properly propagated
- Async errors are handled correctly

## Testing

Run the test script to verify the installation:

```bash
python test_python_bindings.py
```

## Development

When developing the Python bindings:

1. Make changes to the Rust code in `crates/python/src/lib.rs`
2. Rebuild with `maturin develop`
3. Test the changes
4. Update the Python wrapper code in `python/delkalakedb/__init__.py` as needed

The Python wrapper provides a more Pythonic API on top of the Rust bindings, handling imports, type conversions, and providing convenience functions.