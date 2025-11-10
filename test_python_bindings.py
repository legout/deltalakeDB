#!/usr/bin/env python3
"""Test script for delkalakedb Python bindings."""

import asyncio
import tempfile
import os


def test_basic_functionality():
    """Test basic functionality of delkalakedb."""
    print("Testing delkalakedb Python bindings...")

    # Test import
    try:
        import delkalakedb

        print("✓ Successfully imported delkalakedb")
    except ImportError as e:
        print(f"✗ Failed to import delkalakedb: {e}")
        return False

    # Test version
    print(f"✓ Version: {delkalakedb.__version__}")

    # Test hello function
    try:
        result = delkalakedb.hello()
        print(f"✓ hello(): {result}")
    except Exception as e:
        print(f"✗ hello() failed: {e}")
        return False

    # Test rust extension availability
    if delkalakedb.is_rust_extension_available():
        print("✓ Rust extension is available")

        # Test creating a SQLite config
        try:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                db_path = f.name

            config = delkalakedb.create_sqlite_config(db_path)
            print(f"✓ Created SQLite config: {type(config)}")

            # Test creating multi-table config
            mt_config = delkalakedb.get_multi_table_config()()
            print(f"✓ Created MultiTableConfig: {type(mt_config)}")

            # Test creating client
            client = delkalakedb.MultiTableClient(config, mt_config)
            print(f"✓ Created MultiTableClient: {type(client)}")

            # Test creating transaction
            tx = client.create_transaction()
            print(f"✓ Created transaction: {type(tx)}")
            print(f"  Transaction ID: {tx.transaction_id()}")
            print(f"  Table count: {tx.table_count()}")
            print(f"  Action count: {tx.total_action_count()}")

            # Test TableActionsBuilder
            builder = delkalakedb.TableActionsBuilder("test_table", 0)
            print(f"✓ Created TableActionsBuilder: {type(builder)}")

            # Test creating AddFile
            AddFile = delkalakedb.get_add_file()
            add_file = AddFile(path="test.parquet", size=1000, data_change=True)
            print(f"✓ Created AddFile: {type(add_file)}")

            # Test adding files to builder
            builder.add_files([add_file])
            print("✓ Added files to builder")

            # Test building actions
            actions = builder.build()
            print(f"✓ Built actions: {type(actions)}")
            print(f"  Table ID: {actions.table_id()}")
            print(f"  Expected version: {actions.expected_version()}")
            print(f"  Action count: {actions.action_count()}")

            # Clean up
            os.unlink(db_path)

        except Exception as e:
            print(f"✗ Rust extension test failed: {e}")
            import traceback

            traceback.print_exc()
            return False
    else:
        print("ℹ Rust extension is not available (expected during development)")

    print("✓ All tests passed!")
    return True


async def test_async_functionality():
    """Test async functionality of delkalakedb."""
    if not delkalakedb.is_rust_extension_available():
        print("ℹ Skipping async tests (Rust extension not available)")
        return True

    print("Testing async functionality...")

    try:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        config = delkalakedb.create_sqlite_config(db_path)
        mt_config = delkalakedb.get_multi_table_config()()
        client = delkalakedb.MultiTableClient(config, mt_config)

        # Test creating and staging actions
        AddFile = delkalakedb.get_add_file()
        builder1 = delkalakedb.TableActionsBuilder("table1", 0)
        builder1.add_files([AddFile(path="file1.parquet", size=1000)])
        actions1 = builder1.build()

        builder2 = delkalakedb.TableActionsBuilder("table2", 0)
        builder2.add_files([AddFile(path="file2.parquet", size=2000)])
        actions2 = builder2.build()

        # Test begin_and_stage
        tx = await client.begin_and_stage([actions1, actions2])
        print(f"✓ Created and staged transaction: {tx.transaction_id()}")
        print(f"  Table count: {tx.table_count()}")
        print(f"  Action count: {tx.total_action_count()}")

        # Test consistency validation
        violations = await client.validate_consistency(tx)
        print(f"✓ Consistency validation completed: {len(violations)} violations")

        # Clean up
        os.unlink(db_path)

    except Exception as e:
        print(f"✗ Async test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    print("✓ All async tests passed!")
    return True


def main():
    """Main test function."""
    print("delkalakedb Python Bindings Test")
    print("=" * 40)

    # Test basic functionality
    if not test_basic_functionality():
        return 1

    # Test async functionality
    try:
        if not asyncio.run(test_async_functionality()):
            return 1
    except Exception as e:
        print(f"✗ Async test runner failed: {e}")
        return 1

    print("\n🎉 All tests passed!")
    return 0


if __name__ == "__main__":
    exit(main())
