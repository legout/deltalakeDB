#!/usr/bin/env python3
"""
DeltaLake Integration Demo

This script demonstrates the integration between deltalakedb and the deltalake package,
including URI scheme registration and seamless table access.
"""

import sys

def demo_deltalake_integration():
    """Demonstrate deltalake package integration."""
    print("=== DeltaLake Integration Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    # Check if deltalake package is available
    try:
        import deltalake
        print("✅ deltalake package is available")
    except ImportError:
        print("⚠️  deltalake package not found, using deltalakedb only")

    # Create deltalake bridge
    try:
        bridge = dl.DeltaLakeBridge()
        info = bridge.get_info()
        print("✅ DeltaLake bridge created successfully")
        print(f"   - deltalake available: {info.get('deltalake_available', 'N/A')}")
        print(f"   - supported schemes: {info.get('supported_schemes', 'N/A')}")
    except Exception as e:
        print(f"❌ Failed to create deltalake bridge: {e}")
        return False

    # Test URI compatibility checking
    test_uris = [
        "deltasql://postgres://localhost:5432/mydb#mytable",
        "deltasql://sqlite:///:memory:#analytics",
        "s3://bucket/path/to/table",
        "file:///local/path/table",
    ]

    print("\n--- URI Compatibility Check ---")
    for uri in test_uris:
        try:
            compatibility = dl.check_uri_compatibility(uri)
            scheme = compatibility.get("scheme", "unknown")
            supported = compatibility.get("supported", "false")
            backend = compatibility.get("backend", "none")
            print(f"✅ {uri}")
            print(f"   Scheme: {scheme}")
            print(f"   Supported: {supported}")
            print(f"   Backend: {backend}")

            if scheme == "deltasql":
                print(f"   Database type: {compatibility.get('database_type', 'N/A')}")
                if "table_name" in compatibility:
                    print(f"   Table name: {compatibility['table_name']}")
        except Exception as e:
            print(f"❌ {uri}: {e}")
        print()

    # Test creating a table with deltasql:// URI
    print("--- Creating DeltaTable with deltasql:// URI ---")
    try:
        table = dl.load_table("deltasql://sqlite:///:memory:#demo_table")
        print(f"✅ Created table: {table.name}")
        print(f"   Table ID: {table.table_id}")
        print(f"   Version: {table.version}")
        print(f"   Path: {table.path}")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")

    # Test URI handler directly
    print("\n--- Direct URI Handler Test ---")
    try:
        table = dl.handle_deltasql_uri("deltasql://duckdb:///data.duckdb#users")
        print(f"✅ Handled URI: {table.name}")
        print(f"   Database type inferred from URI")
    except Exception as e:
        print(f"❌ URI handler failed: {e}")

    # Test scheme registrar
    print("\n--- Scheme Registration ---")
    try:
        registrar = dl.register_uri_handler()
        print(f"✅ Registered URI schemes: {registrar.get_registered_schemes()}")
        print(f"   Integration enabled: {registrar.integration_enabled}")
    except Exception as e:
        print(f"❌ Scheme registration failed: {e}")

    # Test deltalake patching
    print("\n--- DeltaLake Package Patching ---")
    try:
        patched = dl.patch_deltalake()
        print(f"✅ Patched deltalake package: {patched}")
    except Exception as e:
        print(f"⚠️  Patching failed (may be expected): {e}")

    print("\n=== Demo Complete ===")
    return True

def demo_time_travel():
    """Demonstrate time travel functionality."""
    print("\n=== Time Travel Demo ===\n")

    try:
        import deltalakedb as dl

        # Create a table
        table = dl.load_table("deltasql://sqlite:///:memory:#time_travel_demo")
        print(f"✅ Created table: {table.name} (version {table.version})")

        # Test version-based time travel
        table_v1 = table.as_of(1)
        print(f"✅ Time travel to version 1: {table_v1.version}")

        # Test timestamp-based time travel
        import time
        timestamp = int(time.time())
        table_ts = table.as_of_timestamp(timestamp)
        print(f"✅ Time travel to timestamp {timestamp}: {table_ts.updated_at}")

        # Get table history
        history = table.history(limit=3)
        print(f"✅ Retrieved {len(history)} historical commits")
        for i, commit in enumerate(history[:2]):
            print(f"   Commit {commit.get('version', '?')}: {commit.get('operation', 'Unknown')}")

    except Exception as e:
        print(f"❌ Time travel demo failed: {e}")
        return False

    print("\n=== Time Travel Demo Complete ===")
    return True

def demo_multi_table_transactions():
    """Demonstrate multi-table transaction functionality."""
    print("\n=== Multi-table Transaction Demo ===\n")

    try:
        import deltalakedb as dl

        # Create a connection
        conn = dl.SqlConnection("deltasql://sqlite:///:memory:")
        print("✅ Created SQL connection")

        # Create transaction options
        options = dl.TransactionOptions()
        print(f"✅ Created transaction options: {options.isolation_level}")

        # Create multi-table transaction
        tx = dl.MultiTableTransaction(conn)
        print(f"✅ Created multi-table transaction")

        # Begin transaction
        tx_id = tx.begin()
        print(f"✅ Began transaction: {tx_id}")

        # Register tables
        tx.register_table("users", None)
        tx.register_table("orders", None)
        print(f"✅ Registered tables: {list(tx.get_registered_tables().keys())}")

        # Create tables in transaction
        users_id = tx.create_table("users", None, ["user_id"], "User data")
        orders_id = tx.create_table("orders", None, ["order_date"], "Order data")
        print(f"✅ Created tables: users ({users_id}), orders ({orders_id})")

        # Commit transaction
        result = tx.commit()
        print(f"✅ Committed transaction: {result.success}")
        print(f"   Tables affected: {result.tables_affected}")
        print(f"   Duration: {result.duration_ms}ms")

    except Exception as e:
        print(f"❌ Multi-table transaction demo failed: {e}")
        return False

    print("\n=== Multi-table Transaction Demo Complete ===")
    return True

if __name__ == "__main__":
    success = True

    # Run demos
    success &= demo_deltalake_integration()
    success &= demo_time_travel()
    success &= demo_multi_table_transactions()

    if success:
        print("\n🎉 All demos completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Some demos failed!")
        sys.exit(1)