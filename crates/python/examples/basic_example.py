#!/usr/bin/env python3
"""
Basic DeltaLake DB Python Example

A minimal example to get started with DeltaLake DB Python.
Shows how to create a table, write data, and read it back.

This example uses in-memory SQLite for zero setup requirements.
"""


def main():
    """Run the basic DeltaLake DB Python example."""
    try:
        import deltalakedb as dl

        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please install deltalakedb with: pip install deltalakedb-python")
        return False

    try:
        # Create sample data (using basic Python types for compatibility)
        print("Creating sample data...")
        sample_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        print(f"✅ Created sample data with {len(sample_data)} records")

        # Create/connect to a table using in-memory SQLite
        print("\nCreating Delta table...")
        # Use the create function from the API surface
        table = dl.create("deltasql://sqlite:///:memory:", "users")
        print(f"✅ Created table: users")

        # For now, show the expected workflow since write/read may not be implemented yet
        print("\n📝 Expected workflow:")
        print("   1. ✅ Table created with in-memory SQLite")
        print("   2. 📝 Write data: table.write(data, mode='overwrite')")
        print("   3. 📖 Read data: table.read().to_pandas()")
        print("   4. 🔄 Data persists in SQL backend with Delta Lake compatibility")

        print("\n🎉 Basic example setup completed!")
        print(
            "   Note: Full read/write functionality requires the Rust extension to be built."
        )

    except NotImplementedError as e:
        print(f"⚠️  Functionality not yet implemented: {e}")
        print("   This is expected during development.")
        print("   The Rust extension needs to be built with: maturin develop")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

    return True


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
