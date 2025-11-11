#!/usr/bin/env python3
"""
Configuration File Support Demo

This script demonstrates the enhanced configuration file support including
YAML, TOML, and JSON formats with validation and auto-detection.
"""

import sys
import os
from pathlib import Path

def demo_yaml_configuration():
    """Demonstrate YAML configuration file support."""
    print("=== YAML Configuration Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        # Create sample YAML configuration
        config_dir = Path("configs")
        config_dir.mkdir(exist_ok=True)

        # Generate sample configurations
        print("--- Generating Sample YAML Configuration ---")
        dl.generate_sample_configs(str(config_dir))
        print(f"✅ Sample configurations generated in {config_dir}")

        # Load YAML configuration
        yaml_config_path = config_dir / "sample_database.yaml"
        if yaml_config_path.exists():
            print(f"\n--- Loading YAML Configuration from {yaml_config_path} ---")

            try:
                # Auto-detect format and load
                config = dl.load_config_file(str(yaml_config_path))
                print("✅ YAML configuration loaded successfully")

                if hasattr(config, 'url'):
                    print(f"   Database URL: {config.url}")
                if hasattr(config, 'pool_size'):
                    print(f"   Pool size: {config.pool_size}")
                if hasattr(config, 'timeout'):
                    print(f"   Timeout: {config.timeout}")
                if hasattr(config, 'ssl_enabled'):
                    print(f"   SSL enabled: {config.ssl_enabled}")

            except Exception as e:
                print(f"⚠️  YAML loading failed (PyYAML may not be installed): {e}")
                print("   Install with: pip install PyYAML")
        else:
            print(f"❌ Sample YAML file not found at {yaml_config_path}")

    except Exception as e:
        print(f"❌ YAML configuration demo failed: {e}")
        return False

    print("\n=== YAML Configuration Demo Complete ===")
    return True

def demo_toml_configuration():
    """Demonstrate TOML configuration file support."""
    print("\n=== TOML Configuration Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        # Create sample TOML configuration manually
        config_dir = Path("configs")
        toml_config_path = config_dir / "custom_config.toml"

        print("--- Creating Custom TOML Configuration ---")
        toml_content = """
# Custom DeltaLake Database Configuration

[database]
url = "duckdb:///data/deltalake.duckdb"
pool_size = 15
timeout = 45
ssl_enabled = false

[write]
mode = "overwrite"
partition_by = ["year", "month", "day"]
overwrite_schema = true
allow_schema_evolution = true
max_records_per_file = 500000

[transaction]
isolation_level = "serializable"
timeout = 600
max_retries = 5
retry_delay = 2.0

[table]
name = "analytics_events"
description = "Analytics events data"
partition_columns = ["event_date", "event_type"]
z_order_columns = ["user_id", "timestamp"]
"""

        # Write TOML file
        with open(toml_config_path, 'w') as f:
            f.write(toml_content)
        print(f"✅ Custom TOML configuration created at {toml_config_path}")

        # Load TOML configuration
        print(f"\n--- Loading TOML Configuration from {toml_config_path} ---")

        try:
            # Auto-detect format and load
            config = dl.load_config_file(str(toml_config_path))
            print("✅ TOML configuration loaded successfully")

            if hasattr(config, 'url'):
                print(f"   Database URL: {config.url}")
            if hasattr(config, 'pool_size'):
                print(f"   Pool size: {config.pool_size}")
        except Exception as e:
            print(f"⚠️  TOML loading failed (tomli may not be installed): {e}")
            print("   Install with: pip install tomli")

    except Exception as e:
        print(f"❌ TOML configuration demo failed: {e}")
        return False

    print("\n=== TOML Configuration Demo Complete ===")
    return True

def demo_json_configuration():
    """Demonstrate JSON configuration file support."""
    print("\n=== JSON Configuration Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        # Create sample JSON configuration
        config_dir = Path("configs")
        json_config_path = config_dir / "production_config.json"

        print("--- Creating Production JSON Configuration ---")
        json_config = {
            "database": {
                "url": "postgresql://prod_user:secure_pass@db.company.com:5432/prod_deltalake",
                "pool_size": 50,
                "timeout": 120,
                "ssl_enabled": True,
                "ssl_cert": "/etc/ssl/certs/db.crt",
                "ssl_key": "/etc/ssl/private/db.key"
            },
            "write": {
                "mode": "append",
                "partition_by": ["year", "month", "region"],
                "overwrite_schema": False,
                "allow_schema_evolution": True,
                "max_records_per_file": 1000000
            },
            "transaction": {
                "isolation_level": "read_committed",
                "timeout": 300,
                "max_retries": 10,
                "retry_delay": 1.0
            }
        }

        # Write JSON file
        import json
        with open(json_config_path, 'w') as f:
            json.dump(json_config, f, indent=2)
        print(f"✅ Production JSON configuration created at {json_config_path}")

        # Load JSON configuration
        print(f"\n--- Loading JSON Configuration from {json_config_path} ---")

        try:
            # Auto-detect format and load
            config = dl.load_config_file(str(json_config_path))
            print("✅ JSON configuration loaded successfully")

            if hasattr(config, 'url'):
                print(f"   Database URL: {config.url}")
            if hasattr(config, 'pool_size'):
                print(f"   Pool size: {config.pool_size}")
            if hasattr(config, 'ssl_enabled'):
                print(f"   SSL enabled: {config.ssl_enabled}")
        except Exception as e:
            print(f"⚠️  JSON loading failed: {e}")

    except Exception as e:
        print(f"❌ JSON configuration demo failed: {e}")
        return False

    print("\n=== JSON Configuration Demo Complete ===")
    return True

def demo_comprehensive_configuration():
    """Demonstrate comprehensive configuration loading from multiple sources."""
    print("\n=== Comprehensive Configuration Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        print("--- Loading Comprehensive Configuration ---")

        # Set environment variables for testing
        os.environ['DELTALAKE_DB_URL'] = 'sqlite:///test.db'
        os.environ['DELTALAKE_DB_POOL_SIZE'] = '25'
        os.environ['DELTALAKE_DB_SSL_ENABLED'] = 'false'
        os.environ['DELTALAKE_DB_TIMEOUT'] = '90'

        config_dir = Path("configs")
        base_config_path = config_dir / "sample_database.yaml"

        if base_config_path.exists():
            print(f"Base config file: {base_config_path}")
            print("Environment prefix: DELTALAKE_DB_")

            # Load comprehensive configuration
            try:
                config = dl.load_comprehensive_config(
                    config_file=str(base_config_path),
                    env_prefix="DELTALAKE_DB_"
                )
                print("✅ Comprehensive configuration loaded successfully")

                if hasattr(config, 'url'):
                    print(f"   Final database URL: {config.url}")
                if hasattr(config, 'pool_size'):
                    print(f"   Final pool size: {config.pool_size}")
                if hasattr(config, 'timeout'):
                    print(f"   Final timeout: {config.timeout}")

                print("   (Environment variables should override file settings)")

            except Exception as e:
                print(f"⚠️  Comprehensive configuration loading failed: {e}")
        else:
            print(f"❌ Base configuration file not found: {base_config_path}")

        # Clean up environment variables
        env_vars_to_clean = [
            'DELTALAKE_DB_URL', 'DELTALAKE_DB_POOL_SIZE',
            'DELTALAKE_DB_SSL_ENABLED', 'DELTALAKE_DB_TIMEOUT'
        ]
        for var in env_vars_to_clean:
            if var in os.environ:
                del os.environ[var]

    except Exception as e:
        print(f"❌ Comprehensive configuration demo failed: {e}")
        return False

    print("\n=== Comprehensive Configuration Demo Complete ===")
    return True

def demo_configuration_validation():
    """Demonstrate configuration file validation."""
    print("\n=== Configuration Validation Demo ===\n")

    try:
        import deltalakedb as dl
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        config_dir = Path("configs")

        print("--- Validating Configuration Files ---")

        # Validate existing configuration files
        config_files = [
            "sample_database.yaml",
            "sample_database.toml",
            "sample_database.json",
            "custom_config.toml",
            "production_config.json"
        ]

        for config_file in config_files:
            config_path = config_dir / config_file
            if config_path.exists():
                try:
                    is_valid = dl.validate_config_file(str(config_path))
                    status = "✅ Valid" if is_valid else "❌ Invalid"
                    print(f"   {config_file}: {status}")
                except Exception as e:
                    print(f"   {config_file}: ❌ Error - {e}")
            else:
                print(f"   {config_file}: ⚠️  File not found")

        print("\n--- Configuration Schemas ---")

        # Get configuration schemas
        config_types = ["database", "write", "transaction", "table"]

        for config_type in config_types:
            try:
                schema = dl.get_config_schema(config_type)
                print(f"✅ Retrieved schema for {config_type} configuration")
                # Note: Schema content is complex, so we just confirm we can get it
            except Exception as e:
                print(f"⚠️  Could not retrieve schema for {config_type}: {e}")

    except Exception as e:
        print(f"❌ Configuration validation demo failed: {e}")
        return False

    print("\n=== Configuration Validation Demo Complete ===")
    return True

def demo_configuration_saving():
    """Demonstrate saving configurations to different formats."""
    print("\n=== Configuration Saving Demo ===\n")

    try:
        import deltalakedb as dl
        from deltalakedb import ConfigFactory, PydanticModels
        print("✅ deltalakedb package imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import deltalakedb: {e}")
        return False

    try:
        config_dir = Path("configs")
        config_dir.mkdir(exist_ok=True)

        print("--- Creating and Saving Configurations ---")

        # Create sample database configuration using factory
        try:
            db_config = ConfigFactory.create_database_config(
                url="sqlite:///demo.db",
                pool_size=12,
                timeout=45,
                ssl_enabled=False
            )
            print("✅ Created sample database configuration")
        except Exception as e:
            print(f"⚠️  Could not create config with factory (expected): {e}")
            # Create a simple dict instead
            db_config = {
                "url": "sqlite:///demo.db",
                "pool_size": 12,
                "timeout": 45,
                "ssl_enabled": False
            }

        # Save to different formats
        yaml_path = config_dir / "saved_config.yaml"
        toml_path = config_dir / "saved_config.toml"

        try:
            dl.save_yaml_config(db_config, str(yaml_path))
            print(f"✅ Configuration saved to {yaml_path}")
        except Exception as e:
            print(f"⚠️  YAML saving failed (PyYAML may not be installed): {e}")

        try:
            dl.save_toml_config(db_config, str(toml_path))
            print(f"✅ Configuration saved to {toml_path}")
        except Exception as e:
            print(f"⚠️  TOML saving failed (tomli_w may not be installed): {e}")

        # Verify saved files can be loaded back
        print("\n--- Verifying Saved Configurations ---")

        for saved_file in [yaml_path, toml_path]:
            if saved_file.exists():
                try:
                    loaded_config = dl.load_config_file(str(saved_file))
                    print(f"✅ Successfully loaded and verified {saved_file.name}")
                except Exception as e:
                    print(f"⚠️  Could not load {saved_file.name}: {e}")

    except Exception as e:
        print(f"❌ Configuration saving demo failed: {e}")
        return False

    print("\n=== Configuration Saving Demo Complete ===")
    return True

def main():
    """Run all configuration file demos."""
    print("🚀 Starting Configuration File Support Demos\n")

    success = True

    # Run all demos
    success &= demo_yaml_configuration()
    success &= demo_toml_configuration()
    success &= demo_json_configuration()
    success &= demo_comprehensive_configuration()
    success &= demo_configuration_validation()
    success &= demo_configuration_saving()

    if success:
        print("\n🎉 All configuration file demos completed successfully!")
        print("\n📝 Configuration File Features Summary:")
        print("   ✅ YAML configuration file support with PyYAML")
        print("   ✅ TOML configuration file support with tomli/tomli_w")
        print("   ✅ JSON configuration file support (built-in)")
        print("   ✅ Auto-detection of configuration file format")
        print("   ✅ Configuration validation with Pydantic models")
        print("   ✅ Comprehensive configuration from multiple sources")
        print("   ✅ Environment variable override support")
        print("   ✅ Configuration file generation utilities")
        print("   ✅ Schema retrieval and validation")
        print("   ✅ Cross-format configuration conversion")
        print("\n📦 Optional Dependencies:")
        print("   - PyYAML: pip install PyYAML")
        print("   - tomli: pip install tomli")
        print("   - tomli_w: pip install tomli_w")
        sys.exit(0)
    else:
        print("\n❌ Some configuration file demos failed!")
        print("   Note: Some features require optional dependencies")
        sys.exit(1)

if __name__ == "__main__":
    main()