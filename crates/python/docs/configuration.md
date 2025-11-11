# Configuration File Support

This document describes the comprehensive configuration file support in DeltaLake DB Python bindings, including YAML, TOML, and JSON formats with validation and auto-detection.

## Overview

The DeltaLake Python package provides extensive configuration file support that allows you to:

- Load configurations from YAML, TOML, and JSON files
- Auto-detect file format based on file extension
- Validate configurations using Pydantic models
- Merge configurations from multiple sources (files, environment variables, defaults)
- Generate sample configuration files
- Convert between configuration formats

## Supported Formats

### YAML Configuration

```yaml
# database_config.yaml
database:
  url: "postgresql://user:password@localhost:5432/deltalake"
  pool_size: 20
  timeout: 60
  ssl_enabled: true
  ssl_cert: "/path/to/cert.pem"

write:
  mode: "append"
  partition_by: ["year", "month"]
  overwrite_schema: false
  allow_schema_evolution: true
  max_records_per_file: 1000000
```

### TOML Configuration

```toml
# database_config.toml
[database]
url = "postgresql://user:password@localhost:5432/deltalake"
pool_size = 20
timeout = 60
ssl_enabled = true
ssl_cert = "/path/to/cert.pem"

[write]
mode = "append"
partition_by = ["year", "month"]
overwrite_schema = false
allow_schema_evolution = true
max_records_per_file = 1000000
```

### JSON Configuration

```json
{
  "database": {
    "url": "postgresql://user:password@localhost:5432/deltalake",
    "pool_size": 20,
    "timeout": 60,
    "ssl_enabled": true,
    "ssl_cert": "/path/to/cert.pem"
  },
  "write": {
    "mode": "append",
    "partition_by": ["year", "month"],
    "overwrite_schema": false,
    "allow_schema_evolution": true,
    "max_records_per_file": 1000000
  }
}
```

## API Reference

### Loading Configuration Files

#### Auto-detect Format

```python
import deltalakedb as dl

# Automatically detects format based on file extension
config = dl.load_config_file("config.yaml")  # YAML
config = dl.load_config_file("config.toml")  # TOML
config = dl.load_config_file("config.json")  # JSON
```

#### Specific Format Loading

```python
# YAML format
config = dl.load_yaml_config("config.yaml")

# TOML format
config = dl.load_toml_config("config.toml")

# JSON format
config = dl.load_json_config("config.json")
```

### Comprehensive Configuration Loading

```python
# Load from multiple sources with precedence:
# 1. Defaults
# 2. Configuration file
# 3. Environment variables (highest precedence)
config = dl.load_comprehensive_config(
    config_file="config.yaml",
    env_prefix="DELTALAKE_DB_"
)
```

### Saving Configuration Files

```python
# Save to different formats
dl.save_yaml_config(config, "config.yaml")
dl.save_toml_config(config, "config.toml")

# JSON saving can be done with standard library
import json
with open("config.json", "w") as f:
    json.dump(config.dict(), f, indent=2)
```

### Configuration Validation

```python
# Validate configuration file format
is_valid = dl.validate_config_file("config.yaml")

# Get configuration schema
schema = dl.get_config_schema("database")  # "database", "write", "transaction", "table"
```

### Sample Configuration Generation

```python
# Generate sample configuration files
dl.generate_sample_configs("configs/")
# Creates:
# - configs/sample_database.yaml
# - configs/sample_database.toml
# - configs/sample_database.json
```

## Environment Variable Configuration

Environment variables override file configuration when using `load_comprehensive_config()`:

```bash
export DELTALAKE_DB_URL="postgresql://prod_user:pass@prod-db:5432/deltalake"
export DELTALAKE_DB_POOL_SIZE="50"
export DELTALAKE_DB_TIMEOUT="120"
export DELTALAKE_DB_SSL_ENABLED="true"
```

```python
import deltalakedb as dl

# Environment variables will override file settings
config = dl.load_comprehensive_config(
    config_file="config.yaml",
    env_prefix="DELTALAKE_DB_"
)
```

## Configuration Schema

### Database Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `str` | `"sqlite:///:memory:"` | Database connection URL |
| `pool_size` | `int` | `10` | Connection pool size |
| `timeout` | `int` | `30` | Connection timeout in seconds |
| `ssl_enabled` | `bool` | `False` | Enable SSL connections |
| `ssl_cert` | `Optional[str]` | `None` | SSL certificate path |

### Write Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `str` | `"append"` | Write mode: "append", "overwrite", "merge" |
| `partition_by` | `List[str]` | `[]` | Partition columns |
| `overwrite_schema` | `bool` | `False` | Allow schema overwrites |
| `allow_schema_evolution` | `bool` | `True` | Allow schema evolution |
| `max_records_per_file` | `int` | `1000000` | Maximum records per file |

### Transaction Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `isolation_level` | `str` | `"read_committed"` | Transaction isolation level |
| `timeout` | `int` | `300` | Transaction timeout in seconds |
| `max_retries` | `int` | `3` | Maximum retry attempts |
| `retry_delay` | `float` | `1.0` | Retry delay in seconds |

### Table Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | Required | Table name |
| `description` | `Optional[str]` | `None` | Table description |
| `partition_columns` | `List[str]` | `[]` | Partition columns |
| `z_order_columns` | `List[str]` | `[]` | Z-order columns |

## Dependencies

### Required Dependencies

- `deltalakedb` - Core package

### Optional Dependencies

For YAML support:
```bash
pip install PyYAML
```

For TOML support:
```bash
pip install tomli tomli_w
```

For full functionality:
```bash
pip install deltalakedb[config]
```

## Examples

### Basic Usage

```python
import deltalakedb as dl

# Load configuration
config = dl.load_config_file("config.yaml")

# Create connection with configuration
conn = dl.SqlConnection(config.url, pool_size=config.pool_size)

# Use for operations
table = dl.connect_to_table("deltasql://my_table", connection=conn)
```

### Development vs Production

```python
# Development
dev_config = dl.load_config_file("config.dev.yaml")

# Production
prod_config = dl.load_comprehensive_config(
    config_file="config.prod.yaml",
    env_prefix="DELTALAKE_PROD_"
)

# Choose based on environment
import os
env = os.getenv("DELTALAKE_ENV", "dev")

config = dev_config if env == "dev" else prod_config
```

### Configuration Validation

```python
import deltalakedb as dl

# Validate configuration file
if not dl.validate_config_file("config.yaml"):
    raise ValueError("Invalid configuration file")

# Get schema for documentation
schema = dl.get_config_schema("database")
print(f"Database configuration schema: {schema}")
```

### Dynamic Configuration

```python
import deltalakedb as dl
from deltalakedb import ConfigFactory

# Create configuration programmatically
config = ConfigFactory.create_database_config(
    url="postgresql://localhost:5432/test",
    pool_size=15,
    timeout=45
)

# Save for later use
dl.save_yaml_config(config, "dynamic_config.yaml")

# Load when needed
loaded_config = dl.load_yaml_config("dynamic_config.yaml")
```

## Best Practices

1. **Use Environment Variables for Sensitive Data**: Never store passwords or certificates in configuration files.
   ```bash
   export DELTALAKE_DB_PASSWORD="secret_password"
   ```

2. **Configuration Hierarchy**: Use the comprehensive loader to support default values, file configuration, and environment overrides.

3. **Validation**: Always validate configuration files before using them in production.

4. **Format Selection**:
   - YAML: Human-readable, supports comments
   - TOML: More structured, good for simple configurations
   - JSON: Universal, good for programmatic generation

5. **Sample Files**: Generate sample configuration files to document available options.

6. **Environment-Specific Configs**: Use separate configuration files for different environments (dev, staging, prod).

## Troubleshooting

### Common Issues

1. **Missing Dependencies**: Install required packages for format support.
   ```bash
   pip install PyYAML tomli tomli_w
   ```

2. **File Not Found**: Check file paths and permissions.
   ```python
   import os
   if not os.path.exists("config.yaml"):
       print("Configuration file not found")
   ```

3. **Validation Errors**: Check configuration against schema.
   ```python
   is_valid = dl.validate_config_file("config.yaml")
   ```

4. **Format Detection**: Ensure file extensions are correct (.yaml, .yml, .toml, .json).

### Error Messages

- `"PyYAML is required for YAML configuration"`: Install PyYAML
- `"toml is required for TOML configuration"`: Install tomli
- `"Unsupported configuration file format"`: Check file extension
- `"Configuration file not found"`: Verify file path exists