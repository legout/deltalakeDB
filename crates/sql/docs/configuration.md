# SQL Adapters Configuration Guide

This guide provides comprehensive configuration options for Delta Lake SQL adapters across all supported databases.

## Overview

The SQL adapters support flexible configuration through environment variables, configuration files, and programmatic settings. Configuration is designed to work seamlessly across development, testing, and production environments.

## DatabaseConfig Structure

```rust
pub struct DatabaseConfig {
    pub url: String,                    // Database connection URL
    pub pool_size: u32,                 // Connection pool size
    pub timeout: u64,                   // Connection timeout (seconds)
    pub ssl_enabled: bool,              // Enable SSL (PostgreSQL only)
    pub read_only: bool,                // Read-only mode
    pub max_connections: Option<u32>,   // Maximum connections (overrides pool_size)
    pub min_connections: Option<u32>,   // Minimum connections to maintain
    pub connection_timeout: Option<u64>, // Connection establishment timeout
    pub idle_timeout: Option<u64>,      // Idle connection timeout
    pub max_lifetime: Option<u64>,      // Maximum connection lifetime
    pub health_check_interval: Option<u64>, // Health check interval
    pub retry_attempts: Option<u32>,    // Connection retry attempts
    pub retry_delay: Option<u64>,       // Delay between retries
}
```

## Connection URLs

### PostgreSQL URLs

```rust
// Basic PostgreSQL
let config = DatabaseConfig {
    url: "postgresql://user:password@localhost:5432/database".to_string(),
    // ... other settings
};

// PostgreSQL with SSL
let config = DatabaseConfig {
    url: "postgresql://user:password@localhost:5432/database?sslmode=require".to_string(),
    ssl_enabled: true,
    // ... other settings
};

// PostgreSQL with connection parameters
let config = DatabaseConfig {
    url: "postgresql://user:password@localhost:5432/database?\
           sslmode=require&\
           connect_timeout=30&\
           application_name=deltalake&\
           tcp_user_timeout=10000".to_string(),
    // ... other settings
};

// PostgreSQL Unix socket
let config = DatabaseConfig {
    url: "postgresql://user@/database?host=/var/run/postgresql".to_string(),
    // ... other settings
};
```

#### PostgreSQL URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | localhost | Database server hostname |
| `port` | 5432 | Database server port |
| `user` | - | Database username |
| `password` | - | Database password |
| `dbname` | - | Database name |
| `sslmode` | prefer | SSL mode (disable, allow, prefer, require) |
| `application_name` | deltalake | Application name for monitoring |
| `connect_timeout` | 30 | Connection timeout in seconds |
| `tcp_user_timeout` | 0 | TCP user timeout in milliseconds |
| `target_session_attrs` | any | Session requirements (read-write, read-only, etc.) |

### SQLite URLs

```rust
// In-memory SQLite
let config = DatabaseConfig {
    url: "sqlite::memory:".to_string(),
    // ... other settings
};

// File-based SQLite
let config = DatabaseConfig {
    url: "sqlite:///path/to/database.db".to_string(),
    // ... other settings
};

// SQLite with relative path
let config = DatabaseConfig {
    url: "sqlite://./data/deltalake.db".to_string(),
    // ... other settings
};

// SQLite with URI parameters
let config = DatabaseConfig {
    url: "sqlite:///path/to/database.db?\
           mode=rw&\
           cache=shared&\
           journal_mode=WAL".to_string(),
    // ... other settings
};
```

#### SQLite URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mode` | rwc | File mode (ro, rw, rwc, memory) |
| `cache` | private | Cache mode (private, shared) |
| `journal_mode` | delete | Journal mode (delete, truncate, persist, WAL) |
| `synchronous` | FULL | Synchronization level (OFF, NORMAL, FULL) |
| `query_only` | false | Query-only mode |
| `immutable` | false | Database is immutable |

### DuckDB URLs

```rust
// In-memory DuckDB
let config = DatabaseConfig {
    url: "duckdb::memory:".to_string(),
    // ... other settings
};

// File-based DuckDB
let config = DatabaseConfig {
    url: "duckdb:///path/to/database.duckdb".to_string(),
    // ... other settings
};

// DuckDB with configuration
let config = DatabaseConfig {
    url: "duckdb:///path/to/database.duckdb?\
           memory_limit=1GB&\
           threads=4&\
           temp_directory=/tmp".to_string(),
    // ... other settings
};
```

#### DuckDB URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `memory_limit` | - | Memory limit (e.g., 1GB, 512MB) |
| `threads` | - | Number of threads |
| `temp_directory` | - | Temporary directory path |
| `wal_autocheckpoint` | - | WAL autocheckpoint threshold |
| `access_mode` | AUTOMATIC | Access mode (AUTOMATIC, READ_ONLY, READ_WRITE) |

## Connection Pool Configuration

### PostgreSQL Connection Pooling

```rust
use deadpool_postgres::{Config as PgConfig, Pool};

let mut pg_config = PgConfig::new();
pg_config.host = Some("localhost".to_string());
pg_config.port = Some(5432);
pg_config.dbname = Some("deltalake".to_string());
pg_config.user = Some("postgres".to_string());
pg_config.password = Some("password".to_string());

// Pool configuration
pg_config.max_pool_size = Some(20);           // Maximum connections
pg_config.min_pool_idle = Some(5);            // Minimum idle connections
pg_config.idle_timeout = Some(Duration::from_secs(600)); // 10 minutes
pg_config.max_lifetime = Some(Duration::from_secs(1800)); // 30 minutes
pg_config.connection_timeout = Some(Duration::from_secs(30)); // 30 seconds
pg_config.retries = Some(3);                  // Connection retry attempts

let pool = pg_config.create_pool(tokio_postgres::NoTls)?;
```

### SQLite Connection Pooling

```rust
// SQLite typically uses single connection for most workloads
let config = DatabaseConfig {
    url: "sqlite:///path/to/database.db".to_string(),
    pool_size: 1,                    // Single connection for consistency
    timeout: 30,
    ssl_enabled: false,
    // SQLite-specific settings
    min_connections: Some(1),
    max_connections: Some(2),        // Allow occasional second connection
    idle_timeout: None,              // Keep connection alive
    max_lifetime: None,              // Reuse connections indefinitely
};
```

### DuckDB Connection Pooling

```rust
// DuckDB supports multiple connections
let config = DatabaseConfig {
    url: "duckdb:///analytics.duckdb".to_string(),
    pool_size: 10,                   // Multiple connections for parallel queries
    timeout: 30,
    ssl_enabled: false,
    min_connections: Some(2),
    max_connections: Some(20),
    idle_timeout: Some(Duration::from_secs(300)), // 5 minutes
    max_lifetime: Some(Duration::from_secs(900)), // 15 minutes
};
```

## Environment Variables

The adapters support configuration through environment variables with the `DELTALAKE_DB_` prefix:

```bash
# Basic connection
export DELTALAKE_DB_URL="postgresql://user:password@localhost/deltalake"
export DELTALAKE_DB_POOL_SIZE="20"
export DELTALAKE_DB_TIMEOUT="30"
export DELTALAKE_DB_SSL_ENABLED="true"

# Advanced configuration
export DELTALAKE_DB_MAX_CONNECTIONS="50"
export DELTALAKE_DB_MIN_CONNECTIONS="5"
export DELTALAKE_DB_CONNECTION_TIMEOUT="60"
export DELTALAKE_DB_IDLE_TIMEOUT="600"
export DELTALAKE_DB_MAX_LIFETIME="1800"
export DELTALAKE_DB_HEALTH_CHECK_INTERVAL="30"
export DELTALAKE_DB_RETRY_ATTEMPTS="3"
export DELTALAKE_DB_RETRY_DELAY="1000"

# Database-specific
export DELTALAKE_DB_POSTGRES_APPLICATION_NAME="deltalake_worker"
export DELTALAKE_DB_SQLITE_JOURNAL_MODE="WAL"
export DELTALAKE_DB_DUCKDB_MEMORY_LIMIT="2GB"
export DELTALAKE_DB_DUCKDB_THREADS="4"
```

### Loading Configuration from Environment

```rust
use std::env;

impl DatabaseConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let url = env::var("DELTALAKE_DB_URL")
            .map_err(|_| ConfigError::MissingVariable("DELTALAKE_DB_URL"))?;

        let pool_size = env::var("DELTALAKE_DB_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let timeout = env::var("DELTALAKE_DB_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let ssl_enabled = env::var("DELTALAKE_DB_SSL_ENABLED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        Ok(DatabaseConfig {
            url,
            pool_size,
            timeout,
            ssl_enabled,
            // ... load other fields from environment
        })
    }
}
```

## Configuration Files

### TOML Configuration

```toml
# deltalake.toml
[database]
url = "postgresql://user:password@localhost/deltalake"
pool_size = 20
timeout = 30
ssl_enabled = true
read_only = false

[database.pool]
max_connections = 50
min_connections = 5
connection_timeout = 60
idle_timeout = 600
max_lifetime = 1800

[database.health_check]
enabled = true
interval = 30
retry_attempts = 3
retry_delay = 1000

[database.postgres]
application_name = "deltalake_worker"
sslmode = "require"
connect_timeout = 30

[database.sqlite]
journal_mode = "WAL"
synchronous = "NORMAL"
cache_size = 10000

[database.duckdb]
memory_limit = "2GB"
threads = 4
temp_directory = "/tmp/deltalake_temp"
```

### YAML Configuration

```yaml
# deltalake.yaml
database:
  url: "postgresql://user:password@localhost/deltalake"
  pool_size: 20
  timeout: 30
  ssl_enabled: true
  read_only: false

  pool:
    max_connections: 50
    min_connections: 5
    connection_timeout: 60
    idle_timeout: 600
    max_lifetime: 1800

  health_check:
    enabled: true
    interval: 30
    retry_attempts: 3
    retry_delay: 1000

  postgres:
    application_name: "deltalake_worker"
    sslmode: "require"
    connect_timeout: 30

  sqlite:
    journal_mode: "WAL"
    synchronous: "NORMAL"
    cache_size: 10000

  duckdb:
    memory_limit: "2GB"
    threads: 4
    temp_directory: "/tmp/deltalake_temp"
```

### Loading Configuration from Files

```rust
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Deserialize, Serialize)]
struct DeltaLakeConfig {
    database: DatabaseConfigSection,
}

#[derive(Debug, Deserialize, Serialize)]
struct DatabaseConfigSection {
    url: String,
    pool_size: u32,
    timeout: u64,
    ssl_enabled: bool,
    read_only: bool,
    pool: Option<PoolConfig>,
    health_check: Option<HealthCheckConfig>,
    postgres: Option<PostgresConfig>,
    sqlite: Option<SqliteConfig>,
    duckdb: Option<DuckDBConfig>,
}

impl DeltaLakeConfig {
    pub fn from_toml_file(path: &str) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: DeltaLakeConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn from_yaml_file(path: &str) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: DeltaLakeConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn to_database_config(&self) -> DatabaseConfig {
        DatabaseConfig {
            url: self.database.url.clone(),
            pool_size: self.database.pool_size,
            timeout: self.database.timeout,
            ssl_enabled: self.database.ssl_enabled,
            read_only: self.database.read_only,
            max_connections: self.database.pool.as_ref()
                .and_then(|p| p.max_connections),
            min_connections: self.database.pool.as_ref()
                .and_then(|p| p.min_connections),
            connection_timeout: self.database.pool.as_ref()
                .and_then(|p| p.connection_timeout),
            idle_timeout: self.database.pool.as_ref()
                .and_then(|p| p.idle_timeout),
            max_lifetime: self.database.pool.as_ref()
                .and_then(|p| p.max_lifetime),
            // ... other fields
        }
    }
}
```

## Database-Specific Configuration

### PostgreSQL Configuration

```rust
use deadpool_postgres::Config;

pub fn create_postgres_config() -> Config {
    let mut config = Config::new();

    // Connection settings
    config.host = Some("localhost".to_string());
    config.port = Some(5432);
    config.dbname = Some("deltalake".to_string());
    config.user = Some("postgres".to_string());
    config.password = Some("password".to_string());

    // Pool settings
    config.max_pool_size = Some(20);
    config.min_pool_idle = Some(5);
    config.idle_timeout = Some(Duration::from_secs(600));
    config.max_lifetime = Some(Duration::from_secs(1800));
    config.connection_timeout = Some(Duration::from_secs(30));
    config.retries = Some(3);

    // SSL settings
    config.ssl_mode = Some(tokio_postgres::config::SslMode::Require);

    config
}
```

#### PostgreSQL Connection Parameters

```rust
// Advanced PostgreSQL configuration
let pg_url = "postgresql://user:password@localhost:5432/deltalake?\
             sslmode=require&\
             application_name=deltalake_worker&\
             connect_timeout=30&\
             statement_timeout=300000&\
             idle_in_transaction_session_timeout=60000&\
             lock_timeout=30000&\
             tcp_user_timeout=10000&\
             target_session_attrs=read-write&\
             keepalives=1&\
             keepalives_idle=300&\
             keepalives_interval=30&\
             keepalives_count=3";

let config = DatabaseConfig {
    url: pg_url.to_string(),
    pool_size: 20,
    timeout: 30,
    ssl_enabled: true,
    // ... other settings
};
```

### SQLite Configuration

```rust
pub fn create_sqlite_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "sqlite:///path/to/deltalake.db".to_string(),
        pool_size: 1,                    // Single connection for consistency
        timeout: 30,
        ssl_enabled: false,
        read_only: false,

        // SQLite-specific optimizations
        min_connections: Some(1),
        max_connections: Some(2),        // Allow occasional second connection
        idle_timeout: None,              // Keep connection alive
        max_lifetime: None,              // Reuse connections indefinitely

        // SQLite PRAGMA settings applied on connection
        pragma_settings: vec![
            ("journal_mode".to_string(), "WAL".to_string()),
            ("synchronous".to_string(), "NORMAL".to_string()),
            ("cache_size".to_string(), "10000".to_string()),
            ("temp_store".to_string(), "MEMORY".to_string()),
            ("mmap_size".to_string(), "268435456".to_string()), // 256MB
        ],
    }
}
```

#### SQLite PRAGMA Configuration

```rust
// Advanced SQLite configuration with PRAGMAs
let config = SqliteConfig {
    url: "sqlite:///deltalake.db".to_string(),

    // Performance settings
    journal_mode: "WAL".to_string(),        // Enable WAL mode
    synchronous: "NORMAL".to_string(),      // Balance safety and performance
    cache_size: 10000,                      // 10MB cache
    temp_store: "MEMORY".to_string(),       // Store temp tables in memory
    mmap_size: Some(268435456),             // 256MB memory-mapped I/O

    // Locking and timeout
    locking_mode: "NORMAL".to_string(),
    busy_timeout: 30000,                    // 30 seconds

    // Query optimization
    optimize: true,
    query_only: false,

    // Maintenance
    auto_vacuum: "INCREMENTAL".to_string(),
    checkpoint_fullfsync: false,
    wal_autocheckpoint: 1000,
};
```

### DuckDB Configuration

```rust
pub fn create_duckdb_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "duckdb:///analytics.duckdb".to_string(),
        pool_size: 10,                   // Multiple connections for parallelism
        timeout: 30,
        ssl_enabled: false,
        read_only: false,

        min_connections: Some(2),
        max_connections: Some(20),
        idle_timeout: Some(300),         // 5 minutes
        max_lifetime: Some(900),         // 15 minutes

        // DuckDB-specific settings
        memory_limit: Some("2GB".to_string()),
        threads: Some(4),
        temp_directory: Some("/tmp/deltalake".to_string()),
        wal_autocheckpoint: Some(1000000),

        // Performance settings
        enable_optimizer: Some(true),
        enable_profiling: Some(false),
        force_parallelism: Some(true),
        preserve_insertion_order: Some(false),
    }
}
```

#### DuckDB Extension Configuration

```rust
// DuckDB with extensions
let duckdb_url = "duckdb:///analytics.duckdb?\
                 memory_limit=2GB&\
                 threads=4&\
                 temp_directory=/tmp/deltalake&\
                 enable_optimizer=true&\
                 force_parallelism=true&\
                 preserve_insertion_order=false&\
                 enable_progress_bar=false&\
                 access_mode=AUTOMATIC";

let config = DatabaseConfig {
    url: duckdb_url.to_string(),
    pool_size: 10,
    timeout: 30,
    ssl_enabled: false,

    // Extension loading
    extensions: vec![
        "httpfs".to_string(),       // For remote file systems
        "json".to_string(),         // For JSON operations
        "fts".to_string(),          // For full-text search
    ],

    // Custom settings
    custom_settings: vec![
        ("checkpoint_threshold".to_string(), "1GB".to_string()),
        ("profiling_output".to_string(), "query_profiler.log".to_string()),
    ],
};
```

## Production Configuration

### Production PostgreSQL

```rust
pub fn production_postgres_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "postgresql://deltalake_user:secure_pass@prod-db:5432/deltalake_prod?sslmode=require".to_string(),
        pool_size: 50,                   // Large pool for production
        timeout: 60,                     // Longer timeout
        ssl_enabled: true,
        read_only: false,

        max_connections: Some(100),      // Allow bursts
        min_connections: Some(10),       // Keep warm connections
        connection_timeout: Some(60),    // Longer connection timeout
        idle_timeout: Some(600),         // 10 minutes
        max_lifetime: Some(1800),        // 30 minutes

        health_check_interval: Some(30), // Health check every 30s
        retry_attempts: Some(3),         // Retry failed connections
        retry_delay: Some(5000),         // 5 second delay between retries
    }
}
```

### Production SQLite

```rust
pub fn production_sqlite_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "sqlite:///var/lib/deltalake/production.db".to_string(),
        pool_size: 1,                    // Single connection
        timeout: 60,
        ssl_enabled: false,
        read_only: false,

        min_connections: Some(1),
        max_connections: Some(3),        // Allow backup connections
        idle_timeout: None,              // Keep primary connection alive
        max_lifetime: Some(3600),        // 1 hour max lifetime

        // Production SQLite optimizations
        journal_mode: Some("WAL".to_string()),
        synchronous: Some("NORMAL".to_string()),
        cache_size: Some(50000),         // 50MB cache
        mmap_size: Some(1073741824),     // 1GB memory-mapped I/O

        health_check_interval: Some(60),
        retry_attempts: Some(5),
        retry_delay: Some(1000),
    }
}
```

### Production DuckDB

```rust
pub fn production_duckdb_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "duckdb:///var/lib/deltalake/analytics.duckdb".to_string(),
        pool_size: 20,                   // More connections for analytics
        timeout: 60,
        ssl_enabled: false,
        read_only: false,

        min_connections: Some(5),
        max_connections: Some(50),
        connection_timeout: Some(60),
        idle_timeout: Some(600),         // Keep connections warm
        max_lifetime: Some(1800),

        memory_limit: Some("8GB".to_string()),
        threads: Some(8),                // Use all available cores
        temp_directory: Some("/var/tmp/deltalake".to_string()),

        // Production optimizations
        wal_autocheckpoint: Some(5000000),
        checkpoint_threshold: Some("2GB".to_string()),

        health_check_interval: Some(30),
        retry_attempts: Some(3),
        retry_delay: Some(2000),
    }
}
```

## Development and Testing Configuration

### Development Configuration

```rust
pub fn development_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 5,
        timeout: 10,
        ssl_enabled: false,
        read_only: false,

        min_connections: Some(1),
        max_connections: Some(10),
        idle_timeout: Some(60),          // Short idle timeout
        max_lifetime: Some(300),         // 5 minutes

        health_check_interval: Some(10), // Frequent health checks
        retry_attempts: Some(2),
        retry_delay: Some(500),
    }
}
```

### Testing Configuration

```rust
pub fn testing_config() -> DatabaseConfig {
    DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 1,                    // Minimal pool for tests
        timeout: 5,
        ssl_enabled: false,
        read_only: false,

        min_connections: Some(1),
        max_connections: Some(2),
        idle_timeout: Some(10),          // Very short for tests
        max_lifetime: Some(60),          // 1 minute for tests

        health_check_interval: Some(5),
        retry_attempts: Some(1),         // Minimal retries for tests
        retry_delay: Some(100),
    }
}
```

## Configuration Validation

```rust
impl DatabaseConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate URL
        if self.url.is_empty() {
            return Err(ConfigError::InvalidField("url", "URL cannot be empty"));
        }

        // Validate pool size
        if self.pool_size == 0 {
            return Err(ConfigError::InvalidField("pool_size", "Pool size must be greater than 0"));
        }

        // Validate timeout
        if self.timeout == 0 {
            return Err(ConfigError::InvalidField("timeout", "Timeout must be greater than 0"));
        }

        // Validate database-specific URL format
        self.validate_url_format()?;

        // Validate pool configuration consistency
        if let Some(max_conn) = self.max_connections {
            if max_conn < self.pool_size {
                return Err(ConfigError::InvalidField(
                    "max_connections",
                    "max_connections must be >= pool_size"
                ));
            }
        }

        if let Some(min_conn) = self.min_connections {
            if min_conn > self.pool_size {
                return Err(ConfigError::InvalidField(
                    "min_connections",
                    "min_connections must be <= pool_size"
                ));
            }
        }

        Ok(())
    }

    fn validate_url_format(&self) -> Result<(), ConfigError> {
        if self.url.starts_with("postgresql://") {
            self.validate_postgres_url()
        } else if self.url.starts_with("sqlite://") || self.url == "sqlite::memory:" {
            self.validate_sqlite_url()
        } else if self.url.starts_with("duckdb://") {
            self.validate_duckdb_url()
        } else {
            Err(ConfigError::UnsupportedDatabase(self.url.clone()))
        }
    }

    fn validate_postgres_url(&self) -> Result<(), ConfigError> {
        // Validate PostgreSQL URL format
        let url = url::Url::parse(&self.url)
            .map_err(|e| ConfigError::InvalidUrl(format!("Invalid PostgreSQL URL: {}", e)))?;

        if url.scheme() != "postgresql" {
            return Err(ConfigError::InvalidUrl("Scheme must be postgresql".to_string()));
        }

        // Check required components
        if url.host().is_none() {
            return Err(ConfigError::InvalidUrl("Host is required".to_string()));
        }

        Ok(())
    }

    // Similar validation methods for SQLite and DuckDB...
}
```

This comprehensive configuration guide covers all aspects of configuring the SQL adapters for different use cases, environments, and database types.