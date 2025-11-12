# Deployment and Operations Guide for SQL-Backed Delta Lake

This comprehensive guide covers the deployment, maintenance, and operational aspects of running SQL-Backed Delta Lake in production environments.

## Table of Contents

1. [Deployment Overview](#deployment-overview)
2. [System Requirements](#system-requirements)
3. [Installation and Setup](#installation-and-setup)
4. [Configuration Management](#configuration-management)
5. [Database Deployment](#database-deployment)
6. [Application Deployment](#application-deployment)
7. [High Availability and Scaling](#high-availability-and-scaling)
8. [Backup and Recovery](#backup-and-recovery)
9. [Monitoring and Alerting](#monitoring-and-alerting)
10. [Security](#security)
11. [Maintenance Operations](#maintenance-operations)
12. [Troubleshooting](#troubleshooting)
13. [Disaster Recovery](#disaster-recovery)

## Deployment Overview

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    Application Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  Python Applications │  Data Processing │  Analytics Tools    │
│  (deltalakedb)       │  (Spark, Dask)   │  (Jupyter, BI)      │
├─────────────────────────────────────────────────────────────────┤
│                    Connection Layer                             │
├─────────────────────────────────────────────────────────────────┤
│  Connection Pool │  Async Executor │  Cache Manager          │
├─────────────────────────────────────────────────────────────────┤
│                    Storage Layer                                │
├─────────────────────────────────────────────────────────────────┤
│  SQL Database (PostgreSQL/MySQL/SQLite) │  Data Files          │
│  - Metadata                            │  - Parquet Files     │
│  - Transaction Logs                    │  - Delta Logs        │
├─────────────────────────────────────────────────────────────────┤
│                    Infrastructure                               │
├─────────────────────────────────────────────────────────────────┤
│  Compute Resources │  Storage │  Network │  Monitoring          │
└─────────────────────────────────────────────────────────────────┘
```

### Deployment Patterns

1. **Single-Node Development**: All components on one machine
2. **Multi-Node Production**: Distributed database with multiple application nodes
3. **Cloud-Native**: Containerized deployment with managed database services
4. **Hybrid**: On-premise database with cloud-based analytics

## System Requirements

### Minimum Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| Memory | 8 GB | 32+ GB |
| Storage | 100 GB SSD | 1 TB+ SSD |
| Network | 1 Gbps | 10+ Gbps |
| Database | PostgreSQL 12+ | PostgreSQL 15+ |

### Software Dependencies

```bash
# Core Python packages
python >= 3.8
deltalakedb >= 1.0.0
deltalake >= 0.15.0

# Database drivers (choose based on database)
psycopg2-binary >= 2.9.0  # PostgreSQL
pymysql >= 1.0.0          # MySQL
aiosqlite >= 0.17.0       # SQLite

# Optional dependencies
pydantic >= 2.0.0         # Configuration validation
pyyaml >= 6.0            # YAML configuration support
tomli >= 0.4.0           # TOML configuration support
```

### Database Requirements

#### PostgreSQL
```sql
-- Minimum required version
SELECT version();  -- PostgreSQL 12.0 or higher

-- Required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
```

#### MySQL
```sql
-- Minimum required version
SELECT VERSION();  -- MySQL 8.0.0 or higher

-- Required configuration
SET GLOBAL innodb_file_per_table = ON;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
```

## Installation and Setup

### 1. Package Installation

```bash
# Install from PyPI
pip install deltalakedb

# Install with database drivers
pip install deltalakedb[postgresql]  # PostgreSQL
pip install deltalakedb[mysql]       # MySQL
pip install deltalakedb[sqlite]      # SQLite

# Install with all dependencies
pip install deltalakedb[all]

# Install development version
pip install git+https://github.com/your-org/deltalakedb-python.git
```

### 2. Source Installation

```bash
# Clone repository
git clone https://github.com/your-org/deltalakedb-python.git
cd deltalakedb-python

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest tests/

# Build package
python -m build
```

### 3. Docker Installation

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install deltalakedb
COPY . .
RUN pip install -e .

# Set working directory
WORKDIR /app

# Expose port (if running web interface)
EXPOSE 8000

# Default command
CMD ["python", "-m", "deltalakedb.cli"]
```

```yaml
# docker-compose.yml
version: '3.8'

services:
  deltalakedb-app:
    build: .
    environment:
      - DATABASE_URL=postgresql://deltalake:password@db:5432/deltalake
      - LOG_LEVEL=INFO
    depends_on:
      - db
    volumes:
      - ./data:/app/data
      - ./config:/app/config

  db:
    image: postgres:15
    environment:
      - POSTGRES_DB=deltalake
      - POSTGRES_USER=deltalake
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

## Configuration Management

### 1. Configuration File Structure

```yaml
# config/production.yaml
environment: production

# Database configuration
database:
  url: "${DATABASE_URL}"
  pool_size: 50
  max_overflow: 100
  pool_timeout: 60
  pool_recycle: 3600
  ssl_mode: "require"

# Performance settings
performance:
  # Caching
  cache:
    enabled: true
    max_size: 5000
    eviction_policy: "LRU"
    ttl_seconds: 1800
    compression: true

  # Lazy loading
  lazy_loading:
    enabled: true
    strategy: "paginated"
    chunk_size: 1000
    cache_size: 500

  # Async I/O
  async_io:
    enabled: true
    max_concurrent_tasks: 50
    task_timeout: 300
    queue_size: 1000

# Transaction settings
transactions:
  isolation_level: "READ_COMMITTED"
  timeout_seconds: 300
  retry_attempts: 3
  enable_auto_recovery: true

# Logging
logging:
  level: "INFO"
  format: "json"
  file: "/var/log/deltalakedb/app.log"
  max_size_mb: 100
  backup_count: 5

# Monitoring
monitoring:
  metrics_enabled: true
  health_check_interval: 30
  performance_stats: true

# Security
security:
  encryption_at_rest: true
  audit_logging: true
  access_control_enabled: true
```

### 2. Environment Variables

```bash
# .env.production
# Database configuration
DATABASE_URL=postgresql://user:password@db.example.com:5432/deltalake
DATABASE_POOL_SIZE=50
DATABASE_MAX_OVERFLOW=100

# Application settings
LOG_LEVEL=INFO
ENVIRONMENT=production

# Cache settings
REDIS_URL=redis://cache.example.com:6379/0
CACHE_TTL_SECONDS=1800

# Security
ENCRYPTION_KEY=your-encryption-key-here
AUDIT_LOG_FILE=/var/log/deltalakedb/audit.log

# Performance
MAX_CONCURRENT_TASKS=50
ASYNC_TIMEOUT_SECONDS=300
```

### 3. Configuration Validation

```python
import deltalakedb as dl
from deltalakedb.config import ConfigLoader, ConfigValidator

def validate_configuration(config_path: str):
    """Validate configuration before deployment."""

    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)

        # Validate configuration
        validator = ConfigValidator()
        validation_result = validator.validate(config)

        if not validation_result.is_valid:
            print("❌ Configuration validation failed:")
            for error in validation_result.errors:
                print(f"   - {error}")
            return False

        print("✅ Configuration validation passed")
        print(f"   Environment: {config.get('environment')}")
        print(f"   Database URL: {config['database']['url'][:20]}...")
        print(f"   Pool size: {config['database']['pool_size']}")

        return True

    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        return False

# Validate configuration
if not validate_configuration("config/production.yaml"):
    exit(1)
```

## Database Deployment

### 1. PostgreSQL Deployment

```sql
-- Create database and user
CREATE DATABASE deltalake_metadata
    WITH ENCODING 'UTF8'
    LC_COLLATE='en_US.UTF-8'
    LC_CTYPE='en_US.UTF-8'
    TEMPLATE=template0;

CREATE USER deltalake_app WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE deltalake_metadata TO deltalake_app;

-- Connect to database
\c deltalake_metadata;

-- Create schema
CREATE SCHEMA IF NOT EXISTS deltalake;
GRANT ALL ON SCHEMA deltalake TO deltalake_app;

-- Set default schema
ALTER ROLE deltalake_app SET search_path TO deltalake, public;

-- Create required tables
CREATE TABLE deltalake.table_metadata (
    id SERIAL PRIMARY KEY,
    table_uuid UUID DEFAULT uuid_generate_v4() NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    metadata_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, schema_name, version)
);

CREATE TABLE deltalake.table_files (
    id BIGSERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES deltalake.table_metadata(id),
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    modification_time TIMESTAMP WITH TIME ZONE NOT NULL,
    partition_values JSONB,
    stats_json JSONB,
    checksum VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE deltalake.table_commits (
    id BIGSERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES deltalake.table_metadata(id),
    version BIGINT NOT NULL,
    operation VARCHAR(50) NOT NULL,
    parameters JSONB,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    commit_info JSONB,
    UNIQUE(table_id, version)
);

-- Create indexes for performance
CREATE INDEX idx_table_metadata_name_version
ON deltalake.table_metadata(table_name, version DESC);

CREATE INDEX idx_table_files_table_id
ON deltalake.table_files(table_id);

CREATE INDEX idx_table_commits_table_version
ON deltalake.table_commits(table_id, version DESC);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON deltalake.table_metadata TO deltalake_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON deltalake.table_files TO deltalake_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON deltalake.table_commits TO deltalake_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA deltalake TO deltalake_app;
```

### 2. MySQL Deployment

```sql
-- Create database and user
CREATE DATABASE deltalake_metadata
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

CREATE USER 'deltalake_app'@'%' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON deltalake_metadata.* TO 'deltalake_app'@'%';

FLUSH PRIVILEGES;

-- Use database
USE deltalake_metadata;

-- Create tables
CREATE TABLE table_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_uuid BINARY(16) DEFAULT (UUID_TO_BIN(UUID())) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    metadata_json JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_table_version (table_name, schema_name, version),
    INDEX idx_table_name_version (table_name, version DESC)
);

CREATE TABLE table_files (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    modification_time TIMESTAMP NOT NULL,
    partition_values JSON,
    stats_json JSON,
    checksum VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES table_metadata(id),
    INDEX idx_table_id (table_id)
);

CREATE TABLE table_commits (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    version BIGINT NOT NULL,
    operation VARCHAR(50) NOT NULL,
    parameters JSON,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    commit_info JSON,
    FOREIGN KEY (table_id) REFERENCES table_metadata(id),
    UNIQUE KEY unique_commit_version (table_id, version),
    INDEX idx_table_version (table_id, version DESC)
);
```

### 3. Database Optimization

```sql
-- PostgreSQL optimizations
-- postgresql.conf
shared_buffers = '4GB'
effective_cache_size = '12GB'
work_mem = '256MB'
maintenance_work_mem = '1GB'
checkpoint_completion_target = 0.9
wal_buffers = '64MB'
default_statistics_target = 1000
random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 200
superuser_reserved_connections = 3

-- MySQL optimizations
-- my.cnf
[mysqld]
innodb_buffer_pool_size = 8G
innodb_log_file_size = 1G
innodb_log_buffer_size = 256M
innodb_flush_log_at_trx_commit = 2
max_connections = 200
thread_cache_size = 50
query_cache_type = 1
query_cache_size = 512M
innodb_io_capacity = 2000
innodb_io_capacity_max = 4000
innodb_flush_method = O_DIRECT
innodb_file_per_table = 1
```

## Application Deployment

### 1. Production Application Structure

```
/opt/deltalakedb/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── api/
│   ├── services/
│   └── models/
├── config/
│   ├── production.yaml
│   ├── staging.yaml
│   └── development.yaml
├── scripts/
│   ├── deploy.sh
│   ├── backup.sh
│   └── migrate.sh
├── tests/
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

### 2. Application Entrypoint

```python
# app/main.py
import os
import logging
from deltalakedb.config import ConfigLoader
from deltalakedb.logging import DeltaLogger
from deltalakedb import __version__

class DeltaLakeApp:
    """Main application class."""

    def __init__(self):
        self.config = None
        self.logger = None
        self.connection_pool = None

    def initialize(self, config_path: str):
        """Initialize application."""

        # Load configuration
        self.config = ConfigLoader.load_config_file(config_path)

        # Setup logging
        self.logger = DeltaLogger(
            name="deltalakedb",
            config=self.config.get('logging', {})
        )

        self.logger.info(f"Starting DeltaLake DB v{__version__}")
        self.logger.info(f"Environment: {self.config.get('environment')}")

        # Initialize database connection
        self._setup_database()

        # Initialize performance optimizations
        self._setup_performance_features()

        self.logger.info("Application initialized successfully")

    def _setup_database(self):
        """Setup database connections."""

        from deltalakedb.config import ConfigFactory
        from deltalakedb.connection import ConnectionPool

        db_config = self.config['database']
        sql_config = ConfigFactory.create_sql_config(db_config)

        self.connection_pool = ConnectionPool(sql_config)

        # Test connection
        connection = self.connection_pool.get_connection()
        connection.execute("SELECT 1")
        self.connection_pool.return_connection(connection)

        self.logger.info("Database connection established")

    def _setup_performance_features(self):
        """Setup performance optimizations."""

        performance_config = self.config.get('performance', {})

        # Initialize caching
        if performance_config.get('cache', {}).get('enabled', False):
            from deltalakedb.caching import DeltaLakeCacheManager, ConfigFactory as CacheFactory

            cache_config = CacheFactory.create_cache_config(
                performance_config['cache']
            )
            self.cache_manager = DeltaLakeCacheManager(cache_config)
            self.logger.info("Caching initialized")

        # Initialize lazy loading
        if performance_config.get('lazy_loading', {}).get('enabled', False):
            from deltalakedb.lazy_loading import LazyLoadingManager, ConfigFactory as LazyFactory

            lazy_config = LazyFactory.create_lazy_loading_config(
                performance_config['lazy_loading']
            )
            self.lazy_manager = LazyLoadingManager(lazy_config)
            self.logger.info("Lazy loading initialized")

        # Initialize async I/O
        if performance_config.get('async_io', {}).get('enabled', False):
            from deltalakedb.async_io import AsyncIOExecutor, ConfigFactory as AsyncFactory

            async_config = AsyncFactory.create_async_task_config(
                performance_config['async_io']
            )
            self.async_executor = AsyncIOExecutor(async_config)
            self.logger.info("Async I/O initialized")

    def health_check(self) -> dict:
        """Perform health check."""

        health_status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'version': __version__,
            'components': {}
        }

        try:
            # Check database
            connection = self.connection_pool.get_connection()
            connection.execute("SELECT 1")
            self.connection_pool.return_connection(connection)
            health_status['components']['database'] = 'healthy'
        except Exception as e:
            health_status['components']['database'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'

        # Check cache if enabled
        if hasattr(self, 'cache_manager'):
            try:
                stats = self.cache_manager.get_stats()
                health_status['components']['cache'] = f'healthy (hit ratio: {stats.hit_ratio:.2%})'
            except Exception as e:
                health_status['components']['cache'] = f'unhealthy: {str(e)}'
                health_status['status'] = 'degraded'

        return health_status

def create_app():
    """Application factory."""

    app = DeltaLakeApp()

    # Load configuration from environment
    config_path = os.getenv('DELTA_CONFIG_PATH', 'config/production.yaml')
    app.initialize(config_path)

    return app

if __name__ == '__main__':
    app = create_app()

    # Start application
    health = app.health_check()
    print(f"Application status: {health['status']}")
```

### 3. Deployment Script

```bash
#!/bin/bash
# scripts/deploy.sh

set -e

# Configuration
APP_DIR="/opt/deltalakedb"
BACKUP_DIR="/backup/deltalakedb"
LOG_FILE="/var/log/deltalakedb/deploy.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${RED}This script should not be run as root${NC}"
   exit 1
fi

log "Starting deployment..."

# Check prerequisites
log "Checking prerequisites..."

# Check Python version
python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
required_version="3.8"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
    echo -e "${RED}Python $required_version or higher is required${NC}"
    exit 1
fi

log "Python version: $python_version ✓"

# Create backup
log "Creating backup..."
mkdir -p "$BACKUP_DIR"
if [[ -d "$APP_DIR" ]]; then
    backup_name="backup_$(date +%Y%m%d_%H%M%S)"
    cp -r "$APP_DIR" "$BACKUP_DIR/$backup_name"
    log "Backup created: $BACKUP_DIR/$backup_name"
fi

# Install dependencies
log "Installing dependencies..."
cd "$APP_DIR"
pip install -r requirements.txt

# Run database migrations
log "Running database migrations..."
python scripts/migrate.py

# Validate configuration
log "Validating configuration..."
if ! python -c "
import sys
sys.path.append('.')
from deltalakedb.config import ConfigLoader
try:
    config = ConfigLoader.load_config_file('config/production.yaml')
    print('Configuration is valid')
except Exception as e:
    print(f'Configuration error: {e}')
    sys.exit(1)
"; then
    echo -e "${RED}Configuration validation failed${NC}"
    exit 1
fi

# Run tests
log "Running tests..."
if ! python -m pytest tests/ -v; then
    echo -e "${RED}Tests failed${NC}"
    exit 1
fi

# Health check
log "Performing health check..."
if ! python -c "
from app.main import create_app
app = create_app()
health = app.health_check()
if health['status'] != 'healthy':
    print(f'Health check failed: {health}')
    exit(1)
print('Health check passed')
"; then
    echo -e "${RED}Health check failed${NC}"
    exit 1
fi

# Restart services
log "Restarting services..."
if systemctl --user is-active --quiet deltalakedb; then
    systemctl --user restart deltalakedb
    log "Service restarted"
else
    systemctl --user start deltalakedb
    log "Service started"
fi

# Verify deployment
log "Verifying deployment..."
sleep 5
if systemctl --user is-active --quiet deltalakedb; then
    echo -e "${GREEN}Deployment completed successfully!${NC}"
    log "Deployment completed successfully"
else
    echo -e "${RED}Deployment failed - service not running${NC}"
    log "Deployment failed - service not running"
    exit 1
fi

log "Deployment finished at $(date)"
```

## High Availability and Scaling

### 1. Database High Availability

#### PostgreSQL Streaming Replication

```bash
# Primary server setup
# postgresql.conf on primary
wal_level = replica
max_wal_senders = 3
max_replication_slots = 3
synchronous_commit = on
synchronous_standby_names = 'standby1,standby2'

# pg_hba.conf on primary
host replication replicator 10.0.1.0/24 md5
host replication replicator 10.0.2.0/24 md5

# Standby server setup
# recovery.conf on standby
standby_mode = 'on'
primary_conninfo = 'host=primary-db.example.com port=5432 user=replicator'
restore_command = 'cp /var/lib/postgresql/wal_archive/%f %p'
```

#### MySQL Group Replication

```sql
-- On all servers
SET GLOBAL group_replication_bootstrap_group=OFF;
SET GLOBAL group_replication_start_on_boot=ON;
SET GLOBAL group_replication_group_name='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
SET GLOBAL group_replication_local_address='server1:33061';
SET GLOBAL group_replication_group_seeds='server1:33061,server2:33061,server3:33061';
SET GLOBAL group_replication_bootstrap_group=ON;
START GROUP_REPLICATION;
```

### 2. Application Scaling

```python
# app/scaling.py
import asyncio
import aioredis
from deltalakedb.connection import ConnectionPool
from deltalakedb.caching import DeltaLakeCacheManager

class ScalableApplication:
    """Scalable application with distributed features."""

    def __init__(self, config):
        self.config = config
        self.connection_pools = {}
        self.redis_client = None
        self.distributed_cache = None

    async def initialize_distributed_features(self):
        """Initialize distributed components."""

        # Setup Redis for distributed caching
        redis_url = self.config.get('redis_url', 'redis://localhost:6379')
        self.redis_client = await aioredis.from_url(redis_url)

        # Setup distributed cache
        self.distributed_cache = DistributedCache(self.redis_client)

        # Setup connection pooling for multiple databases
        for db_name, db_config in self.config['databases'].items():
            pool = ConnectionPool(db_config)
            self.connection_pools[db_name] = pool

    async def get_connection(self, db_name: str = 'primary'):
        """Get connection with load balancing."""

        if db_name in self.connection_pools:
            return self.connection_pools[db_name].get_connection()
        else:
            raise ValueError(f"Unknown database: {db_name}")

    async def distributed_cache_get(self, key: str):
        """Get value from distributed cache."""

        if self.distributed_cache:
            return await self.distributed_cache.get(key)
        return None

    async def distributed_cache_set(self, key: str, value, ttl: int = 3600):
        """Set value in distributed cache."""

        if self.distributed_cache:
            await self.distributed_cache.set(key, value, ttl)

class DistributedCache:
    """Distributed cache using Redis."""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def get(self, key: str):
        """Get value from cache."""

        value = await self.redis.get(key)
        if value:
            return pickle.loads(value)
        return None

    async def set(self, key: str, value, ttl: int = 3600):
        """Set value in cache."""

        serialized = pickle.dumps(value)
        await self.redis.setex(key, ttl, serialized)

    async def invalidate(self, key: str):
        """Invalidate cache entry."""

        await self.redis.delete(key)
```

### 3. Load Balancer Configuration

```nginx
# nginx.conf
upstream deltalakedb_backend {
    least_conn;
    server app1.example.com:8000 weight=5 max_fails=3 fail_timeout=30s;
    server app2.example.com:8000 weight=5 max_fails=3 fail_timeout=30s;
    server app3.example.com:8000 weight=3 max_fails=3 fail_timeout=30s backup;
}

server {
    listen 80;
    server_name deltalakedb.example.com;

    # Health check endpoint
    location /health {
        proxy_pass http://deltalakedb_backend/health;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    # API endpoints
    location /api/ {
        proxy_pass http://deltalakedb_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # Timeouts
        proxy_connect_timeout 30s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;

        # Enable keep-alive
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }

    # Static files
    location /static/ {
        alias /opt/deltalakedb/static/;
        expires 1d;
        add_header Cache-Control "public, immutable";
    }
}
```

## Backup and Recovery

### 1. Database Backup Strategy

```bash
#!/bin/bash
# scripts/backup_database.sh

set -e

# Configuration
BACKUP_DIR="/backup/database"
RETENTION_DAYS=30
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="deltalake_metadata"
DB_USER="deltalake_app"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Generate backup filename
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/deltalake_backup_$TIMESTAMP.sql"

echo "Starting database backup..."
echo "Backup file: $BACKUP_FILE"

# Create backup
pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    --verbose --clean --if-exists --create \
    --format=custom --compress=9 \
    --file="$BACKUP_FILE"

# Compress backup
gzip "$BACKUP_FILE"
BACKUP_FILE="${BACKUP_FILE}.gz"

# Verify backup
if [[ -f "$BACKUP_FILE" ]]; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "Backup completed successfully!"
    echo "Backup size: $BACKUP_SIZE"
else
    echo "Backup failed!"
    exit 1
fi

# Cleanup old backups
find "$BACKUP_DIR" -name "deltalake_backup_*.sql.gz" -mtime +$RETENTION_DAYS -delete

echo "Old backups cleaned up"

# Upload to cloud storage (optional)
if [[ -n "$CLOUD_STORAGE_BUCKET" ]]; then
    echo "Uploading backup to cloud storage..."
    aws s3 cp "$BACKUP_FILE" "s3://$CLOUD_STORAGE_BUCKET/database-backups/"
    echo "Backup uploaded to cloud storage"
fi

echo "Backup process completed at $(date)"
```

### 2. Automated Backup Script

```python
# scripts/automated_backup.py
import os
import subprocess
import logging
from datetime import datetime, timedelta
from typing import List, Dict

class DatabaseBackupManager:
    """Automated database backup management."""

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def create_backup(self, backup_type: str = 'full') -> Dict:
        """Create database backup."""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"deltalake_{backup_type}_backup_{timestamp}.sql"
        backup_path = os.path.join(
            self.config['backup_directory'],
            backup_filename
        )

        try:
            if backup_type == 'full':
                self._create_full_backup(backup_path)
            elif backup_type == 'incremental':
                self._create_incremental_backup(backup_path)

            # Compress backup
            compressed_path = f"{backup_path}.gz"
            self._compress_backup(backup_path, compressed_path)

            # Get backup info
            backup_info = {
                'filename': backup_filename,
                'path': compressed_path,
                'size_mb': os.path.getsize(compressed_path) / (1024 * 1024),
                'timestamp': timestamp,
                'type': backup_type
            }

            # Upload to cloud if configured
            if self.config.get('cloud_upload', {}).get('enabled', False):
                self._upload_to_cloud(compressed_path, backup_filename)

            # Update backup catalog
            self._update_backup_catalog(backup_info)

            self.logger.info(f"Backup created successfully: {backup_filename}")
            return backup_info

        except Exception as e:
            self.logger.error(f"Backup creation failed: {e}")
            raise

    def _create_full_backup(self, backup_path: str):
        """Create full database backup."""

        db_config = self.config['database']

        cmd = [
            'pg_dump',
            f"--host={db_config['host']}",
            f"--port={db_config['port']}",
            f"--username={db_config['user']}",
            f"--dbname={db_config['name']}",
            '--verbose',
            '--clean',
            '--if-exists',
            '--create',
            '--format=custom',
            '--compress=9',
            f"--file={backup_path}"
        ]

        # Set password environment variable
        env = os.environ.copy()
        env['PGPASSWORD'] = db_config['password']

        subprocess.run(cmd, env=env, check=True)

    def _compress_backup(self, source_path: str, target_path: str):
        """Compress backup file."""

        subprocess.run(['gzip', source_path], check=True)

    def _upload_to_cloud(self, backup_path: str, filename: str):
        """Upload backup to cloud storage."""

        cloud_config = self.config['cloud_upload']

        if cloud_config['provider'] == 'aws':
            import boto3
            s3 = boto3.client('s3')
            s3.upload_file(
                backup_path,
                cloud_config['bucket'],
                f"database-backups/{filename}",
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )

    def schedule_backups(self):
        """Schedule automated backups."""

        import schedule
        import time

        # Daily full backup at 2 AM
        schedule.every().day.at("02:00").do(
            lambda: self.create_backup('full')
        )

        # Hourly incremental backup
        schedule.every().hour.do(
            lambda: self.create_backup('incremental')
        )

        self.logger.info("Backup scheduler started")

        while True:
            schedule.run_pending()
            time.sleep(60)

    def cleanup_old_backups(self):
        """Clean up old backups based on retention policy."""

        backup_dir = self.config['backup_directory']
        retention_days = self.config.get('retention_days', 30)
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        for filename in os.listdir(backup_dir):
            if filename.endswith('.gz'):
                filepath = os.path.join(backup_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))

                if file_time < cutoff_date:
                    os.remove(filepath)
                    self.logger.info(f"Deleted old backup: {filename}")

# Usage example
backup_config = {
    'database': {
        'host': 'localhost',
        'port': 5432,
        'user': 'deltalake_app',
        'password': 'secure_password',
        'name': 'deltalake_metadata'
    },
    'backup_directory': '/backup/database',
    'retention_days': 30,
    'cloud_upload': {
        'enabled': True,
        'provider': 'aws',
        'bucket': 'my-backup-bucket'
    }
}

backup_manager = DatabaseBackupManager(backup_config)

# Create a backup
backup_info = backup_manager.create_backup('full')
print(f"Backup created: {backup_info['filename']} ({backup_info['size_mb']:.1f} MB)")
```

### 3. Point-in-Time Recovery

```python
# scripts/recovery.py
import os
import subprocess
from datetime import datetime
from typing import Optional

class DatabaseRecovery:
    """Database point-in-time recovery."""

    def __init__(self, config):
        self.config = config

    def restore_from_backup(self, backup_file: str, target_time: Optional[datetime] = None):
        """Restore database from backup."""

        if not os.path.exists(backup_file):
            raise FileNotFoundError(f"Backup file not found: {backup_file}")

        try:
            # Stop application
            self._stop_application()

            # Drop existing database
            self._drop_database()

            # Create new database
            self._create_database()

            # Restore from backup
            self._restore_backup(backup_file)

            # Apply transaction logs if point-in-time recovery
            if target_time:
                self._apply_transaction_logs(target_time)

            # Verify restore
            self._verify_restore()

            # Start application
            self._start_application()

            print(f"Database restored successfully from {backup_file}")

        except Exception as e:
            print(f"Restore failed: {e}")
            # Attempt to rollback
            self._rollback_restore()
            raise

    def _restore_backup(self, backup_file: str):
        """Restore backup to database."""

        db_config = self.config['database']

        # Decompress if needed
        if backup_file.endswith('.gz'):
            decompressed_file = backup_file[:-3]
            subprocess.run(['gunzip', '-c', backup_file],
                          stdout=open(decompressed_file, 'wb'), check=True)
            backup_file = decompressed_file

        # Restore using pg_restore
        cmd = [
            'pg_restore',
            f"--host={db_config['host']}",
            f"--port={db_config['port']}",
            f"--username={db_config['user']}",
            f"--dbname={db_config['name']}",
            '--verbose',
            '--clean',
            '--if-exists',
            backup_file
        ]

        env = os.environ.copy()
        env['PGPASSWORD'] = db_config['password']

        subprocess.run(cmd, env=env, check=True)

    def list_available_backups(self) -> List[Dict]:
        """List available backups for recovery."""

        backup_dir = self.config['backup_directory']
        backups = []

        for filename in os.listdir(backup_dir):
            if filename.endswith('.gz'):
                filepath = os.path.join(backup_dir, filename)
                stat = os.stat(filepath)

                backups.append({
                    'filename': filename,
                    'path': filepath,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'created_at': datetime.fromtimestamp(stat.st_ctime)
                })

        # Sort by creation time (newest first)
        backups.sort(key=lambda x: x['created_at'], reverse=True)

        return backups

# Usage example
recovery = DatabaseRecovery(backup_config)

# List available backups
backups = recovery.list_available_backups()
print("Available backups:")
for backup in backups[:5]:  # Show latest 5
    print(f"  {backup['filename']} - {backup['created_at']} ({backup['size_mb']:.1f} MB)")

# Restore from latest backup
if backups:
    latest_backup = backups[0]['path']
    recovery.restore_from_backup(latest_backup)
```

## Monitoring and Alerting

### Temporary External Inconsistency Window

Multi-table commits land atomically in the SQL catalog, but `_delta_log/` mirroring fan-outs per table.
External engines that read directly from object storage can therefore observe divergent table versions
until all mirror workers finish. Instrument and document this window clearly for operators:

- Budget no more than 60 seconds for the window; target p95 completion under 5 seconds.
- Treat `IN_FLIGHT` entries that are older than the SLO as lag violations and page the on-call.
- Communicate the expected mirror completion time in runbooks so downstream teams know when it is
safe to rely on `_delta_log/` for multi-table atomicity.

### Mirror Lag, Status, and Parity Metrics

Each `(table_id, version)` transition writes to the `dl_mirror_status` table and emits the following metrics:

- `mirror_status{table_id,version}`: 0=`IN_FLIGHT`, 1=`COMPLETE`, 2=`FAILED`.
- `mirror_lag_seconds{table_id,version}`: difference between DB commit time and last successful mirror write.
- `mirror_reconciliation_attempts_total{table_id,version}`: counter incremented on each retry.
- `mirror_parity_drift_total{table_id,version}`: counter for JSON/Checkpoint mismatches detected post-commit.

`dl diff` now emits machine-readable metrics for every table you validate. Schedule it via
cron, CI, or orchestration systems to produce Prometheus/JSON payloads:

```bash
dl diff \
  --database /var/lib/deltalake/metadata.sqlite \
  --table-id <uuid> \
  --log-dir /tables/sales/_delta_log \
  --lag-threshold-seconds 5 \
  --max-drift-files 0 \
  --metrics-format prometheus \
  --metrics-output /var/lib/deltalake/metrics/sales.prom
```

Sample output:

```
dl_mirror_lag_seconds{table_id="b1b..."} 0.8
dl_mirror_status{table_id="b1b...",version="128"} 1
dl_mirror_reconciliation_attempts_total{table_id="b1b...",version="128"} 0
dl_file_drift_count{table_id="b1b..."} 0
dl_metadata_drift{table_id="b1b..."} 0
dl_protocol_drift{table_id="b1b..."} 0
dl_version_delta{table_id="b1b..."} 0
```

Ship these metrics through Prometheus `file_sd`, StatsD, or your preferred agent so
dashboards can chart lag, parity, and version deltas per table.

### Alert Thresholds and Exit Codes

| Metric/Signal | Threshold | Response |
|---------------|-----------|----------|
| `mirror_lag_seconds` | > 5 seconds for longer than 1 minute | Page on-call, inspect mirror workers |
| `file_drift_count` | > 0 | Stop cutovers, investigate parity diff |
| `metadata_drift` or `protocol_drift` | == 1 | Treat as correctness incident |
| CLI exit code | `2` (drift) / `3` (lag only) | Fail the pipeline or raise an alert |

### CI/CD Integration

Add a "Parity" stage to your release pipelines:

```yaml
steps:
  - name: Parity Check
    run: |
      dl diff \
        --database /var/lib/deltalake/metadata.sqlite \
        --table-id $TABLE_ID \
        --log-dir $DELTA_LOG \
        --format json \
        --json-output artifacts/diff.json
```

Archive the JSON artifact so incident responders can review the precise drift details.

This comprehensive deployment and operations guide provides SQL-Backed Delta Lake users with everything needed for production deployments, including database setup, application deployment, high availability configurations, backup strategies, and operational procedures. The guide includes practical scripts, configuration examples, and best practices for maintaining a robust and scalable Delta Lake deployment.
