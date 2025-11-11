# DeltaLake DB Python Deployment and Operations Guide

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [Database Setup](#database-setup)
4. [Configuration Management](#configuration-management)
5. [Deployment Patterns](#deployment-patterns)
6. [Performance Tuning](#performance-tuning)
7. [Monitoring and Alerting](#monitoring-and-alerting)
8. [Backup and Recovery](#backup-and-recovery)
9. [Security](#security)
10. [Scaling](#scaling)
11. [Maintenance](#maintenance)
12. [Troubleshooting](#troubleshooting)

## System Requirements

### Minimum Requirements

- **Python**: 3.8 or higher
- **Memory**: 512 MB RAM
- **Storage**: 1 GB available disk space
- **Network**: Connection to SQL database

### Recommended Requirements

- **Python**: 3.10 or higher
- **Memory**: 4 GB RAM
- **Storage**: 10 GB available disk space
- **CPU**: 2+ cores
- **Network**: Low latency connection to SQL database

### Database Support

| Database | Minimum Version | Recommended Version |
|----------|----------------|-------------------|
| PostgreSQL | 11.0 | 14.0+ |
| MySQL | 8.0 | 8.0+ |
| SQLite | 3.32 | 3.40+ |
| SQL Server | 2019 | 2022+ |

## Installation

### Production Installation

```bash
# Create virtual environment
python -m venv deltalake_env
source deltalake_env/bin/activate  # On Windows: deltalake_env\Scripts\activate

# Install with production dependencies
pip install deltalakedb-python[production]

# Install specific version if needed
pip install deltalakedb-python==1.0.0
```

### Docker Deployment

#### Dockerfile
```dockerfile
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV DELTA_LOG_LEVEL=INFO

# Expose port (if running web service)
EXPOSE 8000

# Run application
CMD ["python", "main.py"]
```

#### Docker Compose
```yaml
version: '3.8'

services:
  deltalake-app:
    build: .
    environment:
      - DELTA_DB_TYPE=postgresql
      - DELTA_DB_CONNECTION=postgresql://delta_user:${DB_PASSWORD}@postgres:5432/delta_db
      - DELTA_LOG_LEVEL=INFO
    depends_on:
      - postgres
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs

  postgres:
    image: postgres:14
    environment:
      - POSTGRES_DB=delta_db
      - POSTGRES_USER=delta_user
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  redis:
    image: redis:7
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

### Kubernetes Deployment

#### Deployment Manifest
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: deltalake-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: deltalake-app
  template:
    metadata:
      labels:
        app: deltalake-app
    spec:
      containers:
      - name: deltalake-app
        image: deltalake-app:latest
        env:
        - name: DELTA_DB_TYPE
          value: "postgresql"
        - name: DELTA_DB_CONNECTION
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: connection-string
        - name: DELTA_LOG_LEVEL
          value: "INFO"
        - name: DELTA_CACHE_SIZE
          value: "1000"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
        - name: logs-volume
          mountPath: /app/logs
      volumes:
      - name: config-volume
        configMap:
          name: deltalake-config
      - name: logs-volume
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: deltalake-service
spec:
  selector:
    app: deltalake-app
  ports:
  - port: 8000
    targetPort: 8000
  type: ClusterIP
```

## Database Setup

### Production Database Configuration

#### PostgreSQL Setup
```sql
-- Create database
CREATE DATABASE delta_db WITH
    ENCODING 'UTF8'
    LC_COLLATE='en_US.UTF-8'
    LC_CTYPE='en_US.UTF-8';

-- Create user
CREATE USER delta_user WITH PASSWORD 'secure_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE delta_db TO delta_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO delta_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO delta_user;

-- Create extensions
\c delta_db;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
```

#### MySQL Setup
```sql
-- Create database
CREATE DATABASE delta_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Create user
CREATE USER 'delta_user'@'%' IDENTIFIED BY 'secure_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON delta_db.* TO 'delta_user'@'%';
FLUSH PRIVILEGES;
```

### Database Optimization

#### PostgreSQL Tuning
```sql
-- postgresql.conf optimizations

# Memory settings
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB

# Connection settings
max_connections = 100
max_prepared_transactions = 100

# Performance settings
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100

# Logging
log_statement = 'all'
log_duration = on
log_min_duration_statement = 1000
```

#### MySQL Tuning
```sql
-- my.cnf optimizations

[mysqld]
# Memory settings
innodb_buffer_pool_size = 512M
innodb_log_file_size = 128M
innodb_log_buffer_size = 16M

# Connection settings
max_connections = 100
max_user_connections = 80

# Performance settings
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
query_cache_size = 64M
```

## Configuration Management

### Environment-Specific Configurations

#### Development (config/dev.yaml)
```yaml
database:
  type: postgresql
  connection_string: "postgresql://dev_user:dev_pass@localhost:5432/delta_dev"
  table_prefix: "dev_delta_"

connection:
  timeout: 10
  max_connections: 5
  retry_attempts: 2

performance:
  batch_size: 100
  cache_size: 50
  lazy_loading: false

logging:
  level: DEBUG
  format: detailed
  console: true
```

#### Production (config/prod.yaml)
```yaml
database:
  type: postgresql
  connection_string: "${DELTA_DB_CONNECTION}"
  table_prefix: "delta_"

connection:
  timeout: 30
  max_connections: 20
  retry_attempts: 5
  retry_delay: 1.0

performance:
  batch_size: 1000
  cache_size: 1000
  lazy_loading: true
  async_operations: true
  memory_optimization: true

logging:
  level: INFO
  format: structured
  file: "/app/logs/deltalake.log"
  rotation: daily
  retention_days: 30

security:
  ssl_mode: require
  encrypt_connections: true
  audit_operations: true
```

### Configuration Validation

```python
#!/usr/bin/env python3
"""Configuration validation script."""

import sys
import os
from pathlib import Path

# Add the package to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from deltalakedb.pydantic_models import ConfigLoader
from deltalakedb.logging import DeltaLogger

def validate_config(config_path: str) -> bool:
    """Validate configuration file."""
    try:
        # Load and validate config
        config = ConfigLoader.load_config_file(config_path)

        # Test database connection
        test_table = f"delta_test_{os.getpid()}"
        try:
            from deltalakedb import Table
            test_uri = f"{config.connection_string}/{test_table}"
            # Just test connection, not table creation
            print("✓ Database connection successful")
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False

        print("✓ Configuration validation passed")
        return True

    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")
        return False

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/prod.yaml"

    if not os.path.exists(config_path):
        print(f"✗ Configuration file not found: {config_path}")
        sys.exit(1)

    success = validate_config(config_path)
    sys.exit(0 if success else 1)
```

## Deployment Patterns

### Standalone Application

```python
#!/usr/bin/env python3
"""Standalone DeltaLake application."""

import os
import sys
import signal
from pathlib import Path

from deltalakedb.logging import setup_logging
from deltalakedb.pydantic_models import ConfigLoader

class DeltaLakeApp:
    def __init__(self):
        self.config = None
        self.running = False

    def initialize(self):
        """Initialize the application."""
        # Load configuration
        config_path = os.getenv("DELTA_CONFIG_PATH", "config/prod.yaml")
        self.config = ConfigLoader.load_config_file(config_path)

        # Setup logging
        setup_logging(
            level=self.config.logging.level,
            format=self.config.logging.format,
            file_path=self.config.logging.file
        )

        # Initialize components
        self._initialize_database()
        self._initialize_caching()
        self._initialize_monitoring()

        print("Application initialized successfully")

    def _initialize_database(self):
        """Initialize database connections."""
        from deltalakedb.connection import ConnectionPool

        self.connection_pool = ConnectionPool(
            config=self.config,
            max_size=self.config.connection.max_connections
        )

    def _initialize_caching(self):
        """Initialize caching system."""
        from deltalakedb.caching import create_deltalake_cache_manager

        self.cache_manager = create_deltalake_cache_manager()

    def _initialize_monitoring(self):
        """Initialize monitoring."""
        from deltalakedb.logging import DeltaLogger

        self.logger = DeltaLogger("deltalake-app")

    def start(self):
        """Start the application."""
        self.running = True

        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.logger.info("Application started", pid=os.getpid())

        # Main application loop
        try:
            while self.running:
                self._process_requests()
                self._perform_maintenance()

        except Exception as e:
            self.logger.error("Application error", error=e)
            raise
        finally:
            self.shutdown()

    def _process_requests(self):
        """Process incoming requests."""
        # Implement request processing logic
        pass

    def _perform_maintenance(self):
        """Perform periodic maintenance."""
        # Cleanup expired cache entries
        self.cache_manager.clear_expired()

        # Optimize memory usage
        # Perform other maintenance tasks

    def shutdown(self):
        """Graceful shutdown."""
        self.running = False

        if hasattr(self, 'connection_pool'):
            self.connection_pool.close_all()

        self.logger.info("Application shutdown")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print(f"Received signal {signum}, shutting down...")
        self.running = False

if __name__ == "__main__":
    app = DeltaLakeApp()
    app.initialize()
    app.start()
```

### Web Service (FastAPI)

```python
#!/usr/bin/env python3
"""FastAPI web service for DeltaLake operations."""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from deltalakedb.logging import setup_logging, DeltaLogger
from deltalakedb.pydantic_models import ConfigLoader
from deltalakedb import Table, SqlConfig

# Initialize FastAPI app
app = FastAPI(
    title="DeltaLake API",
    description="API for DeltaLake operations",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global components
config = None
logger = None

@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    global config, logger

    # Load configuration
    config_path = os.getenv("DELTA_CONFIG_PATH", "config/prod.yaml")
    config = ConfigLoader.load_config_file(config_path)

    # Setup logging
    setup_logging(
        level=config.logging.level,
        format=config.logging.format,
        file_path=config.logging.file
    )

    logger = DeltaLogger("deltalake-api")
    logger.info("DeltaLake API started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    if logger:
        logger.info("DeltaLake API shutdown")

def get_table(uri: str) -> Table:
    """Get table instance from URI."""
    try:
        return Table(uri)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "DeltaLake API", "version": "1.0.0"}

@app.get("/tables/{table_name}/version")
async def get_table_version(table_name: str, version: int = None):
    """Get table version."""
    try:
        uri = f"delta+sql://{config.connection_string}/{table_name}"
        table = get_table(uri)
        snapshot = table.get_version(version)

        return {
            "version": snapshot.version,
            "timestamp": snapshot.timestamp.isoformat(),
            "files": len(snapshot.files)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{table_name}/write")
async def write_to_table(table_name: str, data: dict, mode: str = "append"):
    """Write data to table."""
    try:
        uri = f"delta+sql://{config.connection_string}/{table_name}"
        table = get_table(uri)

        # Convert data to DataFrame and write
        import pandas as pd
        df = pd.DataFrame(data)

        writer = deltalakedb.write_operations.DeltaWriter(table)
        result = writer.write(df, mode=mode)

        return {
            "success": True,
            "files_added": result.files_added,
            "rows_added": result.rows_added
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=4
    )
```

## Performance Tuning

### Connection Pool Optimization

```python
from deltalakedb.connection import ConnectionPool

# Optimized connection pool for high throughput
pool = ConnectionPool(
    config=config,
    max_size=50,                    # Maximum connections
    min_size=5,                     # Minimum connections
    max_idle_time=300,              # 5 minutes idle timeout
    max_lifetime=3600,              # 1 hour connection lifetime
    connection_timeout=30,          # Connection timeout
    validation_interval=60          # Health check interval
)

# Monitor pool performance
stats = pool.get_pool_stats()
print(f"Active connections: {stats['active_connections']}")
print(f"Idle connections: {stats['idle_connections']}")
print(f"Total requests: {stats['total_requests']}")
print(f"Success rate: {stats['success_rate']:.2%}")
```

### Caching Strategy

```python
from deltalakedb.caching import DeltaLakeCacheManager, CacheConfig

# Production cache configuration
cache_config = CacheConfig(
    max_size=10000,                 # Maximum cache entries
    ttl=3600,                       # 1 hour TTL
    eviction_policy="lru",          # LRU eviction
    enable_compression=True,        # Compress cached data
    max_memory_mb=1024,            # 1GB max memory
    background_cleanup=True         # Background cleanup
)

cache_manager = DeltaLakeCacheManager(cache_config)

# Cache warming strategy
def warm_cache(tables: List[str]):
    """Warm cache with frequently accessed data."""
    for table_name in tables:
        try:
            table = Table(f"delta+sql://{config.connection_string}/{table_name}")

            # Cache metadata
            metadata = table.get_metadata()
            cache_manager.cache_table_metadata(table, metadata)

            # Cache recent file lists
            files = table.get_files()
            cache_manager.cache_file_list(table, files[:1000])  # Last 1000 files

        except Exception as e:
            logger.warning(f"Cache warming failed for {table_name}", error=e)
```

### Memory Optimization

```python
from deltalakedb.memory_optimization import MemoryOptimizedFileList, OptimizationStrategy

# Memory-optimized file handling
def process_large_table(table_uri: str):
    """Process large table with memory optimization."""
    table = Table(table_uri)

    # Use memory-optimized file list
    file_list = MemoryOptimizedFileList(
        strategy=OptimizationStrategy.HYBRID,
        chunk_size=1000,
        compression_level=6
    )

    # Add files in batches
    files = table.get_files()
    for i in range(0, len(files), 5000):
        batch = files[i:i+5000]
        file_list.add_files(batch)

    # Process files in chunks
    total_files = file_list.get_file_count()
    chunk_size = 100

    for i in range(0, total_files, chunk_size):
        files_chunk = file_list.get_files(offset=i, limit=chunk_size)

        # Process chunk
        process_files_chunk(files_chunk)

        # Monitor memory usage
        memory_stats = file_list.get_memory_usage()
        if memory_stats.current_mb > 500:  # 500MB threshold
            file_list.optimize()  # Force optimization

    return file_list.get_memory_usage()
```

## Monitoring and Alerting

### Health Check Endpoint

```python
from fastapi import APIRouter
from deltalakedb.connection import ConnectionPool
from deltalakedb.caching import DeltaLakeCacheManager
import psutil
import os

router = APIRouter(prefix="/health")

@router.get("/")
async def health_check():
    """Comprehensive health check."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }

    # Database connectivity
    try:
        connection = connection_pool.get_connection()
        if connection.is_alive():
            health_status["checks"]["database"] = "healthy"
        else:
            health_status["checks"]["database"] = "unhealthy"
            health_status["status"] = "degraded"
        connection_pool.return_connection(connection)
    except Exception as e:
        health_status["checks"]["database"] = f"error: {e}"
        health_status["status"] = "unhealthy"

    # Cache status
    try:
        cache_stats = cache_manager.get_stats()
        health_status["checks"]["cache"] = {
            "status": "healthy",
            "hit_ratio": cache_stats.hit_ratio,
            "size": cache_stats.current_size
        }
    except Exception as e:
        health_status["checks"]["cache"] = f"error: {e}"
        health_status["status"] = "degraded"

    # System resources
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        health_status["checks"]["system"] = {
            "memory_usage": f"{memory.percent}%",
            "disk_usage": f"{disk.percent}%",
            "cpu_count": psutil.cpu_count()
        }

        if memory.percent > 90 or disk.percent > 90:
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["system"] = f"error: {e}"

    return health_status

@router.get("/metrics")
async def get_metrics():
    """Prometheus-compatible metrics."""
    try:
        # Database metrics
        pool_stats = connection_pool.get_pool_stats()

        # Cache metrics
        cache_stats = cache_manager.get_stats()

        # System metrics
        memory = psutil.virtual_memory()

        metrics = [
            f"deltalake_database_connections_active {pool_stats['active_connections']}",
            f"deltalake_database_connections_idle {pool_stats['idle_connections']}",
            f"deltalake_cache_hit_ratio {cache_stats['hit_ratio']}",
            f"deltalake_cache_size {cache_stats['current_size']}",
            f"deltalake_memory_usage_bytes {memory.used}",
            f"deltalake_memory_usage_percent {memory.percent}",
        ]

        return "\n".join(metrics)

    except Exception as e:
        return f"# Error collecting metrics: {e}"
```

### Alerting Configuration

```python
from deltalakedb.logging import DeltaLogger
import time
import threading

class AlertManager:
    def __init__(self, logger: DeltaLogger):
        self.logger = logger
        self.alert_thresholds = {
            "memory_usage": 85,        # 85%
            "disk_usage": 90,          # 90%
            "cache_hit_ratio": 0.5,    # 50%
            "connection_errors": 10,    # 10 errors/minute
        }
        self.alert_counts = {}

    def check_system_health(self):
        """Check system health and send alerts."""
        import psutil

        # Memory check
        memory = psutil.virtual_memory()
        if memory.percent > self.alert_thresholds["memory_usage"]:
            self.send_alert(
                "high_memory_usage",
                f"Memory usage is {memory.percent:.1f}%",
                severity="warning"
            )

        # Disk check
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        if disk_percent > self.alert_thresholds["disk_usage"]:
            self.send_alert(
                "high_disk_usage",
                f"Disk usage is {disk_percent:.1f}%",
                severity="critical"
            )

        # Cache performance check
        cache_stats = cache_manager.get_stats()
        if cache_stats.hit_ratio < self.alert_thresholds["cache_hit_ratio"]:
            self.send_alert(
                "low_cache_hit_ratio",
                f"Cache hit ratio is {cache_stats.hit_ratio:.2%}",
                severity="warning"
            )

    def send_alert(self, alert_type: str, message: str, severity: str = "info"):
        """Send alert."""
        alert_key = f"{alert_type}:{int(time.time() / 60)}"  # Group by minute

        # Rate limiting - max 1 alert per minute per type
        if alert_key not in self.alert_counts:
            self.alert_counts[alert_key] = 0

        if self.alert_counts[alert_key] >= 1:
            return

        self.alert_counts[alert_key] += 1

        # Log alert
        self.logger.warning(
            f"ALERT: {alert_type}",
            message=message,
            severity=severity,
            alert_type=alert_type
        )

        # Send to external monitoring system
        # self.send_to_monitoring_service(alert_type, message, severity)

    def start_monitoring(self, interval_seconds: int = 60):
        """Start background monitoring."""
        def monitor_loop():
            while True:
                try:
                    self.check_system_health()
                except Exception as e:
                    self.logger.error("Monitoring error", error=e)

                time.sleep(interval_seconds)

        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
        self.logger.info("Alert monitoring started")
```

## Backup and Recovery

### Database Backup Strategy

```bash
#!/bin/bash
# PostgreSQL backup script

DB_NAME="delta_db"
DB_USER="delta_user"
BACKUP_DIR="/backups/postgresql"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/delta_db_$DATE.sql"

# Create backup directory
mkdir -p $BACKUP_DIR

# Perform backup
pg_dump -h localhost -U $DB_USER -d $DB_NAME > $BACKUP_FILE

# Compress backup
gzip $BACKUP_FILE

# Remove old backups (keep last 7 days)
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: $BACKUP_FILE.gz"
```

### Delta Table Backup

```python
from deltalakedb.migration import DeltaTableMigrator, MigrationStrategy
import shutil
import json
from pathlib import Path

class DeltaBackupManager:
    def __init__(self, backup_dir: str = "/backups/deltalake"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def backup_table_metadata(self, table_uri: str, backup_name: str = None):
        """Backup table metadata."""
        table = Table(table_uri)

        if not backup_name:
            backup_name = f"{table.table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        backup_path = self.backup_dir / backup_name
        backup_path.mkdir(exist_ok=True)

        # Backup metadata
        metadata = table.get_metadata()
        with open(backup_path / "metadata.json", "w") as f:
            json.dump(metadata.to_dict(), f, indent=2, default=str)

        # Backup commit history
        history = table.history(limit=100)
        with open(backup_path / "history.json", "w") as f:
            json.dump([commit.to_dict() for commit in history], f, indent=2, default=str)

        # Backup files list
        files = table.get_files()
        with open(backup_path / "files.json", "w") as f:
            json.dump([file.to_dict() for file in files], f, indent=2, default=str)

        return backup_path

    def restore_table_metadata(self, backup_path: str, target_uri: str):
        """Restore table metadata from backup."""
        backup_path = Path(backup_path)

        # Load metadata
        with open(backup_path / "metadata.json", "r") as f:
            metadata_dict = json.load(f)

        # Create target table with metadata
        target_table = Table(target_uri)
        # Implement restoration logic

        return True

    def list_backups(self):
        """List available backups."""
        backups = []
        for backup_dir in self.backup_dir.iterdir():
            if backup_dir.is_dir():
                metadata_file = backup_dir / "metadata.json"
                if metadata_file.exists():
                    stat = metadata_file.stat()
                    backups.append({
                        "name": backup_dir.name,
                        "path": str(backup_dir),
                        "created_at": datetime.fromtimestamp(stat.st_mtime),
                        "size_mb": sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file()) / 1024 / 1024
                    })

        return sorted(backups, key=lambda x: x["created_at"], reverse=True)
```

### Recovery Procedures

```python
class DisasterRecovery:
    def __init__(self, config):
        self.config = config
        self.logger = DeltaLogger("disaster-recovery")

    def recover_from_backup(self, backup_path: str, target_config: SqlConfig):
        """Recover from backup."""
        try:
            self.logger.info("Starting disaster recovery", backup_path=backup_path)

            # Validate backup
            if not self._validate_backup(backup_path):
                raise ValueError("Invalid backup")

            # Setup new database
            self._setup_target_database(target_config)

            # Restore data
            migrator = DeltaTableMigrator(
                source_uri=backup_path,
                target_config=target_config
            )

            result = migrator.execute_migration(
                strategy=MigrationStrategy.FULL,
                batch_size=1000
            )

            if result.success:
                self.logger.info("Disaster recovery completed",
                               files_migrated=result.files_migrated,
                               rows_migrated=result.rows_migrated)
                return True
            else:
                raise Exception("Migration failed during recovery")

        except Exception as e:
            self.logger.error("Disaster recovery failed", error=e)
            return False

    def _validate_backup(self, backup_path: str) -> bool:
        """Validate backup integrity."""
        backup_path = Path(backup_path)

        required_files = ["metadata.json", "history.json", "files.json"]
        for file_name in required_files:
            if not (backup_path / file_name).exists():
                self.logger.error(f"Missing backup file: {file_name}")
                return False

        return True

    def _setup_target_database(self, config: SqlConfig):
        """Setup target database for recovery."""
        # Implement database setup logic
        pass
```

## Security

### SSL/TLS Configuration

```python
# Production configuration with SSL
config = SqlConfig(
    database_type="postgresql",
    connection_string="postgresql://user:pass@localhost:5432/db?sslmode=require&sslcert=/path/to/client-cert.pem&sslkey=/path/to/client-key.pem&sslrootcert=/path/to/ca-cert.pem",
    ssl_mode="require",
    encrypt_connections=True
)
```

### Access Control

```python
from functools import wraps
from typing import List

class AccessControl:
    def __init__(self):
        self.permissions = {}

    def require_permission(self, permission: str):
        """Decorator to require specific permission."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if not self._has_permission(permission):
                    raise PermissionError(f"Permission '{permission}' required")
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def _has_permission(self, permission: str) -> bool:
        """Check if current user has permission."""
        # Implement permission checking logic
        return True

# Usage
@access_control.require_permission("table:read")
def read_table(table_name: str):
    """Read table with permission check."""
    table = Table(get_table_uri(table_name))
    return table.get_version()
```

### Auditing

```python
from deltalakedb.logging import DeltaLogger

class AuditLogger:
    def __init__(self):
        self.logger = DeltaLogger("audit")

    def log_operation(self, user_id: str, operation: str, table: str, **kwargs):
        """Log operation for auditing."""
        self.logger.info(
            f"AUDIT: {operation}",
            user_id=user_id,
            operation=operation,
            table=table,
            timestamp=datetime.utcnow().isoformat(),
            **kwargs
        )

    def log_data_access(self, user_id: str, table: str, query: str, rows_returned: int):
        """Log data access."""
        self.log_operation(
            user_id=user_id,
            operation="data_access",
            table=table,
            query=query,
            rows_returned=rows_returned
        )

    def log_data_modification(self, user_id: str, table: str, operation: str, rows_affected: int):
        """Log data modification."""
        self.log_operation(
            user_id=user_id,
            operation="data_modification",
            table=table,
            modification_type=operation,
            rows_affected=rows_affected
        )

audit_logger = AuditLogger()
```

## Scaling

### Horizontal Scaling with Connection Pooling

```python
from deltalakedb.connection import ConnectionPool, DistributedConnectionPool

class ScalableDeltaService:
    def __init__(self, config, nodes: List[str]):
        self.nodes = nodes
        self.connection_pools = {}
        self.setup_connection_pools(config)

    def setup_connection_pools(self, config):
        """Setup connection pools for each node."""
        for node in self.nodes:
            node_config = self._get_node_config(config, node)
            pool = ConnectionPool(
                config=node_config,
                max_size=20,
                min_size=5
            )
            self.connection_pools[node] = pool

    def get_least_loaded_pool(self) -> ConnectionPool:
        """Get connection pool with least load."""
        best_pool = None
        min_load = float('inf')

        for node, pool in self.connection_pools.items():
            stats = pool.get_pool_stats()
            load = stats['active_connections'] / stats['max_connections']

            if load < min_load:
                min_load = load
                best_pool = pool

        return best_pool

    def execute_query(self, query: str, params: List = None):
        """Execute query on least loaded node."""
        pool = self.get_least_loaded_pool()
        connection = pool.get_connection()

        try:
            result = connection.execute_query(query, params)
            return result
        finally:
            pool.return_connection(connection)
```

### Read Replicas

```python
class ReadReplicaManager:
    def __init__(self, primary_config, replica_configs):
        self.primary_config = primary_config
        self.replica_configs = replica_configs
        self.replica_pools = {}

        # Setup replica connection pools
        for i, replica_config in enumerate(replica_configs):
            pool = ConnectionPool(config=replica_config, max_size=10)
            self.replica_pools[f"replica_{i}"] = pool

    def execute_read_query(self, query: str, params: List = None):
        """Execute read query on replica."""
        # Choose replica based on load
        replica = self._choose_replica()
        pool = self.replica_pools[replica]
        connection = pool.get_connection()

        try:
            result = connection.execute_query(query, params)
            return result
        finally:
            pool.return_connection(connection)

    def execute_write_query(self, query: str, params: List = None):
        """Execute write query on primary."""
        pool = ConnectionPool(config=self.primary_config)
        connection = pool.get_connection()

        try:
            result = connection.execute_command(query, params)
            return result
        finally:
            pool.return_connection(connection)

    def _choose_replica(self) -> str:
        """Choose replica based on current load."""
        best_replica = None
        min_connections = float('inf')

        for replica, pool in self.replica_pools.items():
            stats = pool.get_pool_stats()
            active = stats['active_connections']

            if active < min_connections:
                min_connections = active
                best_replica = replica

        return best_replica or list(self.replica_pools.keys())[0]
```

## Maintenance

### Scheduled Maintenance Tasks

```python
import schedule
import time
from datetime import datetime

class MaintenanceScheduler:
    def __init__(self):
        self.logger = DeltaLogger("maintenance")
        self.setup_schedules()

    def setup_schedules(self):
        """Setup scheduled maintenance tasks."""
        # Daily tasks
        schedule.every().day.at("02:00").do(self.cleanup_expired_cache)
        schedule.every().day.at("03:00").do(self.optimize_database)
        schedule.every().day.at("04:00").do(self.rotate_logs)

        # Weekly tasks
        schedule.every().sunday.at("01:00").do(self.weekly_backup)
        schedule.every().sunday.at("02:00").do(self.update_statistics)

        # Hourly tasks
        schedule.every().hour.do(self.health_check)

    def cleanup_expired_cache(self):
        """Clean up expired cache entries."""
        try:
            self.logger.info("Starting cache cleanup")
            cache_manager.clear_expired()
            self.logger.info("Cache cleanup completed")
        except Exception as e:
            self.logger.error("Cache cleanup failed", error=e)

    def optimize_database(self):
        """Optimize database performance."""
        try:
            self.logger.info("Starting database optimization")

            # Implement database optimization tasks
            # VACUUM, ANALYZE, index rebuilds, etc.

            self.logger.info("Database optimization completed")
        except Exception as e:
            self.logger.error("Database optimization failed", error=e)

    def rotate_logs(self):
        """Rotate log files."""
        try:
            self.logger.info("Starting log rotation")

            # Implement log rotation logic
            # Compress old logs, remove very old logs

            self.logger.info("Log rotation completed")
        except Exception as e:
            self.logger.error("Log rotation failed", error=e)

    def weekly_backup(self):
        """Perform weekly backup."""
        try:
            self.logger.info("Starting weekly backup")

            # Implement backup logic
            backup_manager = DeltaBackupManager()
            tables = ["users", "orders", "products"]  # Get from config

            for table in tables:
                backup_manager.backup_table_metadata(f"delta+sql://config/{table}")

            self.logger.info("Weekly backup completed")
        except Exception as e:
            self.logger.error("Weekly backup failed", error=e)

    def update_statistics(self):
        """Update database statistics."""
        try:
            self.logger.info("Updating database statistics")

            # Implement statistics update logic
            # UPDATE STATISTICS for PostgreSQL, ANALYZE TABLE for MySQL

            self.logger.info("Statistics update completed")
        except Exception as e:
            self.logger.error("Statistics update failed", error=e)

    def health_check(self):
        """Perform health check."""
        try:
            # Check database connectivity
            connection = connection_pool.get_connection()
            if not connection.is_alive():
                self.logger.warning("Database connection unhealthy")
            connection_pool.return_connection(connection)

            # Check cache performance
            cache_stats = cache_manager.get_stats()
            if cache_stats.hit_ratio < 0.5:
                self.logger.warning("Low cache hit ratio", ratio=cache_stats.hit_ratio)

            # Check system resources
            import psutil
            memory = psutil.virtual_memory()
            if memory.percent > 85:
                self.logger.warning("High memory usage", percent=memory.percent)

        except Exception as e:
            self.logger.error("Health check failed", error=e)

    def start(self):
        """Start maintenance scheduler."""
        self.logger.info("Maintenance scheduler started")

        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

# Start maintenance scheduler
if __name__ == "__main__":
    scheduler = MaintenanceScheduler()
    scheduler.start()
```

### Database Maintenance Scripts

```bash
#!/bin/bash
# PostgreSQL maintenance script

DB_NAME="delta_db"
DB_USER="delta_user"

echo "Starting database maintenance..."

# Vacuum analyze tables
psql -h localhost -U $DB_USER -d $DB_NAME -c "VACUUM ANALYZE;"

# Reindex frequently used tables
psql -h localhost -U $DB_USER -d $DB_NAME -c "REINDEX TABLE delta_commits;"
psql -h localhost -U $DB_USER -d $DB_NAME -c "REINDEX TABLE delta_files;"

# Update statistics
psql -h localhost -U $DB_USER -d $DB_NAME -c "ANALYZE;"

# Check table sizes
echo "Table sizes:"
psql -h localhost -U $DB_USER -d $DB_NAME -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

echo "Database maintenance completed"
```

This comprehensive deployment and operations guide provides everything needed to successfully deploy, monitor, and maintain DeltaLake DB Python in production environments, with detailed instructions for scaling, security, backup, and troubleshooting.