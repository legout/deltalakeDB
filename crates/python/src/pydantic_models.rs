//! Pydantic models for configuration and validation
//!
//! This module provides Pydantic models for configuration validation, API input validation,
//! and ensuring type safety across the Python interface.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Helper function to check if an object has an attribute
fn hasattr(obj: &PyAny, attr: &str) -> PyResult<bool> {
    Ok(obj.hasattr(attr)?)
}

/// Import pydantic functionality - this will be available in Python
const PYDANTIC_CODE: &str = r#"
from typing import Optional, List, Dict, Any, Union, Literal
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, validator, root_validator
import re

class DatabaseConfig(BaseModel):
    """Pydantic model for database configuration"""

    url: str = Field(..., description="Database connection URL")
    pool_size: int = Field(default=10, ge=1, le=1000, description="Connection pool size")
    timeout: int = Field(default=30, ge=1, le=3600, description="Connection timeout in seconds")
    ssl_enabled: bool = Field(default=False, description="Enable SSL for PostgreSQL")
    read_only: bool = Field(default=False, description="Read-only mode")
    max_connections: Optional[int] = Field(None, ge=1, le=1000)
    min_connections: Optional[int] = Field(None, ge=1, le=1000)
    connection_timeout: Optional[int] = Field(None, ge=1, le=3600)
    idle_timeout: Optional[int] = Field(None, ge=1, le=3600)
    max_lifetime: Optional[int] = Field(None, ge=1, le=3600)
    health_check_interval: Optional[int] = Field(None, ge=1, le=3600)
    retry_attempts: Optional[int] = Field(None, ge=0, le=10)
    retry_delay: Optional[int] = Field(None, ge=0, le=60000)

    @validator('url')
    def validate_url(cls, v):
        if not v:
            raise ValueError('URL cannot be empty')

        # Basic URL validation for supported databases
        if not any(v.startswith(prefix) for prefix in [
            'postgresql://', 'postgres://',
            'sqlite://', 'duckdb://'
        ]):
            raise ValueError('URL must be a supported database connection string')

        return v

    @validator('max_connections', 'min_connections')
    def validate_pool_size_relationship(cls, v, values):
        if 'min_connections' in values and values['min_connections'] is not None:
            if v is not None and v < values['min_connections']:
                raise ValueError('max_connections must be >= min_connections')
        return v

    class Config:
        extra = "forbid"
        schema_extra = {
            "example": {
                "url": "postgresql://user:password@localhost/deltalake",
                "pool_size": 20,
                "timeout": 60,
                "ssl_enabled": True
            }
        }

class SqlConfig(DatabaseModel):
    """Extended SQL configuration with database-specific settings"""

    postgres_config: Optional[PostgresConfig] = None
    sqlite_config: Optional[SqliteConfig] = None
    duckdb_config: Optional[DuckDBConfig] = None

    @root_validator
    def validate_config_consistency(cls, values):
        url = values.get('url', '')
        configs = [k for k in ['postgres_config', 'sqlite_config', 'duckdb_config']
                 if values.get(k) is not None]

        if configs and not url:
            raise ValueError('Database-specific config provided but no URL')

        return values

class PostgresConfig(BaseModel):
    """PostgreSQL-specific configuration"""

    application_name: str = Field(default="deltalakedb", description="Application name")
    sslmode: Literal['disable', 'allow', 'prefer', 'require'] = Field(default="prefer")
    connect_timeout: int = Field(default=30, ge=1, le=3600)
    statement_timeout: Optional[int] = Field(None, ge=1, le=3600000)
    idle_in_transaction_session_timeout: Optional[int] = Field(None, ge=1, le=3600000)
    lock_timeout: Optional[int] = Field(None, ge=1, le=3600000)
    tcp_user_timeout: Optional[int] = Field(None, ge=1, le=3600000)
    target_session_attrs: Literal['any', 'read-write', 'read-only', 'primary', 'standby'] = Field(default="any")
    keepalives: bool = Field(default=True)
    keepalives_idle: int = Field(default=300, ge=0, le=3600)
    keepalives_interval: int = Field(default=30, ge=0, le=3600)
    keepalives_count: int = Field(default=3, ge=1, le=10)

    class Config:
        schema_extra = {
            "example": {
                "application_name": "deltalake_worker",
                "sslmode": "require",
                "connect_timeout": 60,
                "statement_timeout": 300000
            }
        }

class SqliteConfig(BaseModel):
    """SQLite-specific configuration"""

    journal_mode: Literal['DELETE', 'TRUNCATE', 'PERSIST', 'WAL', 'MEMORY'] = Field(default="WAL")
    synchronous: Literal['OFF', 'NORMAL', 'FULL', 'EXTRA'] = Field(default="NORMAL")
    cache_size: int = Field(default=10000, ge=-2000, le=1000000)
    temp_store: Literal['DEFAULT', 'FILE', 'MEMORY'] = Field(default="MEMORY")
    mmap_size: Optional[int] = Field(None, ge=0, le=1073741824)
    busy_timeout: int = Field(default=30000, ge=1, le=3600000)
    query_only: bool = Field(default=False)
    immutable: bool = Field(default=False)
    auto_vacuum: Literal['NONE', 'FULL', 'INCREMENTAL'] = Field(default="INCREMENTAL")
    checkpoint_fullfsync: bool = Field(default=False)
    wal_autocheckpoint: int = Field(default=1000, ge=1, le=1000000)

    @validator('cache_size')
    def validate_cache_size(cls, v):
        if v < -2000 or v > 1000000:
            raise ValueError('cache_size must be between -2000 and 1000000')
        return v

    class Config:
        schema_extra = {
            "example": {
                "journal_mode": "WAL",
                "synchronous": "NORMAL",
                "cache_size": 50000,
                "mmap_size": 268435456
            }
        }

class DuckDBConfig(BaseModel):
    """DuckDB-specific configuration"""

    memory_limit: Optional[str] = Field(None, description="Memory limit (e.g., '1GB', '512MB')")
    threads: Optional[int] = Field(None, ge=1, le=1024)
    temp_directory: Optional[str] = Field(None, description="Temporary directory path")
    wal_autocheckpoint: Optional[int] = Field(None, ge=1, le=10000000)
    access_mode: Literal['AUTOMATIC', 'READ_ONLY', 'READ_WRITE'] = Field(default="AUTOMATIC")
    enable_optimizer: bool = Field(default=True)
    enable_profiling: bool = Field(default=False)
    force_parallelism: bool = Field(default=False)
    preserve_insertion_order: bool = Field(default=False)

    @validator('memory_limit')
    def validate_memory_limit(cls, v):
        if v is not None:
            pattern = r'^\d+[KMGT]?B$'
            if not re.match(pattern, v, re.IGNORECASE):
                raise ValueError('memory_limit must be in format like "1GB", "512MB", etc.')
        return v

    @validator('threads')
    def validate_threads(cls, v):
        import os
        if v is not None and v > os.cpu_count() * 2:
            raise ValueError(f'threads should not exceed {os.cpu_count() * 2} (2x CPU count)')
        return v

    class Config:
        schema_extra = {
            "example": {
                "memory_limit": "2GB",
                "threads": 4,
                "temp_directory": "/tmp/duckdb_temp",
                "access_mode": "READ_WRITE"
            }
        }

class WriteConfig(BaseModel):
    """Pydantic model for write operation configuration"""

    mode: Literal['append', 'overwrite', 'merge', 'error', 'ignore'] = Field(default="append")
    partition_by: Optional[List[str]] = Field(None, description="Partition column names")
    overwrite_schema: bool = Field(default=False)
    schema_mode: Literal['merge', 'overwrite'] = Field(default="merge")
    predicate: Optional[str] = Field(None, description="Write predicate for merge operations")
    writer_properties: Dict[str, str] = Field(default_factory=dict)

    # Performance settings
    max_records_per_file: Optional[int] = Field(None, ge=1, le=10000000)
    min_records_per_file: Optional[int] = Field(None, ge=1, le=10000000)
    file_size_limit: Optional[int] = Field(None, ge=1024, le=10737418240)  # 1KB to 10GB

    # Schema evolution settings
    allow_schema_evolution: bool = Field(default=True)
    strict_schema: bool = Field(default=False)

    # Transaction settings
    atomic_write: bool = Field(default=True)
    validate_on_write: bool = Field(default=True)

    @validator('partition_by')
    def validate_partition_columns(cls, v):
        if v is not None:
            if not v:
                raise ValueError('partition_by cannot be empty list')
            # Basic column name validation
            for col in v:
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                    raise ValueError(f'Invalid partition column name: {col}')
        return v

    @validator('max_records_per_file', 'min_records_per_file')
    def validate_record_limits(cls, v, values):
        if 'max_records_per_file' in values and values['max_records_per_file'] is not None:
            if v is not None and v > values['max_records_per_file']:
                raise ValueError('min_records_per_file must be <= max_records_per_file')
        return v

    @validator('file_size_limit')
    def validate_file_size(cls, v):
        if v is not None:
            if v < 1024 or v > 10 * 1024 * 1024 * 1024:  # 10GB
                raise ValueError('file_size_limit must be between 1KB and 10GB')
        return v

    class Config:
        schema_extra = {
            "example": {
                "mode": "append",
                "partition_by": ["year", "month"],
                "overwrite_schema": False,
                "max_records_per_file": 1000000,
                "allow_schema_evolution": True
            }
        }

class TransactionConfig(BaseModel):
    """Pydantic model for transaction configuration"""

    isolation_level: Literal['READ_UNCOMMITTED', 'READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE'] = Field(default="READ_COMMITTED")
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_delay_ms: int = Field(default=1000, ge=100, le=60000)
    retry_backoff_factor: float = Field(default=2.0, ge=1.0, le=10.0)

    # Transaction size limits
    max_tables: Optional[int] = Field(None, ge=1, le=10000)
    max_operations: Optional[int] = Field(None, ge=1, le=100000)
    max_memory_mb: Optional[int] = Field(None, ge=1, le=10240)  # 10GB

    # Monitoring and logging
    enable_metrics: bool = Field(default=True)
    log_level: Literal['DEBUG', 'INFO', 'WARN', 'ERROR'] = Field(default="INFO")

    @validator('retry_backoff_factor')
    def validate_backoff(cls, v):
        if v < 1.0 or v > 10.0:
            raise ValueError('retry_backoff_factor must be between 1.0 and 10.0')
        return v

    class Config:
        schema_extra = {
            "example": {
                "isolation_level": "READ_COMMITTED",
                "timeout_seconds": 600,
                "max_retries": 5,
                "retry_delay_ms": 2000,
                "max_tables": 1000
            }
        }

class TableConfig(BaseModel):
    """Pydantic model for table configuration"""

    table_id: UUID = Field(..., description="Table unique identifier")
    table_name: str = Field(..., min_length=1, max_length=255, description="Table name")
    table_path: str = Field(..., description="Table path")
    description: Optional[str] = Field(None, max_length=1000, description="Table description")

    # Schema and partitioning
    partition_columns: List[str] = Field(default_factory=list, description="Partition column names")
    configuration: Dict[str, Any] = Field(default_factory=dict, description="Table configuration")

    # Table properties
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    properties: Dict[str, str] = Field(default_factory=dict, description="Table properties")
    tags: List[str] = Field(default_factory=list, description="Table tags")

    @validator('table_name')
    def validate_table_name(cls, v):
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError('Table name must be a valid identifier')
        return v

    @validator('partition_columns')
    def validate_partition_columns(cls, v):
        if v:
            for col in v:
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                    raise ValueError(f'Invalid partition column name: {col}')
        return v

    class Config:
        schema_extra = {
            "example": {
                "table_id": "123e4567-e89b-12d3-a456-426614174000",
                "table_name": "sales_data",
                "table_path": "/data/sales",
                "partition_columns": ["year", "region"],
                "description": "Sales transaction data",
                "properties": {"owner": "data_team"},
                "tags": ["sales", "transactional"]
            }
        }

class CommitConfig(BaseModel):
    """Pydantic model for commit configuration"""

    commit_id: UUID = Field(..., description="Commit unique identifier")
    table_id: UUID = Field(..., description="Table ID")
    version: int = Field(..., ge=0, description="Commit version")
    operation_type: str = Field(..., min_length=1, description="Operation type")

    # Commit metadata
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    operation_parameters: Dict[str, Any] = Field(default_factory=dict)
    commit_info: Dict[str, Any] = Field(default_factory=dict)

    # File information
    files_added: int = Field(default=0, ge=0, description="Number of files added")
    files_removed: int = Field(default=0, ge=0, description="Number of files removed")
    bytes_added: int = Field(default=0, ge=0, description="Bytes added")
    bytes_removed: int = Field(default=0, ge=0, description="Bytes removed")

    @validator('operation_type')
    def validate_operation_type(cls, v):
        allowed_ops = ['WRITE', 'DELETE', 'MERGE', 'UPDATE', 'CREATE', 'DROP']
        if v not in allowed_ops:
            raise ValueError(f'operation_type must be one of: {allowed_ops}')
        return v

    class Config:
        schema_extra = {
            "example": {
                "commit_id": "123e4567-e89b-12d3-a456-426614174001",
                "table_id": "123e4567-e89b-12d3-a456-426614174000",
                "version": 1,
                "operation_type": "WRITE",
                "files_added": 10,
                "bytes_added": 1048576
            }
        }

class ConnectionPoolConfig(BaseModel):
    """Pydantic model for connection pool configuration"""

    # Pool size settings
    min_connections: int = Field(default=1, ge=1, le=1000)
    max_connections: int = Field(default=10, ge=1, le=1000)

    # Timeout settings
    connection_timeout: int = Field(default=30, ge=1, le=3600)
    idle_timeout: Optional[int] = Field(None, ge=1, le=3600)
    max_lifetime: Optional[int] = Field(None, ge=1, le=3600)

    # Pool behavior
    test_on_borrow: bool = Field(default=True)
    test_on_return: bool = Field(default=False)
    test_on_idle: bool = Field(default=True)

    # Health checking
    health_check_interval: int = Field(default=30, ge=1, le=3600)
    max_idle_time: Optional[int] = Field(None, ge=1, le=3600)

    # Performance tuning
    pre_ping: bool = Field(default=True)
    pool_recycle: Optional[int] = Field(None, ge=3600)

    @validator('max_connections')
    def validate_max_connections(cls, v, values):
        if 'min_connections' in values and v < values['min_connections']:
            raise ValueError('max_connections must be >= min_connections')
        return v

    class Config:
        schema_extra = {
            "example": {
                "min_connections": 2,
                "max_connections": 20,
                "connection_timeout": 60,
                "idle_timeout": 600,
                "test_on_borrow": True
            }
        }

class PerformanceConfig(BaseModel):
    """Pydantic model for performance configuration"""

    # Caching settings
    enable_query_cache: bool = Field(default=True)
    query_cache_size: int = Field(default=1000, ge=1, le=100000)
    enable_metadata_cache: bool = Field(default=True)
    metadata_cache_ttl: int = Field(default=300, ge=1, le=3600)

    # Connection settings
    enable_connection_pooling: bool = Field(default=True)
    pool_size: int = Field(default=10, ge=1, le=1000)

    # Query optimization
    enable_query_optimization: bool = Field(default=True)
    batch_size: int = Field(default=1000, ge=1, le=100000)

    # Memory settings
    max_memory_mb: Optional[int] = Field(None, ge=1, le=10240)  # 10GB

    # Monitoring
    enable_metrics: bool = Field(default=True)
    metrics_port: Optional[int] = Field(None, ge=1024, le=65535)

    class Config:
        schema_extra = {
            "example": {
                "enable_query_cache": True,
                "query_cache_size": 5000,
                "enable_connection_pooling": True,
                "pool_size": 20,
                "batch_size": 5000,
                "enable_metrics": True
            }
        }
"#;

/// Python data class provider with Pydantic models
#[pyclass]
#[derive(Debug, Clone)]
pub struct PydanticModels;

#[pymethods]
impl PydanticModels {
    /// Get Pydantic model definitions as Python code
    #[staticmethod]
    fn get_model_definitions() -> String {
        PYDANTIC_CODE.to_string()
    }

    /// Create Pydantic models in the current Python environment
    fn create_models(&self, py: Python) -> PyResult<()> {
        let globals = PyDict::new(py);

        // Import required modules
        let builtins = py.import("builtins")?;
        globals.set_item("__builtins__", builtins)?;

        // Execute the Pydantic code
        py.run(PYDANTIC_CODE, Some(globals), None)?;

        // Make models available in the module namespace
        let sys = py.import("sys")?;
        let modules = sys.getattr("modules")?;
        let current_module = modules.get_item("deltalakedb")?;

        // Extract individual classes from globals and add to current module
        let model_names = [
            "DatabaseConfig", "PostgresConfig", "SqliteConfig", "DuckDBConfig",
            "WriteConfig", "TransactionConfig", "TableConfig", "CommitConfig",
            "ConnectionPoolConfig", "PerformanceConfig"
        ];

        for model_name in &model_names {
            if let Some(model_class) = globals.get_item(model_name)? {
                current_module.set_item(model_name, model_class)?;
            }
        }

        Ok(())
    }

    /// Validate configuration using Pydantic models
    fn validate_config(
        &self,
        py: Python,
        config_dict: &PyDict,
        config_type: &str,
    ) -> PyResult<bool> {
        // This would use the Pydantic models created above
        // For now, return True as placeholder
        // In a real implementation, this would:
        // 1. Load the appropriate Pydantic model class
        // 2. Parse the dictionary with the model
        // 3. Return validation result

        let model_name = match config_type {
            "database" => "DatabaseConfig",
            "write" => "WriteConfig",
            "transaction" => "TransactionConfig",
            "table" => "TableConfig",
            "commit" => "CommitConfig",
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown config type: {}", config_type)
            )),
        };

        // Placeholder implementation
        println!("Validating config using {} model", model_name);
        Ok(true)
    }
}

/// Factory for creating validated configuration objects
#[pyclass]
#[derive(Debug, Clone)]
pub struct ConfigFactory;

#[pymethods]
impl ConfigFactory {
    /// Create database configuration from dictionary
    #[staticmethod]
    fn create_database_config(py: Python, config_dict: &PyDict) -> PyResult<PyObject> {
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        // Import the DatabaseConfig model
        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        // Create validated instance
        database_config_class.call((config_dict,), None)
    }

    /// Create write configuration from dictionary
    #[staticmethod]
    fn create_write_config(py: Python, config_dict: &PyDict) -> PyResult<PyObject> {
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        // Import the WriteConfig model
        let config_module = py.import("deltalakedb")?;
        let write_config_class = config_module.getattr("WriteConfig")?;

        // Create validated instance
        write_config_class.call((config_dict,), None)
    }

    /// Create transaction configuration from dictionary
    #[staticmethod]
    fn create_transaction_config(py: Python, config_dict: &PyDict) -> PyResult<PyObject> {
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        // Import the TransactionConfig model
        let config_module = py.import("deltalakedb")?;
        let transaction_config_class = config_module.getattr("TransactionConfig")?;

        // Create validated instance
        transaction_config_class.call((config_dict,), None)
    }

    /// Create table configuration from dictionary
    #[staticmethod]
    fn create_table_config(py: Python, config_dict: &PyDict) -> PyResult<PyObject> {
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        // Import the TableConfig model
        let config_module = py.import("deltalakedb")?;
        let table_config_class = config_module.getattr("TableConfig")?;

        // Create validated instance
        table_config_class.call((config_dict,), None)
    }
}

/// Validation utilities for data integrity
#[pyclass]
#[derive(Debug, Clone)]
pub struct ValidationUtils;

#[pymethods]
impl ValidationUtils {
    /// Validate database URI format
    #[staticmethod]
    fn validate_database_uri(uri: &str) -> PyResult<bool> {
        if uri.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI cannot be empty"
            ));
        }

        // Check for supported database prefixes
        let supported_prefixes = [
            "postgresql://",
            "postgres://",
            "sqlite://",
            "sqlite::memory:",
            "duckdb://",
            "duckdb::memory:",
        ];

        if !supported_prefixes.iter().any(|&prefix| uri.starts_with(prefix)) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI must start with a supported database prefix"
            ));
        }

        Ok(true)
    }

    /// Validate table name
    #[staticmethod]
    fn validate_table_name(name: &str) -> PyResult<bool> {
        if name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Table name cannot be empty"
            ));
        }

        // Basic identifier validation
        let re = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Failed to compile regex"
            ))?;

        if !re.is_match(name) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Table name must be a valid identifier"
            ));
        }

        Ok(true)
    }

    /// Validate UUID format
    #[staticmethod]
    fn validate_uuid(uuid_str: &str) -> PyResult<bool> {
        Uuid::parse_str(uuid_str)
            .map(|_| true)
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid UUID format"
            ))
    }

    /// Validate partition column names
    #[staticmethod]
    fn validate_partition_columns(columns: Vec<String>) -> PyResult<bool> {
        if columns.is_empty() {
            return Ok(true); // Empty list is valid
        }

        let re = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Failed to compile regex"
            ))?;

        for column in &columns {
            if !re.is_match(column) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Invalid partition column name: {}", column)
                ));
            }
        }

        Ok(true)
    }

    /// Validate file path format
    #[staticmethod]
    fn validate_file_path(path: &str) -> PyResult<bool> {
        if path.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "File path cannot be empty"
            ));
        }

        // Basic path validation - no null bytes, reasonable length
        if path.contains('\0') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "File path cannot contain null bytes"
            ));
        }

        if path.len() > 4096 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "File path too long (max 4096 characters)"
            ));
        }

        Ok(true)
    }

    /// Validate memory limit format
    #[staticmethod]
    fn validate_memory_limit(limit: &str) -> PyResult<bool> {
        if limit.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Memory limit cannot be empty"
            ));
        }

        let re = regex::Regex::new(r"^\d+[KMGT]?B$")
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Failed to compile regex"
            ))?;

        if !re.is_match(limit) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Memory limit must be in format like '1GB', '512MB', etc."
            ));
        }

        Ok(true)
    }

    /// Validate commit version
    #[staticmethod]
    fn validate_commit_version(version: i64) -> PyResult<bool> {
        if version < 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Commit version must be non-negative"
            ));
        }

        Ok(true)
    }
}

/// Configuration file loader with validation
#[pyclass]
#[derive(Debug, Clone)]
pub struct ConfigLoader;

#[pymethods]
impl ConfigLoader {
    /// Load configuration from TOML file with validation
    #[staticmethod]
    fn load_toml_config(py: Python, file_path: &str) -> PyResult<PyObject> {
        let tomllib = py.import("tomllib")?;
        let builtins = py.import("builtins")?;

        // Open and read file
        let file = builtins.call_method1("open", (file_path, "rb"))?;
        let data = tomllib.call_method1("load", (file,))?;

        // Validate using Pydantic models
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        // Try to validate as database config
        database_config_class.call((data,), None)
    }

    /// Load configuration from YAML file with validation
    #[staticmethod]
    fn load_yaml_config(py: Python, file_path: &str) -> PyResult<PyObject> {
        // Try to import yaml module
        let yaml = match py.import("yaml") {
            Ok(module) => module,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                    "PyYAML is required for YAML configuration. Install with: pip install PyYAML"
                ));
            }
        };

        let builtins = py.import("builtins")?;

        // Open and read file
        let file = builtins.call_method1("open", (file_path, "r"))?;
        let data = yaml.call_method1("safe_load", (file,))?;

        // Validate using Pydantic models
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        // Try to validate as database config
        database_config_class.call((data,), None)
    }

    /// Load configuration from TOML file with validation
    #[staticmethod]
    fn load_toml_config(py: Python, file_path: &str) -> PyResult<PyObject> {
        // Try to import toml module
        let toml = match py.import("toml") {
            Ok(module) => module,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                    "toml is required for TOML configuration. Install with: pip install tomli"
                ));
            }
        };

        let builtins = py.import("builtins")?;

        // Open and read file
        let file = builtins.call_method1("open", (file_path, "rb"))?;
        let content = file.call_method0("read")?;

        // Parse TOML content
        let data = toml.call_method1("loads", (content,))?;

        // Validate using Pydantic models
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        // Try to validate as database config
        database_config_class.call((data,), None)
    }

    /// Load configuration from file with auto-detection of format
    #[staticmethod]
    fn load_config_file(py: Python, file_path: &str) -> PyResult<PyObject> {
        let os = py.import("os")?;
        let pathlib = py.import("pathlib")?;

        // Get file extension
        let path_obj = pathlib.call_method1("Path", (file_path,))?;
        let suffix = path_obj.getattr("suffix")?.extract::<String>()?;

        match suffix.to_lowercase().as_str() {
            ".yaml" | ".yml" => Self::load_yaml_config(py, file_path),
            ".toml" => Self::load_toml_config(py, file_path),
            ".json" => Self::load_json_config(py, file_path),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unsupported configuration file format: {}. Supported formats: .yaml, .yml, .toml, .json", suffix)
            ))
        }
    }

    /// Save configuration to YAML file
    #[staticmethod]
    fn save_yaml_config(py: Python, config: &PyAny, file_path: &str) -> PyResult<()> {
        // Try to import yaml module
        let yaml = match py.import("yaml") {
            Ok(module) => module,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                    "PyYAML is required for YAML configuration. Install with: pip install PyYAML"
                ));
            }
        };

        let builtins = py.import("builtins")?;

        // Convert config to dictionary if it's a Pydantic model
        let config_dict = if hasattr(config, "dict")? {
            config.call_method0("dict")?
        } else {
            config.to_object(py)
        };

        // Open and write file
        let file = builtins.call_method1("open", (file_path, "w"))?;
        yaml.call_method1("safe_dump", (config_dict, file))?;

        Ok(())
    }

    /// Save configuration to TOML file
    #[staticmethod]
    fn save_toml_config(py: Python, config: &PyAny, file_path: &str) -> PyResult<()> {
        // Try to import toml module
        let toml = match py.import("tomli_w") {
            Ok(module) => module,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                    "tomli_w is required for TOML writing. Install with: pip install tomli_w"
                ));
            }
        };

        let builtins = py.import("builtins")?;

        // Convert config to dictionary if it's a Pydantic model
        let config_dict = if hasattr(config, "dict")? {
            config.call_method0("dict")?
        } else {
            config.to_object(py)
        };

        // Serialize to TOML
        let toml_content = toml.call_method1("dumps", (config_dict,))?;

        // Open and write file
        let file = builtins.call_method1("open", (file_path, "w"))?;
        file.call_method1("write", (toml_content,))?;

        Ok(())
    }

    /// Load configuration from environment variables with validation
    #[staticmethod]
    fn load_env_config(py: Python, prefix: Option<&str>) -> PyResult<PyObject> {
        let os = py.import("os")?;
        let builtins = py.import("builtins")?;

        let prefix = prefix.unwrap_or("DELTALAKE_DB_");

        // Get all environment variables with the prefix
        let environ = os.getattr("environ")?;
        let items = environ.call_method0("items")?;

        let mut config_dict = PyDict::new(py);

        for item in items.downcast::<PyList>()? {
            let key_value: (String, String) = item.extract()?;
            let (key, value) = key_value;

            if key.starts_with(prefix) {
                let config_key = key.strip_prefix(prefix)
                    .unwrap_or(&key)
                    .to_lowercase();

                // Convert environment variable format to config format
                let py_key = config_key.replace("_", "");

                // Try to parse as appropriate type
                if let Ok(int_val) = value.parse::<i64>() {
                    config_dict.set_item(py_key, int_val)?;
                } else if let Ok(bool_val) = value.parse::<bool>() {
                    config_dict.set_item(py_key, bool_val)?;
                } else {
                    config_dict.set_item(py_key, value)?;
                }
            }
        }

        // Validate using Pydantic models
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        // Try to validate as database config
        database_config_class.call((config_dict,), None)
    }

    /// Create a comprehensive configuration from multiple sources
    #[staticmethod]
    fn load_comprehensive_config(py: Python, config_file: Option<&str>, env_prefix: Option<&str>) -> PyResult<PyObject> {
        let mut final_config = PyDict::new(py);

        // 1. Load default configuration
        let default_config = HashMap::from([
            ("url", "sqlite:///:memory:"),
            ("pool_size", 10),
            ("timeout", 30),
            ("ssl_enabled", false),
        ]);

        for (key, value) in default_config {
            final_config.set_item(key, value)?;
        }

        // 2. Load from configuration file if provided
        if let Some(file_path) = config_file {
            let file_config = Self::load_config_file(py, file_path)?;
            if hasattr(&file_config, "dict")? {
                let file_dict = file_config.call_method0("dict")?;
                for (key, value) in file_dict.downcast::<PyDict>()? {
                    final_config.set_item(key, value)?;
                }
            }
        }

        // 3. Override with environment variables
        let env_config = Self::load_env_config(py, env_prefix)?;
        if hasattr(&env_config, "dict")? {
            let env_dict = env_config.call_method0("dict")?;
            for (key, value) in env_dict.downcast::<PyDict>()? {
                final_config.set_item(key, value)?;
            }
        }

        // 4. Validate the final configuration
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;
        let database_config_class = config_module.getattr("DatabaseConfig")?;

        database_config_class.call((final_config,), None)
    }

    /// Generate sample configuration files
    #[staticmethod]
    fn generate_sample_configs(py: Python, output_dir: &str) -> PyResult<()> {
        let pathlib = py.import("pathlib")?;
        let builtins = py.import("builtins")?;

        // Create output directory if it doesn't exist
        let path_obj = pathlib.call_method1("Path", (output_dir,))?;
        path_obj.call_method1("mkdir", (py.None(), py.None(), true))?;

        // Sample database configuration
        let sample_db_config = PyDict::new(py);
        sample_db_config.set_item("url", "postgresql://user:password@localhost:5432/deltalake")?;
        sample_db_config.set_item("pool_size", 20)?;
        sample_db_config.set_item("timeout", 60)?;
        sample_db_config.set_item("ssl_enabled", true)?;
        sample_db_config.set_item("ssl_cert", "/path/to/cert.pem")?;

        // Sample write configuration
        let sample_write_config = PyDict::new(py);
        sample_write_config.set_item("mode", "append")?;
        sample_write_config.set_item("partition_by", vec!["year", "month"])?;
        sample_write_config.set_item("overwrite_schema", false)?;
        sample_write_config.set_item("allow_schema_evolution", true)?;

        // Sample table configuration
        let sample_table_config = PyDict::new(py);
        sample_table_config.set_item("name", "sales_data")?;
        sample_table_config.set_item("description", "Sales transaction data")?;
        sample_table_config.set_item("partition_columns", vec!["year", "region"])?;
        sample_table_config.set_item("z_order_columns", vec!["customer_id", "transaction_date"])?;

        // Save configurations in different formats
        let yaml_path = format!("{}/sample_database.yaml", output_dir);
        let toml_path = format!("{}/sample_database.toml", output_dir);
        let json_path = format!("{}/sample_database.json", output_dir);

        Self::save_yaml_config(py, sample_db_config, &yaml_path)?;
        Self::save_toml_config(py, sample_db_config, &toml_path)?;

        let json_file = builtins.call_method1("open", (json_path, "w"))?;
        let json_module = py.import("json")?;
        json_module.call_method1("dump", (sample_db_config, json_file))?;

        println!("Sample configuration files generated in: {}", output_dir);
        println!("- sample_database.yaml");
        println!("- sample_database.toml");
        println!("- sample_database.json");

        Ok(())
    }

    /// Validate configuration file format
    #[staticmethod]
    fn validate_config_file(py: Python, file_path: &str) -> PyResult<bool> {
        let pathlib = py.import("pathlib")?;

        // Check if file exists
        let path_obj = pathlib.call_method1("Path", (file_path,))?;
        if !path_obj.call_method0("exists")?.extract::<bool>()? {
            return Err(PyErr::new::<pyo3::exceptions::PyFileNotFoundError, _>(
                format!("Configuration file not found: {}", file_path)
            ));
        }

        // Try to load and validate the file
        match Self::load_config_file(py, file_path) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false)
        }
    }

    /// Get configuration schema
    #[staticmethod]
    fn get_config_schema(py: Python, config_type: &str) -> PyResult<PyObject> {
        let pydantic = PydanticModels {};
        pydantic.create_models(py)?;

        let config_module = py.import("deltalakedb")?;

        let class_name = match config_type.to_lowercase().as_str() {
            "database" => "DatabaseConfig",
            "write" => "WriteConfig",
            "transaction" => "TransactionConfig",
            "table" => "TableConfig",
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown configuration type: {}. Available types: database, write, transaction, table", config_type)
            ))
        };

        let config_class = config_module.getattr(class_name)?;

        if hasattr(&config_class, "schema_json")? {
            config_class.call_method0("schema_json")
        } else {
            // Fallback: return class documentation
            config_class.getattr("__doc__")
        }
    }
}