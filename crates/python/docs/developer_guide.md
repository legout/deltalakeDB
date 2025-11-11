# DeltaLake DB Python Developer Guide

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Extending the Library](#extending-the-library)
5. [Testing](#testing)
6. [Performance Profiling](#performance-profiling)
7. [Debugging](#debugging)
8. [Contributing Guidelines](#contributing-guidelines)
9. [Code Examples](#code-examples)
10. [Best Practices](#best-practices)

## Development Environment Setup

### Prerequisites

- Python 3.8+
- Rust 1.70+ (for building the Rust components)
- PostgreSQL/MySQL/SQLite (for testing)
- Git

### Repository Structure

```
deltalakedb/
├── crates/
│   ├── python/                 # Python bindings
│   │   ├── src/               # Rust source code
│   │   ├── examples/          # Python examples
│   │   ├── tests/             # Python tests
│   │   └── docs/              # Documentation
│   └── core/                  # Rust core library
├── Cargo.toml
├── pyproject.toml
└── README.md
```

### Building from Source

```bash
# Clone repository
git clone https://github.com/your-org/deltalakedb.git
cd deltalakedb

# Setup Python development environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Build Rust components
cargo build --release

# Install in development mode
maturin develop
```

### Development Workflow

```bash
# Run tests
pytest crates/python/tests/

# Run Rust tests
cargo test

# Run linting
flake8 crates/python/src/
clippy -- -D warnings

# Format code
black crates/python/src/
rustfmt

# Build documentation
cd crates/python && mkdocs build
```

## Architecture Overview

### High-Level Architecture

```mermaid
graph TB
    subgraph "Python Layer"
        A[Python API] --> B[PyO3 Bindings]
        B --> C[Rust Core]
    end

    subgraph "Rust Core"
        C --> D[SQL Adapters]
        C --> E[Delta Lake Engine]
        C --> F[Transaction Manager]
        C --> G[Performance Optimizations]
    end

    subgraph "Storage Layer"
        D --> H[PostgreSQL]
        D --> I[MySQL]
        D --> J[SQLite]
        D --> K[SQL Server]
    end

    subgraph "Python Ecosystem"
        A --> L[Pandas]
        A --> M[PyArrow]
        A --> N[DeltaLake]
    end
```

### Component Overview

#### Core Components

1. **SQL Adapters**: Database-specific implementations for storing Delta Lake metadata
2. **Delta Lake Engine**: Core logic for Delta Lake operations
3. **Transaction Manager**: ACID transaction support across multiple tables
4. **Performance Optimizations**: Caching, lazy loading, memory optimization
5. **Python Bindings**: PyO3-based Python interface

#### Key Modules

```python
# Core bindings (crates/python/src/bindings.rs)
- Table, Commit, File, Protocol, Metadata
- Core Delta Lake operations

# Connection management (crates/python/src/connection.rs)
- SqlConnection, ConnectionPool, TransactionContext
- Database connection handling

# Type system (crates/python/src/types.rs)
- DeltaDataType, SchemaField, TableSchema
- Type conversion and validation

# Performance optimization (crates/python/src/)
- lazy_loading.rs     # Lazy loading strategies
- caching.rs         # Multi-level caching
- memory_optimization.rs  # Memory usage optimization
- async_io.rs        # Async I/O operations

# Multi-table transactions (crates/python/src/multi_table_transactions.rs)
- Distributed transaction management
- Two-phase commit protocol
```

## Core Components

### Database Adapters

#### PostgreSQL Adapter

```rust
// crates/core/src/adapters/postgres.rs
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

pub struct PostgresAdapter {
    pool: sqlx::PgPool,
}

impl PostgresAdapter {
    pub async fn new(connection_string: &str) -> Result<Self, AdapterError> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1800))
            .connect(connection_string)
            .await?;

        Ok(Self { pool })
    }

    pub async fn initialize_schema(&self) -> Result<(), AdapterError> {
        // Create Delta Lake tables
        sqlx::include_file!("sql/postgres_schema.sql")
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl DeltaAdapter for PostgresAdapter {
    async fn create_table(&self, name: &str, schema: &TableSchema) -> Result<TableHandle, AdapterError> {
        // Implementation for creating tables
        todo!()
    }

    async fn load_table(&self, name: &str) -> Result<Option<TableHandle>, AdapterError> {
        // Implementation for loading tables
        todo!()
    }

    async fn commit_transaction(&self, actions: &[DeltaAction]) -> Result<CommitInfo, AdapterError> {
        // Implementation for committing transactions
        todo!()
    }
}
```

#### MySQL Adapter

```rust
// crates/core/src/adapters/mysql.rs
use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;

pub struct MySqlAdapter {
    pool: sqlx::MySqlPool,
}

impl MySqlAdapter {
    pub async fn new(connection_string: &str) -> Result<Self, AdapterError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(connection_string)
            .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl DeltaAdapter for MySqlAdapter {
    async fn create_table(&self, name: &str, schema: &TableSchema) -> Result<TableHandle, AdapterError> {
        // MySQL-specific implementation
        todo!()
    }
}
```

### Type System

#### Delta Data Types

```rust
// crates/core/src/types.rs
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeltaDataType {
    Primitive(PrimitiveType),
    Array {
        element_type: Box<DeltaDataType>,
        contains_null: bool,
    },
    Struct(Vec<SchemaField>),
    Map {
        key_type: Box<DeltaDataType>,
        value_type: Box<DeltaDataType>,
        value_contains_null: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrimitiveType {
    String,
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Decimal { precision: u8, scale: i8 },
    Date,
    Timestamp,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: DeltaDataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

impl SchemaField {
    pub fn new(name: &str, data_type: DeltaDataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

// Python bindings
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct PySchemaField {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: PyDeltaDataType,
    #[pyo3(get)]
    pub nullable: bool,
    #[pyo3(get)]
    pub metadata: HashMap<String, String>,
}

#[pymethods]
impl PySchemaField {
    #[new]
    pub fn new(name: String, data_type: PyDeltaDataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    pub fn to_dict(&self) -> HashMap<String, pyo3::PyObject> {
        let mut dict = HashMap::new();
        dict.insert("name".to_string(), self.name.clone().into_py(Py {}));
        dict.insert("data_type".to_string(), self.data_type.to_string().into_py(Py {}));
        dict.insert("nullable".to_string(), self.nullable.into_py(Py {}));
        dict
    }
}
```

### Transaction Management

#### Multi-table Transactions

```rust
// crates/core/src/transactions.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

#[derive(Debug)]
pub struct MultiTableTransaction {
    id: String,
    participants: Vec<TransactionParticipant>,
    status: Arc<RwLock<TransactionStatus>>,
    timeout: Duration,
}

impl MultiTableTransaction {
    pub fn new(participants: Vec<TransactionParticipant>, timeout: Duration) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            participants,
            status: Arc::new(RwLock::new(TransactionStatus::Preparing)),
            timeout,
        }
    }

    pub async fn prepare(&self) -> Result<(), TransactionError> {
        let mut status = self.status.write().await;
        *status = TransactionStatus::Preparing;

        // Phase 1: Prepare all participants
        for participant in &self.participants {
            participant.prepare(&self.id).await?;
        }

        *status = TransactionStatus::Prepared;
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), TransactionError> {
        let mut status = self.status.write().await;

        match *status {
            TransactionStatus::Prepared => {
                *status = TransactionStatus::Committing;

                // Phase 2: Commit all participants
                let mut committed_participants = Vec::new();

                for participant in &self.participants {
                    if let Err(e) = participant.commit(&self.id).await {
                        // Rollback committed participants
                        for committed_participant in &committed_participants {
                            let _ = committed_participant.rollback(&self.id).await;
                        }
                        return Err(e);
                    }
                    committed_participants.push(participant);
                }

                *status = TransactionStatus::Committed;
                Ok(())
            }
            _ => Err(TransactionError::InvalidState),
        }
    }

    pub async fn rollback(&self) -> Result<(), TransactionError> {
        let mut status = self.status.write().await;
        *status = TransactionStatus::Aborting;

        // Rollback all participants
        let results: Vec<Result<(), TransactionError>> = futures::future::join_all(
            self.participants.iter().map(|p| p.rollback(&self.id))
        ).await;

        let errors: Vec<TransactionError> = results.into_iter()
            .filter_map(Result::err)
            .collect();

        if errors.is_empty() {
            *status = TransactionStatus::Aborted;
            Ok(())
        } else {
            Err(TransactionError::MultipleErrors(errors))
        }
    }
}

#[derive(Debug)]
pub struct TransactionParticipant {
    table_name: String,
    adapter: Arc<dyn DeltaAdapter>,
    operations: Vec<DeltaAction>,
}

impl TransactionParticipant {
    pub async fn prepare(&self, transaction_id: &str) -> Result<(), TransactionError> {
        // Prepare participant for two-phase commit
        self.adapter.prepare_transaction(transaction_id, &self.operations).await
    }

    pub async fn commit(&self, transaction_id: &str) -> Result<(), TransactionError> {
        // Commit prepared transaction
        self.adapter.commit_transaction(transaction_id).await
    }

    pub async fn rollback(&self, transaction_id: &str) -> Result<(), TransactionError> {
        // Rollback prepared transaction
        self.adapter.rollback_transaction(transaction_id).await
    }
}
```

## Extending the Library

### Adding New Database Support

#### 1. Implement DeltaAdapter Trait

```rust
// crates/core/src/adapters/new_database.rs
use async_trait::async_trait;
use crate::delta::{DeltaAdapter, AdapterError, TableHandle, DeltaAction, CommitInfo};

pub struct NewDatabaseAdapter {
    // Database-specific fields
    connection_pool: NewDatabasePool,
}

impl NewDatabaseAdapter {
    pub async fn new(connection_string: &str) -> Result<Self, AdapterError> {
        // Initialize database connection
        let connection_pool = NewDatabasePool::connect(connection_string).await?;

        Ok(Self { connection_pool })
    }

    pub async fn initialize_schema(&self) -> Result<(), AdapterError> {
        // Create necessary tables and indexes
        let schema_sql = include_str!("sql/new_database_schema.sql");
        self.connection_pool.execute_batch(schema_sql).await?;
        Ok(())
    }
}

#[async_trait]
impl DeltaAdapter for NewDatabaseAdapter {
    async fn create_table(&self, name: &str, schema: &TableSchema) -> Result<TableHandle, AdapterError> {
        // Database-specific table creation logic
        let create_sql = generate_create_table_sql(name, schema)?;
        self.connection_pool.execute(&create_sql).await?;

        Ok(TableHandle::new(name.to_string(), self.connection_pool.clone()))
    }

    async fn load_table(&self, name: &str) -> Result<Option<TableHandle>, AdapterError> {
        // Load table metadata
        let exists = self.connection_pool.table_exists(name).await?;
        if exists {
            Ok(Some(TableHandle::new(name.to_string(), self.connection_pool.clone())))
        } else {
            Ok(None)
        }
    }

    async fn commit_transaction(&self, actions: &[DeltaAction]) -> Result<CommitInfo, AdapterError> {
        // Begin transaction
        let mut tx = self.connection_pool.begin_transaction().await?;

        try {
            // Apply actions
            for action in actions {
                self.apply_action(&mut tx, action).await?;
            }

            // Commit transaction
            let commit_info = tx.commit().await?;
            Ok(commit_info)
        } catch (e) {
            // Rollback on error
            tx.rollback().await?;
            Err(e)
        }
    }

    async fn prepare_transaction(&self, transaction_id: &str, actions: &[DeltaAction]) -> Result<(), AdapterError> {
        // Prepare transaction for two-phase commit
        let prepare_sql = generate_prepare_transaction_sql(transaction_id, actions)?;
        self.connection_pool.execute(&prepare_sql).await?;
        Ok(())
    }

    async fn commit_transaction(&self, transaction_id: &str) -> Result<(), AdapterError> {
        // Commit prepared transaction
        let commit_sql = format!("COMMIT PREPARED '{}'", transaction_id);
        self.connection_pool.execute(&commit_sql).await?;
        Ok(())
    }

    async fn rollback_transaction(&self, transaction_id: &str) -> Result<(), AdapterError> {
        // Rollback prepared transaction
        let rollback_sql = format!("ROLLBACK PREPARED '{}'", transaction_id);
        self.connection_pool.execute(&rollback_sql).await?;
        Ok(())
    }
}

fn generate_create_table_sql(name: &str, schema: &TableSchema) -> Result<String, AdapterError> {
    // Convert Delta schema to database-specific CREATE TABLE statement
    todo!()
}

fn generate_prepare_transaction_sql(transaction_id: &str, actions: &[DeltaAction]) -> Result<String, AdapterError> {
    // Generate SQL for preparing transaction
    todo!()
}
```

#### 2. Add Python Bindings

```rust
// crates/python/src/adapters/new_database.rs
use pyo3::prelude::*;
use crate::core::adapters::new_database::NewDatabaseAdapter;

#[pyclass]
pub struct PyNewDatabaseAdapter {
    inner: NewDatabaseAdapter,
}

#[pymethods]
impl PyNewDatabaseAdapter {
    #[new]
    pub fn new(connection_string: String) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new()?;
        let inner = rt.block_on(async {
            NewDatabaseAdapter::new(&connection_string).await
        })?;

        Ok(Self { inner })
    }

    pub fn initialize_schema(&self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            self.inner.initialize_schema().await
        })?;
        Ok(())
    }
}
```

#### 3. Register in Main Module

```rust
// crates/python/src/lib.rs
#[pymodule]
fn deltalakedb(_py: Python, m: &PyModule) -> PyResult<()> {
    // Add new adapter to module
    m.add_class::<PyNewDatabaseAdapter>()?;

    // Register adapter factory
    m.add_function(wrap_pyfunction!(create_new_database_adapter))?;

    Ok(())
}

#[pyfunction]
fn create_new_database_adapter(connection_string: String) -> PyResult<PyNewDatabaseAdapter> {
    Ok(PyNewDatabaseAdapter::new(connection_string)?)
}
```

### Adding New Performance Optimizations

#### Custom Caching Strategy

```rust
// crates/core/src/caching/custom_strategy.rs
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct CustomCacheStrategy {
    entries: HashMap<String, CacheEntry>,
    policy: CustomPolicy,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    data: Vec<u8>,
    access_count: u64,
    last_access: Instant,
    created: Instant,
}

#[derive(Debug, Clone)]
pub struct CustomPolicy {
    max_size: usize,
    ttl: Duration,
    access_threshold: u64,
}

impl CustomCacheStrategy {
    pub fn new(policy: CustomPolicy) -> Self {
        Self {
            entries: HashMap::new(),
            policy,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_count += 1;
            entry.last_access = Instant::now();

            if self.is_expired(entry) {
                self.entries.remove(key);
                return None;
            }

            Some(entry.data.clone())
        } else {
            None
        }
    }

    pub fn set(&mut self, key: String, data: Vec<u8>) {
        // Evict if necessary
        if self.entries.len() >= self.policy.max_size {
            self.evict_entries();
        }

        let entry = CacheEntry {
            data,
            access_count: 1,
            last_access: Instant::now(),
            created: Instant::now(),
        };

        self.entries.insert(key, entry);
    }

    pub fn invalidate(&mut self, key: &str) {
        self.entries.remove(key);
    }

    fn is_expired(&self, entry: &CacheEntry) -> bool {
        entry.created.elapsed() > self.policy.ttl
    }

    fn evict_entries(&mut self) {
        // Custom eviction logic
        let mut entries_to_remove: Vec<_> = self.entries
            .iter()
            .filter(|(_, entry)| {
                entry.access_count < self.policy.access_threshold ||
                self.is_expired(entry)
            })
            .map(|(key, _)| key.clone())
            .collect();

        // If still at capacity, remove least recently used
        if self.entries.len() - entries_to_remove.len() >= self.policy.max_size {
            let mut lru_entries: Vec<_> = self.entries
                .iter()
                .filter(|(key, _)| !entries_to_remove.contains(key))
                .collect();

            lru_entries.sort_by_key(|(_, entry)| entry.last_access);

            let remove_count = self.entries.len() - entries_to_remove.len() - self.policy.max_size + 1;
            for (key, _) in lru_entries.iter().take(remove_count) {
                entries_to_remove.push(key.clone());
            }
        }

        for key in entries_to_remove {
            self.entries.remove(&key);
        }
    }
}
```

### Adding New Data Types

#### Custom Delta Data Type

```rust
// crates/core/src/types/custom.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoPolygon {
    pub points: Vec<GeoPoint>,
}

// Extend DeltaDataType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExtendedDeltaDataType {
    // Existing types
    Primitive(PrimitiveType),
    Array {
        element_type: Box<ExtendedDeltaDataType>,
        contains_null: bool,
    },
    Struct(Vec<SchemaField>),
    Map {
        key_type: Box<ExtendedDeltaDataType>,
        value_type: Box<ExtendedDeltaDataType>,
        value_contains_null: bool,
    },

    // New types
    Geographic(GeoType),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GeoType {
    Point,
    Polygon,
    MultiPoint,
    MultiPolygon,
}

// Database-specific mapping
impl ExtendedDeltaDataType {
    pub fn to_database_type(&self, database_type: DatabaseType) -> String {
        match (self, database_type) {
            (ExtendedDeltaDataType::Geographic(GeoType::Point), DatabaseType::PostgreSQL) => {
                "POINT".to_string()
            }
            (ExtendedDeltaDataType::Geographic(GeoType::Polygon), DatabaseType::PostgreSQL) => {
                "POLYGON".to_string()
            }
            _ => self.to_string(),
        }
    }
}

// Python bindings
#[pyclass]
#[derive(Clone)]
pub struct PyGeoPoint {
    #[pyo3(get)]
    pub latitude: f64,
    #[pyo3(get)]
    pub longitude: f64,
}

#[pymethods]
impl PyGeoPoint {
    #[new]
    pub fn new(latitude: f64, longitude: f64) -> Self {
        Self { latitude, longitude }
    }

    pub fn distance_to(&self, other: &PyGeoPoint) -> f64 {
        // Haversine formula
        const EARTH_RADIUS: f64 = 6371.0; // kilometers

        let lat1_rad = self.latitude.to_radians();
        let lat2_rad = other.latitude.to_radians();
        let delta_lat = (other.latitude - self.latitude).to_radians();
        let delta_lon = (other.longitude - self.longitude).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2) +
                lat1_rad.cos() * lat2_rad.cos() *
                (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS * c
    }
}
```

## Testing

### Unit Tests

```rust
// crates/core/src/tests/mod.rs
#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn test_table_creation() {
        let adapter = create_test_adapter().await;
        let schema = create_test_schema();

        let result = adapter.create_table("test_table", &schema).await;
        assert!(result.is_ok());

        let table_handle = result.unwrap();
        assert_eq!(table_handle.name(), "test_table");
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let adapter = create_test_adapter().await;
        let table = create_test_table(&adapter).await;

        let actions = vec![
            DeltaAction::AddFile {
                path: "data.parquet".to_string(),
                size: 1024,
                modification_time: SystemTime::now(),
                data_change: true,
            }
        ];

        let result = adapter.commit_transaction(&actions).await;
        assert!(result.is_ok());

        let commit_info = result.unwrap();
        assert_eq!(commit_info.version, 0);
    }

    #[test]
    fn test_schema_serialization() {
        let schema = create_test_schema();
        let serialized = serde_json::to_string(&schema).unwrap();
        let deserialized: TableSchema = serde_json::from_str(&serialized).unwrap();

        assert_eq!(schema, deserialized);
    }

    fn create_test_schema() -> TableSchema {
        TableSchema::new(vec![
            SchemaField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
            SchemaField::new("name", DeltaDataType::Primitive(PrimitiveType::String), true),
            SchemaField::new("value", DeltaDataType::Primitive(PrimitiveType::Double), true),
        ])
    }

    async fn create_test_adapter() -> impl DeltaAdapter {
        // Use in-memory database for testing
        PostgresAdapter::new("postgresql://localhost/test_db").await.unwrap()
    }

    async fn create_test_table(adapter: &impl DeltaAdapter) -> TableHandle {
        let schema = create_test_schema();
        adapter.create_table("test_table", &schema).await.unwrap()
    }
}
```

### Integration Tests

```python
# crates/python/tests/test_integration.py
import pytest
import tempfile
import os
from deltalakedb import Table, SqlConfig

@pytest.fixture
def test_config():
    """Create test database configuration."""
    return SqlConfig(
        database_type="sqlite",
        connection_string="sqlite:///:memory:",
        table_prefix="test_"
    )

@pytest.fixture
def sample_table(test_config):
    """Create a sample table for testing."""
    from deltalakedb.types import TableSchema, SchemaField, DeltaDataType

    schema = TableSchema([
        SchemaField("id", DeltaDataType.primitive("long"), nullable=False),
        SchemaField("name", DeltaDataType.primitive("string"), nullable=True),
        SchemaField("value", DeltaDataType.primitive("double"), nullable=True),
    ])

    table = deltalakedb.create_table(
        "delta+sql://sqlite:///:memory:/test_data",
        schema,
        test_config
    )

    return table

class TestTableOperations:
    """Test table operations."""

    def test_table_creation(self, sample_table):
        """Test table creation."""
        snapshot = sample_table.get_version()
        assert snapshot.version == 0
        assert len(snapshot.files) == 0

    def test_schema_retrieval(self, sample_table):
        """Test schema retrieval."""
        metadata = sample_table.get_metadata()
        schema = metadata.schema

        assert len(schema.fields) == 3
        assert schema.get_field("id").name == "id"
        assert schema.get_field("id").data_type.type_name == "long"
        assert not schema.get_field("id").nullable

    def test_write_and_read(self, sample_table):
        """Test writing and reading data."""
        import pandas as pd

        data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [1.1, 2.2, 3.3]
        })

        writer = deltalakedb.write_operations.DeltaWriter(sample_table)
        result = writer.write(data)

        assert result.files_added > 0
        assert result.rows_added == 3

class TestMultiTableTransactions:
    """Test multi-table transactions."""

    def test_transaction_commit(self, test_config):
        """Test multi-table transaction commit."""
        from deltalakedb.multi_table_transactions import create_transaction_context

        table1 = Table("delta+sql://sqlite:///:memory:/table1")
        table2 = Table("delta+sql://sqlite:///:memory:/table2")

        transaction = create_transaction_context([table1, table2])

        # Add operations
        transaction.add_participant(table1, [{"operation": "test"}])
        transaction.add_participant(table2, [{"operation": "test"}])

        # Commit should succeed (mock implementation)
        assert transaction.prepare() is not None

class TestPerformanceOptimizations:
    """Test performance optimization features."""

    def test_lazy_loading(self, sample_table):
        """Test lazy loading functionality."""
        from deltalakedb.lazy_loading import LazyLoadingConfig, LoadingStrategy

        config = LazyLoadingConfig(
            strategy=LoadingStrategy.LAZY,
            chunk_size=100
        )

        manager = deltalakedb.lazy_loading.create_lazy_loading_manager(config)
        metadata = manager.load_table_metadata(sample_table)

        assert metadata is not None

    def test_caching(self, sample_table):
        """Test caching functionality."""
        from deltalakedb.caching import DeltaLakeCacheManager

        cache_manager = deltalakedb.caching.create_deltalake_cache_manager()

        # Cache some data
        metadata = sample_table.get_metadata()
        cache_manager.cache_table_metadata(sample_table, metadata)

        # Retrieve from cache
        cached = cache_manager.get(f"table:{sample_table.uri}:metadata")
        assert cached is not None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

### Performance Benchmarks

```python
# crates/python/tests/test_performance.py
import time
import pytest
import pandas as pd
from deltalakedb import Table, SqlConfig
from deltalakedb.caching import DeltaLakeCacheManager
from deltalakedb.memory_optimization import MemoryOptimizedFileList

class TestPerformance:
    """Performance tests and benchmarks."""

    def test_write_performance(self, sample_table):
        """Benchmark write performance."""
        # Create large dataset
        data = pd.DataFrame({
            'id': range(100000),
            'value': [i * 2 for i in range(100000)],
            'category': ['A', 'B', 'C'] * 33333 + ['A']
        })

        writer = deltalakedb.write_operations.DeltaWriter(sample_table)

        start_time = time.time()
        result = writer.write(data)
        duration = time.time() - start_time

        print(f"Write performance: {result.rows_added / duration:.0f} rows/second")

        # Performance assertions
        assert duration < 60  # Should complete within 60 seconds
        assert result.rows_added == 100000

    def test_cache_performance(self):
        """Benchmark cache performance."""
        cache_manager = DeltaLakeCacheManager()

        # Cache many entries
        start_time = time.time()
        for i in range(10000):
            cache_manager.set(f"key_{i}", f"value_{i}")
        cache_duration = time.time() - start_time

        # Retrieve entries
        start_time = time.time()
        hits = 0
        for i in range(10000):
            if cache_manager.get(f"key_{i}"):
                hits += 1
        retrieve_duration = time.time() - start_time

        print(f"Cache set: {10000 / cache_duration:.0f} ops/second")
        print(f"Cache get: {10000 / retrieve_duration:.0f} ops/second")
        print(f"Hit ratio: {hits / 10000:.2%}")

        # Performance assertions
        assert cache_duration < 5.0
        assert retrieve_duration < 5.0
        assert hits == 10000

    def test_memory_optimization(self):
        """Test memory optimization performance."""
        file_list = MemoryOptimizedFileList()

        # Add many files
        start_time = time.time()
        for i in range(50000):
            file_list.add_files([create_mock_file(i)])
        add_duration = time.time() - start_time

        # Test retrieval performance
        start_time = time.time()
        chunk = file_list.get_files(offset=0, limit=1000)
        retrieve_duration = time.time() - start_time

        # Check memory usage
        memory_stats = file_list.get_memory_usage()

        print(f"Add files: {50000 / add_duration:.0f} files/second")
        print(f"Retrieve chunk: {len(chunk) / retrieve_duration:.0f} files/second")
        print(f"Memory usage: {memory_stats.current_mb:.2f} MB")

        # Performance assertions
        assert add_duration < 30.0
        assert retrieve_duration < 1.0
        assert memory_stats.current_mb < 500  # Should use less than 500MB

def create_mock_file(index):
    """Create mock file for testing."""
    from deltalakedb.bindings import File

    return File(
        path=f"file_{index}.parquet",
        size=1024 * 1024,  # 1MB
        modification_time=time.time(),
        stats=f"{{\"num_records\": {index}}}",
        partition_values={}
    )

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
```

## Performance Profiling

### CPU Profiling

```python
# crates/python/examples/profiling.py
import cProfile
import pstats
from deltalakedb import Table, SqlConfig
import pandas as pd

def profile_table_operations():
    """Profile table operations."""
    config = SqlConfig(
        database_type="sqlite",
        connection_string="sqlite:///:memory:"
    )

    # Create table
    table = deltalakedb.create_table(
        "delta+sql://sqlite:///:memory:/profile_test",
        create_test_schema(),
        config
    )

    # Profile write operation
    data = pd.DataFrame({
        'id': range(10000),
        'value': [i * 2 for i in range(10000)]
    })

    profiler = cProfile.Profile()
    profiler.enable()

    writer = deltalakedb.write_operations.DeltaWriter(table)
    result = writer.write(data)

    profiler.disable()

    # Save profiling results
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(20)  # Top 20 functions

    return result

def create_test_schema():
    """Create test schema for profiling."""
    from deltalakedb.types import TableSchema, SchemaField, DeltaDataType

    return TableSchema([
        SchemaField("id", DeltaDataType.primitive("long"), nullable=False),
        SchemaField("value", DeltaDataType.primitive("double"), nullable=True),
    ])

if __name__ == "__main__":
    result = profile_table_operations()
    print(f"Profiled operation completed: {result.rows_added} rows")
```

### Memory Profiling

```python
# crates/python/examples/memory_profiling.py
import tracemalloc
import time
from deltalakedb.caching import DeltaLakeCacheManager
from deltalakedb.memory_optimization import MemoryOptimizedFileList

def profile_memory_usage():
    """Profile memory usage of different operations."""

    # Start memory tracking
    tracemalloc.start()

    # Profile caching
    print("Profiling cache memory usage...")
    cache_manager = DeltaLakeCacheManager()

    # Cache many large objects
    for i in range(1000):
        large_data = "x" * 10000  # 10KB string
        cache_manager.set(f"key_{i}", large_data)

    current, peak = tracemalloc.get_traced_memory()
    print(f"Cache memory usage: {current / 1024 / 1024:.2f} MB current, {peak / 1024 / 1024:.2f} MB peak")

    # Profile memory optimization
    print("\nProfiling file list memory usage...")
    file_list = MemoryOptimizedFileList()

    for i in range(10000):
        file_list.add_files([create_mock_file(i)])

    current, peak = tracemalloc.get_traced_memory()
    print(f"File list memory usage: {current / 1024 / 1024:.2f} MB current, {peak / 1024 / 1024:.2f} MB peak")

    # Get optimization statistics
    memory_stats = file_list.get_memory_usage()
    print(f"Optimization savings: {memory_stats.optimization_savings_mb:.2f} MB")

    tracemalloc.stop()

def create_mock_file(index):
    """Create mock file for memory profiling."""
    from deltalakedb.bindings import File

    return File(
        path=f"large_file_{index}.parquet",
        size=1024 * 1024 * 10,  # 10MB
        modification_time=time.time(),
        stats=f"{{\"num_records\": {index * 1000}, \"data\": \"{'x' * 1000}\"}}",
        partition_values={"category": f"cat_{index % 10}"}
    )

if __name__ == "__main__":
    profile_memory_usage()
```

## Debugging

### Debug Configuration

```python
# crates/python/src/debug.rs
use pyo3::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};

static DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);

#[pyfunction]
pub fn enable_debug_mode() {
    DEBUG_ENABLED.store(true, Ordering::SeqCst);
    env_logger::init();
}

#[pyfunction]
pub fn disable_debug_mode() {
    DEBUG_ENABLED.store(false, Ordering::SeqCst);
}

#[pyfunction]
pub fn is_debug_enabled() -> bool {
    DEBUG_ENABLED.load(Ordering::SeqCst)
}

// Debug logging macro
macro_rules! debug_log {
    ($($arg:tt)*) => {
        if crate::debug::is_debug_enabled() {
            log::debug!($($arg)*);
        }
    };
}

pub(crate) use debug_log;

// Python bindings for debug utilities
#[pymodule]
fn debug_utils(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(enable_debug_mode))?;
    m.add_function(wrap_pyfunction!(disable_debug_mode))?;
    m.add_function(wrap_pyfunction!(is_debug_enabled))?;

    Ok(())
}
```

### Debug Tools

```python
# crates/python/examples/debug_tools.py
import time
import traceback
from deltalakedb import Table, SqlConfig
from deltalakedb.debug_utils import enable_debug_mode

def debug_table_operations():
    """Debug table operations with detailed logging."""

    # Enable debug mode
    enable_debug_mode()

    try:
        config = SqlConfig(
            database_type="sqlite",
            connection_string="sqlite:///:memory:"
        )

        print("Creating table...")
        table = deltalakedb.create_table(
            "delta+sql://sqlite:///:memory:/debug_test",
            create_test_schema(),
            config
        )
        print("Table created successfully")

        print("Writing data...")
        import pandas as pd
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [1.1, 2.2, 3.3]
        })

        writer = deltalakedb.write_operations.DeltaWriter(table)
        result = writer.write(data)
        print(f"Write completed: {result.rows_added} rows")

        print("Reading data...")
        snapshot = table.get_version()
        print(f"Read completed: {len(snapshot.files)} files")

    except Exception as e:
        print(f"Error occurred: {e}")
        traceback.print_exc()
        return False

    return True

def create_test_schema():
    """Create test schema."""
    from deltalakedb.types import TableSchema, SchemaField, DeltaDataType

    return TableSchema([
        SchemaField("id", DeltaDataType.primitive("long"), nullable=False),
        SchemaField("value", DeltaDataType.primitive("double"), nullable=True),
    ])

if __name__ == "__main__":
    success = debug_table_operations()
    print(f"Debug session completed: {'Success' if success else 'Failed'}")
```

## Contributing Guidelines

### Code Style

#### Rust

```rust
// Use rustfmt for formatting
// Use clippy for linting

// Naming conventions
// - Functions: snake_case
// - Types: PascalCase
// - Constants: SCREAMING_SNAKE_CASE
// - Modules: snake_case

// Error handling
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("Table not found: {name}")]
    TableNotFound { name: String },

    #[error("Invalid schema: {reason}")]
    InvalidSchema { reason: String },

    #[error("Database error: {source}")]
    DatabaseError {
        #[from]
        source: sqlx::Error,
    },
}

// Documentation comments
/// Represents a Delta Lake table with SQL backend storage.
///
/// # Examples
///
/// ```rust
/// let table = Table::new("delta+sql://postgresql://user:pass@localhost/db/table");
/// let snapshot = table.get_version().await?;
/// ```
pub struct Table {
    name: String,
    adapter: Arc<dyn DeltaAdapter>,
}
```

#### Python

```python
# Use black for formatting
# Use flake8 for linting
# Use mypy for type checking

# Naming conventions
# - Functions: snake_case
# - Classes: PascalCase
# - Constants: UPPER_CASE
# - Private members: _underscore_prefix

# Type hints
from typing import List, Dict, Optional, Union
import pandas as pd

def process_data(
    data: pd.DataFrame,
    config: Dict[str, str],
    options: Optional[List[str]] = None
) -> pd.DataFrame:
    """Process data with given configuration.

    Args:
        data: Input DataFrame to process
        config: Configuration dictionary
        options: Optional list of processing options

    Returns:
        Processed DataFrame

    Raises:
        ValueError: If configuration is invalid
    """
    if not config:
        raise ValueError("Configuration cannot be empty")

    # Implementation
    return data

# Docstring format: Google style or NumPy style
class TableProcessor:
    """Processes Delta Lake tables with various operations.

    Attributes:
        table: The Delta table to process
        config: Processing configuration

    Example:
        >>> processor = TableProcessor(table, config)
        >>> result = processor.transform_data()
    """

    def __init__(self, table: Table, config: Dict[str, Any]) -> None:
        """Initialize TableProcessor.

        Args:
            table: Delta table instance
            config: Configuration dictionary
        """
        self.table = table
        self.config = config
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration."""
        required_keys = ['operation', 'output_format']
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")
```

### Testing Guidelines

#### Test Organization

```
crates/python/tests/
├── unit/                    # Unit tests
│   ├── test_types.py
│   ├── test_connection.py
│   └── test_transactions.py
├── integration/             # Integration tests
│   ├── test_end_to_end.py
│   ├── test_database_ops.py
│   └── test_performance.py
├── fixtures/               # Test data and fixtures
│   ├── sample_data.json
│   └── test_schemas.py
└── conftest.py             # Pytest configuration
```

#### Test Writing Guidelines

```python
# Use descriptive test names
def test_multi_table_transaction_rollback_on_participant_failure():
    """Test that transaction rolls back when a participant fails."""
    pass

# Use fixtures for setup
@pytest.fixture
def sample_table():
    """Create a sample table for testing."""
    config = SqlConfig(database_type="sqlite", connection_string="sqlite:///:memory:")
    schema = create_test_schema()
    return deltalakedb.create_table("delta+sql://sqlite:///:memory:/test", schema, config)

# Use parameterized tests
@pytest.mark.parametrize("database_type", ["postgresql", "mysql", "sqlite"])
def test_table_creation_different_databases(database_type):
    """Test table creation with different database types."""
    config = get_test_config(database_type)
    # Test implementation

# Test both success and failure cases
def test_transaction_success_and_failure():
    """Test both successful and failed transactions."""
    # Test success case
    assert transaction.commit().success

    # Test failure case
    with pytest.raises(TransactionError):
        failing_transaction.commit()

# Use assertions that provide clear error messages
assert result.row_count == expected_count, f"Expected {expected_count}, got {result.row_count}"
assert "error" in str(exception.value), f"Expected error message, got: {exception.value}"
```

### Pull Request Process

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Make changes** with appropriate tests
4. **Run the test suite**: `pytest && cargo test`
5. **Update documentation** if needed
6. **Submit pull request** with:
   - Clear description of changes
   - Links to related issues
   - Test coverage report
   - Performance impact analysis

## Code Examples

### Complete Example: E-commerce Data Pipeline

```python
#!/usr/bin/env python3
"""Complete e-commerce data pipeline example."""

import pandas as pd
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict

from deltalakedb import Table, SqlConfig
from deltalakedb.types import TableSchema, SchemaField, DeltaDataType
from deltalakedb.multi_table_transactions import create_transaction_context
from deltalakedb.caching import DeltaLakeCacheManager
from deltalakedb.async_io import AsyncIOExecutor

class EcommerceDataPipeline:
    """E-commerce data processing pipeline."""

    def __init__(self, config: SqlConfig):
        self.config = config
        self.cache_manager = DeltaLakeCacheManager()
        self.async_executor = AsyncIOExecutor()

        # Initialize tables
        self.customers_table = None
        self.orders_table = None
        self.products_table = None
        self.analytics_table = None

    async def initialize(self):
        """Initialize pipeline tables."""
        print("Initializing pipeline...")

        # Create tables
        self.customers_table = await self._create_customers_table()
        self.orders_table = await self._create_orders_table()
        self.products_table = await self._create_products_table()
        self.analytics_table = await self._create_analytics_table()

        print("Pipeline initialized successfully")

    async def _create_customers_table(self) -> Table:
        """Create customers table."""
        schema = TableSchema([
            SchemaField("customer_id", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("name", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("email", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("created_at", DeltaDataType.primitive("timestamp"), nullable=False),
            SchemaField("last_active", DeltaDataType.primitive("timestamp"), nullable=True),
            SchemaField("metadata", DeltaDataType.struct([
                SchemaField("source", DeltaDataType.primitive("string")),
                SchemaField("campaign", DeltaDataType.primitive("string")),
            ]), nullable=True),
        ])

        return deltalakedb.create_table(
            "delta+sql://postgresql://localhost/ecommerce/customers",
            schema,
            self.config
        )

    async def _create_orders_table(self) -> Table:
        """Create orders table."""
        schema = TableSchema([
            SchemaField("order_id", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("customer_id", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("order_date", DeltaDataType.primitive("timestamp"), nullable=False),
            SchemaField("total_amount", DeltaDataType.primitive("double"), nullable=False),
            SchemaField("status", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("items", DeltaDataType.array(
                DeltaDataType.struct([
                    SchemaField("product_id", DeltaDataType.primitive("string")),
                    SchemaField("quantity", DeltaDataType.primitive("integer")),
                    SchemaField("price", DeltaDataType.primitive("double")),
                ])
            ), nullable=False),
        ])

        return deltalakedb.create_table(
            "delta+sql://postgresql://localhost/ecommerce/orders",
            schema,
            self.config
        )

    async def _create_products_table(self) -> Table:
        """Create products table."""
        schema = TableSchema([
            SchemaField("product_id", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("name", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("category", DeltaDataType.primitive("string"), nullable=False),
            SchemaField("price", DeltaDataType.primitive("double"), nullable=False),
            SchemaField("inventory", DeltaDataType.primitive("integer"), nullable=False),
        ])

        return deltalakedb.create_table(
            "delta+sql://postgresql://localhost/ecommerce/products",
            schema,
            self.config
        )

    async def _create_analytics_table(self) -> Table:
        """Create analytics table."""
        schema = TableSchema([
            SchemaField("date", DeltaDataType.primitive("date"), nullable=False),
            SchemaField("total_orders", DeltaDataType.primitive("long"), nullable=False),
            SchemaField("total_revenue", DeltaDataType.primitive("double"), nullable=False),
            SchemaField("unique_customers", DeltaDataType.primitive("long"), nullable=False),
            SchemaField("avg_order_value", DeltaDataType.primitive("double"), nullable=False),
        ])

        return deltalakedb.create_table(
            "delta+sql://postgresql://localhost/ecommerce/analytics",
            schema,
            self.config
        )

    async def process_orders_batch(self, orders_data: List[Dict]) -> None:
        """Process a batch of orders with transaction guarantees."""
        print(f"Processing {len(orders_data)} orders...")

        # Convert to DataFrame
        orders_df = pd.DataFrame(orders_data)

        # Extract customer and product updates
        customer_updates = self._extract_customer_updates(orders_df)
        product_updates = self._extract_product_updates(orders_df)

        # Create multi-table transaction
        transaction = create_transaction_context(
            tables=[self.orders_table, self.customers_table, self.products_table],
            timeout_seconds=300
        )

        try:
            # Add operations to transaction
            transaction.add_participant(self.orders_table, [
                {"operation": "write", "data": orders_df}
            ])

            if customer_updates:
                transaction.add_participant(self.customers_table, [
                    {"operation": "upsert", "data": customer_updates}
                ])

            if product_updates:
                transaction.add_participant(self.products_table, [
                    {"operation": "update", "data": product_updates}
                ])

            # Commit transaction
            result = transaction.commit()

            if result.success:
                print(f"Successfully processed {len(orders_data)} orders")
                # Update cache
                self.cache_manager.invalidate_pattern("analytics:*")
            else:
                print(f"Failed to process orders: {result.error_message}")

        except Exception as e:
            print(f"Error processing orders: {e}")
            transaction.rollback()

    async def generate_analytics(self, date_range: tuple) -> None:
        """Generate analytics for given date range."""
        start_date, end_date = date_range
        print(f"Generating analytics for {start_date} to {end_date}")

        # Use async operations for better performance
        queries = [
            f"SELECT COUNT(*) as total_orders, SUM(total_amount) as total_revenue "
            f"FROM orders WHERE order_date >= '{start_date}' AND order_date <= '{end_date}'",

            f"SELECT COUNT(DISTINCT customer_id) as unique_customers "
            f"FROM orders WHERE order_date >= '{start_date}' AND order_date <= '{end_date}'",
        ]

        results = await asyncio.gather(*[
            self.async_executor.execute_query_async(query) for query in queries
        ])

        total_orders = results[0][0]['total_orders']
        total_revenue = results[0][0]['total_revenue'] or 0.0
        unique_customers = results[1][0]['unique_customers']
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0.0

        # Create analytics record
        analytics_data = pd.DataFrame([{
            'date': end_date,
            'total_orders': total_orders,
            'total_revenue': total_revenue,
            'unique_customers': unique_customers,
            'avg_order_value': avg_order_value,
        }])

        # Write analytics data
        writer = deltalakedb.write_operations.DeltaWriter(self.analytics_table)
        result = writer.write(analytics_data, mode=WriteMode.APPEND)

        print(f"Analytics generated: {result.rows_added} records")

    def _extract_customer_updates(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Extract customer updates from orders data."""
        # Get latest order per customer
        latest_orders = orders_df.groupby('customer_id').last().reset_index()

        customer_updates = pd.DataFrame({
            'customer_id': latest_orders['customer_id'],
            'last_active': latest_orders['order_date'],
        })

        return customer_updates

    def _extract_product_updates(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Extract product inventory updates from orders data."""
        # Sum quantities by product
        inventory_updates = []

        for _, order in orders_df.iterrows():
            for item in order['items']:
                inventory_updates.append({
                    'product_id': item['product_id'],
                    'quantity_change': -item['quantity'],  # Subtract from inventory
                })

        if inventory_updates:
            updates_df = pd.DataFrame(inventory_updates)
            return updates_df.groupby('product_id').sum().reset_index()

        return pd.DataFrame()

    async def run_pipeline(self):
        """Run the complete pipeline."""
        print("Starting e-commerce data pipeline...")

        # Generate sample data
        orders_data = self._generate_sample_orders(1000)

        # Process orders
        await self.process_orders_batch(orders_data)

        # Generate analytics for today
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        await self.generate_analytics((yesterday.isoformat(), today.isoformat()))

        print("Pipeline completed successfully")

    def _generate_sample_orders(self, count: int) -> List[Dict]:
        """Generate sample order data for testing."""
        import random

        orders = []
        customers = [f"customer_{i}" for i in range(1, 101)]  # 100 customers
        products = [f"product_{i}" for i in range(1, 51)]     # 50 products

        for i in range(count):
            order = {
                "order_id": f"order_{i+1}",
                "customer_id": random.choice(customers),
                "order_date": datetime.now() - timedelta(hours=random.randint(0, 24)),
                "total_amount": round(random.uniform(10.0, 500.0), 2),
                "status": random.choice(["pending", "completed", "shipped"]),
                "items": [
                    {
                        "product_id": random.choice(products),
                        "quantity": random.randint(1, 5),
                        "price": round(random.uniform(10.0, 100.0), 2),
                    }
                    for _ in range(random.randint(1, 4))
                ]
            }
            orders.append(order)

        return orders

async def main():
    """Main function."""
    # Configuration
    config = SqlConfig(
        database_type="postgresql",
        connection_string="postgresql://localhost/ecommerce",
        table_prefix="delta_"
    )

    # Create and run pipeline
    pipeline = EcommerceDataPipeline(config)
    await pipeline.initialize()
    await pipeline.run_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
```

## Best Practices

### Performance Best Practices

1. **Use Connection Pooling**: Always use connection pools for database access
2. **Batch Operations**: Process data in batches rather than row-by-row
3. **Caching Strategy**: Cache frequently accessed metadata and query results
4. **Async Operations**: Use async I/O for concurrent database operations
5. **Memory Management**: Use memory optimization for large datasets

### Error Handling Best Practices

```python
# Use specific exception types
from deltalakedb.error import DeltaLakeError, TransactionError

def robust_operation():
    try:
        # Delta Lake operation
        result = perform_operation()
        return result
    except TransactionError as e:
        # Handle transaction-specific errors
        logger.error(f"Transaction failed: {e}")
        rollback_transaction()
        raise
    except DeltaLakeError as e:
        # Handle general Delta Lake errors
        logger.error(f"Delta Lake operation failed: {e}")
        raise
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error: {e}")
        raise

# Use retry logic for transient failures
import time
from functools import wraps

def retry_on_transient_error(max_retries=3, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, TimeoutError) as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator

@retry_on_transient_error(max_retries=3)
def database_operation():
    return perform_database_query()
```

### Security Best Practices

1. **Parameterized Queries**: Always use parameterized queries to prevent SQL injection
2. **Connection Security**: Use SSL/TLS for database connections
3. **Access Control**: Implement proper authentication and authorization
4. **Audit Logging**: Log all data access and modifications
5. **Secrets Management**: Use environment variables or secret managers for credentials

### Testing Best Practices

1. **Test Coverage**: Aim for high test coverage (80%+)
2. **Integration Tests**: Test database interactions with real databases
3. **Performance Tests**: Include performance benchmarks in CI/CD
4. **Property-Based Testing**: Use property-based testing for complex logic
5. **Test Isolation**: Ensure tests are independent and don't share state

This comprehensive developer guide provides everything needed to extend, test, debug, and contribute to the DeltaLake DB Python library, with detailed examples and best practices for building high-performance data applications.