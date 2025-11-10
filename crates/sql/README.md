# Multi-Table Transactions for Delta Lake SQL Backend

This document provides comprehensive documentation for the multi-table transaction functionality in the deltalakedb-sql crate.

## Overview

The multi-table transaction system enables atomic operations across multiple Delta tables while maintaining ACID properties and cross-table consistency. This is particularly useful for scenarios like:

- **Multi-table data pipelines**: Coordinating data ingestion across related tables
- **Schema migrations**: Applying consistent schema changes across multiple tables
- **Data archiving**: Moving data between tables atomically
- **Complex ETL operations**: Ensuring consistency during multi-table transformations

## Key Features

### 🔄 Atomic Operations
- All-or-nothing commits across multiple tables
- Automatic rollback on failures
- Cross-table consistency validation

### 🔒 Isolation Levels
- **ReadCommitted**: Reads see committed data (default)
- **RepeatableRead**: Consistent snapshot throughout transaction
- **Serializable**: Full serializability with conflict detection

### 🛡️ Safety Features
- Deadlock detection and resolution
- Comprehensive validation
- Configurable limits and timeouts
- Detailed error reporting

### ⚡ Performance Optimizations
- Configurable validation levels
- Batch processing for large transactions
- Efficient memory usage
- Performance monitoring

## Quick Start

### Basic Usage

```rust
use deltalakedb_sql::multi_table::{MultiTableWriter, MultiTableConfig};
use deltalakedb_core::writer::TxnLogWriterExt;

// Create writer with default configuration
let config = MultiTableConfig::default();
let writer = MultiTableWriter::new(connection, None, config);

// Begin transaction
let mut tx = writer.begin_transaction();

// Add files to multiple tables
tx.add_files("customers".to_string(), 0, customer_files)?;
tx.add_files("orders".to_string(), 0, order_files)?;

// Commit atomically
let result = writer.commit_transaction(tx).await?;
```

### Builder Pattern

```rust
use deltalakedb_sql::multi_table::TableActionsBuilder;

let table_actions = TableActionsBuilder::new("customers".to_string(), 5)
    .add_files(new_files)
    .remove_files(old_files)
    .update_metadata(new_metadata)
    .with_operation("MERGE".to_string())
    .build_and_validate()?;

let mut tx = writer.begin_transaction();
tx.stage_actions(table_actions)?;
```

## Configuration

### Default Configuration

```rust
let config = MultiTableConfig::default();
```

Default settings provide a good balance between safety and performance:
- Consistency validation: Enabled
- Ordered mirroring: Disabled
- Deadlock detection: Enabled
- Max tables per transaction: 100
- Max actions per transaction: 10,000
- Transaction timeout: 1 hour

### High-Performance Configuration

```rust
let high_perf_config = MultiTableConfig {
    enable_consistency_validation: false,  // Skip for speed
    enable_ordered_mirroring: false,       // Skip for speed
    max_actions_per_transaction: 50000,     // Larger batches
    max_transaction_age_seconds: 7200,      // Longer timeout
    ..Default::default()
};
```

### Safety-First Configuration

```rust
let safety_config = MultiTableConfig {
    enable_consistency_validation: true,   // Enable all checks
    enable_ordered_mirroring: true,        // Enable mirroring
    max_tables_per_transaction: 10,         // Limit scope
    max_actions_per_transaction: 1000,      // Smaller batches
    max_retry_attempts: 5,                  // More retries
    ..Default::default()
};
```

## API Reference

### Core Types

#### `MultiTableWriter`
Main interface for multi-table operations.

```rust
pub struct MultiTableWriter {
    pub connection: Arc<DatabaseConnection>,
    pub mirror_engine: Option<Arc<dyn MirrorEngine>>,
    pub config: MultiTableConfig,
    pub single_writer: SqlTxnLogWriter,
}
```

**Key Methods:**
- `begin_transaction()` - Start a new transaction
- `commit_transaction(tx)` - Commit a transaction atomically
- `rollback_transaction(tx)` - Rollback a transaction
- `get_table_version(table_id)` - Get current table version
- `validate_tables_exist(tx)` - Validate all tables exist

#### `MultiTableTransaction`
Represents a transaction spanning multiple tables.

```rust
pub struct MultiTableTransaction {
    pub transaction_id: String,
    pub staged_tables: HashMap<String, TableActions>,
    pub state: TransactionState,
    pub isolation_level: TransactionIsolationLevel,
    pub priority: i32,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Key Methods:**
- `add_files(table_id, version, files)` - Add files to a table
- `remove_files(table_id, version, files)` - Remove files from a table
- `mixed_operation(table_id, version, adds, removes)` - Mixed add/remove
- `validate()` - Validate transaction state
- `summary()` - Get transaction summary

#### `TableActions`
Actions for a specific table within a transaction.

```rust
pub struct TableActions {
    pub table_id: String,
    pub expected_version: i64,
    pub actions: Vec<DeltaAction>,
    pub operation: Option<String>,
    pub operation_params: Option<HashMap<String, String>>,
}
```

#### `TableActionsBuilder`
Fluent API for building table actions.

```rust
let actions = TableActionsBuilder::new("table".to_string(), version)
    .add_files(files)
    .remove_files(old_files)
    .update_metadata(metadata)
    .with_operation("MERGE".to_string())
    .build_and_validate()?;
```

### Error Types

#### `ConsistencyViolation`
Cross-table consistency validation errors.

```rust
pub enum ConsistencyViolation {
    TableNotFound { table_id: String },
    VersionMismatch { table_id: String, expected: i64, actual: i64 },
    EmptyActionList { table_id: String },
    DuplicateFile { table_id: String, path: String },
    TooManyTables { count: usize, max: usize },
    TransactionTooLarge { action_count: usize, max_actions: usize },
    TableTransactionTooLarge { table_id: String, action_count: usize, max_actions: usize },
    TransactionTooOld { created_at: DateTime<Utc>, max_age_seconds: i64 },
}
```

## Use Cases

### 1. Multi-Table Data Ingestion

```rust
async fn ingest_customer_data(
    writer: &MultiTableWriter,
    customer_files: Vec<AddFile>,
    order_files: Vec<AddFile>,
) -> TxnLogResult<()> {
    let mut tx = writer.begin_transaction();
    
    // Add customer data
    tx.add_files("customers".to_string(), 0, customer_files)?;
    
    // Add corresponding order data
    tx.add_files("orders".to_string(), 0, order_files)?;
    
    // Commit atomically - both tables succeed or both fail
    writer.commit_transaction(tx).await?;
    
    Ok(())
}
```

### 2. Schema Migration

```rust
async fn migrate_schema(
    writer: &MultiTableWriter,
    table_ids: Vec<String>,
    new_metadata: Metadata,
) -> TxnLogResult<()> {
    let mut tx = writer.begin_transaction();
    
    // Update metadata for all tables atomically
    for table_id in table_ids {
        let version = writer.get_table_version(&table_id).await?;
        tx.update_metadata(table_id, version, new_metadata.clone())?;
    }
    
    writer.commit_transaction(tx).await?;
    Ok(())
}
```

### 3. Data Archiving

```rust
async fn archive_old_data(
    writer: &MultiTableWriter,
    old_files: Vec<RemoveFile>,
    archive_files: Vec<AddFile>,
) -> TxnLogResult<()> {
    let mut tx = writer.begin_transaction();
    
    // Remove from active table
    tx.remove_files("active_data".to_string(), 0, old_files)?;
    
    // Add to archive table
    tx.add_files("archive_data".to_string(), 0, archive_files)?;
    
    writer.commit_transaction(tx).await?;
    Ok(())
}
```

### 4. Complex ETL Operations

```rust
async fn complex_etl(
    writer: &MultiTableWriter,
    source_data: Vec<AddFile>,
    transformed_data: Vec<AddFile>,
    audit_data: Vec<AddFile>,
) -> TxnLogResult<()> {
    let mut tx = writer.begin_transaction();
    
    // Stage raw data
    tx.add_files("raw_staging".to_string(), 0, source_data)?;
    
    // Stage transformed data
    tx.add_files("processed_data".to_string(), 0, transformed_data)?;
    
    // Stage audit trail
    tx.add_files("audit_log".to_string(), 0, audit_data)?;
    
    writer.commit_transaction(tx).await?;
    Ok(())
}
```

## Performance Considerations

### Transaction Size Limits

- **Default**: 100 tables, 10,000 actions per transaction
- **Recommended**: Keep transactions under 1,000 actions for optimal performance
- **Large transactions**: Consider splitting into multiple smaller transactions

### Memory Usage

- Each action consumes memory proportional to its size
- Large file metadata can increase memory usage significantly
- Monitor memory usage for transactions with many actions

### Validation Overhead

- Consistency validation adds ~10-20% overhead
- Can be disabled for performance-critical scenarios
- Recommended to keep validation enabled in production

### Isolation Level Performance

- **ReadCommitted**: Best performance, suitable for most use cases
- **RepeatableRead**: ~15-25% overhead, good for long-running transactions
- **Serializable**: ~30-50% overhead, use only when required

## Monitoring and Observability

### Transaction Metrics

```rust
// Get transaction summary
let summary = tx.summary();
println!("Transaction: {}", summary.transaction_id);
println!("Tables: {}", summary.table_count);
println!("Actions: {}", summary.total_action_count);
println!("State: {:?}", summary.state);
```

### Table Action Metrics

```rust
// Get table action summary
let table_summary = table_actions.summary();
println!("Table: {}", table_summary.table_id);
println!("Adds: {}", table_summary.add_count);
println!("Removes: {}", table_summary.remove_count);
println!("Metadata: {}", table_summary.metadata_count);
```

### Mirroring Status

```rust
// Check mirroring status
let status = writer.get_mirroring_status("table", version).await?;
if let Some(info) = status {
    println!("Status: {}", info.status);
    println!("Attempts: {}", info.attempts);
    if let Some(error) = info.error {
        println!("Error: {}", error);
    }
}
```

## Best Practices

### 1. Transaction Design

- Keep transactions focused and small
- Group related operations together
- Avoid long-running transactions
- Use appropriate isolation levels

### 2. Error Handling

- Always handle transaction errors appropriately
- Implement retry logic for transient failures
- Log transaction failures for debugging
- Use validation to catch errors early

### 3. Performance Optimization

- Use builder pattern for complex operations
- Batch similar operations together
- Configure appropriate limits
- Monitor performance metrics

### 4. Testing

- Test transaction rollback scenarios
- Validate error handling paths
- Test with different isolation levels
- Include performance testing

## Troubleshooting

### Common Issues

#### Transaction Timeout
```
Error: Transaction too old: created at 2024-01-01T10:00:00Z, max age: 3600 seconds
```
**Solution**: Increase `max_transaction_age_seconds` or optimize transaction performance.

#### Too Many Tables
```
Error: Too many tables in transaction: 150 (max: 100)
```
**Solution**: Split into multiple transactions or increase `max_tables_per_transaction`.

#### Version Mismatch
```
Error: Version mismatch for table 'customers': expected 5, actual 6
```
**Solution**: Refresh table version before staging actions or use optimistic concurrency.

#### Deadlock Detection
```
Warning: Deadlock detected between tx-123 and tx-456, resolving by priority
```
**Solution**: Review transaction ordering or adjust priorities.

### Debugging Tips

1. **Enable detailed logging**:
   ```rust
   use tracing::Level;
   tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
   ```

2. **Use transaction summaries** for debugging:
   ```rust
   println!("Transaction summary: {:?}", tx.summary());
   ```

3. **Validate before committing**:
   ```rust
   tx.validate()?; // Catch errors early
   ```

4. **Monitor mirroring status**:
   ```rust
   let status = writer.get_mirroring_status(table, version).await?;
   ```

## Examples

See the `examples/multi_table_transactions.rs` file for comprehensive examples covering:

- Basic multi-table transactions
- Builder pattern usage
- Different isolation levels
- Error handling and validation
- Configuration and performance tuning
- Concurrent transactions
- Mirroring and replication

Run examples with:
```bash
cargo run --example multi_table_transactions
```

## Testing

The multi-table transaction system includes comprehensive tests:

```bash
# Run all multi-table tests
cargo test multi_table

# Run specific test categories
cargo test multi_table::concurrent_acid_tests
cargo test multi_table::isolation_test
cargo test multi_table::benchmarks
```

## Contributing

When contributing to the multi-table transaction system:

1. Add comprehensive tests for new features
2. Update documentation and examples
3. Consider performance implications
4. Ensure backward compatibility
5. Follow existing code patterns and conventions

## License

This project is licensed under the Apache License 2.0.