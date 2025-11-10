//! Performance benchmarks for multi-table transaction operations.
//!
//! This module provides comprehensive benchmarks for measuring the performance
//! of multi-table transactions under various scenarios and load conditions.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use deltalakedb_sql::multi_table::{
    MultiTableConfig, MultiTableWriter, MultiTableTransaction, TableActions,
};
use deltalakedb_core::{
    transaction::{TransactionIsolationLevel, TransactionState},
    actions::{AddFile, RemoveFile, Metadata, Protocol, DeltaAction},
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Creates a test database and multi-table writer for benchmarking
fn setup_benchmark_writer() -> (TempDir, MultiTableWriter) {
    let temp_dir = tempfile::tempdir().unwrap();
    
    let config = deltalakedb_sql::connection::DatabaseConfig {
        url: format!("sqlite:{}", temp_dir.path().join("bench.db").to_string_lossy()),
        max_connections: 10,
        min_connections: 2,
        connect_timeout_secs: 30,
        idle_timeout_secs: 300,
    };
    
    let rt = Runtime::new().unwrap();
    let connection = rt.block_on(config.connect()).unwrap();
    let multi_config = MultiTableConfig {
        enable_consistency_validation: false, // Disable for pure performance
        enable_ordered_mirroring: false,
        default_isolation_level: TransactionIsolationLevel::ReadCommitted,
        max_tables_per_transaction: 100,
        max_actions_per_transaction: 10000,
        max_actions_per_table: 1000,
        max_transaction_age_seconds: 300,
        max_retry_attempts: 3,
        retry_base_delay_ms: 10,
        retry_max_delay_ms: 100,
    };
    
    let writer = MultiTableWriter::new(
        connection,
        None, // No mirror engine for benchmarks
        multi_config,
    );
    
    (temp_dir, writer)
}

/// Creates test AddFile actions for benchmarking
fn create_test_add_files(count: usize) -> Vec<AddFile> {
    (0..count).map(|i| AddFile {
        path: format!("bench_file_{}.parquet", i),
        size: 1024 * 1024, // 1MB files
        modification_time: 1234567890 + i as i64,
        data_change: true,
        partition_values: None,
        tags: HashMap::new(),
    }).collect()
}

/// Creates test RemoveFile actions for benchmarking
fn create_test_remove_files(count: usize) -> Vec<RemoveFile> {
    (0..count).map(|i| RemoveFile {
        path: format!("bench_file_{}.parquet", i),
        deletion_timestamp: Some(1234567890 + i as i64),
        data_change: true,
        extended_file_metadata: None,
        partition_values: None,
        tags: HashMap::new(),
    }).collect()
}

/// Benchmarks single-table transaction performance with varying action counts
fn bench_single_table_transactions(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("single_table_transactions");
    
    for action_count in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("commit_actions", action_count),
            action_count,
            |b, &action_count| {
                b.to_async(&rt).iter(|| {
                    let tx = writer.begin_transaction();
                    let files = create_test_add_files(action_count);
                    
                    rt.block_on(async {
                        let table_actions = TableActions::new("bench_table".to_string(), 0)
                            .with_operation("BENCHMARK".to_string())
                            .with_files(files);
                        
                        black_box(writer.commit_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks multi-table transaction performance with varying table counts
fn bench_multi_table_transactions(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("multi_table_transactions");
    
    for table_count in [2, 5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::new("commit_multi_table", table_count),
            table_count,
            |b, &table_count| {
                b.to_async(&rt).iter(|| {
                    let mut tx = writer.begin_transaction();
                    
                    rt.block_on(async {
                        // Add actions for each table
                        for i in 0..table_count {
                            let table_id = format!("bench_table_{}", i);
                            let files = create_test_add_files(10); // 10 files per table
                            
                            tx = tx.add_table_actions(
                                table_id,
                                TableActions::new(table_id, 0)
                                    .with_operation("BENCHMARK".to_string())
                                    .with_files(files)
                            ).await.unwrap();
                        }
                        
                        black_box(writer.commit_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks transaction creation overhead
fn bench_transaction_creation(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    c.bench_function("transaction_creation", |b| {
        b.to_async(&rt).iter(|| {
            black_box(writer.begin_transaction())
        });
    });
}

/// Benchmarks table actions creation overhead
fn bench_table_actions_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_actions_creation");
    
    for action_count in [10, 50, 100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::new("create_actions", action_count),
            action_count,
            |b, &action_count| {
                let files = create_test_add_files(action_count);
                
                b.iter(|| {
                    black_box(
                        TableActions::new("bench_table".to_string(), 0)
                            .with_operation("BENCHMARK".to_string())
                            .with_files(files.clone())
                    )
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks validation performance with different consistency levels
fn bench_consistency_validation(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("consistency_validation");
    
    for validation_enabled in [true, false].iter() {
        group.bench_with_input(
            BenchmarkId::new("transaction_commit", validation_enabled),
            validation_enabled,
            |b, &validation_enabled| {
                // Create writer with appropriate validation setting
                let config = MultiTableConfig {
                    enable_consistency_validation: *validation_enabled,
                    enable_ordered_mirroring: false,
                    default_isolation_level: TransactionIsolationLevel::ReadCommitted,
                    max_tables_per_transaction: 10,
                    max_actions_per_transaction: 1000,
                    max_actions_per_table: 100,
                    max_transaction_age_seconds: 300,
                    max_retry_attempts: 3,
                    retry_base_delay_ms: 10,
                    retry_max_delay_ms: 100,
                };
                
                let (_temp_dir, validation_writer) = setup_benchmark_writer();
                let validation_writer = MultiTableWriter::new(
                    validation_writer.connection.clone(),
                    None,
                    config,
                );
                
                b.to_async(&rt).iter(|| {
                    let tx = validation_writer.begin_transaction();
                    let files = create_test_add_files(50);
                    
                    rt.block_on(async {
                        let table_actions = TableActions::new("bench_table".to_string(), 0)
                            .with_operation("BENCHMARK".to_string())
                            .with_files(files);
                        
                        black_box(validation_writer.commit_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks different isolation levels
fn bench_isolation_levels(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("isolation_levels");
    
    for isolation_level in [
        TransactionIsolationLevel::ReadCommitted,
        TransactionIsolationLevel::RepeatableRead,
        TransactionIsolationLevel::Serializable,
    ].iter() {
        group.bench_with_input(
            BenchmarkId::new("transaction_commit", format!("{:?}", isolation_level)),
            isolation_level,
            |b, &isolation_level| {
                b.to_async(&rt).iter(|| {
                    let tx = writer.begin_transaction().with_isolation_level(*isolation_level);
                    let files = create_test_add_files(100);
                    
                    rt.block_on(async {
                        let table_actions = TableActions::new("bench_table".to_string(), 0)
                            .with_operation("BENCHMARK".to_string())
                            .with_files(files);
                        
                        black_box(writer.commit_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks concurrent transaction performance
fn bench_concurrent_transactions(c: &mut Criterion) {
    let (_temp_dir, writer) = Arc::new(setup_benchmark_writer().1);
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_transactions");
    
    for concurrent_count in [1, 2, 5, 10].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_commits", concurrent_count),
            concurrent_count,
            |b, &concurrent_count| {
                b.to_async(&rt).iter(|| {
                    let mut handles = Vec::new();
                    
                    for i in 0..*concurrent_count {
                        let writer_clone = writer.clone();
                        let table_id = format!("bench_table_{}", i);
                        
                        let handle = rt.spawn(async move {
                            let tx = writer_clone.begin_transaction();
                            let files = create_test_add_files(50);
                            
                            let table_actions = TableActions::new(table_id, 0)
                                .with_operation("CONCURRENT_BENCHMARK".to_string())
                                .with_files(files);
                            
                            writer_clone.commit_transaction(tx).await
                        });
                        
                        handles.push(handle);
                    }
                    
                    rt.block_on(async {
                        for handle in handles {
                            black_box(handle.await.unwrap());
                        }
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks memory usage with large transactions
fn bench_memory_usage(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("memory_usage");
    
    for action_count in [1000, 5000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::new("large_transaction", action_count),
            action_count,
            |b, &action_count| {
                b.to_async(&rt).iter(|| {
                    let tx = writer.begin_transaction();
                    let files = create_test_add_files(*action_count);
                    
                    rt.block_on(async {
                        let table_actions = TableActions::new("bench_table".to_string(), 0)
                            .with_operation("MEMORY_BENCHMARK".to_string())
                            .with_files(files);
                        
                        black_box(writer.commit_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmarks rollback performance
fn bench_transaction_rollback(c: &mut Criterion) {
    let (_temp_dir, writer) = setup_benchmark_writer();
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("transaction_rollback");
    
    for action_count in [10, 50, 100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::new("rollback_actions", action_count),
            action_count,
            |b, &action_count| {
                b.to_async(&rt).iter(|| {
                    let mut tx = writer.begin_transaction();
                    let files = create_test_add_files(*action_count);
                    
                    rt.block_on(async {
                        let table_actions = TableActions::new("bench_table".to_string(), 0)
                            .with_operation("ROLLBACK_BENCHMARK".to_string())
                            .with_files(files);
                        
                        tx = tx.add_table_actions("bench_table".to_string(), table_actions).await.unwrap();
                        
                        // Force rollback by simulating an error
                        black_box(writer.rollback_transaction(tx).await)
                    })
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_table_transactions,
    bench_multi_table_transactions,
    bench_transaction_creation,
    bench_table_actions_creation,
    bench_consistency_validation,
    bench_isolation_levels,
    bench_concurrent_transactions,
    bench_memory_usage,
    bench_transaction_rollback
);

criterion_main!(benches);