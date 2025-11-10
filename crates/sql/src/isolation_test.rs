//! Simple test for isolation level functionality

use deltalakedb_sql::multi_table::{
    MultiTableConfig, MultiTableTransaction, TransactionIsolationLevel
};

#[tokio::test]
async fn test_isolation_level_basic() {
    let config = MultiTableConfig::default();
    
    // Test default isolation level
    let tx1 = MultiTableTransaction::new(config.clone());
    assert_eq!(tx1.isolation_level, TransactionIsolationLevel::ReadCommitted);
    assert_eq!(tx1.priority, 0);
    
    // Test custom isolation level
    let tx2 = MultiTableTransaction::with_isolation_level(
        config.clone(),
        TransactionIsolationLevel::Serializable
    );
    assert_eq!(tx2.isolation_level, TransactionIsolationLevel::Serializable);
    assert_eq!(tx2.priority, 0);
    
    // Test custom isolation level and priority
    let tx3 = MultiTableTransaction::with_isolation_and_priority(
        config,
        TransactionIsolationLevel::RepeatableRead,
        10
    );
    assert_eq!(tx3.isolation_level, TransactionIsolationLevel::RepeatableRead);
    assert_eq!(tx3.priority, 10);
}

#[tokio::test]
async fn test_isolation_level_display() {
    assert_eq!(format!("{}", TransactionIsolationLevel::ReadCommitted), "READ COMMITTED");
    assert_eq!(format!("{}", TransactionIsolationLevel::RepeatableRead), "REPEATABLE READ");
    assert_eq!(format!("{}", TransactionIsolationLevel::Serializable), "SERIALIZABLE");
}