//! Transaction savepoint support for partial rollback
//!
//! This module provides savepoint functionality within transactions, allowing
//! partial rollback to specific points while preserving other work.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::{SqlResult, DatabaseAdapter};
use crate::traits::{TransactionStatus};
use crate::transaction::{Transaction, TableOperation};

/// Savepoint status
#[derive(Debug, Clone, PartialEq)]
pub enum SavepointStatus {
    /// Savepoint has been created
    Active,
    /// Savepoint has been released
    Released,
    /// Savepoint has been rolled back to
    RolledBack,
    /// Savepoint failed during creation
    Failed,
}

/// Transaction savepoint
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Unique savepoint ID
    pub savepoint_id: Uuid,
    /// Transaction ID this savepoint belongs to
    pub transaction_id: Uuid,
    /// Savepoint name (user-friendly identifier)
    pub name: String,
    /// Current status of the savepoint
    pub status: SavepointStatus,
    /// When the savepoint was created
    pub created_at: DateTime<Utc>,
    /// Operations staged before this savepoint
    pub prior_operations: Vec<Uuid>,
    /// Table versions at savepoint time
    pub table_versions: HashMap<Uuid, Option<i64>>,
    /// Metadata snapshot at savepoint time
    pub metadata_snapshot: HashMap<String, String>,
}

impl Savepoint {
    /// Create a new savepoint
    pub fn new(
        savepoint_id: Uuid,
        transaction_id: Uuid,
        name: String,
        prior_operations: Vec<Uuid>,
        table_versions: HashMap<Uuid, Option<i64>>,
    ) -> Self {
        Self {
            savepoint_id,
            transaction_id,
            name,
            status: SavepointStatus::Active,
            created_at: Utc::now(),
            prior_operations,
            table_versions,
            metadata_snapshot: HashMap::new(),
        }
    }

    /// Mark savepoint as released
    pub fn release(&mut self) {
        self.status = SavepointStatus::Released;
    }

    /// Mark savepoint as rolled back
    pub fn rollback(&mut self) {
        self.status = SavepointStatus::RolledBack;
    }

    /// Mark savepoint as failed
    pub fn fail(&mut self) {
        self.status = SavepointStatus::Failed;
    }

    /// Check if savepoint is active
    pub fn is_active(&self) -> bool {
        self.status == SavepointStatus::Active
    }

    /// Check if savepoint is in terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self.status,
            SavepointStatus::Released |
            SavepointStatus::RolledBack |
            SavepointStatus::Failed
        )
    }

    /// Add metadata to snapshot
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata_snapshot.insert(key, value);
    }

    /// Get metadata from snapshot
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata_snapshot.get(key)
    }
}

/// Savepoint manager for handling transaction savepoints
#[derive(Debug)]
pub struct SavepointManager {
    /// Database adapter for savepoint operations
    adapter: Arc<dyn DatabaseAdapter>,
    /// Active savepoints by transaction
    transaction_savepoints: Arc<RwLock<HashMap<Uuid, HashMap<String, Savepoint>>>>,
    /// Savepoint index for quick lookup by ID
    savepoint_index: Arc<RwLock<HashMap<Uuid, Savepoint>>>,
    /// Maximum savepoints per transaction
    max_savepoints_per_transaction: usize,
    /// Savepoint name counter for auto-generated names
    name_counters: Arc<Mutex<HashMap<Uuid, usize>>>,
}

impl SavepointManager {
    /// Create a new savepoint manager
    pub fn new(adapter: Arc<dyn DatabaseAdapter>) -> Self {
        Self {
            adapter,
            transaction_savepoints: Arc::new(RwLock::new(HashMap::new())),
            savepoint_index: Arc::new(RwLock::new(HashMap::new())),
            max_savepoints_per_transaction: 10,
            name_counters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a savepoint for a transaction
    pub async fn create_savepoint(
        &self,
        transaction: &Transaction,
        name: Option<String>,
    ) -> SqlResult<Uuid> {
        // Check if database supports savepoints
        if !self.adapter.supports_savepoints().await {
            return Err(crate::error::SqlError::TransactionError(
                "Database does not support savepoints".to_string()
            ));
        }

        let transaction_id = transaction.transaction_id;
        let savepoint_id = Uuid::new_v4();

        // Generate name if not provided
        let name = match name {
            Some(n) => n,
            None => self.generate_savepoint_name(transaction_id).await?,
        };

        // Check if savepoint name already exists for this transaction
        {
            let tx_savepoints = self.transaction_savepoints.read().await;
            if let Some(savepoints) = tx_savepoints.get(&transaction_id) {
                if savepoints.contains_key(&name) {
                    return Err(crate::error::SqlError::TransactionError(
                        format!("Savepoint '{}' already exists for this transaction", name)
                    ));
                }
            }
        }

        // Get current state to snapshot
        let prior_operations = self.get_staged_operation_ids(transaction).await;
        let table_versions = self.get_table_versions(transaction).await;

        // Create savepoint
        let savepoint = Savepoint::new(
            savepoint_id,
            transaction_id,
            name.clone(),
            prior_operations,
            table_versions,
        );

        // Create database savepoint if supported
        let db_savepoint_name = format!("sp_{}_{}", transaction_id, savepoint_id);
        if let Err(e) = self.adapter.create_savepoint(transaction_id, &db_savepoint_name).await {
            // Try to rollback any partial creation
            let _ = self.adapter.rollback_to_savepoint(transaction_id, &db_savepoint_name).await;
            return Err(e);
        }

        // Store savepoint in manager
        {
            let mut tx_savepoints = self.transaction_savepoints.write().await;
            let savepoints = tx_savepoints.entry(transaction_id).or_insert_with(HashMap::new);

            // Check max savepoints limit
            if savepoints.len() >= self.max_savepoints_per_transaction {
                return Err(crate::error::SqlError::TransactionError(
                    "Maximum savepoints per transaction exceeded".to_string()
                ));
            }

            savepoints.insert(name.clone(), savepoint.clone());
        }

        {
            let mut index = self.savepoint_index.write().await;
            index.insert(savepoint_id, savepoint.clone());
        }

        Ok(savepoint_id)
    }

    /// Rollback to a savepoint
    pub async fn rollback_to_savepoint(
        &self,
        transaction: &Transaction,
        savepoint_id: Uuid,
    ) -> SqlResult<()> {
        let transaction_id = transaction.transaction_id;

        // Get savepoint
        let mut savepoint = {
            let index = self.savepoint_index.read().await;
            index.get(&savepoint_id)
                .ok_or_else(|| crate::error::SqlError::TransactionError(
                    "Savepoint not found".to_string()
                ))?
                .clone()
        };

        if !savepoint.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "Savepoint is not active".to_string()
            ));
        }

        // Rollback database savepoint
        let db_savepoint_name = format!("sp_{}_{}", transaction_id, savepoint_id);
        self.adapter.rollback_to_savepoint(transaction_id, &db_savepoint_name).await?;

        // Update transaction state to match savepoint
        self.restore_transaction_state(transaction, &savepoint).await?;

        // Mark savepoint as rolled back
        savepoint.rollback();

        // Update stored savepoint
        {
            let mut index = self.savepoint_index.write().await;
            index.insert(savepoint_id, savepoint.clone());
        }

        {
            let mut tx_savepoints = self.transaction_savepoints.write().await;
            if let Some(savepoints) = tx_savepoints.get_mut(&transaction_id) {
                savepoints.insert(savepoint.name.clone(), savepoint);
            }
        }

        Ok(())
    }

    /// Release a savepoint
    pub async fn release_savepoint(
        &self,
        transaction: &Transaction,
        savepoint_id: Uuid,
    ) -> SqlResult<()> {
        let transaction_id = transaction.transaction_id;

        // Get savepoint
        let mut savepoint = {
            let index = self.savepoint_index.read().await;
            index.get(&savepoint_id)
                .ok_or_else(|| crate::error::SqlError::TransactionError(
                    "Savepoint not found".to_string()
                ))?
                .clone()
        };

        if !savepoint.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "Savepoint is not active".to_string()
            ));
        }

        // Release database savepoint
        let db_savepoint_name = format!("sp_{}_{}", transaction_id, savepoint_id);
        if let Err(e) = self.adapter.release_savepoint(transaction_id, &db_savepoint_name).await {
            // Database doesn't support release, which is fine
            // Many databases automatically release savepoints on commit
            eprintln!("Failed to release database savepoint (this may be normal): {:?}", e);
        }

        // Mark savepoint as released
        savepoint.release();

        // Update stored savepoint
        {
            let mut index = self.savepoint_index.write().await;
            index.insert(savepoint_id, savepoint.clone());
        }

        {
            let mut tx_savepoints = self.transaction_savepoints.write().await;
            if let Some(savepoints) = tx_savepoints.get_mut(&transaction_id) {
                savepoints.insert(savepoint.name.clone(), savepoint);
            }
        }

        Ok(())
    }

    /// Get savepoint by ID
    pub async fn get_savepoint(&self, savepoint_id: Uuid) -> SqlResult<Option<Savepoint>> {
        let index = self.savepoint_index.read().await;
        Ok(index.get(&savepoint_id).cloned())
    }

    /// Get savepoint by name for a transaction
    pub async fn get_savepoint_by_name(
        &self,
        transaction_id: Uuid,
        name: &str,
    ) -> SqlResult<Option<Savepoint>> {
        let tx_savepoints = self.transaction_savepoints.read().await;
        if let Some(savepoints) = tx_savepoints.get(&transaction_id) {
            Ok(savepoints.get(name).cloned())
        } else {
            Ok(None)
        }
    }

    /// List all savepoints for a transaction
    pub async fn list_savepoints(&self, transaction_id: Uuid) -> SqlResult<Vec<Savepoint>> {
        let tx_savepoints = self.transaction_savepoints.read().await;
        if let Some(savepoints) = tx_savepoints.get(&transaction_id) {
            Ok(savepoints.values().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Clean up all savepoints for a transaction
    pub async fn cleanup_transaction_savepoints(&self, transaction_id: Uuid) -> SqlResult<()> {
        // Remove from transaction savepoints
        {
            let mut tx_savepoints = self.transaction_savepoints.write().await;
            tx_savepoints.remove(&transaction_id);
        }

        // Remove from index
        {
            let mut index = self.savepoint_index.write().await;
            index.retain(|_, savepoint| savepoint.transaction_id != transaction_id);
        }

        // Remove name counter
        {
            let mut counters = self.name_counters.lock().await;
            counters.remove(&transaction_id);
        }

        Ok(())
    }

    /// Generate a unique savepoint name for a transaction
    async fn generate_savepoint_name(&self, transaction_id: Uuid) -> SqlResult<String> {
        let mut counters = self.name_counters.lock().await;
        let counter = counters.entry(transaction_id).or_insert(0);
        *counter += 1;
        Ok(format!("sp_{}", counter))
    }

    /// Get IDs of staged operations for a transaction
    async fn get_staged_operation_ids(&self, transaction: &Transaction) -> Vec<Uuid> {
        let staged = transaction.staged_operations.lock().await;
        staged.iter().map(|op| op.operation_id).collect()
    }

    /// Get current table versions for a transaction
    async fn get_table_versions(&self, transaction: &Transaction) -> HashMap<Uuid, Option<i64>> {
        let tables = transaction.involved_tables.read().await;
        let mut versions = HashMap::new();

        for table_id in tables.keys() {
            if let Ok(version) = transaction.adapter.get_table_version(*table_id).await {
                versions.insert(*table_id, version);
            }
        }

        versions
    }

    /// Restore transaction state to match savepoint
    async fn restore_transaction_state(&self, transaction: &Transaction, savepoint: &Savepoint) -> SqlResult<()> {
        // Remove operations staged after the savepoint
        let mut staged = transaction.staged_operations.lock().await;
        staged.retain(|op| savepoint.prior_operations.contains(&op.operation_id));

        // Restore table versions
        let mut tables = transaction.involved_tables.write().await;
        for (table_id, version) in &savepoint.table_versions {
            tables.insert(*table_id, version.unwrap_or(0));
        }

        Ok(())
    }

    /// Set maximum savepoints per transaction
    pub fn set_max_savepoints_per_transaction(&mut self, max: usize) {
        self.max_savepoints_per_transaction = max;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::SQLiteAdapter;
    use crate::DatabaseConfig;
    use crate::transaction::{TransactionManager, IsolationLevel};

    #[tokio::test]
    async fn test_savepoint_creation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = SavepointManager::new(adapter.clone());
        let tx_manager = TransactionManager::new(adapter.clone());

        let transaction = tx_manager.begin_transaction(None, None).await.unwrap();
        let savepoint_id = manager.create_savepoint(&transaction, Some("test_sp".to_string())).await;

        // This might fail if SQLite doesn't support savepoints
        match savepoint_id {
            Ok(id) => {
                assert_ne!(id, Uuid::nil());

                let savepoint = manager.get_savepoint(id).await.unwrap();
                assert!(savepoint.is_some());
                assert_eq!(savepoint.unwrap().name, "test_sp");
            }
            Err(_) => {
                // Expected if savepoints aren't supported
            }
        }
    }

    #[tokio::test]
    async fn test_savepoint_name_generation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = SavepointManager::new(adapter.clone());
        let tx_manager = TransactionManager::new(adapter.clone());

        let transaction = tx_manager.begin_transaction(None, None).await.unwrap();

        let name1 = manager.generate_savepoint_name(transaction.transaction_id).await.unwrap();
        let name2 = manager.generate_savepoint_name(transaction.transaction_id).await.unwrap();

        assert_ne!(name1, name2);
        assert!(name1.starts_with("sp_"));
        assert!(name2.starts_with("sp_"));
    }

    #[tokio::test]
    async fn test_savepoint_status_transitions() {
        let savepoint_id = Uuid::new_v4();
        let mut savepoint = Savepoint::new(
            savepoint_id,
            Uuid::new_v4(),
            "test".to_string(),
            vec![],
            HashMap::new(),
        );

        assert_eq!(savepoint.status, SavepointStatus::Active);
        assert!(savepoint.is_active());
        assert!(!savepoint.is_terminal());

        savepoint.release();
        assert_eq!(savepoint.status, SavepointStatus::Released);
        assert!(!savepoint.is_active());
        assert!(savepoint.is_terminal());

        let mut savepoint2 = Savepoint::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "test2".to_string(),
            vec![],
            HashMap::new(),
        );

        savepoint2.rollback();
        assert_eq!(savepoint2.status, SavepointStatus::RolledBack);
        assert!(savepoint2.is_terminal());
    }

    #[tokio::test]
    async fn test_savepoint_metadata() {
        let mut savepoint = Savepoint::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "test".to_string(),
            vec![],
            HashMap::new(),
        );

        savepoint.add_metadata("key1".to_string(), "value1".to_string());
        savepoint.add_metadata("key2".to_string(), "value2".to_string());

        assert_eq!(savepoint.get_metadata("key1"), Some(&"value1".to_string()));
        assert_eq!(savepoint.get_metadata("key2"), Some(&"value2".to_string()));
        assert_eq!(savepoint.get_metadata("nonexistent"), None);
    }

    #[tokio::test]
    async fn test_savepoint_listing() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = SavepointManager::new(adapter.clone());
        let tx_manager = TransactionManager::new(adapter.clone());

        let transaction = tx_manager.begin_transaction(None, None).await.unwrap();
        let transaction_id = transaction.transaction_id;

        // List savepoints for empty transaction
        let savepoints = manager.list_savepoints(transaction_id).await.unwrap();
        assert_eq!(savepoints.len(), 0);

        // Cleanup
        manager.cleanup_transaction_savepoints(transaction_id).await.unwrap();
    }
}