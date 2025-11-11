//! SQL-based transaction log writer.

use crate::connection::DatabaseConnection;
use crate::mirror::MirrorEngine;
use async_trait::async_trait;
use deltalakedb_core::error::{TxnLogError, TxnLogResult};
use deltalakedb_core::{DeltaAction, Metadata, Protocol, Transaction, TableMetadata, Format};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, instrument};
use uuid::Uuid;
use chrono::Utc;

/// Configuration for SQL transaction log writer.
#[derive(Debug, Clone)]
pub struct SqlWriterConfig {
    /// Enable automatic mirroring after commits
    pub enable_mirroring: bool,
    /// Maximum number of retries for failed operations
    pub max_retries: u32,
    /// Base delay for exponential backoff in milliseconds
    pub retry_base_delay_ms: u64,
    /// Maximum delay for exponential backoff in milliseconds
    pub retry_max_delay_ms: u64,
    /// Transaction timeout in seconds
    pub transaction_timeout_secs: u64,
    /// Maximum retry attempts (alias for max_retries)
    pub max_retry_attempts: u32,
    /// Checkpoint interval in versions
    pub checkpoint_interval: i64,
}

impl Default for SqlWriterConfig {
    fn default() -> Self {
        Self {
            enable_mirroring: true,
            max_retries: 3,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 5000,
            transaction_timeout_secs: 300,
            max_retry_attempts: 3,
            checkpoint_interval: 10,
        }
    }
}

/// SQL-based transaction log writer.
#[derive(Debug, Clone)]
pub struct SqlTxnLogWriter {
    /// Database connection
    pub connection: std::sync::Arc<DatabaseConnection>,
    /// Mirror engine for object storage
    pub mirror_engine: Option<std::sync::Arc<dyn MirrorEngine>>,
    /// Writer configuration
    pub config: SqlWriterConfig,
}

impl SqlTxnLogWriter {
    /// Create a new SQL transaction log writer.
    pub fn new(
        connection: std::sync::Arc<DatabaseConnection>,
        mirror_engine: Option<std::sync::Arc<dyn MirrorEngine>>,
        config: SqlWriterConfig,
    ) -> Self {
        Self {
            connection,
            mirror_engine,
            config,
        }
    }

    /// Create a writer with default configuration.
    pub fn with_defaults(
        connection: std::sync::Arc<DatabaseConnection>,
        mirror_engine: Option<std::sync::Arc<dyn MirrorEngine>>,
    ) -> Self {
        Self::new(connection, mirror_engine, SqlWriterConfig::default())
    }

    /// Create a writer from an Arc connection (alias for new).
    pub fn from_arc(
        connection: std::sync::Arc<DatabaseConnection>,
        config: SqlWriterConfig,
    ) -> Self {
        Self::new(connection, None, config)
    }

    /// Get current version for a table.
    async fn get_current_version(&self, table_id: &str) -> TxnLogResult<i64> {
        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        let version = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT COALESCE(MAX(version), -1) FROM dl_table_versions WHERE table_id = ?"
        )
        .bind(table_id)
        .fetch_one(pool)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to get current version: {}", e)))?;

        Ok(version.unwrap_or(-1))
    }

    /// Get next version with optimistic locking.
    async fn get_next_version_with_lock<'a, E>(
        &self,
        tx: &'a mut sqlx::Transaction<'a, E>,
        table_id: &str,
    ) -> TxnLogResult<i64>
    where
        E: sqlx::Database,
    {
        // Get current version within transaction
        let current_version = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT COALESCE(MAX(version), -1) FROM dl_table_versions WHERE table_id = ? FOR UPDATE"
        )
        .bind(table_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to lock version: {}", e)))?;

        Ok(current_version.unwrap_or(-1) + 1)
    }

    /// Insert a single action into the appropriate table.
    async fn insert_action<'a, E>(
        &self,
        tx: &'a mut sqlx::Transaction<'a, E>,
        table_id: &str,
        version: i64,
        action: &DeltaAction,
    ) -> TxnLogResult<()>
    where
        E: sqlx::Database,
    {
        match action {
            DeltaAction::Add(add_file) => {
                sqlx::query(
                    "INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values, stats, tags) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(table_id)
                .bind(version)
                .bind(&add_file.path)
                .bind(add_file.size)
                .bind(add_file.modification_time)
                .bind(add_file.data_change)
                .bind(serde_json::to_string(&add_file.partition_values).unwrap_or_default())
                .bind(add_file.stats.as_deref())
                .bind(add_file.tags.as_ref().map(|tags| serde_json::to_string(tags).unwrap_or_default()))
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to insert add file: {}", e)))?;
            }
            DeltaAction::Remove(remove_file) => {
                sqlx::query(
                    "INSERT INTO dl_remove_files (table_id, version, path, deletion_timestamp, data_change, extended_file_metadata) 
                     VALUES (?, ?, ?, ?, ?, ?)"
                )
                .bind(table_id)
                .bind(version)
                .bind(&remove_file.path)
                .bind(remove_file.deletion_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()))
                .bind(remove_file.data_change)
                .bind(remove_file.extended_file_metadata.as_ref().map(|metadata| serde_json::to_string(metadata).unwrap_or_default()))
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to insert remove file: {}", e)))?;
            }
            DeltaAction::Metadata(metadata) => {
                sqlx::query(
                    "INSERT INTO dl_metadata_updates (table_id, version, schema_string, configuration) 
                     VALUES (?, ?, ?, ?)"
                )
                .bind(table_id)
                .bind(version)
                .bind(&metadata.schema_string)
                .bind(metadata.configuration.as_ref().map(|config| serde_json::to_string(config).unwrap_or_default()))
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to insert metadata: {}", e)))?;
            }
            DeltaAction::Protocol(protocol) => {
                sqlx::query(
                    "INSERT INTO dl_protocol_updates (table_id, version, min_reader_version, min_writer_version, reader_features, writer_features) 
                     VALUES (?, ?, ?, ?, ?, ?)"
                )
                .bind(table_id)
                .bind(version)
                .bind(protocol.min_reader_version)
                .bind(protocol.min_writer_version)
                .bind(protocol.reader_features.as_ref().map(|features| serde_json::to_string(features).unwrap_or_default()))
                .bind(protocol.writer_features.as_ref().map(|features| serde_json::to_string(features).unwrap_or_default()))
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to insert protocol: {}", e)))?;
            }
            DeltaAction::CommitInfo(commit_info) => {
                // Store commit info in the table_versions operation_params
                let commit_json = serde_json::to_string(commit_info)
                    .map_err(|e| TxnLogError::SerializationError(e.to_string()))?;
                
                sqlx::query(
                    "UPDATE dl_table_versions SET operation_params = ? WHERE table_id = ? AND version = ?"
                )
                .bind(commit_json)
                .bind(table_id)
                .bind(version)
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to update commit info: {}", e)))?;
            }
            DeltaAction::Cdc(cdc) => {
                // CDC actions are stored in a separate table
                sqlx::query(
                    "INSERT INTO dl_cdc_actions (table_id, version, change_type, data, timestamp) 
                     VALUES (?, ?, ?, ?, ?)"
                )
                .bind(table_id)
                .bind(version)
                .bind(&cdc.change_type)
                .bind(serde_json::to_string(&cdc.data).unwrap_or_default())
                .bind(cdc.timestamp)
                .execute(&mut *tx)
                .await
                .map_err(|e| TxnLogError::DatabaseError(format!("Failed to insert CDC action: {}", e)))?;
            }
        }

        Ok(())
    }

    /// Check for external concurrent writers by examining _delta_log directory and version consistency.
    async fn check_external_writers(&self, table_id: &str) -> TxnLogResult<bool> {
        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Get table location and current SQL version
        let (location, sql_version) = sqlx::query_as::<_, (Option<String>, i64)>(
            "SELECT location, COALESCE(MAX(version), -1) FROM dl_tables t 
             LEFT JOIN dl_table_versions v ON t.table_id = v.table_id 
             WHERE t.table_id = ?"
        )
        .bind(table_id)
        .fetch_one(pool)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to get table info: {}", e)))?;

        if let Some(location) = location {
            debug!("Checking for external writers in {} (SQL version: {})", location, sql_version);

            // Method 1: Check _delta_log directory for files newer than SQL version
            let has_newer_files = self.check_delta_log_files(&location, sql_version).await?;
            
            // Method 2: Check for version gaps in SQL vs file system
            let has_version_gaps = self.check_version_gaps(&location, sql_version).await?;
            
            // Method 3: Check for unexpected file modifications
            let has_unexpected_mods = self.check_unexpected_modifications(&location, sql_version).await?;

            let external_writers_detected = has_newer_files || has_version_gaps || has_unexpected_mods;
            
            if external_writers_detected {
                tracing::warn!("External concurrent writers detected for table {} at {}", table_id, location);
            }
            
            Ok(external_writers_detected)
        } else {
            Err(TxnLogError::TableNotFound(format!("Table {} not found", table_id)))
        }
    }

    /// Check _delta_log directory for files with versions newer than SQL version.
    async fn check_delta_log_files(&self, table_location: &str, sql_version: i64) -> TxnLogResult<bool> {
        // In a real implementation, this would:
        // 1. List all .json files in _delta_log directory
        // 2. Parse version numbers from filenames
        // 3. Compare with SQL version
        // 4. Return true if any file version > sql_version
        
        // For now, we'll simulate this check
        let delta_log_path = format!("{}/_delta_log", table_location);
        
        // Simulate file system check
        // In practice, this would use std::fs or tokio::fs
        debug!("Checking _delta_log files in {}", delta_log_path);
        
        // Return false for now (no newer files detected)
        Ok(false)
    }

    /// Check for version gaps between SQL and file system.
    async fn check_version_gaps(&self, table_location: &str, sql_version: i64) -> TxnLogResult<bool> {
        // In a real implementation, this would:
        // 1. Get max version from _delta_log files
        // 2. Compare with SQL version
        // 3. Return true if file_version > sql_version + 1
        
        debug!("Checking for version gaps in {}", table_location);
        
        // Return false for now (no gaps detected)
        Ok(false)
    }

    /// Check for unexpected file modifications in _delta_log.
    async fn check_unexpected_modifications(&self, table_location: &str, sql_version: i64) -> TxnLogResult<bool> {
        // In a real implementation, this would:
        // 1. Check file modification times
        // 2. Verify checksums if available
        // 3. Look for files not matching expected patterns
        
        debug!("Checking for unexpected modifications in {}", table_location);
        
        // Return false for now (no unexpected modifications)
        Ok(false)
    }

    /// Ensure idempotency by checking if actions already exist.
    async fn ensure_idempotency<'a, E>(
        &self,
        tx: &'a mut sqlx::Transaction<'a, E>,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
    ) -> TxnLogResult<bool>
    where
        E: sqlx::Database,
    {
        // Check if version already has actions
        let existing_actions = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT COUNT(*) FROM dl_add_files WHERE table_id = ? AND version = ?
             UNION ALL
             SELECT COUNT(*) FROM dl_remove_files WHERE table_id = ? AND version = ?
             UNION ALL
             SELECT COUNT(*) FROM dl_metadata_updates WHERE table_id = ? AND version = ?
             UNION ALL
             SELECT COUNT(*) FROM dl_protocol_updates WHERE table_id = ? AND version = ?"
        )
        .bind(table_id).bind(version)
        .bind(table_id).bind(version)
        .bind(table_id).bind(version)
        .bind(table_id).bind(version)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to check idempotency: {}", e)))?;

        Ok(existing_actions.unwrap_or(0) == 0)
    }
}

#[async_trait]
impl deltalakedb_core::TxnLogWriter for SqlTxnLogWriter {
    #[instrument(skip(self))]
    async fn begin_transaction(&self, table_id: &str) -> TxnLogResult<Transaction> {
        debug!("Beginning transaction for table {}", table_id);
        
        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;
        
        // Get current version for optimistic concurrency
        let current_version = self.get_current_version(table_id).await?;
        
        // Generate transaction ID
        let transaction_id = Uuid::new_v4().to_string();
        
        Ok(Transaction {
            table_id: table_id.to_string(),
            transaction_id,
            started_at: Utc::now(),
            current_version,
            staged_actions: vec![],
            state: deltalakedb_core::TransactionState::Active,
        })
    }

    #[instrument(skip(self, actions))]
    async fn commit_actions(
        &self,
        table_id: &str,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<i64> {
        debug!(
            "Committing {} actions for table {}",
            actions.len(),
            table_id
        );

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Start database transaction
        let mut tx = pool.begin().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to begin transaction: {}", e)))?;

        // Get next version with optimistic locking
        let next_version = self.get_next_version_with_lock(&mut tx, table_id).await?;

        // Ensure idempotency - check if actions already exist
        let is_idempotent = self.ensure_idempotency(&mut tx, table_id, next_version, &actions).await?;
        if !is_idempotent {
            return Err(TxnLogError::IdempotencyError(format!("Actions for table {} version {} already exist", table_id, next_version)));
        }

        // Insert table version record
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation, operation_params) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(table_id)
        .bind(next_version)
        .bind(now)
        .bind("deltalakedb-sql")
        .bind(operation)
        .bind(operation_params.as_ref().map(|params| serde_json::to_string(params).unwrap_or_default()))
        .execute(&mut *tx)
        .await
        .map_err(|e| TxnLogError::TransactionError(format!("Failed to insert version: {}", e)))?;

        // Insert actions
        for action in &actions {
            self.insert_action(&mut tx, table_id, next_version, action).await?;
        }

        // Commit transaction
        tx.commit().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to commit transaction: {}", e)))?;

        // Trigger mirroring if enabled
        if self.config.enable_mirroring {
            if let Some(mirror_engine) = &self.mirror_engine {
                // Mirror asynchronously - don't block commit
                let table_id = table_id.to_string();
                let actions_clone = actions.clone();
                let mirror_engine_clone = mirror_engine.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = mirror_engine_clone.mirror_commit(&table_id, next_version, &actions_clone).await {
                        tracing::error!("Failed to mirror commit for table {} version {}: {}", table_id, next_version, e);
                    }
                });
            }
        }

        Ok(next_version)
    }

    #[instrument(skip(self, actions))]
    async fn commit_actions_with_version(
        &self,
        table_id: &str,
        version: i64,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<()> {
        debug!(
            "Committing {} actions for table {} at version {}",
            actions.len(),
            table_id,
            version
        );

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Start database transaction
        let mut tx = pool.begin().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to begin transaction: {}", e)))?;

        // Validate version doesn't already exist (optimistic concurrency)
        let existing = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT version FROM dl_table_versions WHERE table_id = ? AND version = ?"
        )
        .bind(table_id)
        .bind(version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| TxnLogError::TransactionError(format!("Failed to check version: {}", e)))?;

        if existing.is_some() {
            return Err(TxnLogError::VersionConflict(format!("Version {} already exists for table {}", version, table_id)));
        }

        // Ensure idempotency - check if actions already exist
        let is_idempotent = self.ensure_idempotency(&mut tx, table_id, version, &actions).await?;
        if !is_idempotent {
            return Err(TxnLogError::IdempotencyError(format!("Actions for table {} version {} already exist", table_id, version)));
        }

        // Insert table version record
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation, operation_params) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(table_id)
        .bind(version)
        .bind(now)
        .bind("deltalakedb-sql")
        .bind(operation)
        .bind(operation_params.as_ref().map(|params| serde_json::to_string(params).unwrap_or_default()))
        .execute(&mut *tx)
        .await
        .map_err(|e| TxnLogError::TransactionError(format!("Failed to insert version: {}", e)))?;

        // Insert actions
        for action in &actions {
            self.insert_action(&mut tx, table_id, version, action).await?;
        }

        // Commit transaction
        tx.commit().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to commit transaction: {}", e)))?;

        // Trigger mirroring if enabled
        if self.config.enable_mirroring {
            if let Some(mirror_engine) = &self.mirror_engine {
                let table_id = table_id.to_string();
                let actions_clone = actions.clone();
                let mirror_engine_clone = mirror_engine.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = mirror_engine_clone.mirror_commit(&table_id, version, &actions_clone).await {
                        tracing::error!("Failed to mirror commit for table {} version {}: {}", table_id, version, e);
                    }
                });
            }
        }

        Ok(())
    }

    #[instrument(skip(self, metadata))]
    async fn create_table(
        &self,
        table_id: &str,
        name: &str,
        location: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()> {
        debug!("Creating table {} with name {}", table_id, name);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Start database transaction
        let mut tx = pool.begin().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to begin transaction: {}", e)))?;

        // Check if table already exists
        let existing = sqlx::query_scalar::<_, Option<String>>(
            "SELECT table_id FROM dl_tables WHERE name = ? AND location = ?"
        )
        .bind(name)
        .bind(location)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| TxnLogError::TransactionError(format!("Failed to check table existence: {}", e)))?;

        if existing.is_some() {
            return Err(TxnLogError::TableExists(format!("Table {} at {} already exists", name, location)));
        }

        // Insert table record
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| TxnLogError::SerializationError(e.to_string()))?;

        sqlx::query(
            "INSERT INTO dl_tables (table_id, name, location, metadata, created_at, updated_at) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(table_id)
        .bind(name)
        .bind(location)
        .bind(metadata_json)
        .bind(Utc::now().timestamp_millis())
        .bind(Utc::now().timestamp_millis())
        .execute(&mut *tx)
        .await
        .map_err(|e| TxnLogError::TransactionError(format!("Failed to create table: {}", e)))?;

        // Commit transaction
        tx.commit().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to commit table creation: {}", e)))?;

        Ok(())
    }

    async fn update_table_metadata(
        &self,
        table_id: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()> {
        debug!("Updating metadata for table {}", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| TxnLogError::SerializationError(e.to_string()))?;

        sqlx::query(
            "UPDATE dl_tables SET metadata = ?, updated_at = ? WHERE table_id = ?"
        )
        .bind(metadata_json)
        .bind(Utc::now().timestamp_millis())
        .bind(table_id)
        .execute(pool)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to update table metadata: {}", e)))?;

        Ok(())
    }

    async fn delete_table(&self, table_id: &str) -> TxnLogResult<()> {
        debug!("Deleting table {}", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Start database transaction
        let mut tx = pool.begin().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to begin transaction: {}", e)))?;

        // Delete all related records (cascading deletes should handle this, but being explicit)
        sqlx::query("DELETE FROM dl_table_versions WHERE table_id = ?")
            .bind(table_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete table versions: {}", e)))?;

        sqlx::query("DELETE FROM dl_tables WHERE table_id = ?")
            .bind(table_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete table: {}", e)))?;

        // Commit transaction
        tx.commit().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to commit table deletion: {}", e)))?;

        Ok(())
    }

    async fn get_next_version(&self, table_id: &str) -> TxnLogResult<i64> {
        debug!("Getting next version for table {}", table_id);
        let current = self.get_current_version(table_id).await?;
        Ok(current + 1)
    }

    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool> {
        debug!("Checking if table {} exists", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        let exists = sqlx::query_scalar::<_, Option<bool>>(
            "SELECT EXISTS(SELECT 1 FROM dl_tables WHERE table_id = ?)"
        )
        .bind(table_id)
        .fetch_one(pool)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to check table existence: {}", e)))?;

        Ok(exists.unwrap_or(false))
    }

    async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<TableMetadata> {
        debug!("Getting metadata for table {}", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Get table info
        let (name, location, metadata_json): (String, String, Option<String>) = sqlx::query_as(
            "SELECT name, location, metadata FROM dl_tables WHERE table_id = ?"
        )
        .bind(table_id)
        .fetch_one(pool)
        .await
        .map_err(|e| TxnLogError::DatabaseError(format!("Failed to get table info: {}", e)))?;

        // Get current version
        let current_version = self.get_current_version(table_id).await?;

        // Get protocol info
        let (min_reader_version, min_writer_version): (i32, i32) = sqlx::query_as(
            "SELECT COALESCE(min_reader_version, 1), COALESCE(min_writer_version, 1) 
             FROM dl_protocol_updates WHERE table_id = ? ORDER BY version DESC LIMIT 1"
        )
        .bind(table_id)
        .fetch_one(pool)
        .await
        .unwrap_or((1, 1)); // Default if no protocol updates

        // Parse metadata
        let metadata = if let Some(json) = metadata_json {
            serde_json::from_str(&json)
                .map_err(|e| TxnLogError::SerializationError(e.to_string()))?
        } else {
            Metadata::default()
        };

        Ok(TableMetadata {
            table_id: table_id.to_string(),
            name,
            location,
            version: current_version,
            protocol: Protocol {
                min_reader_version,
                min_writer_version,
            },
            metadata,
            created_at: Utc::now(), // This should come from DB, but using now for now
        })
    }

    async fn vacuum(
        &self,
        table_id: &str,
        retain_last_n_versions: Option<i64>,
        retain_hours: Option<i64>,
    ) -> TxnLogResult<()> {
        debug!("Vacuuming table {}", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // Start database transaction
        let mut tx = pool.begin().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to begin transaction: {}", e)))?;

        // Calculate cutoff version based on retention policy
        let cutoff_version = if let Some(retain_n) = retain_last_n_versions {
            let current_version = self.get_current_version(table_id).await?;
            Some(current_version - retain_n)
        } else if let Some(hours) = retain_hours {
            let cutoff_time = Utc::now().timestamp_millis() - (hours * 3600 * 1000);
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT COALESCE(MIN(version), -1) FROM dl_table_versions 
                 WHERE table_id = ? AND committed_at < ?"
            )
            .bind(table_id)
            .bind(cutoff_time)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| TxnLogError::DatabaseError(format!("Failed to calculate cutoff version: {}", e)))?
        } else {
            None
        };

        if let Some(cutoff) = cutoff_version {
            if cutoff > 0 {
                // Delete old versions and their actions
                sqlx::query("DELETE FROM dl_add_files WHERE table_id = ? AND version < ?")
                    .bind(table_id)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete old add files: {}", e)))?;

                sqlx::query("DELETE FROM dl_remove_files WHERE table_id = ? AND version < ?")
                    .bind(table_id)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete old remove files: {}", e)))?;

                sqlx::query("DELETE FROM dl_metadata_updates WHERE table_id = ? AND version < ?")
                    .bind(table_id)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete old metadata: {}", e)))?;

                sqlx::query("DELETE FROM dl_protocol_updates WHERE table_id = ? AND version < ?")
                    .bind(table_id)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete old protocol: {}", e)))?;

                sqlx::query("DELETE FROM dl_table_versions WHERE table_id = ? AND version < ?")
                    .bind(table_id)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(format!("Failed to delete old versions: {}", e)))?;
            }
        }

        // Commit transaction
        tx.commit().await
            .map_err(|e| TxnLogError::TransactionError(format!("Failed to commit vacuum: {}", e)))?;

        Ok(())
    }

    async fn optimize(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()> {
        debug!("Optimizing table {}", table_id);

        let pool = self.connection.as_any_pool()
            .map_err(|e| TxnLogError::ConnectionError(e.to_string()))?;

        // For now, optimization is a no-op in SQL backend
        // In a real implementation, this might:
        // - Update statistics
        // - Rebuild indexes
        // - Compact tables
        // - Update materialized views

        if let Some(mirror_engine) = &self.mirror_engine {
            // Trigger mirror optimization
            mirror_engine.optimize_table(table_id, target_size, max_concurrent_tasks).await?;
        }

        Ok(())
    }


}

/// Transaction log entry for SQL storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlTxnLogEntry {
    /// Unique identifier for the transaction
    pub transaction_id: String,
    /// Table identifier
    pub table_id: String,
    /// Version number
    pub version: i64,
    /// Timestamp when the transaction was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Actions in this transaction
    pub actions: Vec<DeltaAction>,
    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{DatabaseConfig, DatabaseConnection};
    use crate::mirror::NoOpMirrorEngine;

    #[tokio::test]
    async fn test_sql_writer_config() {
        let config = SqlWriterConfig::default();
        assert!(config.enable_mirroring);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_base_delay_ms, 100);
        assert_eq!(config.retry_max_delay_ms, 5000);
        assert_eq!(config.transaction_timeout_secs, 300);
    }

    #[tokio::test]
    async fn test_sql_writer_creation() {
        // This test would require a real database connection
        // For now, we'll just test the creation logic
        
        let config = SqlWriterConfig::default();
        assert_eq!(config.max_retries, 3);
        
        let custom_config = SqlWriterConfig {
            enable_mirroring: false,
            max_retries: 5,
            retry_base_delay_ms: 200,
            retry_max_delay_ms: 10000,
            transaction_timeout_secs: 600,
            max_retry_attempts: 5,
            checkpoint_interval: 100,
        };
        
        assert!(!custom_config.enable_mirroring);
        assert_eq!(custom_config.max_retries, 5);
    }

    #[tokio::test]
    async fn test_sql_txn_log_entry() {
        let entry = SqlTxnLogEntry {
            transaction_id: "tx_123".to_string(),
            table_id: "test_table".to_string(),
            version: 1,
            timestamp: chrono::Utc::now(),
            actions: vec![],
            metadata: None,
        };

        assert_eq!(entry.transaction_id, "tx_123");
        assert_eq!(entry.table_id, "test_table");
        assert_eq!(entry.version, 1);
        assert!(entry.actions.is_empty());
        assert!(entry.metadata.is_none());
    }
}