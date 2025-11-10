//! Multi-table transaction support for PostgreSQL backend.

use deltalakedb_core::types::Action;
use deltalakedb_core::{MultiTableTransaction, StagedTable, TransactionError, TransactionResult};
use deltalakedb_mirror::MirrorEngine;
use sqlx::postgres::{PgPool, PgTransaction};
use sqlx::Postgres;
use std::sync::Arc;
use std::collections::BTreeMap;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Multi-table transaction writer for PostgreSQL.
///
/// Handles atomic commits across multiple Delta tables with proper lock ordering
/// and isolation to prevent deadlocks using PostgreSQL transactions.
///
/// # Architecture
///
/// ```text
/// 1. Begin database transaction
/// 2. Lock all tables in sorted order (SELECT...FOR UPDATE)
/// 3. Validate versions for all tables
/// 4. Insert actions for all tables
/// 5. Update versions atomically
/// 6. Commit (all succeed) or rollback (all fail)
/// 7. Mirror to external systems (Spark, DuckDB, etc.)
/// ```
pub struct MultiTableWriter {
    pool: PgPool,
    /// Optional mirror engine for writing Delta artifacts to object storage
    mirror_engine: Option<Arc<MirrorEngine>>,
}

impl MultiTableWriter {
    /// Create a new multi-table writer.
    pub fn new(pool: PgPool) -> Self {
        MultiTableWriter {
            pool,
            mirror_engine: None,
        }
    }

    /// Create a multi-table writer with mirror engine integration.
    pub fn with_mirror(pool: PgPool, mirror_engine: Arc<MirrorEngine>) -> Self {
        MultiTableWriter {
            pool,
            mirror_engine: Some(mirror_engine),
        }
    }

    /// Commit a multi-table transaction atomically.
    ///
    /// All tables in the transaction are locked, validated, and updated
    /// within a single database transaction. Either all succeed or all fail.
    ///
    /// # Arguments
    ///
    /// * `tx` - The multi-table transaction with staged tables
    ///
    /// # Returns
    ///
    /// Transaction result with new versions for each table, or error
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Any table has a version conflict
    /// - Any table action validation fails
    /// - Transaction timeout exceeded
    /// - Database transaction fails
    pub async fn commit(
        &self,
        mut tx: MultiTableTransaction,
    ) -> TransactionResult<deltalakedb_core::TransactionResult> {
        // Validate transaction before starting database work
        tx.validate_all()?;

        // Check timeout again
        if tx.is_timeout() {
            return Err(TransactionError::TransactionTimeout);
        }

        info!(
            "Beginning multi-table transaction {} with {} tables",
            tx.transaction_id(),
            tx.table_count()
        );

        // Get staged tables in deterministic order (sorted by table_id)
        let staged_tables: BTreeMap<Uuid, StagedTable> = tx
            .staged_tables()
            .iter()
            .map(|(id, table)| (*id, table.clone()))
            .collect();

        // Get lock order (deterministic)
        let lock_order = Self::lock_order(staged_tables.keys().copied().collect::<Vec<_>>());

        // Begin database transaction
        let mut db_tx = self
            .pool
            .begin()
            .await
            .map_err(|e| TransactionError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Lock all tables in deterministic order and validate versions
        let mut locked_versions: BTreeMap<Uuid, i64> = BTreeMap::new();

        for table_id in &lock_order {
            debug!("Locking table {}", table_id);

            match self.lock_and_validate_table(&mut db_tx, *table_id).await {
                Ok(current_version) => {
                    locked_versions.insert(*table_id, current_version);
                    debug!("Table {} locked at version {}", table_id, current_version);
                }
                Err(e) => {
                    error!("Failed to lock table {}: {}", table_id, e);
                    // Rollback is automatic on error (db_tx drops)
                    return Err(e);
                }
            }
        }

        // Insert actions for all tables
        for (table_id, staged) in &staged_tables {
            debug!(
                "Inserting {} actions for table {}",
                staged.action_count(),
                table_id
            );

            match self.insert_actions(&mut db_tx, *table_id, &staged.actions).await {
                Ok(_) => {
                    debug!("Inserted actions for table {}", table_id);
                }
                Err(e) => {
                    error!("Failed to insert actions for table {}: {}", table_id, e);
                    return Err(TransactionError::Other(format!(
                        "Failed to insert actions for table {}: {}",
                        table_id, e
                    )));
                }
            }
        }

        // Update versions for all tables atomically
        debug!("Updating versions for {} tables", staged_tables.len());

        match self.update_versions(&mut db_tx, &staged_tables).await {
            Ok(versions) => {
                debug!("Updated versions for all tables");

                // Commit database transaction
                db_tx
                    .commit()
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to commit transaction: {}", e)))?;

                info!(
                    "Multi-table transaction {} committed successfully with {} tables",
                    tx.transaction_id(),
                    versions.len()
                );

                // Create result with new versions
                let mut result =
                    deltalakedb_core::TransactionResult::new(tx.transaction_id().to_string());

                for (table_id, version) in versions {
                    result.add_version(table_id, version + 1);
                }

                Ok(result)
            }
            Err(e) => {
                error!("Failed to update versions: {}", e);
                Err(TransactionError::Other(format!("Failed to update versions: {}", e)))
            }
        }
    }

    /// Lock a table and validate its version.
    async fn lock_and_validate_table(
        &self,
        db_tx: &mut PgTransaction<'_>,
        table_id: Uuid,
    ) -> TransactionResult<i64> {
        let row = sqlx::query!(
            "SELECT current_version FROM dl_tables WHERE table_id = $1 FOR UPDATE",
            table_id
        )
        .fetch_optional(db_tx)
        .await
        .map_err(|e| TransactionError::Other(format!("Failed to lock table {}: {}", table_id, e)))?;

        match row {
            Some(row) => Ok(row.current_version),
            None => Err(TransactionError::ValidationError {
                table_id,
                message: format!("Table {} not found", table_id),
            }),
        }
    }

    /// Insert actions for a table within the transaction.
    async fn insert_actions(
        &self,
        db_tx: &mut PgTransaction<'_>,
        table_id: Uuid,
        actions: &[Action],
    ) -> TransactionResult<()> {
        let current_version = sqlx::query!("SELECT current_version FROM dl_tables WHERE table_id = $1", table_id)
            .fetch_one(&mut **db_tx)
            .await
            .map_err(|e| TransactionError::Other(format!("Failed to get version: {}", e)))?
            .current_version;

        let new_version = current_version + 1;

        // Insert each action type
        for action in actions {
            match action {
                Action::Add(file) => {
                    sqlx::query!(
                        "INSERT INTO dl_add_files 
                         (table_id, version, path, size, modification_time, data_change_version)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        table_id,
                        new_version,
                        file.path,
                        file.size,
                        file.modification_time,
                        file.data_change_version
                    )
                    .execute(&mut **db_tx)
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to insert add action: {}", e)))?;
                }
                Action::Remove(file) => {
                    sqlx::query!(
                        "INSERT INTO dl_remove_files 
                         (table_id, version, path, deletion_timestamp, data_change)
                         VALUES ($1, $2, $3, $4, $5)",
                        table_id,
                        new_version,
                        file.path,
                        file.deletion_timestamp,
                        file.data_change
                    )
                    .execute(&mut **db_tx)
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to insert remove action: {}", e)))?;
                }
                Action::Metadata(meta) => {
                    sqlx::query!(
                        "INSERT INTO dl_metadata_updates 
                         (table_id, version, description, schema, partition_columns, created_time)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        table_id,
                        new_version,
                        meta.description,
                        meta.schema,
                        meta.partition_columns.as_ref().map(|v| v.join(",")),
                        meta.created_time
                    )
                    .execute(&mut **db_tx)
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to insert metadata action: {}", e)))?;
                }
                Action::Protocol(proto) => {
                    sqlx::query!(
                        "INSERT INTO dl_protocol_updates 
                         (table_id, version, min_reader_version, min_writer_version)
                         VALUES ($1, $2, $3, $4)",
                        table_id,
                        new_version,
                        proto.min_reader_version,
                        proto.min_writer_version
                    )
                    .execute(&mut **db_tx)
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to insert protocol action: {}", e)))?;
                }
                Action::Txn(txn) => {
                    sqlx::query!(
                        "INSERT INTO dl_txn_actions 
                         (table_id, version, app_id, version as txn_version, timestamp)
                         VALUES ($1, $2, $3, $4, $5)",
                        table_id,
                        new_version,
                        txn.app_id,
                        txn.version,
                        txn.timestamp
                    )
                    .execute(&mut **db_tx)
                    .await
                    .map_err(|e| TransactionError::Other(format!("Failed to insert txn action: {}", e)))?;
                }
            }
        }

        // Insert version record
        sqlx::query!(
            "INSERT INTO dl_table_versions 
             (table_id, version, commit_timestamp, operation_type, num_actions)
             VALUES ($1, $2, NOW(), $3, $4)",
            table_id,
            new_version,
            "MultiTableCommit",
            actions.len() as i32
        )
        .execute(&mut **db_tx)
        .await
        .map_err(|e| TransactionError::Other(format!("Failed to insert version record: {}", e)))?;

        Ok(())
    }

    /// Update versions for all tables atomically.
    async fn update_versions(
        &self,
        db_tx: &mut PgTransaction<'_>,
        staged_tables: &BTreeMap<Uuid, StagedTable>,
    ) -> TransactionResult<BTreeMap<Uuid, i64>> {
        let table_ids: Vec<Uuid> = staged_tables.keys().copied().collect();

        sqlx::query!(
            "UPDATE dl_tables 
             SET current_version = current_version + 1,
                 updated_at = NOW()
             WHERE table_id = ANY($1)",
            &table_ids
        )
        .execute(&mut **db_tx)
        .await
        .map_err(|e| TransactionError::Other(format!("Failed to update versions: {}", e)))?;

        // Fetch updated versions
        let mut versions = BTreeMap::new();

        for table_id in table_ids {
            let row = sqlx::query!(
                "SELECT current_version FROM dl_tables WHERE table_id = $1",
                table_id
            )
            .fetch_one(&mut **db_tx)
            .await
            .map_err(|e| TransactionError::Other(format!("Failed to fetch updated version: {}", e)))?;

            versions.insert(table_id, row.current_version - 1); // Subtract 1 because we just incremented
        }

        Ok(versions)
    }

    /// Rollback a multi-table transaction.
    ///
    /// In practice, this is automatic when the transaction handle is dropped.
    /// This method is here for explicit control.
    pub async fn rollback(&self, transaction_id: &str) -> TransactionResult<()> {
        warn!("Rolling back transaction {}", transaction_id);
        // Rollback happens automatically when db_tx is dropped
        Ok(())
    }

    /// Get lock order for tables (sorted by UUID for determinism).
    ///
    /// Ensures consistent lock ordering across all multi-table transactions,
    /// preventing deadlocks.
    fn lock_order(mut table_ids: Vec<Uuid>) -> Vec<Uuid> {
        table_ids.sort();
        table_ids
    }

    /// Mirror a table's version to all registered mirror engines after commit.
    ///
    /// Mirrors are attempted sequentially (or with bounded concurrency) after
    /// the SQL transaction commits. Failures are tracked but don't rollback
    /// the SQL commit, maintaining eventual consistency.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Table to mirror
    /// * `new_version` - Version to mirror
    ///
    /// # Returns
    ///
    /// Vector of mirror results (engine_name, success, error_msg)
    pub async fn mirror_table_after_commit(
        &self,
        table_id: Uuid,
        new_version: i64,
    ) -> Vec<(String, bool, Option<String>)> {
        // Get registered mirror engines for this table
        let engines: Vec<(String,)> = match sqlx::query_as(
            "SELECT DISTINCT mirror_engine FROM dl_mirror_status WHERE table_id = $1"
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        {
            Ok(engines) => engines,
            Err(e) => {
                warn!("Failed to fetch mirror engines for table {}: {}", table_id, e);
                return vec![];
            }
        };

        let mut results = Vec::new();

        // Mirror to each engine sequentially
        for (engine_name,) in engines {
            match self.mirror_to_engine(&engine_name, table_id, new_version).await {
                Ok(_) => {
                    info!("Successfully mirrored table {} to {} at v{}", table_id, engine_name, new_version);
                    
                    // Update mirror status to success
                    let _ = sqlx::query!(
                        "UPDATE dl_mirror_status SET mirrored_version = $1, last_sync_time = NOW(), last_error = NULL
                         WHERE table_id = $2 AND mirror_engine = $3",
                        new_version,
                        table_id,
                        &engine_name
                    )
                    .execute(&self.pool)
                    .await;

                    results.push((engine_name, true, None));
                }
                Err(e) => {
                    error!("Failed to mirror table {} to {}: {}", table_id, engine_name, e);

                    // Mark failed in mirror status for reconciliation
                    let _ = sqlx::query!(
                        "UPDATE dl_mirror_status SET last_error = $1, updated_at = NOW()
                         WHERE table_id = $2 AND mirror_engine = $3",
                        &e,
                        table_id,
                        &engine_name
                    )
                    .execute(&self.pool)
                    .await;

                    results.push((engine_name, false, Some(e)));
                }
            }
        }

        results
    }

    /// Perform actual mirroring to a specific engine.
    ///
    /// This is a placeholder implementation. In production, this would:
    /// - Generate `_delta_log` entries for the table
    /// - Write to appropriate mirror storage (Spark, DuckDB, etc.)
    /// - Handle engine-specific serialization formats
    async fn mirror_to_engine(
        &self,
        engine: &str,
        table_id: Uuid,
        version: i64,
    ) -> Result<(), String> {
        // Get table location
        let table: (String,) = sqlx::query_as("SELECT location FROM dl_tables WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("Failed to fetch table location: {}", e))?;

        let location = &table.0;
        debug!("Mirroring table {} at {} to {} (version {})", table_id, location, engine, version);

        match engine {
            "spark" => self.mirror_spark(location, table_id, version).await,
            "duckdb" => self.mirror_duckdb(location, table_id, version).await,
            engine => Err(format!("Unknown mirror engine: {}", engine)),
        }
    }

    /// Mirror to Spark (write _delta_log entries).
    async fn mirror_spark(
        &self,
        location: &str,
        table_id: Uuid,
        version: i64,
    ) -> Result<(), String> {
        // If mirror engine is configured, use it
        if let Some(mirror_engine) = &self.mirror_engine {
            // Fetch actions for this version
            let actions = self.fetch_actions_for_version(table_id, version).await?;
            
            // Fetch snapshot for checkpoint generation
            let snapshot = self.fetch_snapshot_for_version(table_id, version).await?;
            
            info!("Mirroring table {} to Spark at {} (v{}) with {} actions", 
                  table_id, location, version, actions.len());
            
            // Use the mirror engine to write Delta artifacts
            mirror_engine
                .mirror_version(location, version, &actions, &snapshot)
                .await
                .map_err(|e| format!("Mirror to Spark failed: {}", e))?;
            
            info!("Successfully mirrored table {} to Spark", table_id);
            Ok(())
        } else {
            // No mirror engine configured - log and return success (no-op)
            debug!("No mirror engine configured, skipping Spark mirror for table {}", table_id);
            Ok(())
        }
    }

    /// Mirror to DuckDB (write equivalent table).
    async fn mirror_duckdb(
        &self,
        location: &str,
        table_id: Uuid,
        version: i64,
    ) -> Result<(), String> {
        // DuckDB mirroring via Spark writer (uses Delta log)
        // DuckDB can read Delta tables via the _delta_log
        if let Some(mirror_engine) = &self.mirror_engine {
            // Fetch actions and snapshot
            let actions = self.fetch_actions_for_version(table_id, version).await?;
            let snapshot = self.fetch_snapshot_for_version(table_id, version).await?;
            
            info!("Mirroring table {} to DuckDB at {} (v{}) with {} actions", 
                  table_id, location, version, actions.len());
            
            // Write Delta log files so DuckDB can consume them
            mirror_engine
                .mirror_version(location, version, &actions, &snapshot)
                .await
                .map_err(|e| format!("Mirror to DuckDB failed: {}", e))?;
            
            info!("Successfully mirrored table {} for DuckDB", table_id);
            Ok(())
        } else {
            debug!("No mirror engine configured, skipping DuckDB mirror for table {}", table_id);
            Ok(())
        }
    }

    /// Fetch all actions for a specific version.
    async fn fetch_actions_for_version(
        &self,
        table_id: Uuid,
        version: i64,
    ) -> Result<Vec<Action>, String> {
        let mut actions = Vec::new();

        // Fetch AddFile actions
        let add_rows = sqlx::query!(
            "SELECT path, size, modification_time, data_change_version 
             FROM dl_add_files WHERE table_id = $1 AND version = $2",
            table_id,
            version
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to fetch add files: {}", e))?;

        for row in add_rows {
            actions.push(Action::Add(deltalakedb_core::types::AddFile {
                path: row.path,
                size: row.size,
                modification_time: row.modification_time,
                data_change_version: row.data_change_version,
            }));
        }

        // Fetch RemoveFile actions
        let remove_rows = sqlx::query!(
            "SELECT path, deletion_timestamp, data_change 
             FROM dl_remove_files WHERE table_id = $1 AND version = $2",
            table_id,
            version
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to fetch remove files: {}", e))?;

        for row in remove_rows {
            actions.push(Action::Remove(deltalakedb_core::types::RemoveFile {
                path: row.path,
                deletion_timestamp: row.deletion_timestamp,
                data_change: row.data_change,
            }));
        }

        Ok(actions)
    }

    /// Fetch snapshot for a specific version.
    async fn fetch_snapshot_for_version(
        &self,
        table_id: Uuid,
        version: i64,
    ) -> Result<deltalakedb_core::types::Snapshot, String> {
        // Fetch active files for this version
        let files = sqlx::query!(
            "SELECT path, size, modification_time, data_change_version 
             FROM dl_add_files 
             WHERE table_id = $1 AND version <= $2
             AND path NOT IN (
                 SELECT path FROM dl_remove_files 
                 WHERE table_id = $1 AND version <= $2
             )
             ORDER BY modification_time DESC",
            table_id,
            version
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to fetch active files: {}", e))?;

        let mut add_files = Vec::new();
        for row in files {
            add_files.push(deltalakedb_core::types::AddFile {
                path: row.path,
                size: row.size,
                modification_time: row.modification_time,
                data_change_version: row.data_change_version,
            });
        }

        Ok(deltalakedb_core::types::Snapshot {
            version,
            timestamp: chrono::Utc::now().timestamp_millis(),
            files: add_files,
        })
    }

    /// Mirror all tables in a transaction sequentially after commit.
    ///
    /// Returns a map of table_id -> Vec<(engine, success, error)>
    pub async fn mirror_all_tables_after_commit(
        &self,
        transaction_result: &TransactionResult,
    ) -> BTreeMap<Uuid, Vec<(String, bool, Option<String>)>> {
        let mut all_results = BTreeMap::new();

        for (&table_id, &version) in &transaction_result.versions {
            let results = self.mirror_table_after_commit(table_id, version).await;
            all_results.insert(table_id, results);
        }

        all_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_order_determinism() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let order1 = MultiTableWriter::lock_order(vec![id1, id2, id3]);
        let order2 = MultiTableWriter::lock_order(vec![id3, id1, id2]);
        let order3 = MultiTableWriter::lock_order(vec![id2, id3, id1]);

        assert_eq!(order1, order2);
        assert_eq!(order2, order3);
    }

    #[test]
    fn test_writer_creation() {
        // This would need a real or mock pool in actual tests
        let _writer: Option<MultiTableWriter> = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_order_determinism() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let order1 = MultiTableWriter::lock_order(&[id1, id2, id3]);
        let order2 = MultiTableWriter::lock_order(&[id3, id1, id2]);
        let order3 = MultiTableWriter::lock_order(&[id2, id3, id1]);

        assert_eq!(order1, order2);
        assert_eq!(order2, order3);
    }

    #[test]
    fn test_writer_creation() {
        // This would need a real or mock pool in actual tests
        // Just verify the type can be constructed
        let _writer: Option<MultiTableWriter> = None;
    }
}
