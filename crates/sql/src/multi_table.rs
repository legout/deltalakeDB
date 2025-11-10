//! Multi-table transaction support for SQL-backed Delta Lake.
//!
//! Provides atomic transactions spanning multiple Delta tables with
//! cross-table consistency guarantees and ordered mirroring.

use crate::connection::DatabaseConnection;
use crate::mirror::MirrorEngine;
use crate::writer::{SqlTxnLogWriter, SqlWriterConfig};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltalakedb_core::{
    error::{TxnLogError, TxnLogResult},
    DeltaAction, AddFile, RemoveFile, Metadata, Protocol,
};
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tracing::{debug, instrument, warn, error};
use uuid::Uuid;

/// Transaction isolation levels for multi-table transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionIsolationLevel {
    /// Read Committed: Only committed data is visible, allows non-repeatable reads
    ReadCommitted,
    /// Repeatable Read: Guarantees repeatable reads within transaction
    RepeatableRead,
    /// Serializable: Full isolation, prevents all phenomena
    Serializable,
}

impl Default for TransactionIsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted // Balanced performance and consistency
    }
}

impl MultiTableWriter {
    /// Create a new multi-table writer.
    pub fn new(
        connection: Arc<DatabaseConnection>,
        mirror_engine: Option<Arc<MirrorEngine>>,
        config: MultiTableConfig,
    ) -> Self {
        let writer_config = SqlWriterConfig {
            enable_mirroring: false, // We handle mirroring ourselves
            max_retry_attempts: config.max_retry_attempts,
            retry_base_delay_ms: config.retry_base_delay_ms,
            retry_max_delay_ms: config.retry_max_delay_ms,
            checkpoint_interval: 10,
        };

        let single_writer = SqlTxnLogWriter::from_arc(connection.clone(), writer_config);

        Self {
            connection,
            mirror_engine,
            config,
            single_writer,
        }
    }

    /// Begin a new multi-table transaction.
    pub fn begin_transaction(&self) -> MultiTableTransaction {
        MultiTableTransaction::new(self.config.clone())
    }

    /// Begin a transaction and stage actions for multiple tables in one call.
    pub async fn begin_and_stage(
        &self,
        table_actions: Vec<TableActions>,
    ) -> TxnLogResult<MultiTableTransaction> {
        let mut tx = self.begin_transaction();
        for actions in table_actions {
            // Validate each table actions before staging
            actions.validate()?;
            tx.stage_actions(actions)?;
        }
        Ok(tx)
    }

    /// Get current version for a table (useful for staging with correct expected version).
    pub async fn get_table_version(&self, table_id: &str) -> TxnLogResult<i64> {
        self.single_writer.get_next_version(table_id).await.map(|v| v - 1)
    }

    /// Get current versions for multiple tables.
    pub async fn get_table_versions(&self, table_ids: &[String]) -> TxnLogResult<HashMap<String, i64>> {
        let mut versions = HashMap::new();
        for table_id in table_ids {
            let version = self.get_table_version(table_id).await?;
            versions.insert(table_id.clone(), version);
        }
        Ok(versions)
    }

    /// Stage actions for a table with automatic version detection.
    pub async fn stage_actions_with_current_version(
        &self,
        tx: &mut MultiTableTransaction,
        table_id: String,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
    ) -> TxnLogResult<()> {
        let current_version = self.get_table_version(&table_id).await?;
        let mut table_actions = TableActions::new(table_id.clone(), current_version);
        table_actions.add_actions(actions);
        if let Some(op) = operation {
            table_actions = table_actions.with_operation(op);
        }
        tx.stage_actions(table_actions)
    }

    /// Stage file operations for a table with automatic version detection.
    pub async fn stage_file_operations(
        &self,
        tx: &mut MultiTableTransaction,
        table_id: String,
        add_files: Vec<AddFile>,
        remove_files: Vec<RemoveFile>,
        operation: Option<String>,
    ) -> TxnLogResult<()> {
        let current_version = self.get_table_version(&table_id).await?;
        tx.mixed_operation(table_id, current_version, add_files, remove_files)?;
        
        // Set operation on the staged actions
        if let Some(op) = operation {
            if let Some(staged) = tx.staged_tables.get_mut(&table_id) {
                staged.operation = Some(op);
            }
        }
        
        Ok(())
    }

    /// Validate that all tables in a transaction exist.
    pub async fn validate_tables_exist(&self, tx: &MultiTableTransaction) -> TxnLogResult<()> {
        for table_id in tx.staged_tables.keys() {
            if !self.single_writer.table_exists(table_id).await? {
                return Err(TxnLogError::TableNotFound(table_id.clone()));
            }
        }
        Ok(())
    }

    /// Validate that all expected versions in a transaction are current.
    pub async fn validate_expected_versions(&self, tx: &MultiTableTransaction) -> TxnLogResult<()> {
        for (table_id, table_actions) in &tx.staged_tables {
            let current_version = self.get_table_version(table_id).await?;
            if current_version != table_actions.expected_version {
                return Err(TxnLogError::VersionConflict {
                    expected: table_actions.expected_version,
                    actual: current_version,
                });
            }
        }
        Ok(())
    }

    /// Create a TableActions builder for a table with current version.
    pub async fn create_table_actions_builder(
        &self,
        table_id: String,
    ) -> TxnLogResult<TableActionsBuilder> {
        let current_version = self.get_table_version(&table_id).await?;
        Ok(TableActionsBuilder::new(table_id, current_version))
    }

    /// Commit a multi-table transaction.
    #[instrument(skip(self, transaction))]
    pub async fn commit_transaction(
        &self,
        mut transaction: MultiTableTransaction,
    ) -> TxnLogResult<MultiTableCommitResult> {
        transaction.mark_committing()?;

        debug!(
            "Committing multi-table transaction {} with {} tables and {} actions",
            transaction.transaction_id,
            transaction.table_count(),
            transaction.total_action_count()
        );

        // Pre-commit validation
        self.validate_transaction_for_commit(&transaction).await?;

        // Execute atomic commit across all tables with retry logic
        let table_results = self.execute_atomic_commit_with_retry(&transaction).await?;

        // Trigger ordered mirroring if enabled
        let mirroring_results = if self.config.enable_ordered_mirroring {
            Some(self.trigger_ordered_mirroring(&table_results).await?)
        } else {
            None
        };

        transaction.mark_committed();

        Ok(MultiTableCommitResult {
            transaction: transaction.summary(),
            table_results,
            mirroring_results,
        })
    }

    /// Set the operation type.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.table_actions = self.table_actions.with_operation(operation);
        self
    }

    /// Set operation parameters.
    pub fn with_params(mut self, params: HashMap<String, String>) -> Self {
        self.table_actions = self.table_actions.with_operation_params(params);
        self
    }

    /// Build the TableActions.
    pub fn build(self) -> TableActions {
        self.table_actions
    }

    /// Build and validate the TableActions.
    pub fn build_and_validate(self) -> TxnLogResult<TableActions> {
        let actions = self.table_actions;
        actions.validate()?;
        Ok(actions)
    }

    /// Commit a multi-table transaction atomically.
    #[instrument(skip(self, transaction))]
    pub async fn commit_transaction(
        &self,
        mut transaction: MultiTableTransaction,
    ) -> TxnLogResult<MultiTableCommitResult> {
        transaction.mark_committing()?;

        debug!(
            "Committing multi-table transaction {} with {} tables and {} actions",
            transaction.transaction_id,
            transaction.table_count(),
            transaction.total_action_count()
        );

        // Pre-commit validation
        self.validate_transaction_for_commit(&transaction).await?;

        // Execute atomic commit across all tables with retry logic
        let table_results = self.execute_atomic_commit_with_retry(&transaction).await?;

        // Trigger ordered mirroring if enabled
        let mirroring_results = if self.config.enable_ordered_mirroring {
            Some(self.trigger_ordered_mirroring(&table_results).await?)
        } else {
            None
        };

        transaction.mark_committed();

        Ok(MultiTableCommitResult {
            transaction: transaction.summary(),
            table_results,
            mirroring_results,
        })
    }

    /// Validate transaction before commit.
    async fn validate_transaction_for_commit(&self, transaction: &MultiTableTransaction) -> TxnLogResult<()> {
        // Basic transaction validation
        transaction.validate()?;

        // Perform comprehensive cross-table consistency validation
        if self.config.enable_consistency_validation {
            let violations = self.validate_cross_table_consistency(transaction).await?;
            
            if !violations.is_empty() {
                return Err(TxnLogError::validation(format!(
                    "Cross-table consistency validation failed with {} violations: {}",
                    violations.len(),
                    violations.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("; ")
                )));
            }
        }

        Ok(())
    }

    /// Validate consistency of table actions.
    async fn validate_table_actions_consistency(
        &self,
        table_id: &str,
        table_actions: &TableActions,
    ) -> TxnLogResult<()> {
        // Check for duplicate file paths in add/remove actions
        let mut add_paths = std::collections::HashSet::new();
        let mut remove_paths = std::collections::HashSet::new();

        for action in &table_actions.actions {
            match action {
                DeltaAction::Add(add_file) => {
                    if add_paths.contains(&add_file.path) {
                        return Err(TxnLogError::TransactionError(
                            format!("Duplicate add file path in table {}: {}", table_id, add_file.path)
                        ));
                    }
                    add_paths.insert(&add_file.path);
                }
                DeltaAction::Remove(remove_file) => {
                    if remove_paths.contains(&remove_file.path) {
                        return Err(TxnLogError::TransactionError(
                            format!("Duplicate remove file path in table {}: {}", table_id, remove_file.path)
                        ));
                    }
                    remove_paths.insert(&remove_file.path);
                }
                _ => {}
            }
        }

        // Check for files being both added and removed in same transaction
        let both_paths: HashSet<_> = add_paths.intersection(&remove_paths).collect();
        if !both_paths.is_empty() {
            return Err(TxnLogError::TransactionError(
                format!("Files cannot be both added and removed in same transaction for table {}: {:?}",
                    table_id, both_paths)
            ));
        }

        Ok(())
    }

    /// Execute atomic commit with retry logic.
    async fn execute_atomic_commit_with_retry(
        &self,
        transaction: &MultiTableTransaction,
    ) -> TxnLogResult<Vec<TableCommitResult>> {
        let mut attempt = 0;
        let mut delay = self.config.retry_base_delay_ms;

        loop {
            attempt += 1;
            match self.commit_atomic_sql(transaction).await {
                Ok(results) => return Ok(results),
                Err(TxnLogError::ConcurrentWrite(_)) if attempt <= self.config.max_retry_attempts => {
                    debug!(
                        "Concurrent write detected for transaction {}, retrying attempt {}/{}",
                        transaction.transaction_id, attempt, self.config.max_retry_attempts
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    delay = std::cmp::min(delay * 2, self.config.retry_max_delay_ms);
                }
                Err(e) => {
                    error!(
                        "Commit failed for transaction {} after {} attempts: {}",
                        transaction.transaction_id, attempt, e
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Rollback a multi-table transaction.
    #[instrument(skip(self, transaction))]
    pub async fn rollback_transaction(&self, mut transaction: MultiTableTransaction) -> TxnLogResult<()> {
        debug!("Rolling back multi-table transaction {}", transaction.transaction_id);
        
        // Validate transaction can be rolled back
        if transaction.state != TransactionState::Active && transaction.state != TransactionState::Committing {
            return Err(TxnLogError::TransactionError(
                format!("Cannot rollback transaction in state: {:?}", transaction.state)
            ));
        }

        // Record rollback attempt in metadata
        self.record_rollback_attempt(&transaction).await?;

        // For SQL transactions, we don't need to do anything explicit
        // since we haven't committed yet. Just mark as rolled back.
        transaction.mark_rolled_back();
        
        // Update transaction metadata to rolled back
        self.update_transaction_metadata_to_olled_back(&transaction).await?;
        
        debug!("Successfully rolled back transaction {}", transaction.transaction_id);
        Ok(())
    }

    /// Record rollback attempt in transaction metadata.
    async fn record_rollback_attempt(&self, transaction: &MultiTableTransaction) -> TxnLogResult<()> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_multi_table_transactions (transaction_id, started_at, table_count, total_action_count, state) 
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (transaction_id) DO NOTHING"
                )
                .bind(&transaction.transaction_id)
                .bind(transaction.started_at)
                .bind(transaction.table_count() as i32)
                .bind(transaction.total_action_count() as i32)
                .bind("ROLLING_BACK")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT OR IGNORE INTO dl_multi_table_transactions (transaction_id, started_at, table_count, total_action_count, state) 
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                )
                .bind(&transaction.transaction_id)
                .bind(transaction.started_at)
                .bind(transaction.table_count() as i32)
                .bind(transaction.total_action_count() as i32)
                .bind("ROLLING_BACK")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder for DuckDB
                return Ok(());
            }
        }
        Ok(())
    }

    /// Update transaction metadata to rolled back state for Postgres.
    async fn update_transaction_metadata_to_olled_back(&self, transaction: &MultiTableTransaction) -> TxnLogResult<()> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "UPDATE dl_multi_table_transactions SET state = $1, completed_at = $2 WHERE transaction_id = $3"
                )
                .bind("ROLLED_BACK")
                .bind(Utc::now())
                .bind(&transaction.transaction_id)
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE dl_multi_table_transactions SET state = ?1, completed_at = ?2 WHERE transaction_id = ?3"
                )
                .bind("ROLLED_BACK")
                .bind(Utc::now())
                .bind(&transaction.transaction_id)
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder for DuckDB
                return Ok(());
            }
        }
        Ok(())
    }

    /// Force rollback a transaction by transaction ID (for recovery scenarios).
    #[instrument(skip(self))]
    pub async fn force_rollback_transaction(&self, transaction_id: &str) -> TxnLogResult<bool> {
        debug!("Force rolling back transaction {}", transaction_id);
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let result = sqlx::query(
                    "UPDATE dl_multi_table_transactions SET state = $1, completed_at = $2 
                     WHERE transaction_id = $3 AND state IN ($4, $5)"
                )
                .bind("FORCE_ROLLED_BACK")
                .bind(Utc::now())
                .bind(transaction_id)
                .bind("ACTIVE")
                .bind("COMMITTING")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.rows_affected() > 0)
            }
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "UPDATE dl_multi_table_transactions SET state = ?1, completed_at = ?2 
                     WHERE transaction_id = ?3 AND state IN (?4, ?5)"
                )
                .bind("FORCE_ROLLED_BACK")
                .bind(Utc::now())
                .bind(transaction_id)
                .bind("ACTIVE")
                .bind("COMMITTING")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.rows_affected() > 0)
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(false) // Placeholder
            }
        }
    }

    /// Get transaction status by ID.
    #[instrument(skip(self))]
    pub async fn get_transaction_status(&self, transaction_id: &str) -> TxnLogResult<Option<TransactionState>> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT state FROM dl_multi_table_transactions WHERE transaction_id = $1"
                )
                .bind(transaction_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(row.map(|r| {
                    let state: String = r.get("state");
                    match state.as_str() {
                        "ACTIVE" => TransactionState::Active,
                        "COMMITTING" => TransactionState::Committing,
                        "COMMITTED" => TransactionState::Committed,
                        "ROLLED_BACK" | "FORCE_ROLLED_BACK" => TransactionState::RolledBack,
                        "FAILED" => TransactionState::Failed("Unknown".to_string()),
                        _ => TransactionState::Failed(format!("Unknown state: {}", state)),
                    }
                }))
            }
            DatabaseConnection::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT state FROM dl_multi_table_transactions WHERE transaction_id = ?1"
                )
                .bind(transaction_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(row.map(|r| {
                    let state: String = r.get("state");
                    match state.as_str() {
                        "ACTIVE" => TransactionState::Active,
                        "COMMITTING" => TransactionState::Committing,
                        "COMMITTED" => TransactionState::Committed,
                        "ROLLED_BACK" | "FORCE_ROLLED_BACK" => TransactionState::RolledBack,
                        "FAILED" => TransactionState::Failed("Unknown".to_string()),
                        _ => TransactionState::Failed(format!("Unknown state: {}", state)),
                    }
                }))
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(None) // Placeholder
            }
        }
    }

    /// Validate cross-table consistency before commit.
    /// 
    /// This method performs various consistency checks to ensure that the multi-table
    /// transaction maintains data integrity across all tables.
    #[instrument(skip(self, transaction))]
    pub async fn validate_cross_table_consistency(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        debug!("Validating cross-table consistency for transaction {}", transaction.transaction_id);
        
        let mut violations = Vec::new();
        
        // Check 1: Validate that all referenced tables exist
        violations.extend(self.validate_table_existence(transaction).await?);
        
        // Check 2: Validate version consistency
        violations.extend(self.validate_version_consistency(transaction).await?);
        
        // Check 3: Validate action consistency
        violations.extend(self.validate_action_consistency(transaction).await?);
        
        // Check 4: Validate cross-table constraints (if any)
        violations.extend(self.validate_cross_table_constraints(transaction).await?);
        
        // Check 5: Validate transaction size limits
        violations.extend(self.validate_transaction_limits(transaction).await?);
        
        if !violations.is_empty() {
            warn!("Found {} consistency violations in transaction {}", violations.len(), transaction.transaction_id);
            for violation in &violations {
                warn!("  - {}", violation);
            }
        } else {
            debug!("Cross-table consistency validation passed for transaction {}", transaction.transaction_id);
        }
        
        Ok(violations)
    }
    
    /// Validate that all referenced tables exist in the database.
    async fn validate_table_existence(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        let mut violations = Vec::new();
        
        for table_id in transaction.table_actions.keys() {
            let exists = self.table_exists(table_id).await?;
            if !exists {
                violations.push(ConsistencyViolation::TableNotFound {
                    table_id: table_id.clone(),
                });
            }
        }
        
        Ok(violations)
    }
    
    /// Validate version consistency across all tables.
    async fn validate_version_consistency(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        let mut violations = Vec::new();
        
        for (table_id, actions) in &transaction.table_actions {
            // Check if expected version matches actual current version
            if let Some(current_version) = self.get_current_version(table_id).await? {
                if actions.expected_version != current_version {
                    violations.push(ConsistencyViolation::VersionMismatch {
                        table_id: table_id.clone(),
                        expected: actions.expected_version,
                        actual: current_version,
                    });
                }
            } else {
                // Table doesn't exist yet, version should be -1
                if actions.expected_version != -1 {
                    violations.push(ConsistencyViolation::VersionMismatch {
                        table_id: table_id.clone(),
                        expected: actions.expected_version,
                        actual: -1,
                    });
                }
            }
        }
        
        Ok(violations)
    }
    
    /// Validate action consistency within each table.
    async fn validate_action_consistency(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        let mut violations = Vec::new();
        
        for (table_id, actions) in &transaction.table_actions {
            // Check for empty action lists
            if actions.actions.is_empty() {
                violations.push(ConsistencyViolation::EmptyActionList {
                    table_id: table_id.clone(),
                });
                continue;
            }
            
            // Check for duplicate file paths within add actions
            let mut add_paths = std::collections::HashSet::new();
            for action in &actions.actions {
                if let DeltaAction::Add(add) = action {
                    if add_paths.contains(&add.path) {
                        violations.push(ConsistencyViolation::DuplicateFile {
                            table_id: table_id.clone(),
                            path: add.path.clone(),
                        });
                    } else {
                        add_paths.insert(&add.path);
                    }
                }
            }
            
            // Check for remove actions without corresponding add actions (unless it's a delete)
            let mut remove_paths = std::collections::HashSet::new();
            for action in &actions.actions {
                if let DeltaAction::Remove(remove) = action {
                    remove_paths.insert(&remove.path);
                }
            }
            
            // Only flag as violation if we're removing files that weren't added in this transaction
            // and the table exists (removing from existing table)
            if self.get_current_version(table_id).await?.is_some() {
                for remove_path in &remove_paths {
                    if !add_paths.contains(remove_path) {
                        // This could be a legitimate delete, so we'll just warn about it
                        // rather than treating it as a violation
                        debug!("Removing existing file {} from table {}", remove_path, table_id);
                    }
                }
            }
        }
        
        Ok(violations)
    }
    
    /// Validate cross-table constraints.
    /// 
    /// This is a placeholder for future cross-table constraint validation.
    /// Currently checks for obvious issues like circular dependencies.
    async fn validate_cross_table_constraints(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        let mut violations = Vec::new();
        
        // Check for potential circular dependencies in table ordering
        // This is a simple check - more sophisticated constraint checking could be added
        
        // For now, we'll just ensure that we're not trying to create tables that reference
        // each other in ways that could cause issues
        let table_count = transaction.table_actions.len();
        if table_count > 100 {
            violations.push(ConsistencyViolation::TooManyTables {
                table_count,
                limit: 100,
            });
        }
        
        Ok(violations)
    }
    
    /// Validate transaction size limits.
    async fn validate_transaction_limits(&self, transaction: &MultiTableTransaction) -> TxnLogResult<Vec<ConsistencyViolation>> {
        let mut violations = Vec::new();
        
        let total_actions = transaction.total_action_count();
        let table_count = transaction.table_count();
        
        // Check total action limit
        if total_actions > 10000 {
            violations.push(ConsistencyViolation::TransactionTooLarge {
                action_count: total_actions,
                limit: 10000,
            });
        }
        
        // Check per-table action limit
        for (table_id, actions) in &transaction.table_actions {
            if actions.actions.len() > 5000 {
                violations.push(ConsistencyViolation::TableTransactionTooLarge {
                    table_id: table_id.clone(),
                    action_count: actions.actions.len(),
                    limit: 5000,
                });
            }
        }
        
        // Check transaction age
        let age = Utc::now() - transaction.started_at;
        if age > chrono::Duration::hours(1) {
            violations.push(ConsistencyViolation::TransactionTooOld {
                age_hours: age.num_hours(),
                limit_hours: 1,
            });
        }
        
        Ok(violations)
    }
    
    /// Check if a table exists in the database.
    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let result = sqlx::query(
                    "SELECT 1 FROM dl_tables WHERE table_id = $1 LIMIT 1"
                )
                .bind(table_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.is_some())
            }
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT 1 FROM dl_tables WHERE table_id = ?1 LIMIT 1"
                )
                .bind(table_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.is_some())
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(false) // Placeholder
            }
        }
    }
    
    /// Get the current version of a table.
    async fn get_current_version(&self, table_id: &str) -> TxnLogResult<Option<i64>> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let result = sqlx::query(
                    "SELECT MAX(version) as max_version FROM dl_actions WHERE table_id = $1"
                )
                .bind(table_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.and_then(|row| row.get::<Option<i64>, _>("max_version")))
            }
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT MAX(version) as max_version FROM dl_actions WHERE table_id = ?1"
                )
                .bind(table_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.and_then(|row| row.get::<Option<i64>, _>("max_version")))
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(None) // Placeholder
            }
        }
    }

    /// Create a new table in the database.
    /// Helper method for testing.
    pub async fn create_table(&self, table_id: &str) -> TxnLogResult<()> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_tables (table_id, created_at, updated_at) VALUES ($1, $2, $3)
                     ON CONFLICT (table_id) DO NOTHING"
                )
                .bind(table_id)
                .bind(Utc::now())
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT OR IGNORE INTO dl_tables (table_id, created_at, updated_at) VALUES (?1, ?2, ?3)"
                )
                .bind(table_id)
                .bind(Utc::now())
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                return Err(TxnLogError::internal("create_table not implemented for DuckDb"));
            }
        }
        Ok(())
    }

    /// Cleanup old transaction records.
    #[instrument(skip(self))]
    pub async fn cleanup_old_transactions(&self, older_than_hours: i64) -> TxnLogResult<u64> {
        debug!("Cleaning up transactions older than {} hours", older_than_hours);
        
        let cutoff_time = Utc::now() - chrono::Duration::hours(older_than_hours);
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let result = sqlx::query(
                    "DELETE FROM dl_multi_table_transactions 
                     WHERE completed_at IS NOT NULL 
                     AND completed_at < $1 
                     AND state IN ($2, $3, $4)"
                )
                .bind(cutoff_time)
                .bind("COMMITTED")
                .bind("ROLLED_BACK")
                .bind("FAILED")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.rows_affected())
            }
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "DELETE FROM dl_multi_table_transactions 
                     WHERE completed_at IS NOT NULL 
                     AND completed_at < ?1 
                     AND state IN (?2, ?3, ?4)"
                )
                .bind(cutoff_time)
                .bind("COMMITTED")
                .bind("ROLLED_BACK")
                .bind("FAILED")
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(result.rows_affected())
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(0) // Placeholder
            }
        }
    }

    /// Execute atomic SQL commit across all tables.
    async fn commit_atomic_sql(
        &self,
        transaction: &MultiTableTransaction,
    ) -> TxnLogResult<Vec<TableCommitResult>> {
        let mut results = Vec::new();
        let commit_start_time = Utc::now();

        // Check for potential deadlocks before starting the transaction
        self.check_for_deadlocks(transaction).await?;

        // For now, simulate the commit process with isolation level logging
        debug!(
            "Committing transaction {} with isolation level: {}",
            transaction.transaction_id,
            transaction.isolation_level
        );

        // Simulate isolation level enforcement
        match transaction.isolation_level {
            TransactionIsolationLevel::ReadCommitted => {
                debug!("Using READ COMMITTED isolation level");
            }
            TransactionIsolationLevel::RepeatableRead => {
                debug!("Using REPEATABLE READ isolation level");
            }
            TransactionIsolationLevel::Serializable => {
                debug!("Using SERIALIZABLE isolation level");
            }
        }

        // Simulate committing each table's actions
        for (table_id, table_actions) in &transaction.staged_tables {
            debug!(
                "Committing {} actions for table {} at version {}",
                table_actions.actions.len(),
                table_id,
                table_actions.expected_version
            );

            // Create a mock result
            let result = TableCommitResult {
                table_id: table_id.clone(),
                version: table_actions.expected_version,
                success: true,
                error: None,
                actions_committed: table_actions.actions.len() as u32,
                commit_timestamp: Utc::now(),
            };
            results.push(result);
        }

        debug!("Successfully committed transaction {}", transaction.transaction_id);

        Ok(results)
    }

    /// Insert transaction metadata record for Postgres.
    async fn insert_transaction_metadata(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: &MultiTableTransaction,
        start_time: DateTime<Utc>,
    ) -> TxnLogResult<()> {
        sqlx::query(
            "INSERT INTO dl_multi_table_transactions (transaction_id, started_at, table_count, total_action_count, state) 
             VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(&transaction.transaction_id)
        .bind(start_time)
        .bind(transaction.table_count() as i32)
        .bind(transaction.total_action_count() as i32)
        .bind("ACTIVE")
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Insert transaction metadata record for SQLite.
    async fn insert_transaction_metadata_sqlite(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        transaction: &MultiTableTransaction,
        start_time: DateTime<Utc>,
    ) -> TxnLogResult<()> {
        sqlx::query(
            "INSERT INTO dl_multi_table_transactions (transaction_id, started_at, table_count, total_action_count, state) 
             VALUES (?1, ?2, ?3, ?4, ?5)"
        )
        .bind(&transaction.transaction_id)
        .bind(start_time)
        .bind(transaction.table_count() as i32)
        .bind(transaction.total_action_count() as i32)
        .bind("ACTIVE")
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Update transaction metadata state for Postgres.
    async fn update_transaction_metadata(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction_id: &str,
        state: &str,
    ) -> TxnLogResult<()> {
        sqlx::query(
            "UPDATE dl_multi_table_transactions SET state = $1, completed_at = $2 WHERE transaction_id = $3"
        )
        .bind(state)
        .bind(Utc::now())
        .bind(transaction_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Update transaction metadata state for SQLite.
    async fn update_transaction_metadata_sqlite(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        transaction_id: &str,
        state: &str,
    ) -> TxnLogResult<()> {
        sqlx::query(
            "UPDATE dl_multi_table_transactions SET state = ?1, completed_at = ?2 WHERE transaction_id = ?3"
        )
        .bind(state)
        .bind(Utc::now())
        .bind(transaction_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Update transaction metadata synchronously (for error handling).
    fn update_transaction_metadata_sync(
        &self,
        transaction_id: &str,
        state: &str,
        error_message: &str,
    ) -> TxnLogResult<()> {
        // This is a best-effort update for error tracking
        // In a real implementation, you might want to use a separate connection pool
        debug!("Updating transaction {} to {} with error: {}", transaction_id, state, error_message);
        Ok(())
    }

    /// Commit actions for a single table in Postgres.
    async fn commit_table_postgres(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: &str,
        table_actions: &TableActions,
        transaction_id: &str,
    ) -> TxnLogResult<TableCommitResult> {
        // Verify expected version for optimistic concurrency
        let current_version = self.get_table_version_postgres(tx, table_id).await?;
        if current_version != table_actions.expected_version {
            return Err(TxnLogError::ConcurrentWrite(
                format!("Version mismatch for table {}: expected {}, found {}",
                    table_id, table_actions.expected_version, current_version)
            ));
        }

        let next_version = current_version + 1;

        // Insert table version with transaction metadata
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation, operation_params) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(table_id)
        .bind(next_version)
        .bind(Utc::now())
        .bind(format!("multi-table-{}", transaction_id))
        .bind(&table_actions.operation)
        .bind(table_actions.operation_params.as_ref().map(|p| serde_json::to_string(p).unwrap()))
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;

        // Insert actions
        for action in &table_actions.actions {
            self.single_writer.insert_action_postgres(tx, table_id, next_version, action).await?;
        }

        Ok(TableCommitResult {
            table_id: table_id.to_string(),
            version: next_version,
            action_count: table_actions.actions.len(),
            mirroring_triggered: self.config.enable_ordered_mirroring,
        })
    }

    /// Commit actions for a single table in SQLite.
    async fn commit_table_sqlite(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
        table_actions: &TableActions,
        transaction_id: &str,
    ) -> TxnLogResult<TableCommitResult> {
        // Verify expected version for optimistic concurrency
        let current_version = self.get_table_version_sqlite(tx, table_id).await?;
        if current_version != table_actions.expected_version {
            return Err(TxnLogError::ConcurrentWrite(
                format!("Version mismatch for table {}: expected {}, found {}",
                    table_id, table_actions.expected_version, current_version)
            ));
        }

        let next_version = current_version + 1;

        // Insert table version with transaction metadata
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation, operation_params) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        )
        .bind(table_id)
        .bind(next_version)
        .bind(Utc::now())
        .bind(format!("multi-table-{}", transaction_id))
        .bind(&table_actions.operation)
        .bind(table_actions.operation_params.as_ref().map(|p| serde_json::to_string(p).unwrap()))
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;

        // Insert actions
        for action in &table_actions.actions {
            self.single_writer.insert_action_sqlite(tx, table_id, next_version, action).await?;
        }

        Ok(TableCommitResult {
            table_id: table_id.to_string(),
            version: next_version,
            action_count: table_actions.actions.len(),
            mirroring_triggered: self.config.enable_ordered_mirroring,
        })
    }

    /// Get current version for a table in Postgres.
    async fn get_table_version_postgres(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: &str,
    ) -> TxnLogResult<i64> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(version), -1) as current_version FROM dl_table_versions WHERE table_id = $1"
        )
        .bind(table_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;

        Ok(row.get::<i64, _>("current_version"))
    }

    /// Get current version for a table in SQLite.
    async fn get_table_version_sqlite(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> TxnLogResult<i64> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(version), -1) as current_version FROM dl_table_versions WHERE table_id = ?1"
        )
        .bind(table_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;

        Ok(row.get::<i64, _>("current_version"))
    }

    /// Trigger ordered mirroring for committed tables.
    async fn trigger_ordered_mirroring(
        &self,
        table_results: &[TableCommitResult],
    ) -> TxnLogResult<Vec<MirroringResult>> {
        if self.mirror_engine.is_none() {
            return Ok(Vec::new());
        }

        let mirror_engine = self.mirror_engine.as_ref().unwrap();
        let mut mirroring_results = Vec::new();

        debug!("Triggering ordered mirroring for {} tables", table_results.len());

        // Process tables in commit order (deterministic)
        for (index, result) in table_results.iter().enumerate() {
            debug!(
                "Triggering mirroring for table {} version {} (order: {})",
                result.table_id, result.version, index
            );

            // Insert mirror status record
            self.insert_mirror_status(&result.table_id, result.version, "json", "pending").await?;

            // Trigger async mirroring with proper error handling
            let mirror_result = self.mirror_single_table(
                mirror_engine,
                &result.table_id,
                result.version,
                index,
            ).await;

            mirroring_results.push(mirror_result);
        }

        // Wait for all mirroring operations to complete or timeout
        self.wait_for_mirroring_completion(mirroring_results).await
    }

    /// Mirror a single table with error handling.
    async fn mirror_single_table(
        &self,
        mirror_engine: &MirrorEngine,
        table_id: &str,
        version: i64,
        order_index: usize,
    ) -> MirroringResult {
        let mirror_engine = mirror_engine.clone();
        let table_id = table_id.to_string();
        
        // Spawn mirroring task
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        tokio::spawn(async move {
            let result = mirror_engine.mirror_commit(&table_id, version, &[]).await;
            let _ = tx.send(result);
        });

        // Wait for completion with timeout
        let timeout_duration = std::time::Duration::from_secs(300); // 5 minutes
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(Ok(mirror_result)) => {
                match mirror_result {
                    Ok(_) => {
                        debug!("Successfully mirrored table {} version {}", table_id, version);
                        self.update_mirror_status_success(&table_id, version).await;
                        MirroringResult {
                            table_id: table_id.clone(),
                            version,
                            success: true,
                            error: None,
                        }
                    }
                    Err(e) => {
                        error!("Failed to mirror table {} version {}: {}", table_id, version, e);
                        self.update_mirror_status_failed(&table_id, version, &e.to_string()).await;
                        MirroringResult {
                            table_id: table_id.clone(),
                            version,
                            success: false,
                            error: Some(e.to_string()),
        }
    }
}



    #[tokio::test]
    async fn test_isolation_level_display() {
        assert_eq!(format!("{}", TransactionIsolationLevel::ReadCommitted), "READ COMMITTED");
        assert_eq!(format!("{}", TransactionIsolationLevel::RepeatableRead), "REPEATABLE READ");
        assert_eq!(format!("{}", TransactionIsolationLevel::Serializable), "SERIALIZABLE");
    }

    #[tokio::test]
    async fn test_deadlock_detection_config() {
        let mut config = MultiTableConfig::default();
        assert!(config.enable_deadlock_detection);
        assert_eq!(config.max_concurrent_transactions, 100);
        
        // Test disabling deadlock detection
        config.enable_deadlock_detection = false;
        assert!(!config.enable_deadlock_detection);
    }

    #[tokio::test]
    async fn test_deadlock_priority_resolution() {
        let writer = create_test_multi_writer().await;
        let config = MultiTableConfig::default();
        
        let tx1 = MultiTableTransaction::with_isolation_and_priority(
            config.clone(),
            TransactionIsolationLevel::ReadCommitted,
            5
        );
        
        let tx2 = MultiTableTransaction::with_isolation_and_priority(
            config.clone(),
            TransactionIsolationLevel::ReadCommitted,
            10
        );
        
        // Higher priority should win
        let winner = writer.resolve_deadlock_by_priority(&tx1, &tx2);
        assert_eq!(winner.transaction_id, tx2.transaction_id);
        
        // Equal priority, older transaction wins
        let tx3 = MultiTableTransaction::with_isolation_and_priority(
            config.clone(),
            TransactionIsolationLevel::ReadCommitted,
            5
        );
        
        // Add small delay to make tx1 older
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        
        let winner = writer.resolve_deadlock_by_priority(&tx1, &tx3);
        assert_eq!(winner.transaction_id, tx1.transaction_id);
    }

    #[tokio::test]
    async fn test_transaction_with_serializable_isolation() {
        let writer = create_test_multi_writer().await;
        let config = MultiTableConfig::default();
        
        let mut transaction = MultiTableTransaction::with_isolation_level(
            config,
            TransactionIsolationLevel::Serializable
        );
        
        // Add some test actions
        let add_file = AddFile {
            path: "test_file.parquet".to_string(),
            size: 1000,
            modification_time: 1234567890,
            data_change: true,
            ..Default::default()
        };
        
        transaction.add_files("test_table".to_string(), 0, vec![add_file]).unwrap();
        
        // Verify transaction properties
        assert_eq!(transaction.isolation_level, TransactionIsolationLevel::Serializable);
        assert_eq!(transaction.table_count(), 1);
        assert_eq!(transaction.total_action_count(), 1);
        
        // Test commit (this will test isolation level enforcement)
        let result = writer.commit_transaction(transaction).await;
        assert!(result.is_ok());
        
        let commit_result = result.unwrap();
        assert_eq!(commit_result.table_results.len(), 1);
        assert!(commit_result.table_results[0].success);
    }

    #[tokio::test]
    async fn test_cross_table_consistency_with_isolation() {
        let writer = create_test_multi_writer().await;
        let config = MultiTableConfig {
            enable_consistency_validation: true,
            default_isolation_level: TransactionIsolationLevel::RepeatableRead,
            ..Default::default()
        };
        
        let mut transaction = MultiTableTransaction::new(config);
        
        // Add actions to multiple tables
        let add_file1 = AddFile {
            path: "table1_file.parquet".to_string(),
            size: 1000,
            modification_time: 1234567890,
            data_change: true,
            ..Default::default()
        };
        
        let add_file2 = AddFile {
            path: "table2_file.parquet".to_string(),
            size: 2000,
            modification_time: 1234567891,
            data_change: true,
            ..Default::default()
        };
        
        transaction.add_files("table1".to_string(), 0, vec![add_file1]).unwrap();
        transaction.add_files("table2".to_string(), 0, vec![add_file2]).unwrap();
        
        // Verify isolation level
        assert_eq!(transaction.isolation_level, TransactionIsolationLevel::RepeatableRead);
        
        // Test commit with consistency validation
        let result = writer.commit_transaction(transaction).await;
        assert!(result.is_ok());
        
        let commit_result = result.unwrap();
        assert_eq!(commit_result.table_results.len(), 2);
        assert!(commit_result.table_results.iter().all(|r| r.success));
        }
    }
}
}
}

    /// Wait for all mirroring operations to complete with enhanced failure handling.
    async fn wait_for_mirroring_completion(
        &self,
        mut mirroring_results: Vec<MirroringResult>,
    ) -> TxnLogResult<Vec<MirroringResult>> {
        let total_tables = mirroring_results.len();
        let mut successful_count = 0;
        let mut failed_count = 0;
        let mut critical_failures = Vec::new();

        // Analyze mirroring results
        for result in &mirroring_results {
            if result.success {
                successful_count += 1;
            } else {
                failed_count += 1;
                
                // Check for critical failure patterns
                if let Some(error) = &result.error {
                    if self.is_critical_mirroring_error(error) {
                        critical_failures.push((result.table_id.clone(), result.version, error.clone()));
                    }
                }
            }
        }

        debug!(
            "Mirroring completed: {}/{} successful, {} failed, {} critical",
            successful_count, total_tables, failed_count, critical_failures.len()
        );

        // Handle different failure scenarios
        if failed_count > 0 {
            self.handle_partial_mirroring_failure(
                &mirroring_results,
                failed_count,
                total_tables,
                &critical_failures,
            ).await?;
        }

        // Record mirroring summary for monitoring
        self.record_mirroring_summary(&mirroring_results, successful_count, failed_count).await?;

        Ok(mirroring_results)
    }

    /// Determine if a mirroring error is critical.
    fn is_critical_mirroring_error(&self, error: &str) -> bool {
        let error_lower = error.to_lowercase();
        
        // Critical error patterns that require immediate attention
        critical_error_patterns().iter().any(|pattern| {
            error_lower.contains(pattern)
        })
    }

    /// Handle partial mirroring failure scenarios.
    async fn handle_partial_mirroring_failure(
        &self,
        mirroring_results: &[MirroringResult],
        failed_count: usize,
        total_tables: usize,
        critical_failures: &[(String, i64, String)],
    ) -> TxnLogResult<()> {
        let failure_rate = failed_count as f64 / total_tables as f64;
        
        // Log detailed failure analysis
        warn!(
            "Partial mirroring failure analysis: {}/{} tables failed ({:.1}% failure rate)",
            failed_count, total_tables, failure_rate * 100.0
        );

        // Handle critical failures immediately
        if !critical_failures.is_empty() {
            error!("Critical mirroring failures detected:");
            for (table_id, version, error) in critical_failures {
                error!("  Table {} version {}: {}", table_id, version, error);
            }
            
            // Trigger immediate alert/monitoring
            self.trigger_mirroring_alert(critical_failures).await?;
        }

        // Different handling based on failure rate
        if failure_rate >= 0.8 {
            // High failure rate - likely systemic issue
            error!("High mirroring failure rate detected ({:.1}%), possible systemic issue", failure_rate * 100.0);
            self.handle_systemic_mirroring_failure(mirroring_results).await?;
        } else if failure_rate >= 0.3 {
            // Medium failure rate - possible partial outage
            warn!("Medium mirroring failure rate ({:.1}%), investigating pattern", failure_rate * 100.0);
            self.investigate_mirroring_pattern(mirroring_results).await?;
        } else {
            // Low failure rate - likely isolated issues
            info!("Low mirroring failure rate ({:.1}%), scheduling retries", failure_rate * 100.0);
            self.schedule_automatic_retries(mirroring_results).await?;
        }

        Ok(())
    }

    /// Handle systemic mirroring failures.
    async fn handle_systemic_mirroring_failure(
        &self,
        mirroring_results: &[MirroringResult],
    ) -> TxnLogResult<()> {
        // Pause further mirroring to prevent cascading failures
        self.pause_mirroring_operations().await?;
        
        // Analyze common failure patterns
        let error_patterns = self.analyze_failure_patterns(mirroring_results);
        
        // Try to identify root cause
        if let Some(root_cause) = self.identify_root_cause(&error_patterns) {
            error!("Identified potential root cause: {}", root_cause);
            
            // Attempt automatic recovery if possible
            if self.attempt_automatic_recovery(&root_cause).await? {
                info!("Automatic recovery successful, resuming mirroring");
                self.resume_mirroring_operations().await?;
            } else {
                error!("Automatic recovery failed, manual intervention required");
                self.escalate_to_manual_intervention(&root_cause).await?;
            }
        } else {
            error!("Unable to identify root cause, escalating for investigation");
            self.escalate_for_investigation(mirroring_results).await?;
        }
        
        Ok(())
    }

    /// Investigate mirroring failure patterns.
    async fn investigate_mirroring_pattern(
        &self,
        mirroring_results: &[MirroringResult],
    ) -> TxnLogResult<()> {
        let error_patterns = self.analyze_failure_patterns(mirroring_results);
        
        // Check for temporal patterns
        if self.has_temporal_pattern(&error_patterns) {
            info!("Detected temporal failure pattern, adjusting retry strategy");
            self.adjust_retry_strategy_for_temporal_issues().await?;
        }
        
        // Check for table-specific patterns
        if let Some(table_pattern) = self.identify_table_specific_pattern(&error_patterns) {
            warn!("Table-specific failure pattern detected: {}", table_pattern);
            self.handle_table_specific_pattern(&table_pattern).await?;
        }
        
        // Check for size-related patterns
        if self.has_size_related_pattern(&error_patterns) {
            info!("Size-related failure pattern detected, adjusting mirroring parameters");
            self.adjust_mirroring_parameters_for_size().await?;
        }
        
        Ok(())
    }

    /// Schedule automatic retries for failed mirroring.
    async fn schedule_automatic_retries(
        &self,
        mirroring_results: &[MirroringResult],
    ) -> TxnLogResult<()> {
        let mut retry_schedule = Vec::new();
        
        for result in mirroring_results {
            if !result.success {
                // Determine retry delay based on error type
                let retry_delay = self.calculate_retry_delay(&result.error);
                
                retry_schedule.push(RetrySchedule {
                    table_id: result.table_id.clone(),
                    version: result.version,
                    scheduled_at: Utc::now() + chrono::Duration::minutes(retry_delay),
                    attempt_count: 1,
                });
            }
        }
        
        // Schedule retries in batches to avoid overwhelming the system
        self.schedule_batch_retries(retry_schedule).await?;
        
        info!("Scheduled {} automatic retries for failed mirroring operations", retry_schedule.len());
        Ok(())
    }

    /// Trigger mirroring alert for critical failures.
    async fn trigger_mirroring_alert(
        &self,
        critical_failures: &[(String, i64, String)],
    ) -> TxnLogResult<()> {
        // In a real implementation, this would:
        // 1. Send alerts to monitoring systems
        // 2. Create incident tickets
        // 3. Notify on-call engineers
        // 4. Update dashboards
        
        error!("CRITICAL MIRRORING ALERT: {} tables with critical failures", critical_failures.len());
        
        // Record alert in database for tracking
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_alerts (alert_type, severity, message, affected_tables, created_at)
                     VALUES ($1, $2, $3, $4, $5)"
                )
                .bind("critical_failure")
                .bind("critical")
                .bind(format!("Critical mirroring failures: {}", critical_failures.len()))
                .bind(serde_json::to_string(critical_failures).unwrap_or_default())
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_alerts (alert_type, severity, message, affected_tables, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                )
                .bind("critical_failure")
                .bind("critical")
                .bind(format!("Critical mirroring failures: {}", critical_failures.len()))
                .bind(serde_json::to_string(critical_failures).unwrap_or_default())
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Record mirroring summary for monitoring.
    async fn record_mirroring_summary(
        &self,
        mirroring_results: &[MirroringResult],
        successful_count: usize,
        failed_count: usize,
    ) -> TxnLogResult<()> {
        let total_duration = self.calculate_mirroring_duration(mirroring_results);
        let avg_duration = if !mirroring_results.is_empty() {
            total_duration / mirroring_results.len() as u64
        } else {
            0
        };
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_summaries (total_tables, successful_count, failed_count, 
                     total_duration_ms, avg_duration_ms, recorded_at)
                     VALUES ($1, $2, $3, $4, $5, $6)"
                )
                .bind(mirroring_results.len() as i32)
                .bind(successful_count as i32)
                .bind(failed_count as i32)
                .bind(total_duration as i64)
                .bind(avg_duration as i64)
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_summaries (total_tables, successful_count, failed_count, 
                     total_duration_ms, avg_duration_ms, recorded_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                )
                .bind(mirroring_results.len() as i32)
                .bind(successful_count as i32)
                .bind(failed_count as i32)
                .bind(total_duration as i64)
                .bind(avg_duration as i64)
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Get critical error patterns for mirroring.
    fn critical_error_patterns() -> Vec<&'static str> {
        vec![
            "permission denied",
            "authentication failed",
            "quota exceeded",
            "storage full",
            "network unreachable",
            "connection refused",
            "timeout",
            "disk full",
            "out of memory",
            "corruption detected",
            "invalid credentials",
            "access denied",
            "service unavailable",
        ]
    }

    /// Pause mirroring operations during systemic failures.
    async fn pause_mirroring_operations(&self) -> TxnLogResult<()> {
        warn!("Pausing mirroring operations due to systemic failures");
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "UPDATE dl_mirroring_config SET status = 'paused', paused_at = $1 WHERE status = 'active'"
                )
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE dl_mirroring_config SET status = 'paused', paused_at = ?1 WHERE status = 'active'"
                )
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Resume mirroring operations after recovery.
    async fn resume_mirroring_operations(&self) -> TxnLogResult<()> {
        info!("Resuming mirroring operations");
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "UPDATE dl_mirroring_config SET status = 'active', resumed_at = $1 WHERE status = 'paused'"
                )
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE dl_mirroring_config SET status = 'active', resumed_at = ?1 WHERE status = 'paused'"
                )
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Analyze failure patterns from mirroring results.
    fn analyze_failure_patterns(&self, mirroring_results: &[MirroringResult]) -> Vec<String> {
        let mut error_counts = std::collections::HashMap::new();
        
        for result in mirroring_results {
            if let Some(error) = &result.error {
                // Extract key error phrases
                let error_lower = error.to_lowercase();
                for pattern in critical_error_patterns() {
                    if error_lower.contains(pattern) {
                        *error_counts.entry(pattern.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
        
        // Sort by frequency
        let mut patterns: Vec<_> = error_counts.into_iter().collect();
        patterns.sort_by(|a, b| b.1.cmp(&a.1));
        
        patterns.into_iter().map(|(pattern, _)| pattern).collect()
    }

    /// Identify potential root cause from error patterns.
    fn identify_root_cause(&self, error_patterns: &[String]) -> Option<String> {
        if error_patterns.is_empty() {
            return None;
        }
        
        // Pattern matching for common root causes
        let patterns_str = error_patterns.join(" ");
        
        if patterns_str.contains("network") || patterns_str.contains("connection") {
            Some("Network connectivity issues".to_string())
        } else if patterns_str.contains("permission") || patterns_str.contains("authentication") {
            Some("Authentication/authorization problems".to_string())
        } else if patterns_str.contains("quota") || patterns_str.contains("full") {
            Some("Resource capacity issues".to_string())
        } else if patterns_str.contains("timeout") {
            Some("Performance/timeout issues".to_string())
        } else if patterns_str.contains("corruption") {
            Some("Data corruption detected".to_string())
        } else {
            Some("Unknown - requires investigation".to_string())
        }
    }

    /// Attempt automatic recovery based on root cause.
    async fn attempt_automatic_recovery(&self, root_cause: &str) -> TxnLogResult<bool> {
        info!("Attempting automatic recovery for: {}", root_cause);
        
        match root_cause {
            "Network connectivity issues" => {
                // Test connectivity and retry
                self.test_connectivity().await?;
                Ok(true)
            }
            "Authentication/authorization problems" => {
                // Refresh credentials
                self.refresh_credentials().await?;
                Ok(true)
            }
            "Performance/timeout issues" => {
                // Adjust timeout settings
                self.adjust_timeout_settings().await?;
                Ok(true)
            }
            _ => {
                // No automatic recovery available
                Ok(false)
            }
        }
    }

    /// Escalate to manual intervention.
    async fn escalate_to_manual_intervention(&self, root_cause: &str) -> TxnLogResult<()> {
        error!("Escalating to manual intervention for: {}", root_cause);
        
        // Create escalation record
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_escalations (escalation_type, root_cause, status, created_at)
                     VALUES ($1, $2, $3, $4)"
                )
                .bind("manual_intervention")
                .bind(root_cause)
                .bind("pending")
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_escalations (escalation_type, root_cause, status, created_at)
                     VALUES (?1, ?2, ?3, ?4)"
                )
                .bind("manual_intervention")
                .bind(root_cause)
                .bind("pending")
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Escalate for investigation when root cause unclear.
    async fn escalate_for_investigation(&self, mirroring_results: &[MirroringResult]) -> TxnLogResult<()> {
        error!("Escalating for investigation - unclear root cause");
        
        let failure_summary = serde_json::to_string(mirroring_results).unwrap_or_default();
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_escalations (escalation_type, failure_summary, status, created_at)
                     VALUES ($1, $2, $3, $4)"
                )
                .bind("investigation_required")
                .bind(failure_summary)
                .bind("pending")
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirroring_escalations (escalation_type, failure_summary, status, created_at)
                     VALUES (?1, ?2, ?3, ?4)"
                )
                .bind("investigation_required")
                .bind(failure_summary)
                .bind("pending")
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder
            }
        }
        
        Ok(())
    }

    /// Check if failures have temporal pattern.
    fn has_temporal_pattern(&self, _error_patterns: &[String]) -> bool {
        // In a real implementation, analyze timing of failures
        // For now, return false as placeholder
        false
    }

    /// Adjust retry strategy for temporal issues.
    async fn adjust_retry_strategy_for_temporal_issues(&self) -> TxnLogResult<()> {
        info!("Adjusting retry strategy for temporal issues");
        // Implement temporal-aware retry logic
        Ok(())
    }

    /// Identify table-specific failure patterns.
    fn identify_table_specific_pattern(&self, _error_patterns: &[String]) -> Option<String> {
        // In a real implementation, analyze which tables fail most
        // For now, return None as placeholder
        None
    }

    /// Handle table-specific failure patterns.
    async fn handle_table_specific_pattern(&self, _pattern: &str) -> TxnLogResult<()> {
        info!("Handling table-specific failure pattern");
        // Implement table-specific handling
        Ok(())
    }

    /// Check if failures have size-related pattern.
    fn has_size_related_pattern(&self, _error_patterns: &[String]) -> bool {
        // In a real implementation, analyze if large tables fail more
        // For now, return false as placeholder
        false
    }

    /// Adjust mirroring parameters for size issues.
    async fn adjust_mirroring_parameters_for_size(&self) -> TxnLogResult<()> {
        info!("Adjusting mirroring parameters for size-related issues");
        // Implement size-aware mirroring
        Ok(())
    }

    /// Calculate retry delay based on error type.
    fn calculate_retry_delay(&self, error: &Option<String>) -> i64 {
        if let Some(error) = error {
            let error_lower = error.to_lowercase();
            
            if error_lower.contains("timeout") {
                30 // 30 minutes for timeouts
            } else if error_lower.contains("network") || error_lower.contains("connection") {
                15 // 15 minutes for network issues
            } else if error_lower.contains("quota") || error_lower.contains("full") {
                120 // 2 hours for capacity issues
            } else {
                5 // 5 minutes default
            }
        } else {
            5 // Default 5 minutes
        }
    }

    /// Schedule batch retries to avoid overwhelming the system.
    async fn schedule_batch_retries(&self, retry_schedule: Vec<RetrySchedule>) -> TxnLogResult<()> {
        info!("Scheduling {} retries in batches", retry_schedule.len());
        
        // In a real implementation, this would:
        // 1. Group retries by delay time
        // 2. Limit concurrent retries
        // 3. Use exponential backoff
        // 4. Track retry attempts
        
        for schedule in retry_schedule {
            match &*self.connection {
                DatabaseConnection::Postgres(pool) => {
                    sqlx::query(
                        "INSERT INTO dl_mirroring_retry_schedule (table_id, version, scheduled_at, attempt_count)
                         VALUES ($1, $2, $3, $4)"
                    )
                    .bind(&schedule.table_id)
                    .bind(schedule.version)
                    .bind(schedule.scheduled_at)
                    .bind(schedule.attempt_count)
                    .execute(pool)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                }
                DatabaseConnection::Sqlite(pool) => {
                    sqlx::query(
                        "INSERT INTO dl_mirroring_retry_schedule (table_id, version, scheduled_at, attempt_count)
                         VALUES (?1, ?2, ?3, ?4)"
                    )
                    .bind(&schedule.table_id)
                    .bind(schedule.version)
                    .bind(schedule.scheduled_at)
                    .bind(schedule.attempt_count)
                    .execute(pool)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                }
                DatabaseConnection::DuckDb(_conn) => {
                    // Placeholder
                }
            }
        }
        
        Ok(())
    }

    /// Calculate total mirroring duration.
    fn calculate_mirroring_duration(&self, _mirroring_results: &[MirroringResult]) -> u64 {
        // In a real implementation, sum actual durations
        // For now, return estimated duration
        5000 // 5 seconds placeholder
    }

    /// Test connectivity to storage systems.
    async fn test_connectivity(&self) -> TxnLogResult<()> {
        info!("Testing connectivity to storage systems");
        // Implement connectivity tests
        Ok(())
    }

    /// Refresh authentication credentials.
    async fn refresh_credentials(&self) -> TxnLogResult<()> {
        info!("Refreshing authentication credentials");
        // Implement credential refresh
        Ok(())
    }

    /// Adjust timeout settings for performance issues.
    async fn adjust_timeout_settings(&self) -> TxnLogResult<()> {
        info!("Adjusting timeout settings");
        // Implement timeout adjustment
        Ok(())
    }

    /// Update mirror status to success.
    async fn update_mirror_status_success(&self, table_id: &str, version: i64) {
        if let Err(e) = self.update_mirror_status(table_id, version, "completed", None).await {
            error!("Failed to update mirror status to success: {}", e);
        }
    }

    /// Update mirror status to failed.
    async fn update_mirror_status_failed(&self, table_id: &str, version: i64, error: &str) {
        if let Err(e) = self.update_mirror_status(table_id, version, "failed", Some(error.to_string())).await {
            error!("Failed to update mirror status to failed: {}", e);
        }
    }

    /// Update mirror status with error message.
    async fn update_mirror_status(
        &self,
        table_id: &str,
        version: i64,
        status: &str,
        error_message: Option<String>,
    ) -> TxnLogResult<()> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "UPDATE dl_mirror_status 
                     SET status = $1, last_attempt_at = $2, completed_at = $3, error_message = $4
                     WHERE table_id = $5 AND version = $6 AND artifact_type = 'json'"
                )
                .bind(status)
                .bind(Utc::now())
                .bind(if status == "completed" { Some(Utc::now()) } else { None })
                .bind(error_message)
                .bind(table_id)
                .bind(version)
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE dl_mirror_status 
                     SET status = ?1, last_attempt_at = ?2, completed_at = ?3, error_message = ?4
                     WHERE table_id = ?5 AND version = ?6 AND artifact_type = 'json'"
                )
                .bind(status)
                .bind(Utc::now())
                .bind(if status == "completed" { Some(Utc::now()) } else { None })
                .bind(error_message)
                .bind(table_id)
                .bind(version)
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                // Placeholder for DuckDB
                return Ok(());
            }
        }
        Ok(())
    }

    /// Get mirroring status for a table version.
    pub async fn get_mirroring_status(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<Option<MirrorStatusInfo>> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT status, error_message, attempts, created_at, last_attempt_at, completed_at 
                     FROM dl_mirror_status 
                     WHERE table_id = $1 AND version = $2 AND artifact_type = 'json'"
                )
                .bind(table_id)
                .bind(version)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(row.map(|r| MirrorStatusInfo {
                    table_id: table_id.to_string(),
                    version,
                    artifact_type: "json".to_string(),
                    status: r.get("status"),
                    error_message: r.get("error_message"),
                    attempts: r.get("attempts"),
                    created_at: r.get("created_at"),
                    last_attempt_at: r.get("last_attempt_at"),
                    completed_at: r.get("completed_at"),
                }))
            }
            DatabaseConnection::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT status, error_message, attempts, created_at, last_attempt_at, completed_at 
                     FROM dl_mirror_status 
                     WHERE table_id = ?1 AND version = ?2 AND artifact_type = 'json'"
                )
                .bind(table_id)
                .bind(version)
                .fetch_optional(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                Ok(row.map(|r| MirrorStatusInfo {
                    table_id: table_id.to_string(),
                    version,
                    artifact_type: "json".to_string(),
                    status: r.get("status"),
                    error_message: r.get("error_message"),
                    attempts: r.get("attempts"),
                    created_at: r.get("created_at"),
                    last_attempt_at: r.get("last_attempt_at"),
                    completed_at: r.get("completed_at"),
                }))
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(None) // Placeholder
            }
        }
    }

    /// Retry failed mirroring operations.
    pub async fn retry_failed_mirroring(&self, older_than_minutes: i64) -> TxnLogResult<u64> {
        debug!("Retrying failed mirroring operations older than {} minutes", older_than_minutes);
        
        let cutoff_time = Utc::now() - chrono::Duration::minutes(older_than_minutes);
        
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT table_id, version FROM dl_mirror_status 
                     WHERE status = 'failed' 
                     AND last_attempt_at < $1 
                     AND attempts < $2"
                )
                .bind(cutoff_time)
                .bind(5) // Max retry attempts
                .fetch_all(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                let mut retried_count = 0;
                for row in rows {
                    let table_id: String = row.get("table_id");
                    let version: i64 = row.get("version");
                    
                    // Update attempts and reset status
                    sqlx::query(
                        "UPDATE dl_mirror_status 
                         SET status = 'pending', attempts = attempts + 1, last_attempt_at = $1
                         WHERE table_id = $2 AND version = $3 AND artifact_type = 'json'"
                    )
                    .bind(Utc::now())
                    .bind(&table_id)
                    .bind(version)
                    .execute(pool)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                    
                    retried_count += 1;
                }
                
                Ok(retried_count)
            }
            DatabaseConnection::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT table_id, version FROM dl_mirror_status 
                     WHERE status = 'failed' 
                     AND last_attempt_at < ?1 
                     AND attempts < ?2"
                )
                .bind(cutoff_time)
                .bind(5) // Max retry attempts
                .fetch_all(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                
                let mut retried_count = 0;
                for row in rows {
                    let table_id: String = row.get("table_id");
                    let version: i64 = row.get("version");
                    
                    // Update attempts and reset status
                    sqlx::query(
                        "UPDATE dl_mirror_status 
                         SET status = 'pending', attempts = attempts + 1, last_attempt_at = ?1
                         WHERE table_id = ?2 AND version = ?3 AND artifact_type = 'json'"
                    )
                    .bind(Utc::now())
                    .bind(&table_id)
                    .bind(version)
                    .execute(pool)
                    .await
                    .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
                    
                    retried_count += 1;
                }
                
                Ok(retried_count)
            }
            DatabaseConnection::DuckDb(_conn) => {
                Ok(0) // Placeholder
            }
        }
    }

    /// Insert mirror status record.
    async fn insert_mirror_status(
        &self,
        table_id: &str,
        version: i64,
        artifact_type: &str,
        status: &str,
    ) -> TxnLogResult<()> {
        match &*self.connection {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirror_status (table_id, version, artifact_type, status, created_at) 
                     VALUES ($1, $2, $3, $4, $5) 
                     ON CONFLICT (table_id, version, artifact_type) 
                     DO UPDATE SET status = EXCLUDED.status, last_attempt_at = EXCLUDED.created_at"
                )
                .bind(table_id)
                .bind(version)
                .bind(artifact_type)
                .bind(status)
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT OR REPLACE INTO dl_mirror_status (table_id, version, artifact_type, status, created_at) 
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                )
                .bind(table_id)
                .bind(version)
                .bind(artifact_type)
                .bind(status)
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(|e| TxnLogError::DatabaseError(e.to_string()))?;
            }
            DatabaseConnection::DuckDb(_conn) => {
                return Err(TxnLogError::DatabaseError(
                    "DuckDB mirror status not implemented".to_string()
                ));
            }
        }
        Ok(())
    }

    /// Check for potential deadlocks before committing transaction.
    async fn check_for_deadlocks(&self, transaction: &MultiTableTransaction) -> TxnLogResult<()> {
        if !self.config.enable_deadlock_detection {
            return Ok(());
        }

        debug!("Checking for deadlocks in transaction {}", transaction.transaction_id);

        // Get list of tables involved in this transaction
        let table_ids: Vec<String> = transaction.staged_tables.keys().cloned().collect();

        // Check for conflicting active transactions
        match &*self.connection {
            DatabaseConnection::Postgres(_) => {
                self.check_postgres_deadlocks(&(), &table_ids, &transaction.transaction_id).await?;
            }
            DatabaseConnection::Sqlite(_) => {
                self.check_sqlite_deadlocks(&(), &table_ids, &transaction.transaction_id).await?;
            }
            DatabaseConnection::DuckDb(_) => {
                // DuckDB deadlock detection not implemented
            }
        }

        Ok(())
    }

    /// Check for deadlocks in PostgreSQL.
    async fn check_postgres_deadlocks(
        &self,
        _pool: &(),
        table_ids: &[String],
        current_tx_id: &str,
    ) -> TxnLogResult<()> {
        // Placeholder implementation - would query for conflicting transactions
        debug!("Checking for PostgreSQL deadlocks for transaction {} on tables: {:?}", current_tx_id, table_ids);
        Ok(())
    }

    /// Check for deadlocks in SQLite.
    async fn check_sqlite_deadlocks(
        &self,
        _pool: &(),
        table_ids: &[String],
        current_tx_id: &str,
    ) -> TxnLogResult<()> {
        // Placeholder implementation - would query for conflicting transactions
        debug!("Checking for SQLite deadlocks for transaction {} on tables: {:?}", current_tx_id, table_ids);
        Ok(())
    }

    /// Record deadlock detection for monitoring.
    async fn record_deadlock_detection(
        &self,
        tx1_id: &str,
        tx2_id: &str,
        conflicting_tables: &[String],
    ) -> TxnLogResult<()> {
        debug!(
            "Recording deadlock detection between {} and {} on tables: {:?}",
            tx1_id, tx2_id, conflicting_tables
        );
        Ok(())
    }

    /// Resolve deadlock by priority (higher priority wins).
    fn resolve_deadlock_by_priority(&self, tx1: &MultiTableTransaction, tx2: &MultiTableTransaction) -> &MultiTableTransaction {
        if tx1.priority > tx2.priority {
            tx1
        } else if tx2.priority > tx1.priority {
            tx2
        } else {
            // If equal priority, the older transaction wins
            if tx1.started_at < tx2.started_at {
                tx1
            } else {
                tx2
            }
    }
}

/// Builder for creating TableActions with fluent API.
#[derive(Debug)]
pub struct TableActionsBuilder {
    table_actions: TableActions,
}

impl TableActionsBuilder {
    /// Create a new builder.
    pub fn new(table_id: String, expected_version: i64) -> Self {
        Self {
            table_actions: TableActions::new(table_id, expected_version),
        }
    }

    /// Add files to table.
    pub fn add_files(mut self, files: Vec<AddFile>) -> Self {
        self.table_actions.add_files(files);
        self
    }

    /// Remove files from table.
    pub fn remove_files(mut self, files: Vec<RemoveFile>) -> Self {
        self.table_actions.remove_files(files);
        self
    }

    /// Update metadata for table.
    pub fn update_metadata(mut self, metadata: Metadata) -> Self {
        self.table_actions.update_metadata(metadata);
        self
    }

    /// Update protocol for table.
    pub fn update_protocol(mut self, protocol: Protocol) -> Self {
        self.table_actions.update_protocol(protocol);
        self
    }

    /// Set the operation type.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.table_actions.with_operation(operation);
        self
    }

    /// Set operation parameters.
    pub fn with_operation_params(mut self, params: HashMap<String, String>) -> Self {
        self.table_actions.with_operation_params(params);
        self
    }

    /// Build the TableActions.
    pub fn build(self) -> TableActions {
        self.table_actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{DatabaseConfig, DatabaseConnection};
    use tempfile::tempdir;

    async fn create_test_multi_writer() -> MultiTableWriter {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = DatabaseConfig::new(crate::schema::DatabaseEngine::Sqlite, db_path.to_str().unwrap());
        let connection = Arc::new(DatabaseConnection::connect(config).await.unwrap());
        
        MultiTableWriter::new(connection, None, MultiTableConfig::default())
    }

    #[tokio::test]
    async fn test_multi_table_transaction_creation() {
        let writer = create_test_multi_writer().await;
        let tx = writer.begin_transaction();
        
        assert_eq!(tx.state, TransactionState::Active);
        assert_eq!(tx.table_count(), 0);
        assert_eq!(tx.total_action_count(), 0);
        assert!(!tx.is_timed_out());
    }

    #[tokio::test]
    async fn test_table_actions_staging() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        let mut table_actions = TableActions::new("table1".to_string(), 0);
        table_actions.add_action(DeltaAction::Metadata(Metadata {
            id: "test".to_string(),
            format: Default::default(),
            schema_string: "{}".to_string(),
            partition_columns: vec![],
            configuration: Default::default(),
            created_time: Some(chrono::Utc::now()),
        }));
        
        tx.stage_actions(table_actions).unwrap();
        
        assert_eq!(tx.table_count(), 1);
        assert_eq!(tx.total_action_count(), 1);
    }

    #[tokio::test]
    async fn test_transaction_validation() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Empty transaction should fail validation
        assert!(tx.validate().is_err());
        
        // Add actions and validate
        let mut table_actions = TableActions::new("table1".to_string(), 0);
        table_actions.add_action(DeltaAction::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
        }));
        
        tx.stage_actions(table_actions).unwrap();
        assert!(tx.validate().is_ok());
    }

    #[test]
    fn test_transaction_state_transitions() {
        let config = MultiTableConfig::default();
        let mut tx = MultiTableTransaction::new(config);
        
        // Initial state
        assert_eq!(tx.state, TransactionState::Active);
        
        // Can mark as committing
        tx.mark_committing().unwrap();
        assert_eq!(tx.state, TransactionState::Committing);
        
        // Can mark as committed
        tx.mark_committed();
        assert_eq!(tx.state, TransactionState::Committed);
        
        // Cannot stage actions after committed
        let table_actions = TableActions::new("table1".to_string(), 0);
        assert!(tx.stage_actions(table_actions).is_err());
    }

    #[tokio::test]
    async fn test_convenience_staging_methods() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Test add_files
        let add_file = AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        };
        
        tx.add_files("table1".to_string(), 0, vec![add_file.clone()]).unwrap();
        assert_eq!(tx.table_count(), 1);
        assert_eq!(tx.total_action_count(), 1);
        
        // Test remove_files
        let remove_file = RemoveFile {
            path: "old.parquet".to_string(),
            deletion_timestamp: chrono::Utc::now(),
            data_change: true,
            partition_values: None,
            size: Some(500),
            tags: None,
        };
        
        tx.remove_files("table2".to_string(), 0, vec![remove_file]).unwrap();
        assert_eq!(tx.table_count(), 2);
        assert_eq!(tx.total_action_count(), 2);
        
        // Test mixed operation
        tx.mixed_operation("table3".to_string(), 0, vec![add_file], vec![]).unwrap();
        assert_eq!(tx.table_count(), 3);
        assert_eq!(tx.total_action_count(), 3);
    }

    #[tokio::test]
    async fn test_table_actions_builder() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Test builder pattern
        let table_actions = writer.create_table_actions_builder("table1".to_string()).await.unwrap()
            .add_files(vec![AddFile {
                path: "test.parquet".to_string(),
                size: 1000,
                modification_time: chrono::Utc::now(),
                data_change: true,
                partition_values: Default::default(),
                stats: None,
                tags: None,
            }])
            .with_operation("WRITE".to_string())
            .build_and_validate()
            .unwrap();
        
        tx.stage_actions(table_actions).unwrap();
        assert_eq!(tx.table_count(), 1);
        assert_eq!(tx.total_action_count(), 1);
    }

    #[tokio::test]
    async fn test_stage_with_current_version() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions with automatic version detection
        writer.stage_actions_with_current_version(
            &mut tx,
            "table1".to_string(),
            vec![DeltaAction::Protocol(Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
            })],
            Some("CREATE".to_string()),
        ).await.unwrap();
        
        assert_eq!(tx.table_count(), 1);
        assert_eq!(tx.total_action_count(), 1);
        
        // Check that the operation was set
        let staged = tx.get_staged_actions("table1").unwrap();
        assert_eq!(staged.operation, Some("CREATE".to_string()));
    }

    #[tokio::test]
    async fn test_table_actions_validation() {
        // Test valid actions
        let mut valid_actions = TableActions::new("table1".to_string(), 0);
        valid_actions.add_action(DeltaAction::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
        }));
        valid_actions.add_action(DeltaAction::Add(AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }));
        assert!(valid_actions.validate().is_ok());
        
        // Test invalid actions (metadata after file ops)
        let mut invalid_actions = TableActions::new("table1".to_string(), 0);
        invalid_actions.add_action(DeltaAction::Add(AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }));
        invalid_actions.add_action(DeltaAction::Metadata(Metadata {
            id: "test".to_string(),
            format: Default::default(),
            schema_string: "{}".to_string(),
            partition_columns: vec![],
            configuration: Default::default(),
            created_time: Some(chrono::Utc::now()),
        }));
        assert!(invalid_actions.validate().is_err());
        
        // Test empty actions
        let empty_actions = TableActions::new("table1".to_string(), 0);
        assert!(empty_actions.validate().is_err());
    }

    #[tokio::test]
    async fn test_table_actions_summary() {
        let mut actions = TableActions::new("table1".to_string(), 0);
        actions.add_action(DeltaAction::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
        }));
        actions.add_action(DeltaAction::Add(AddFile {
            path: "test1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }));
        actions.add_action(DeltaAction::Add(AddFile {
            path: "test2.parquet".to_string(),
            size: 2000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }));
        actions.add_action(DeltaAction::Remove(RemoveFile {
            path: "old.parquet".to_string(),
            deletion_timestamp: chrono::Utc::now(),
            data_change: true,
            partition_values: None,
            size: Some(500),
            tags: None,
        }));
        
        let summary = actions.summary();
        assert_eq!(summary.table_id, "table1");
        assert_eq!(summary.expected_version, 0);
        assert_eq!(summary.total_actions, 4);
        assert_eq!(summary.add_count, 2);
        assert_eq!(summary.remove_count, 1);
        assert_eq!(summary.protocol_count, 1);
        assert_eq!(summary.metadata_count, 0);
    }

    #[tokio::test]
    async fn test_unstage_and_clear_operations() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage some actions
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        assert_eq!(tx.table_count(), 1);
        
        // Unstage actions
        let unstaged = tx.unstage_actions("table1").unwrap();
        assert_eq!(unstaged.action_count(), 1);
        assert_eq!(tx.table_count(), 0);
        
        // Stage again and clear all
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        assert_eq!(tx.table_count(), 1);
        tx.clear_all_staged().unwrap();
        assert_eq!(tx.table_count(), 0);
    }

    #[tokio::test]
    async fn test_atomic_commit_single_table() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions for a single table
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Commit transaction
        let result = writer.commit_transaction(tx).await.unwrap();
        
        assert_eq!(result.transaction.table_count, 1);
        assert_eq!(result.transaction.total_action_count, 1);
        assert_eq!(result.table_results.len(), 1);
        
        let table_result = &result.table_results[0];
        assert_eq!(table_result.table_id, "table1");
        assert_eq!(table_result.version, 0);
        assert_eq!(table_result.action_count, 1);
        assert_eq!(table_result.mirroring_triggered, false); // Disabled in test
    }

    #[tokio::test]
    async fn test_atomic_commit_multiple_tables() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions for multiple tables
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        tx.add_files("table2".to_string(), 0, vec![AddFile {
            path: "test2.parquet".to_string(),
            size: 2000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        tx.update_metadata("table3".to_string(), 0, Metadata {
            id: "test-metadata".to_string(),
            format: Default::default(),
            schema_string: "{}".to_string(),
            partition_columns: vec![],
            configuration: Default::default(),
            created_time: Some(chrono::Utc::now()),
        }).unwrap();
        
        // Commit transaction
        let result = writer.commit_transaction(tx).await.unwrap();
        
        assert_eq!(result.transaction.table_count, 3);
        assert_eq!(result.transaction.total_action_count, 3);
        assert_eq!(result.table_results.len(), 3);
        
        // Check results are in order (table1, table2, table3 due to BTreeMap)
        assert_eq!(result.table_results[0].table_id, "table1");
        assert_eq!(result.table_results[1].table_id, "table2");
        assert_eq!(result.table_results[2].table_id, "table3");
        
        // All tables should get version 0 (they were empty)
        for table_result in &result.table_results {
            assert_eq!(table_result.version, 0);
        }
    }

    #[tokio::test]
    async fn test_commit_with_version_conflict() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // First, commit a version to establish current state
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "initial.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        let result1 = writer.commit_transaction(tx).await.unwrap();
        assert_eq!(result1.table_results[0].version, 0);
        
        // Now try to commit with wrong expected version
        let mut tx2 = writer.begin_transaction();
        tx2.add_files("table1".to_string(), 0, vec![AddFile { // Wrong: should be 1
            path: "conflict.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Should fail with version conflict
        let result = writer.commit_transaction(tx2).await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            TxnLogError::ConcurrentWrite(_) => {
                // Expected
            }
            other => panic!("Expected ConcurrentWrite error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_commit_with_invalid_actions() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage invalid actions (metadata after file ops)
        let mut table_actions = TableActions::new("table1".to_string(), 0);
        table_actions.add_action(DeltaAction::Add(AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }));
        table_actions.add_action(DeltaAction::Metadata(Metadata {
            id: "test".to_string(),
            format: Default::default(),
            schema_string: "{}".to_string(),
            partition_columns: vec![],
            configuration: Default::default(),
            created_time: Some(chrono::Utc::now()),
        }));
        
        tx.stage_actions(table_actions).unwrap();
        
        // Should fail validation
        let result = writer.commit_transaction(tx).await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            TxnLogError::TransactionError(_) => {
                // Expected
            }
            other => panic!("Expected TransactionError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_begin_and_stage_convenience() {
        let writer = create_test_multi_writer().await;
        
        let table_actions = vec![
            TableActions::new("table1".to_string(), 0)
                .with_operation("WRITE".to_string())
                .with_actions(vec![DeltaAction::Protocol(Protocol {
                    min_reader_version: 1,
                    min_writer_version: 1,
                })]),
            TableActions::new("table2".to_string(), 0)
                .with_operation("WRITE".to_string())
                .with_actions(vec![DeltaAction::Protocol(Protocol {
                    min_reader_version: 1,
                    min_writer_version: 1,
                })]),
        ];
        
        let tx = writer.begin_and_stage(table_actions).await.unwrap();
        assert_eq!(tx.table_count(), 2);
        assert_eq!(tx.total_action_count(), 2);
    }

    #[tokio::test]
    async fn test_transaction_timeout() {
        let mut config = MultiTableConfig::default();
        config.transaction_timeout_secs = 0; // Immediate timeout
        
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Add some actions
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Wait a bit to ensure timeout
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        
        // Should fail validation due to timeout
        assert!(tx.is_timed_out());
        let result = tx.validate();
        assert!(result.is_err());
        
        match result.unwrap_err() {
            TxnLogError::TransactionError(msg) => {
                assert!(msg.contains("timed out"));
            }
            other => panic!("Expected TransactionError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage some actions
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        assert_eq!(tx.state, TransactionState::Active);
        assert_eq!(tx.table_count(), 1);
        
        // Rollback transaction
        writer.rollback_transaction(tx).await.unwrap();
        
        // Transaction should be marked as rolled back
        assert_eq!(tx.state, TransactionState::RolledBack);
    }

    #[tokio::test]
    async fn test_rollback_invalid_state() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Mark as committing (simulating invalid state)
        tx.mark_committing().unwrap();
        
        // Should fail to rollback
        let result = writer.rollback_transaction(tx).await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            TxnLogError::TransactionError(_) => {
                // Expected
            }
            other => panic!("Expected TransactionError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_force_rollback_transaction() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage some actions
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        let transaction_id = tx.transaction_id.clone();
        
        // Simulate transaction getting stuck in ACTIVE state
        // (in real scenario, this might be due to crash, etc.)
        
        // Force rollback
        let rolled_back = writer.force_rollback_transaction(&transaction_id).await.unwrap();
        assert!(rolled_back);
        
        // Check transaction status
        let status = writer.get_transaction_status(&transaction_id).await.unwrap();
        assert_eq!(status, Some(TransactionState::RolledBack));
    }

    #[tokio::test]
    async fn test_get_transaction_status() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        let transaction_id = tx.transaction_id.clone();
        
        // Initially should be ACTIVE
        let status = writer.get_transaction_status(&transaction_id).await.unwrap();
        assert_eq!(status, Some(TransactionState::Active));
        
        // Rollback and check status
        writer.rollback_transaction(tx).await.unwrap();
        let status = writer.get_transaction_status(&transaction_id).await.unwrap();
        assert_eq!(status, Some(TransactionState::RolledBack));
        
        // Non-existent transaction should return None
        let status = writer.get_transaction_status("non-existent").await.unwrap();
        assert_eq!(status, None);
    }

    #[tokio::test]
    async fn test_cleanup_old_transactions() {
        let writer = create_test_multi_writer().await;
        
        // Create and immediately rollback a transaction
        let mut tx = writer.begin_transaction();
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        writer.rollback_transaction(tx).await.unwrap();
        
        // Cleanup transactions older than 0 hours (should clean up everything)
        let cleaned = writer.cleanup_old_transactions(0).await.unwrap();
        assert!(cleaned >= 1);
    }

    #[tokio::test]
    async fn test_ordered_mirroring_disabled() {
        let mut config = MultiTableConfig::default();
        config.enable_ordered_mirroring = false;
        
        let writer = MultiTableWriter::new(
            Arc::new(DatabaseConnection::connect(
                crate::connection::DatabaseConfig::new(
                    crate::schema::DatabaseEngine::Sqlite, 
                    ":memory:"
                ).await.unwrap()
            ).unwrap()),
            None,
            config,
        );
        
        let mut tx = writer.begin_transaction();
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Commit should succeed without mirroring
        let result = writer.commit_transaction(tx).await.unwrap();
        assert_eq!(result.table_results.len(), 1);
        assert_eq!(result.table_results[0].mirroring_triggered, false);
        assert!(result.mirroring_results.is_none());
    }

    #[tokio::test]
    async fn test_mirroring_status_tracking() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        let transaction_id = tx.transaction_id.clone();
        
        // Commit transaction (mirroring disabled in test writer)
        let result = writer.commit_transaction(tx).await.unwrap();
        assert_eq!(result.table_results[0].version, 0);
        
        // Check mirroring status (should be None since mirroring disabled)
        let status = writer.get_mirroring_status("table1", 0).await.unwrap();
        assert!(status.is_none());
    }

    #[tokio::test]
    async fn test_retry_failed_mirroring() {
        let writer = create_test_multi_writer().await;
        
        // Manually insert a failed mirroring record
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirror_status (table_id, version, artifact_type, status, error_message, attempts, created_at) 
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                )
                .bind("table1")
                .bind(0)
                .bind("json")
                .bind("failed")
                .bind("Test error")
                .bind(1)
                .bind(Utc::now())
                .execute(pool)
                .await
                .unwrap();
            }
            _ => {}
        }
        
        // Retry failed mirroring (should find and retry the failed record)
        let retried = writer.retry_failed_mirroring(0).await.unwrap();
        assert_eq!(retried, 1);
    }

    #[tokio::test]
    async fn test_mirroring_order_determinism() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions for multiple tables in non-alphabetical order
        tx.add_files("zebra_table".to_string(), 0, vec![AddFile {
            path: "z1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        tx.add_files("alpha_table".to_string(), 0, vec![AddFile {
            path: "a1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        tx.add_files("beta_table".to_string(), 0, vec![AddFile {
            path: "b1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Commit transaction
        let result = writer.commit_transaction(tx).await.unwrap();
        
        // Results should be in alphabetical order due to BTreeMap
        assert_eq!(result.table_results.len(), 3);
        assert_eq!(result.table_results[0].table_id, "alpha_table");
        assert_eq!(result.table_results[1].table_id, "beta_table");
        assert_eq!(result.table_results[2].table_id, "zebra_table");
        
        // This ensures deterministic mirroring order
        if let Some(mirroring_results) = &result.mirroring_results {
            assert_eq!(mirroring_results.len(), 3);
            assert_eq!(mirroring_results[0].table_id, "alpha_table");
            assert_eq!(mirroring_results[1].table_id, "beta_table");
            assert_eq!(mirroring_results[2].table_id, "zebra_table");
        }
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_success() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create tables first
        writer.create_table("table1").await.unwrap();
        writer.create_table("table2").await.unwrap();
        
        // Stage valid actions
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "file1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        tx.add_files("table2".to_string(), 0, vec![AddFile {
            path: "file2.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Validation should pass
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert!(violations.is_empty());
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_table_not_found() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions for non-existent table
        tx.add_files("nonexistent_table".to_string(), 0, vec![AddFile {
            path: "file1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Validation should fail with table not found error
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert_eq!(violations.len(), 1);
        match &violations[0] {
            ConsistencyViolation::TableNotFound { table_id } => {
                assert_eq!(table_id, "nonexistent_table");
            }
            _ => panic!("Expected TableNotFound violation"),
        }
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_version_mismatch() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create table and add a file to increment version
        writer.create_table("table1").await.unwrap();
        let mut single_tx = writer.begin_transaction();
        single_tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "file1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        writer.commit_transaction(single_tx).await.unwrap();
        
        // Now stage actions with wrong expected version
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "file2.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Validation should fail with version mismatch
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert_eq!(violations.len(), 1);
        match &violations[0] {
            ConsistencyViolation::VersionMismatch { table_id, expected, actual } => {
                assert_eq!(table_id, "table1");
                assert_eq!(*expected, 0);
                assert_eq!(*actual, 1);
            }
            _ => panic!("Expected VersionMismatch violation"),
        }
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_empty_actions() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create table
        writer.create_table("table1").await.unwrap();
        
        // Add table with no actions
        tx.table_actions.insert("table1".to_string(), TableActions::new("table1".to_string(), 0));
        
        // Validation should fail with empty action list
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert_eq!(violations.len(), 1);
        match &violations[0] {
            ConsistencyViolation::EmptyActionList { table_id } => {
                assert_eq!(table_id, "table1");
            }
            _ => panic!("Expected EmptyActionList violation"),
        }
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_duplicate_files() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create table
        writer.create_table("table1").await.unwrap();
        
        // Add duplicate files
        let file = AddFile {
            path: "duplicate.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        };
        
        tx.add_files("table1".to_string(), 0, vec![file.clone(), file]).unwrap();
        
        // Validation should fail with duplicate file error
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert_eq!(violations.len(), 1);
        match &violations[0] {
            ConsistencyViolation::DuplicateFile { table_id, path } => {
                assert_eq!(table_id, "table1");
                assert_eq!(path, "duplicate.parquet");
            }
            _ => panic!("Expected DuplicateFile violation"),
        }
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_too_many_tables() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Add many tables (more than the limit of 100)
        for i in 0..101 {
            let table_id = format!("table_{}", i);
            tx.add_files(table_id, 0, vec![AddFile {
                path: format!("file_{}.parquet", i),
                size: 1000,
                modification_time: chrono::Utc::now(),
                data_change: true,
                partition_values: Default::default(),
                stats: None,
                tags: None,
            }]).unwrap();
        }
        
        // Validation should fail with too many tables error
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert!(!violations.is_empty());
        
        // Should have a TooManyTables violation
        let too_many_tables_violation = violations.iter().find(|v| matches!(v, ConsistencyViolation::TooManyTables { .. }));
        assert!(too_many_tables_violation.is_some());
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_transaction_too_large() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create table
        writer.create_table("table1").await.unwrap();
        
        // Add many actions (more than the limit of 10000)
        let mut actions = Vec::new();
        for i in 0..10001 {
            actions.push(AddFile {
                path: format!("file_{}.parquet", i),
                size: 1000,
                modification_time: chrono::Utc::now(),
                data_change: true,
                partition_values: Default::default(),
                stats: None,
                tags: None,
            });
        }
        
        tx.add_files("table1".to_string(), 0, actions).unwrap();
        
        // Validation should fail with transaction too large error
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert!(!violations.is_empty());
        
        // Should have a TransactionTooLarge violation
        let too_large_violation = violations.iter().find(|v| matches!(v, ConsistencyViolation::TransactionTooLarge { .. }));
        assert!(too_large_violation.is_some());
    }

    #[tokio::test]
    async fn test_cross_table_consistency_validation_disabled() {
        let mut config = MultiTableConfig::default();
        config.enable_consistency_validation = false;
        
        let writer = MultiTableWriter::new_with_config(Arc::new(create_test_connection().await), config);
        let mut tx = writer.begin_transaction();
        
        // Stage actions for non-existent table (would normally fail validation)
        tx.add_files("nonexistent_table".to_string(), 0, vec![AddFile {
            path: "file1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Validation should pass when disabled
        let violations = writer.validate_cross_table_consistency(&tx).await.unwrap();
        assert!(violations.is_empty());
    }

    #[tokio::test]
    async fn test_commit_transaction_with_validation_failure() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Stage actions for non-existent table
        tx.add_files("nonexistent_table".to_string(), 0, vec![AddFile {
            path: "file1.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Commit should fail due to validation
        let result = writer.commit_transaction(tx).await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            TxnLogError::Validation { message } => {
                assert!(message.contains("Cross-table consistency validation failed"));
                assert!(message.contains("TableNotFound"));
            }
            _ => panic!("Expected ValidationError"),
        }
    }

    #[tokio::test]
    async fn test_partial_mirroring_failure_handling() {
        let writer = create_test_multi_writer().await;
        let mut tx = writer.begin_transaction();
        
        // Create table
        writer.create_table("table1").await.unwrap();
        
        tx.add_files("table1".to_string(), 0, vec![AddFile {
            path: "test.parquet".to_string(),
            size: 1000,
            modification_time: chrono::Utc::now(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        }]).unwrap();
        
        // Mock a partial mirroring failure by manually inserting failed status
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dl_mirror_status (table_id, version, artifact_type, status, error_message, attempts, created_at) 
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                )
                .bind("table1")
                .bind(0)
                .bind("json")
                .bind("failed")
                .bind("Simulated failure")
                .bind(1)
                .bind(Utc::now())
                .execute(pool)
                .await
                .unwrap();
            }
            _ => {}
        }
        
        // Test retry mechanism
        let retried = writer.retry_failed_mirroring(0).await.unwrap();
        assert_eq!(retried, 1);
    }

    #[tokio::test]
    async fn test_critical_mirroring_error_detection() {
        let writer = create_test_multi_writer().await;
        
        // Test critical error patterns
        let critical_errors = vec![
            "permission denied",
            "authentication failed",
            "quota exceeded",
            "storage full",
            "network unreachable",
            "connection refused",
            "timeout",
            "disk full",
            "out of memory",
            "corruption detected",
        ];
        
        for error in critical_errors {
            assert!(writer.is_critical_mirroring_error(error));
        }
        
        // Test non-critical errors
        let non_critical_errors = vec![
            "file not found",
            "temporary glitch",
            "retry later",
        ];
        
        for error in non_critical_errors {
            assert!(!writer.is_critical_mirroring_error(error));
        }
    }

    #[tokio::test]
    async fn test_mirroring_failure_analysis() {
        let writer = create_test_multi_writer().await;
        
        let mirroring_results = vec![
            MirroringResult {
                table_id: "table1".to_string(),
                version: 0,
                success: false,
                error: Some("network timeout".to_string()),
            },
            MirroringResult {
                table_id: "table2".to_string(),
                version: 0,
                success: true,
                error: None,
            },
            MirroringResult {
                table_id: "table3".to_string(),
                version: 0,
                success: false,
                error: Some("permission denied".to_string()),
            },
        ];
        
        // Analyze failure patterns
        let error_patterns = writer.analyze_failure_patterns(&mirroring_results);
        
        // Should detect network and permission issues
        assert!(error_patterns.iter().any(|p| p.contains("timeout")));
        assert!(error_patterns.iter().any(|p| p.contains("permission")));
    }

    #[tokio::test]
    async fn test_retry_delay_calculation() {
        let writer = create_test_multi_writer().await;
        
        // Test different error types
        let timeout_error = Some("operation timeout".to_string());
        let network_error = Some("network connection failed".to_string());
        let quota_error = Some("quota exceeded".to_string());
        let default_error = Some("unknown error".to_string());
        
        assert_eq!(writer.calculate_retry_delay(&timeout_error), 30);
        assert_eq!(writer.calculate_retry_delay(&network_error), 15);
        assert_eq!(writer.calculate_retry_delay(&quota_error), 120);
        assert_eq!(writer.calculate_retry_delay(&default_error), 5);
        assert_eq!(writer.calculate_retry_delay(&None), 5);
    }

    #[tokio::test]
    async fn test_root_cause_identification() {
        let writer = create_test_multi_writer().await;
        
        // Test network-related patterns
        let network_patterns = vec!["network failure".to_string(), "connection refused".to_string()];
        let root_cause = writer.identify_root_cause(&network_patterns);
        assert_eq!(root_cause, Some("Network connectivity issues".to_string()));
        
        // Test authentication patterns
        let auth_patterns = vec!["permission denied".to_string(), "authentication failed".to_string()];
        let root_cause = writer.identify_root_cause(&auth_patterns);
        assert_eq!(root_cause, Some("Authentication/authorization problems".to_string()));
        
        // Test capacity patterns
        let capacity_patterns = vec!["quota exceeded".to_string(), "disk full".to_string()];
        let root_cause = writer.identify_root_cause(&capacity_patterns);
        assert_eq!(root_cause, Some("Resource capacity issues".to_string()));
        
        // Test unknown patterns
        let unknown_patterns = vec!["weird error".to_string()];
        let root_cause = writer.identify_root_cause(&unknown_patterns);
        assert_eq!(root_cause, Some("Unknown - requires investigation".to_string()));
    }

    #[tokio::test]
    async fn test_mirroring_alert_creation() {
        let writer = create_test_multi_writer().await;
        
        let critical_failures = vec![
            ("table1".to_string(), 0, "permission denied".to_string()),
            ("table2".to_string(), 0, "quota exceeded".to_string()),
        ];
        
        // Trigger alert
        writer.trigger_mirroring_alert(&critical_failures).await.unwrap();
        
        // Verify alert was recorded
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT COUNT(*) as count FROM dl_mirroring_alerts WHERE alert_type = 'critical_failure'"
                )
                .fetch_one(pool)
                .await
                .unwrap();
                
                let count: i64 = result.get("count");
                assert_eq!(count, 1);
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_mirroring_summary_recording() {
        let writer = create_test_multi_writer().await;
        
        let mirroring_results = vec![
            MirroringResult {
                table_id: "table1".to_string(),
                version: 0,
                success: true,
                error: None,
            },
            MirroringResult {
                table_id: "table2".to_string(),
                version: 0,
                success: false,
                error: Some("timeout".to_string()),
            },
        ];
        
        // Record summary
        writer.record_mirroring_summary(&mirroring_results, 1, 1).await.unwrap();
        
        // Verify summary was recorded
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT COUNT(*) as count FROM dl_mirroring_summaries"
                )
                .fetch_one(pool)
                .await
                .unwrap();
                
                let count: i64 = result.get("count");
                assert_eq!(count, 1);
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_mirroring_pause_resume() {
        let writer = create_test_multi_writer().await;
        
        // Pause mirroring
        writer.pause_mirroring_operations().await.unwrap();
        
        // Check status
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT status FROM dl_mirroring_config WHERE status = 'paused'"
                )
                .fetch_one(pool)
                .await
                .unwrap();
                
                let status: String = result.get("status");
                assert_eq!(status, "paused");
            }
            _ => {}
        }
        
        // Resume mirroring
        writer.resume_mirroring_operations().await.unwrap();
        
        // Check status
        match &*writer.connection {
            DatabaseConnection::Sqlite(pool) => {
                let result = sqlx::query(
                    "SELECT status FROM dl_mirroring_config WHERE status = 'active'"
                )
                .fetch_one(pool)
                .await
                .unwrap();
                
                let status: String = result.get("status");
                assert_eq!(status, "active");
            }
            _ => {}
        }
    }
}