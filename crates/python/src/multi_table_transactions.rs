//! Multi-table transaction integration with DeltaLake mirroring system
//!
//! This module provides comprehensive multi-table transaction support that integrates
//! seamlessly with the DeltaLake mirroring system, including distributed transactions,
//! two-phase commit protocols, cross-table consistency, and transaction recovery.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

/// Multi-table transaction status
#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub enum MultiTableTransactionStatus {
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
    Recovering,
}

/// Transaction isolation level for multi-table operations
#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub enum MultiTableIsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

/// Cross-table operation type
#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub enum CrossTableOperationType {
    /// Read operation across multiple tables
    Read,
    /// Write operation affecting multiple tables
    Write,
    /// Schema change across multiple tables
    SchemaChange,
    /// Data migration between tables
    Migration,
    /// Constraint validation across tables
    Validation,
    /// Index creation across tables
    IndexOperation,
    /// Backup/restore across multiple tables
    BackupOperation,
}

/// Cross-table transaction configuration
#[pyclass]
#[derive(Debug, Clone)]
pub struct MultiTableTransactionConfig {
    isolation_level: MultiTableIsolationLevel,
    timeout_seconds: u64,
    retry_attempts: u32,
    retry_delay_ms: u64,
    enable_2pc: bool,
    enable_distributed_lock: bool,
    enable_cross_table_validation: bool,
    max_participants: usize,
    enable_recovery: bool,
}

#[pymethods]
impl MultiTableTransactionConfig {
    #[new]
    #[pyo3(signature = (isolation_level=MultiTableIsolationLevel::Serializable, timeout_seconds=300, retry_attempts=3, retry_delay_ms=1000, enable_2pc=true, enable_distributed_lock=true, enable_cross_table_validation=true, max_participants=10, enable_recovery=true))]
    fn new(
        isolation_level: MultiTableIsolationLevel,
        timeout_seconds: u64,
        retry_attempts: u32,
        retry_delay_ms: u64,
        enable_2pc: bool,
        enable_distributed_lock: bool,
        enable_cross_table_validation: bool,
        max_participants: usize,
        enable_recovery: bool,
    ) -> Self {
        Self {
            isolation_level,
            timeout_seconds,
            retry_attempts,
            retry_delay_ms,
            enable_2pc,
            enable_distributed_lock,
            enable_cross_table_validation,
            max_participants,
            enable_recovery,
        }
    }

    #[getter]
    fn isolation_level(&self) -> MultiTableIsolationLevel {
        self.isolation_level.clone()
    }

    #[getter]
    fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    #[getter]
    fn enable_2pc(&self) -> bool {
        self.enable_2pc
    }

    #[getter]
    fn enable_distributed_lock(&self) -> bool {
        self.enable_distributed_lock
    }

    #[getter]
    fn enable_cross_table_validation(&self) -> bool {
        self.enable_cross_table_validation
    }

    #[getter]
    fn max_participants(&self) -> usize {
        self.max_participants
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("isolation_level", format!("{:?}", self.isolation_level))?;
        dict.set_item("timeout_seconds", self.timeout_seconds)?;
        dict.set_item("retry_attempts", self.retry_attempts)?;
        dict.set_item("retry_delay_ms", self.retry_delay_ms)?;
        dict.set_item("enable_2pc", self.enable_2pc)?;
        dict.set_item("enable_distributed_lock", self.enable_distributed_lock)?;
        dict.set_item("enable_cross_table_validation", self.enable_cross_table_validation)?;
        dict.set_item("max_participants", self.max_participants)?;
        dict.set_item("enable_recovery", self.enable_recovery)?;
        Ok(dict.to_object(py))
    }
}

/// Cross-table operation participant
#[pyclass]
#[derive(Debug, Clone)]
pub struct CrossTableParticipant {
    table_id: String,
    table_name: String,
    operation_type: CrossTableOperationType,
    operations: Vec<PyObject>,
    status: MultiTableTransactionStatus,
    vote: Option<bool>, // For 2PC: true=commit, false=abort
    error: Option<String>,
    lock_acquired: bool,
    checkpoint_created: bool,
}

#[pymethods]
impl CrossTableParticipant {
    #[new]
    #[pyo3(signature = (table_id, table_name, operation_type, operations))]
    fn new(
        table_id: String,
        table_name: String,
        operation_type: CrossTableOperationType,
        operations: Vec<PyObject>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            operation_type,
            operations,
            status: MultiTableTransactionStatus::Active,
            vote: None,
            error: None,
            lock_acquired: false,
            checkpoint_created: false,
        }
    }

    #[getter]
    fn table_id(&self) -> String {
        self.table_id.clone()
    }

    #[getter]
    fn table_name(&self) -> String {
        self.table_name.clone()
    }

    #[getter]
    fn operation_type(&self) -> CrossTableOperationType {
        self.operation_type.clone()
    }

    #[getter]
    fn status(&self) -> MultiTableTransactionStatus {
        self.status.clone()
    }

    #[getter]
    fn vote(&self) -> Option<bool> {
        self.vote
    }

    #[setter]
    fn set_vote(&mut self, vote: bool) {
        self.vote = Some(vote);
    }

    #[getter]
    fn error(&self) -> Option<String> {
        self.error.clone()
    }

    #[setter]
    fn set_error(&mut self, error: String) {
        self.error = Some(error);
    }

    #[getter]
    fn lock_acquired(&self) -> bool {
        self.lock_acquired
    }

    #[setter]
    fn set_lock_acquired(&mut self, acquired: bool) {
        self.lock_acquired = acquired;
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("table_id", &self.table_id)?;
        dict.set_item("table_name", &self.table_name)?;
        dict.set_item("operation_type", format!("{:?}", self.operation_type))?;
        dict.set_item("status", format!("{:?}", self.status))?;
        dict.set_item("vote", self.vote)?;
        dict.set_item("error", self.error.clone())?;
        dict.set_item("lock_acquired", self.lock_acquired)?;
        dict.set_item("checkpoint_created", self.checkpoint_created)?;
        Ok(dict.to_object(py))
    }
}

/// Multi-table transaction with comprehensive distributed transaction support
#[pyclass]
#[derive(Debug)]
pub struct MultiTableTransaction {
    transaction_id: String,
    config: MultiTableTransactionConfig,
    participants: Arc<Mutex<Vec<CrossTableParticipant>>>,
    status: Arc<Mutex<MultiTableTransactionStatus>>,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    coordinator_id: String,
    global_locks: Arc<Mutex<HashSet<String>>>,
    checkpoints: Arc<Mutex<Vec<String>>>,
    rollback_stack: Arc<Mutex<Vec<CrossTableParticipant>>>,
    timeout_timer: Arc<Mutex<Instant>>,
    error_log: Arc<Mutex<Vec<String>>>,
}

#[pymethods]
impl MultiTableTransaction {
    #[new]
    #[pyo3(signature = (config=None, coordinator_id=None))]
    fn new(config: Option<MultiTableTransactionConfig>, coordinator_id: Option<String>) -> Self {
        Self {
            transaction_id: Uuid::new_v4().to_string(),
            config: config.unwrap_or_else(|| MultiTableTransactionConfig::new(
                MultiTableIsolationLevel::Serializable,
                300, // 5 minutes
                3,
                1000,
                true,
                true,
                true,
                10,
                true
            )),
            participants: Arc::new(Mutex::new(Vec::new())),
            status: Arc::new(Mutex::new(MultiTableTransactionStatus::Active)),
            start_time: Utc::now(),
            end_time: None,
            coordinator_id: coordinator_id.unwrap_or_else(|| "default_coordinator".to_string()),
            global_locks: Arc::new(Mutex::new(HashSet::new())),
            checkpoints: Arc::new(Mutex::new(Vec::new())),
            rollback_stack: Arc::new(Mutex::new(Vec::new())),
            timeout_timer: Arc::new(Mutex::new(Instant::now())),
            error_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[getter]
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }

    #[getter]
    fn status(&self) -> MultiTableTransactionStatus {
        let status = self.status.lock().unwrap();
        status.clone()
    }

    #[getter]
    fn coordinator_id(&self) -> String {
        self.coordinator_id.clone()
    }

    #[getter]
    fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    #[getter]
    fn duration_ms(&self) -> f64 {
        let end = self.end_time.unwrap_or_else(Utc::now);
        end.signed_duration_since(self.start_time).num_milliseconds() as f64
    }

    /// Add a participant table to the transaction
    fn add_participant(&self, participant: CrossTableParticipant) -> PyResult<()> {
        let mut participants = self.participants.lock().unwrap();

        // Check participant limit
        if participants.len() >= self.config.max_participants {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Maximum participants ({}) exceeded", self.config.max_participants)
            ));
        }

        participants.push(participant);
        Ok(())
    }

    /// Add multiple participants at once
    fn add_participants(&self, participants: Vec<CrossTableParticipant>) -> PyResult<()> {
        for participant in participants {
            self.add_participant(participant)?;
        }
        Ok(())
    }

    /// Get all participants
    fn get_participants(&self) -> PyResult<Vec<CrossTableParticipant>> {
        let participants = self.participants.lock().unwrap();
        Ok(participants.clone())
    }

    /// Begin the multi-table transaction
    fn begin(&self, py: Python) -> PyResult<()> {
        let mut status = self.status.lock().unwrap();
        *status = MultiTableTransactionStatus::Active;

        // Reset timeout timer
        let mut timer = self.timeout_timer.lock().unwrap();
        *timer = Instant::now();

        // Acquire distributed locks if enabled
        if self.config.enable_distributed_lock {
            self.acquire_distributed_locks(py)?;
        }

        // Create initial checkpoint if enabled
        if self.config.enable_recovery {
            self.create_checkpoint(py, "transaction_begin")?;
        }

        Ok(())
    }

    /// Prepare phase of two-phase commit
    fn prepare(&self, py: Python) -> PyResult<bool> {
        let mut status = self.status.lock().unwrap();
        *status = MultiTableTransactionStatus::Preparing;

        let mut participants = self.participants.lock().unwrap();
        let mut all_votes_ok = true;

        for participant in participants.iter_mut() {
            // Execute prepare phase for each participant
            match self.prepare_participant(py, participant) {
                Ok(vote) => {
                    participant.vote = Some(vote);
                    if !vote {
                        all_votes_ok = false;
                    }
                }
                Err(e) => {
                    participant.vote = Some(false);
                    participant.error = Some(format!("Prepare failed: {}", e));
                    all_votes_ok = false;
                }
            }
        }

        if all_votes_ok {
            *status = MultiTableTransactionStatus::Prepared;
        } else {
            *status = MultiTableTransactionStatus::Aborting;
        }

        Ok(all_votes_ok)
    }

    /// Commit the multi-table transaction
    fn commit(&self, py: Python) -> PyResult<()> {
        let mut status = self.status.lock().unwrap();
        *status = MultiTableTransactionStatus::Committing;

        let mut participants = self.participants.lock().unwrap();

        // Commit all participants
        for participant in participants.iter_mut() {
            if let Err(e) = self.commit_participant(py, participant) {
                participant.error = Some(format!("Commit failed: {}", e));
                // Log error but continue with commit for other participants
                self.log_error(format!("Failed to commit participant {}: {}", participant.table_name, e));
            }
        }

        // Release locks
        if self.config.enable_distributed_lock {
            self.release_distributed_locks()?;
        }

        // Final checkpoint
        if self.config.enable_recovery {
            self.create_checkpoint(py, "transaction_commit")?;
        }

        *status = MultiTableTransactionStatus::Committed;

        {
            let mut end_time = self.end_time.lock().unwrap();
            *end_time = Some(Utc::now());
        }

        Ok(())
    }

    /// Abort the multi-table transaction
    fn abort(&self, py: Python, reason: Option<String>) -> PyResult<()> {
        let mut status = self.status.lock().unwrap();
        *status = MultiTableTransactionStatus::Aborting;

        if let Some(r) = reason {
            self.log_error(format!("Transaction aborted: {}", r));
        }

        let mut participants = self.participants.lock().unwrap();

        // Rollback all participants
        for participant in participants.iter_mut() {
            if let Err(e) = self.rollback_participant(py, participant) {
                participant.error = Some(format!("Rollback failed: {}", e));
                self.log_error(format!("Failed to rollback participant {}: {}", participant.table_name, e));
            }
        }

        // Release locks
        if self.config.enable_distributed_lock {
            self.release_distributed_locks()?;
        }

        *status = MultiTableTransactionStatus::Aborted;

        {
            let mut end_time = self.end_time.lock().unwrap();
            *end_time = Some(Utc::now());
        }

        Ok(())
    }

    /// Execute cross-table operation
    fn execute_cross_table_operation(
        &self,
        py: Python,
        operation_type: CrossTableOperationType,
        operations: Vec<PyObject>,
        table_ids: Vec<String>,
        validate_before: Option<bool>,
    ) -> PyResult<bool> {
        let mut participants = Vec::new();

        // Create participants for each table
        for (i, table_id) in table_ids.iter().enumerate() {
            let participant = CrossTableParticipant::new(
                table_id.clone(),
                format!("table_{}", i + 1),
                operation_type.clone(),
                operations.clone()
            );
            participants.push(participant);
        }

        // Add participants to transaction
        self.add_participants(participants)?;

        // Execute validation if enabled
        if self.config.enable_cross_table_validation || validate_before.unwrap_or(false) {
            if let Err(e) = self.validate_cross_table_consistency(py) {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Cross-table validation failed: {}", e)
                ));
            }
        }

        // Execute the operation
        if self.config.enable_2pc {
            // Two-phase commit protocol
            self.begin(py)?;

            if self.prepare(py)? {
                self.commit(py)?;
                Ok(true)
            } else {
                self.abort(py, Some("Prepare phase failed".to_string()))?;
                Ok(false)
            }
        } else {
            // Single-phase commit
            self.begin(py)?;
            self.commit(py)?;
            Ok(true)
        }
    }

    /// Validate cross-table consistency
    fn validate_cross_table_consistency(&self, py: Python) -> PyResult<()> {
        let participants = self.participants.lock().unwrap();

        // Check for conflicting operations
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();

        for participant in participants.iter() {
            match participant.operation_type {
                CrossTableOperationType::Read => {
                    read_set.insert(participant.table_id.clone());
                }
                CrossTableOperationType::Write => {
                    write_set.insert(participant.table_id.clone());
                }
                _ => {
                    write_set.insert(participant.table_id.clone());
                }
            }
        }

        // Check for read-write conflicts
        for table_id in read_set {
            if write_set.contains(&table_id) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Read-write conflict detected on table: {}", table_id)
                ));
            }
        }

        // Validate isolation level constraints
        if self.config.isolation_level == MultiTableIsolationLevel::Serializable {
            // Additional serializability checks would go here
        }

        Ok(())
    }

    /// Get transaction statistics
    fn get_transaction_stats(&self) -> PyResult<HashMap<String, PyObject>> {
        let py = Python::acquire_gil().python();
        let mut stats = HashMap::new();

        let participants = self.participants.lock().unwrap();
        let status = self.status.lock().unwrap();

        stats.insert("transaction_id".to_string(), self.transaction_id.to_object(py));
        stats.insert("coordinator_id".to_string(), self.coordinator_id.to_object(py));
        stats.insert("status".to_string(), format!("{:?}", *status).to_object(py));
        stats.insert("participant_count".to_string(), participants.len().to_object(py));
        stats.insert("duration_ms".to_string(), self.duration_ms().to_object(py));
        stats.insert("enable_2pc".to_string(), self.config.enable_2pc.to_object(py));
        stats.insert("enable_recovery".to_string(), self.config.enable_recovery.to_object(py));

        // Count participants by operation type
        let mut operation_counts = HashMap::new();
        for participant in participants.iter() {
            let op_type = format!("{:?}", participant.operation_type);
            *operation_counts.entry(op_type).or_insert(0) += 1;
        }

        let ops_dict = PyDict::new(py);
        for (op_type, count) in operation_counts {
            ops_dict.set_item(op_type, count)?;
        }
        stats.insert("operations_by_type".to_string(), ops_dict.to_object(py));

        Ok(stats)
    }

    /// Create transaction checkpoint for recovery
    fn create_checkpoint(&self, py: Python, checkpoint_type: &str) -> PyResult<()> {
        let checkpoint_id = Uuid::new_v4().to_string();
        let mut checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.push(checkpoint_id.clone());

        // In real implementation, would save transaction state to persistent storage
        println!("Created checkpoint {}: {}", checkpoint_type, checkpoint_id);

        Ok(())
    }

    /// Recover transaction from checkpoints
    fn recover(&self, py: Python) -> PyResult<bool> {
        let mut status = self.status.lock().unwrap();
        *status = MultiTableTransactionStatus::Recovering;

        let checkpoints = self.checkpoints.lock().unwrap();

        if checkpoints.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No checkpoints available for recovery"
            ));
        }

        // In real implementation, would load state from persistent storage
        println!("Recovering from {} checkpoints", checkpoints.len());

        *status = MultiTableTransactionStatus::Active;
        Ok(true)
    }

    /// Get error log
    fn get_error_log(&self) -> PyResult<Vec<String>> {
        let error_log = self.error_log.lock().unwrap();
        Ok(error_log.clone())
    }

    /// Check if transaction has timed out
    fn is_timed_out(&self) -> bool {
        let timer = self.timeout_timer.lock().unwrap();
        timer.elapsed().as_secs() > self.config.timeout_seconds
    }

    // Private helper methods
    fn acquire_distributed_locks(&self, py: Python) -> PyResult<()> {
        let participants = self.participants.lock().unwrap();
        let mut global_locks = self.global_locks.lock().unwrap();

        for participant in participants.iter() {
            // In real implementation, would acquire distributed lock
            let lock_key = format!("table_{}", participant.table_id);
            global_locks.insert(lock_key);
            println!("Acquired lock for table: {}", participant.table_name);
        }

        Ok(())
    }

    fn release_distributed_locks(&self) -> PyResult<()> {
        let mut global_locks = self.global_locks.lock().unwrap();

        for lock_key in global_locks.iter() {
            println!("Released lock for: {}", lock_key);
        }

        global_locks.clear();
        Ok(())
    }

    fn prepare_participant(&self, py: Python, participant: &mut CrossTableParticipant) -> PyResult<bool> {
        // In real implementation, would execute prepare phase
        participant.status = MultiTableTransactionStatus::Prepared;
        println!("Prepared participant: {}", participant.table_name);
        Ok(true)
    }

    fn commit_participant(&self, py: Python, participant: &mut CrossTableParticipant) -> PyResult<()> {
        // In real implementation, would execute commit phase
        participant.status = MultiTableTransactionStatus::Committed;
        println!("Committed participant: {}", participant.table_name);
        Ok(())
    }

    fn rollback_participant(&self, py: Python, participant: &mut CrossTableParticipant) -> PyResult<()> {
        // In real implementation, would execute rollback phase
        participant.status = MultiTableTransactionStatus::Aborted;
        println!("Rolled back participant: {}", participant.table_name);
        Ok(())
    }

    fn log_error(&self, error_message: String) {
        let mut error_log = self.error_log.lock().unwrap();
        error_log.push(format!("[{}] {}", Utc::now().to_rfc3339(), error_message));
    }
}

/// Multi-table transaction manager
#[pyclass]
#[derive(Debug)]
pub struct MultiTableTransactionManager {
    active_transactions: Arc<Mutex<HashMap<String, Arc<MultiTableTransaction>>>>,
    completed_transactions: Arc<Mutex<VecDeque<Arc<MultiTableTransaction>>>>,
    global_locks: Arc<Mutex<HashMap<String, String>>>, // resource_id -> transaction_id
    recovery_log: Arc<Mutex<Vec<String>>>,
    config: MultiTableTransactionConfig,
}

#[pymethods]
impl MultiTableTransactionManager {
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<MultiTableTransactionConfig>) -> Self {
        Self {
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            completed_transactions: Arc::new(Mutex::new(VecDeque::new())),
            global_locks: Arc::new(Mutex::new(HashMap::new())),
            recovery_log: Arc::new(Mutex::new(Vec::new())),
            config: config.unwrap_or_else(|| MultiTableTransactionConfig::new(
                MultiTableIsolationLevel::Serializable,
                600, // 10 minutes for manager
                5,
                2000,
                true,
                true,
                true,
                20,
                true
            )),
        }
    }

    /// Create a new multi-table transaction
    fn create_transaction(&self, config: Option<MultiTableTransactionConfig>, coordinator_id: Option<String>) -> PyResult<String> {
        let tx_config = config.unwrap_or_else(|| self.config.clone());
        let transaction = Arc::new(MultiTableTransaction::new(Some(tx_config), coordinator_id));
        let transaction_id = transaction.transaction_id.clone();

        let mut active = self.active_transactions.lock().unwrap();
        active.insert(transaction_id.clone(), transaction);

        Ok(transaction_id)
    }

    /// Get an active transaction
    fn get_transaction(&self, transaction_id: &str) -> PyResult<Option<Arc<MultiTableTransaction>>> {
        let active = self.active_transactions.lock().unwrap();
        Ok(active.get(transaction_id).cloned())
    }

    /// Begin a transaction
    fn begin_transaction(&self, py: Python, transaction_id: &str) -> PyResult<()> {
        let active = self.active_transactions.lock().unwrap();

        if let Some(transaction) = active.get(transaction_id) {
            transaction.begin(py)?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                format!("Transaction not found: {}", transaction_id)
            ));
        }

        Ok(())
    }

    /// Prepare a transaction (two-phase commit)
    fn prepare_transaction(&self, py: Python, transaction_id: &str) -> PyResult<bool> {
        let active = self.active_transactions.lock().unwrap();

        if let Some(transaction) = active.get(transaction_id) {
            transaction.prepare(py)
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                format!("Transaction not found: {}", transaction_id)
            ));
        }
    }

    /// Commit a transaction
    fn commit_transaction(&self, py: Python, transaction_id: &str) -> PyResult<()> {
        let mut active = self.active_transactions.lock().unwrap();

        if let Some(transaction) = active.remove(transaction_id) {
            transaction.commit(py)?;

            // Move to completed transactions
            let mut completed = self.completed_transactions.lock().unwrap();
            completed.push_back(transaction.clone());

            // Keep only last 1000 completed transactions
            if completed.len() > 1000 {
                completed.pop_front();
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                format!("Transaction not found: {}", transaction_id)
            ));
        }

        Ok(())
    }

    /// Abort a transaction
    fn abort_transaction(&self, py: Python, transaction_id: &str, reason: Option<String>) -> PyResult<()> {
        let mut active = self.active_transactions.lock().unwrap();

        if let Some(transaction) = active.remove(transaction_id) {
            transaction.abort(py, reason)?;

            // Move to completed transactions
            let mut completed = self.completed_transactions.lock().unwrap();
            completed.push_back(transaction.clone());
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                format!("Transaction not found: {}", transaction_id)
            ));
        }

        Ok(())
    }

    /// Get all active transactions
    fn get_active_transactions(&self) -> PyResult<Vec<Arc<MultiTableTransaction>>> {
        let active = self.active_transactions.lock().unwrap();
        Ok(active.values().cloned().collect())
    }

    /// Get completed transactions
    fn get_completed_transactions(&self, limit: Option<usize>) -> PyResult<Vec<Arc<MultiTableTransaction>>> {
        let completed = self.completed_transactions.lock().unwrap();

        let transactions: Vec<Arc<MultiTableTransaction>> = completed.iter()
            .rev()
            .take(limit.unwrap_or(completed.len()))
            .cloned()
            .collect();

        Ok(transactions)
    }

    /// Cleanup timed out transactions
    fn cleanup_timed_out_transactions(&self) -> PyResult<usize> {
        let mut active = self.active_transactions.lock().unwrap();
        let mut timed_out = Vec::new();

        active.retain(|_, transaction| {
            if transaction.is_timed_out() {
                timed_out.push(transaction.clone());
                false
            } else {
                true
            }
        });

        // Move timed out transactions to completed
        let mut completed = self.completed_transactions.lock().unwrap();
        for transaction in timed_out {
            completed.push_back(transaction);
        }

        Ok(timed_out.len())
    }

    /// Get transaction manager statistics
    fn get_manager_stats(&self) -> PyResult<HashMap<String, PyObject>> {
        let py = Python::acquire_gil().python();
        let mut stats = HashMap::new();

        let active = self.active_transactions.lock().unwrap();
        let completed = self.completed_transactions.lock().unwrap();
        let global_locks = self.global_locks.lock().unwrap();

        stats.insert("active_transactions".to_string(), active.len().to_object(py));
        stats.insert("completed_transactions".to_string(), completed.len().to_object(py));
        stats.insert("global_locks".to_string(), global_locks.len().to_object(py));
        stats.insert("max_participants_per_transaction".to_string(), self.config.max_participants.to_object(py));
        stats.insert("enable_2pc".to_string(), self.config.enable_2pc.to_object(py));
        stats.insert("enable_recovery".to_string(), self.config.enable_recovery.to_object(py));

        Ok(stats)
    }
}

// Convenience functions for module-level exports
#[pyfunction]
pub fn create_multi_table_transaction(
    config: Option<MultiTableTransactionConfig>,
    coordinator_id: Option<String>,
) -> PyResult<MultiTableTransaction> {
    Ok(MultiTableTransaction::new(config, coordinator_id))
}

#[pyfunction]
pub fn create_multi_table_transaction_manager(
    config: Option<MultiTableTransactionConfig>,
) -> PyResult<MultiTableTransactionManager> {
    Ok(MultiTableTransactionManager::new(config))
}