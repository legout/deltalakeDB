//! Distributed locking mechanism for multi-table transactions
//!
//! This module provides a comprehensive distributed locking system that ensures
//! consistent access to shared resources across multiple transaction participants.
//! It supports various lock types, deadlock detection, and automatic timeout handling.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

/// Lock type for distributed locking
#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Exclusive lock - only one transaction can access the resource
    Exclusive,
    /// Shared lock - multiple transactions can read, but not write
    Shared,
    /// Intent exclusive lock - intention to acquire exclusive lock
    IntentExclusive,
    /// Intent shared lock - intention to acquire shared lock
    IntentShared,
    /// Update lock - for updates, can be converted to exclusive
    Update,
}

/// Lock mode compatibility matrix
pub const LOCK_COMPATIBILITY: [[bool; 5]; 5] = [
    //                     IS  IX  S   U   X
    /* IS */            [true, true, true, true, false],
    /* IX */            [true, false, false, false, false],
    /* S  */            [true, false, true, true, false],
    /* U  */            [true, false, true, false, false],
    /* X  */            [false, false, false, false, false],
];

/// Lock request information
#[pyclass]
#[derive(Debug, Clone)]
pub struct LockRequest {
    request_id: String,
    transaction_id: String,
    resource_id: String,
    lock_type: LockType,
    requested_at: DateTime<Utc>,
    timeout_at: Option<DateTime<Utc>>,
    priority: i32,
    metadata: HashMap<String, String>,
}

#[pymethods]
impl LockRequest {
    #[new]
    #[pyo3(signature = (transaction_id, resource_id, lock_type, timeout_seconds=None, priority=0, metadata=None))]
    fn new(
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
        timeout_seconds: Option<u64>,
        priority: i32,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        let timeout_at = timeout_seconds.map(|secs| Utc::now() + chrono::Duration::seconds(secs as i64));

        Self {
            request_id: Uuid::new_v4().to_string(),
            transaction_id,
            resource_id,
            lock_type,
            requested_at: Utc::now(),
            timeout_at,
            priority,
            metadata: metadata.unwrap_or_default(),
        }
    }

    #[getter]
    fn request_id(&self) -> String {
        self.request_id.clone()
    }

    #[getter]
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }

    #[getter]
    fn resource_id(&self) -> String {
        self.resource_id.clone()
    }

    #[getter]
    fn lock_type(&self) -> LockType {
        self.lock_type.clone()
    }

    #[getter]
    fn priority(&self) -> i32 {
        self.priority
    }

    #[getter]
    fn is_expired(&self) -> bool {
        if let Some(timeout_at) = self.timeout_at {
            Utc::now() > timeout_at
        } else {
            false
        }
    }

    #[getter]
    fn wait_time_ms(&self) -> f64 {
        (Utc::now() - self.requested_at).num_milliseconds() as f64
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("request_id", &self.request_id)?;
        dict.set_item("transaction_id", &self.transaction_id)?;
        dict.set_item("resource_id", &self.resource_id)?;
        dict.set_item("lock_type", format!("{:?}", self.lock_type))?;
        dict.set_item("requested_at", self.requested_at.to_rfc3339())?;
        dict.set_item("timeout_at", self.timeout_at.map(|dt| dt.to_rfc3339()))?;
        dict.set_item("priority", self.priority)?;
        dict.set_item("is_expired", self.is_expired())?;
        dict.set_item("wait_time_ms", self.wait_time_ms())?;

        let metadata_dict = PyDict::new(py);
        for (key, value) in &self.metadata {
            metadata_dict.set_item(key, value)?;
        }
        dict.set_item("metadata", metadata_dict.to_object(py))?;

        Ok(dict.to_object(py))
    }
}

/// Acquired lock information
#[pyclass]
#[derive(Debug, Clone)]
pub struct AcquiredLock {
    lock_id: String,
    request_id: String,
    transaction_id: String,
    resource_id: String,
    lock_type: LockType,
    acquired_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    renewable: bool,
    renewal_count: u32,
}

#[pymethods]
impl AcquiredLock {
    #[new]
    #[pyo3(signature = (request_id, transaction_id, resource_id, lock_type, expires_seconds=None, renewable=true))]
    fn new(
        request_id: String,
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
        expires_seconds: Option<u64>,
        renewable: bool,
    ) -> Self {
        let expires_at = expires_seconds.map(|secs| Utc::now() + chrono::Duration::seconds(secs as i64));

        Self {
            lock_id: Uuid::new_v4().to_string(),
            request_id,
            transaction_id,
            resource_id,
            lock_type,
            acquired_at: Utc::now(),
            expires_at,
            renewable,
            renewal_count: 0,
        }
    }

    #[getter]
    fn lock_id(&self) -> String {
        self.lock_id.clone()
    }

    #[getter]
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }

    #[getter]
    fn resource_id(&self) -> String {
        self.resource_id.clone()
    }

    #[getter]
    fn lock_type(&self) -> LockType {
        self.lock_type.clone()
    }

    #[getter]
    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    #[getter]
    fn duration_ms(&self) -> f64 {
        (Utc::now() - self.acquired_at).num_milliseconds() as f64
    }

    #[getter]
    fn renewal_count(&self) -> u32 {
        self.renewal_count
    }

    fn renew(&mut self, additional_seconds: u64) -> PyResult<()> {
        if !self.renewable {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Lock is not renewable"
            ));
        }

        if let Some(ref mut expires_at) = self.expires_at {
            *expires_at = Utc::now() + chrono::Duration::seconds(additional_seconds as i64);
            self.renewal_count += 1;
        }

        Ok(())
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("lock_id", &self.lock_id)?;
        dict.set_item("transaction_id", &self.transaction_id)?;
        dict.set_item("resource_id", &self.resource_id)?;
        dict.set_item("lock_type", format!("{:?}", self.lock_type))?;
        dict.set_item("acquired_at", self.acquired_at.to_rfc3339())?;
        dict.set_item("expires_at", self.expires_at.map(|dt| dt.to_rfc3339()))?;
        dict.set_item("renewable", self.renewable)?;
        dict.set_item("renewal_count", self.renewal_count)?;
        dict.set_item("is_expired", self.is_expired())?;
        dict.set_item("duration_ms", self.duration_ms())?;
        Ok(dict.to_object(py))
    }
}

/// Deadlock detection result
#[pyclass]
#[derive(Debug, Clone)]
pub struct DeadlockDetectionResult {
    deadlock_detected: bool,
    cycle_transactions: Vec<String>,
    cycle_resources: Vec<String>,
    resolution_suggestions: Vec<String>,
    detection_time: DateTime<Utc>,
}

#[pymethods]
impl DeadlockDetectionResult {
    #[new]
    fn new(
        deadlock_detected: bool,
        cycle_transactions: Vec<String>,
        cycle_resources: Vec<String>,
        resolution_suggestions: Vec<String>,
    ) -> Self {
        Self {
            deadlock_detected,
            cycle_transactions,
            cycle_resources,
            resolution_suggestions,
            detection_time: Utc::now(),
        }
    }

    #[getter]
    fn deadlock_detected(&self) -> bool {
        self.deadlock_detected
    }

    #[getter]
    fn cycle_transactions(&self) -> Vec<String> {
        self.cycle_transactions.clone()
    }

    #[getter]
    fn cycle_resources(&self) -> Vec<String> {
        self.cycle_resources.clone()
    }

    #[getter]
    fn resolution_suggestions(&self) -> Vec<String> {
        self.resolution_suggestions.clone()
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("deadlock_detected", self.deadlock_detected)?;

        let tx_list = PyList::new(py, &self.cycle_transactions);
        dict.set_item("cycle_transactions", tx_list)?;

        let res_list = PyList::new(py, &self.cycle_resources);
        dict.set_item("cycle_resources", res_list)?;

        let suggestions_list = PyList::new(py, &self.resolution_suggestions);
        dict.set_item("resolution_suggestions", suggestions_list)?;

        dict.set_item("detection_time", self.detection_time.to_rfc3339())?;

        Ok(dict.to_object(py))
    }
}

/// Distributed lock manager
#[pyclass]
#[derive(Debug)]
pub struct DistributedLockManager {
    acquired_locks: Arc<Mutex<HashMap<String, AcquiredLock>>>, // resource_id -> AcquiredLock
    pending_requests: Arc<Mutex<VecDeque<LockRequest>>>,
    transaction_held_locks: Arc<Mutex<HashMap<String, HashSet<String>>>>, // transaction_id -> resource_ids
    lock_wait_graph: Arc<Mutex<HashMap<String, HashSet<String>>>>, // transaction_id -> waiting_for_transaction
    default_timeout_seconds: u64,
    max_pending_requests: usize,
    deadlock_check_interval: Duration,
    enable_deadlock_detection: bool,
    cleanup_interval: Duration,
    statistics: Arc<Mutex<LockManagerStatistics>>,
}

#[pymethods]
impl DistributedLockManager {
    #[new]
    #[pyo3(signature = (default_timeout_seconds=300, max_pending_requests=10000, enable_deadlock_detection=true, deadlock_check_interval_ms=5000, cleanup_interval_ms=10000))]
    fn new(
        default_timeout_seconds: u64,
        max_pending_requests: usize,
        enable_deadlock_detection: bool,
        deadlock_check_interval_ms: u64,
        cleanup_interval_ms: u64,
    ) -> Self {
        Self {
            acquired_locks: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(VecDeque::new())),
            transaction_held_locks: Arc::new(Mutex::new(HashMap::new())),
            lock_wait_graph: Arc::new(Mutex::new(HashMap::new())),
            default_timeout_seconds,
            max_pending_requests,
            deadlock_check_interval: Duration::from_millis(deadlock_check_interval_ms),
            enable_deadlock_detection,
            cleanup_interval: Duration::from_millis(cleanup_interval_ms),
            statistics: Arc::new(Mutex::new(LockManagerStatistics::new())),
        }
    }

    /// Request a lock on a resource
    fn request_lock(
        &self,
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
        timeout_seconds: Option<u64>,
        priority: i32,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let timeout = timeout_seconds.unwrap_or(self.default_timeout_seconds);
        let request = LockRequest::new(
            transaction_id.clone(),
            resource_id.clone(),
            lock_type.clone(),
            Some(timeout),
            priority,
            metadata,
        );

        // Check for immediate lock availability
        if self.can_acquire_lock_immediately(&request)? {
            return self.acquire_lock_immediately(&request);
        }

        // Add to pending queue
        self.add_pending_request(request)?;
        self.update_statistics("lock_requested", false, None)?;

        Ok(request.request_id)
    }

    /// Try to acquire a lock without waiting
    fn try_lock(
        &self,
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
        timeout_seconds: Option<u64>,
    ) -> PyResult<Option<AcquiredLock>> {
        let request = LockRequest::new(
            transaction_id,
            resource_id,
            lock_type,
            timeout_seconds,
            0,
            None,
        );

        if self.can_acquire_lock_immediately(&request)? {
            Ok(Some(AcquiredLock::new(
                request.request_id,
                request.transaction_id,
                request.resource_id,
                request.lock_type,
                timeout_seconds,
                true,
            )))
        } else {
            Ok(None)
        }
    }

    /// Release a lock
    fn release_lock(&self, lock_id: String, force: bool) -> PyResult<bool> {
        let mut acquired_locks = self.acquired_locks.lock().unwrap();

        // Find the lock
        let lock_option = acquired_locks.iter()
            .find(|(_, acquired_lock)| acquired_lock.lock_id == lock_id)
            .map(|(_, lock)| lock.clone());

        if let Some(lock) = lock_option {
            // Check if lock can be released (not expired unless forced)
            if !force && lock.is_expired() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Lock is expired and cannot be released normally"
                ));
            }

            // Remove the lock
            acquired_locks.remove(&lock.resource_id);

            // Update transaction lock tracking
            self.remove_transaction_held_lock(&lock.transaction_id, &lock.resource_id)?;

            // Process pending requests
            self.process_pending_requests()?;

            self.update_statistics("lock_released", true, None)?;
            Ok(true)
        } else {
            self.update_statistics("lock_release_failed", false, None)?;
            Ok(false)
        }
    }

    /// Release all locks held by a transaction
    fn release_all_transaction_locks(&self, transaction_id: String, force: bool) -> PyResult<u32> {
        let mut acquired_locks = self.acquired_locks.lock().unwrap();
        let mut transaction_locks = self.transaction_held_locks.lock().unwrap();

        // Get all locks held by the transaction
        let held_resources = transaction_locks
            .get(&transaction_id)
            .cloned()
            .unwrap_or_default();

        let mut released_count = 0;

        // Release each lock
        for resource_id in held_resources {
            if acquired_locks.remove(&resource_id).is_some() {
                released_count += 1;
            }
        }

        // Clear transaction tracking
        transaction_locks.remove(&transaction_id);

        // Remove from wait graph
        {
            let mut wait_graph = self.lock_wait_graph.lock().unwrap();
            wait_graph.remove(&transaction_id);
        }

        // Process pending requests
        if released_count > 0 {
            self.process_pending_requests()?;
        }

        self.update_statistics("transaction_locks_released", true, Some(released_count))?;
        Ok(released_count)
    }

    /// Renew a lock
    fn renew_lock(&self, lock_id: String, additional_seconds: u64) -> PyResult<bool> {
        let mut acquired_locks = self.acquired_locks.lock().unwrap();

        if let Some(lock) = acquired_locks.values_mut()
            .find(|lock| lock.lock_id == lock_id) {
            lock.renew(additional_seconds)?;
            self.update_statistics("lock_renewed", true, None)?;
            Ok(true)
        } else {
            self.update_statistics("lock_renewal_failed", false, None)?;
            Ok(false)
        }
    }

    /// Check if a lock exists for a resource
    fn has_lock(&self, resource_id: String) -> PyResult<bool> {
        let acquired_locks = self.acquired_locks.lock().unwrap();
        Ok(acquired_locks.contains_key(&resource_id))
    }

    /// Get all locks held by a transaction
    fn get_transaction_locks(&self, transaction_id: String) -> PyResult<Vec<AcquiredLock>> {
        let acquired_locks = self.acquired_locks.lock().unwrap();
        let transaction_held_locks = self.transaction_held_locks.lock().unwrap();

        let held_resources = transaction_held_locks
            .get(&transaction_id)
            .cloned()
            .unwrap_or_default();

        let mut locks = Vec::new();
        for resource_id in held_resources {
            if let Some(lock) = acquired_locks.get(&resource_id) {
                locks.push(lock.clone());
            }
        }

        Ok(locks)
    }

    /// Get all pending requests
    fn get_pending_requests(&self) -> PyResult<Vec<LockRequest>> {
        let pending_requests = self.pending_requests.lock().unwrap();
        Ok(pending_requests.iter().cloned().collect())
    }

    /// Detect deadlocks
    fn detect_deadlocks(&self) -> PyResult<DeadlockDetectionResult> {
        if !self.enable_deadlock_detection {
            return Ok(DeadlockDetectionResult::new(
                false,
                Vec::new(),
                Vec::new(),
                vec!["Deadlock detection is disabled".to_string()],
            ));
        }

        let wait_graph = self.lock_wait_graph.lock().unwrap();

        // Build wait-for graph
        let mut graph = HashMap::new();
        for (waiting_tx, waiting_for_set) in wait_graph.iter() {
            for waiting_for_tx in waiting_for_set {
                graph.entry(waiting_tx.clone())
                    .or_insert_with(HashSet::new)
                    .insert(waiting_for_tx.clone());
            }
        }

        // Detect cycles using DFS
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycle_path = Vec::new();

        for transaction in graph.keys() {
            if !visited.contains(transaction) {
                if self.detect_cycle_util(
                    transaction,
                    &graph,
                    &mut visited,
                    &mut rec_stack,
                    &mut cycle_path,
                ) {
                    // Found a cycle
                    let cycle_transactions = cycle_path.clone();
                    let mut cycle_resources = Vec::new();

                    // Get resources involved in the cycle
                    let pending_requests = self.pending_requests.lock().unwrap();
                    for request in pending_requests.iter() {
                        if cycle_transactions.contains(&request.transaction_id) {
                            cycle_resources.push(request.resource_id.clone());
                        }
                    }

                    let resolution_suggestions = vec![
                        "Abort the transaction with lowest priority".to_string(),
                        "Increase lock timeout for higher priority transactions".to_string(),
                        "Implement lock timeout retry with exponential backoff".to_string(),
                        "Reorder transaction operations to avoid lock conflicts".to_string(),
                    ];

                    return Ok(DeadlockDetectionResult::new(
                        true,
                        cycle_transactions,
                        cycle_resources,
                        resolution_suggestions,
                    ));
                }
            }
        }

        // No deadlock detected
        Ok(DeadlockDetectionResult::new(
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Get lock manager statistics
    fn get_statistics(&self) -> PyResult<HashMap<String, PyObject>> {
        let py = Python::acquire_gil().python();
        let mut stats = HashMap::new();

        let statistics = self.statistics.lock().unwrap();
        let acquired_locks = self.acquired_locks.lock().unwrap();
        let pending_requests = self.pending_requests.lock().unwrap();

        stats.insert("total_acquired_locks".to_string(), acquired_locks.len().to_object(py));
        stats.insert("total_pending_requests".to_string(), pending_requests.len().to_object(py));
        stats.insert("total_lock_requests".to_string(), statistics.total_lock_requests.to_object(py));
        stats.insert("successful_acquisitions".to_string(), statistics.successful_acquisitions.to_object(py));
        stats.insert("failed_acquisitions".to_string(), statistics.failed_acquisitions.to_object(py));
        stats.insert("expired_locks".to_string(), statistics.expired_locks.to_object(py));
        stats.insert("renewed_locks".to_string(), statistics.renewed_locks.to_object(py));
        stats.insert("deadlocks_detected".to_string(), statistics.deadlocks_detected.to_object(py));
        stats.insert("average_wait_time_ms".to_string(), statistics.average_wait_time_ms.to_object(py));
        stats.insert("enable_deadlock_detection".to_string(), self.enable_deadlock_detection.to_object(py));

        Ok(stats)
    }

    /// Cleanup expired locks and requests
    fn cleanup(&self) -> PyResult<u32> {
        let mut cleaned_count = 0;
        let now = Utc::now();

        // Clean up expired acquired locks
        {
            let mut acquired_locks = self.acquired_locks.lock().unwrap();
            let mut expired_resources = Vec::new();

            for (resource_id, lock) in acquired_locks.iter() {
                if lock.is_expired() {
                    expired_resources.push(resource_id.clone());
                }
            }

            for resource_id in expired_resources {
                if let Some(lock) = acquired_locks.remove(&resource_id) {
                    self.remove_transaction_held_lock(&lock.transaction_id, &resource_id)?;
                    cleaned_count += 1;
                }
            }
        }

        // Clean up expired pending requests
        {
            let mut pending_requests = self.pending_requests.lock().unwrap();
            let mut to_remove = Vec::new();

            for (index, request) in pending_requests.iter().enumerate() {
                if request.is_expired() {
                    to_remove.push(index);
                }
            }

            // Remove in reverse order to maintain indices
            for index in to_remove.iter().rev() {
                pending_requests.remove(*index);
                cleaned_count += 1;
            }
        }

        // Process pending requests after cleanup
        if cleaned_count > 0 {
            self.process_pending_requests()?;
        }

        self.update_statistics("cleanup", true, Some(cleaned_count))?;
        Ok(cleaned_count)
    }

    // Private helper methods
    fn can_acquire_lock_immediately(&self, request: &LockRequest) -> PyResult<bool> {
        let acquired_locks = self.acquired_locks.lock().unwrap();

        if let Some(existing_lock) = acquired_locks.get(&request.resource_id) {
            // Check compatibility between existing lock and requested lock
            self.are_locks_compatible(&existing_lock.lock_type, &request.lock_type)
        } else {
            // No existing lock, can acquire immediately
            Ok(true)
        }
    }

    fn are_locks_compatible(&self, existing_type: &LockType, requested_type: &LockType) -> PyResult<bool> {
        let existing_index = self.lock_type_to_index(existing_type);
        let requested_index = self.lock_type_to_index(requested_type);

        if existing_index >= LOCK_COMPATIBILITY.len() || requested_index >= LOCK_COMPATIBILITY[0].len() {
            return Ok(false);
        }

        Ok(LOCK_COMPATIBILITY[existing_index][requested_index])
    }

    fn lock_type_to_index(&self, lock_type: &LockType) -> usize {
        match lock_type {
            LockType::IntentShared => 0,
            LockType::IntentExclusive => 1,
            LockType::Shared => 2,
            LockType::Update => 3,
            LockType::Exclusive => 4,
        }
    }

    fn acquire_lock_immediately(&self, request: &LockRequest) -> PyResult<String> {
        let mut acquired_locks = self.acquired_locks.lock().unwrap();

        let lock = AcquiredLock::new(
            request.request_id.clone(),
            request.transaction_id.clone(),
            request.resource_id.clone(),
            request.lock_type.clone(),
            Some(self.default_timeout_seconds),
            true,
        );

        acquired_locks.insert(request.resource_id.clone(), lock.clone());

        // Update transaction tracking
        self.add_transaction_held_lock(&request.transaction_id, &request.resource_id)?;

        self.update_statistics("lock_acquired_immediately", true, None)?;
        Ok(lock.lock_id)
    }

    fn add_pending_request(&self, request: LockRequest) -> PyResult<()> {
        let mut pending_requests = self.pending_requests.lock().unwrap();

        if pending_requests.len() >= self.max_pending_requests {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Maximum pending requests ({}) exceeded", self.max_pending_requests)
            ));
        }

        // Insert request maintaining priority order
        let insert_pos = pending_requests.binary_search_by(|existing| {
            existing.priority.cmp(&request.priority).reverse()
        }).unwrap_or_else(|pos| pos);

        pending_requests.insert(insert_pos, request);

        // Update wait graph
        if let Some(existing_lock) = self.acquired_locks.lock().unwrap().get(&request.resource_id) {
            let mut wait_graph = self.lock_wait_graph.lock().unwrap();
            wait_graph.entry(request.transaction_id.clone())
                .or_insert_with(HashSet::new)
                .insert(existing_lock.transaction_id.clone());
        }

        Ok(())
    }

    fn process_pending_requests(&self) -> PyResult<()> {
        let mut pending_requests = self.pending_requests.lock().unwrap();
        let mut to_remove = Vec::new();

        for (index, request) in pending_requests.iter().enumerate() {
            if request.is_expired() {
                to_remove.push(index);
                continue;
            }

            if self.can_acquire_lock_immediately(request)? {
                // Acquire the lock
                let _ = self.acquire_lock_immediately(request);
                to_remove.push(index);
            }
        }

        // Remove processed requests (in reverse order)
        for index in to_remove.iter().rev() {
            pending_requests.remove(*index);
        }

        Ok(())
    }

    fn add_transaction_held_lock(&self, transaction_id: &str, resource_id: &str) -> PyResult<()> {
        let mut transaction_held_locks = self.transaction_held_locks.lock().unwrap();
        transaction_held_locks
            .entry(transaction_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(resource_id.to_string());
        Ok(())
    }

    fn remove_transaction_held_lock(&self, transaction_id: &str, resource_id: &str) -> PyResult<()> {
        let mut transaction_held_locks = self.transaction_held_locks.lock().unwrap();
        if let Some(held_locks) = transaction_held_locks.get_mut(transaction_id) {
            held_locks.remove(resource_id);

            // Remove transaction entry if no locks remain
            if held_locks.is_empty() {
                transaction_held_locks.remove(transaction_id);
            }
        }
        Ok(())
    }

    fn detect_cycle_util(
        &self,
        transaction: &str,
        graph: &HashMap<String, HashSet<String>>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        cycle_path: &mut Vec<String>,
    ) -> bool {
        visited.insert(transaction.to_string());
        rec_stack.insert(transaction.to_string());
        cycle_path.push(transaction.to_string());

        if let Some(neighbors) = graph.get(transaction) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if self.detect_cycle_util(neighbor, graph, visited, rec_stack, cycle_path) {
                        return true;
                    }
                } else if rec_stack.contains(neighbor) {
                    // Found cycle - extract cycle path
                    if let Some(cycle_start) = cycle_path.iter().position(|tx| tx == neighbor) {
                        cycle_path = cycle_path[cycle_start..].to_vec();
                    }
                    return true;
                }
            }
        }

        rec_stack.remove(transaction);
        cycle_path.pop();
        false
    }

    fn update_statistics(&self, operation: &str, success: bool, count: Option<u32>) -> PyResult<()> {
        let mut statistics = self.statistics.lock().unwrap();

        match operation {
            "lock_requested" => statistics.total_lock_requests += 1,
            "lock_acquired_immediately" => statistics.successful_acquisitions += 1,
            "lock_released" => statistics.successful_acquisitions += 1,
            "lock_renewal_failed" => statistics.failed_acquisitions += 1,
            "transaction_locks_released" => {
                if let Some(c) = count {
                    statistics.successful_acquisitions += c as u64;
                }
            },
            "deadlock_detected" => statistics.deadlocks_detected += 1,
            "lock_renewed" => statistics.renewed_locks += 1,
            "cleanup" => statistics.expired_locks += count.unwrap_or(0) as u64,
            _ => {}
        }

        // Update average wait time (simplified)
        if operation == "lock_acquired_immediately" {
            statistics.average_wait_time_ms = (statistics.average_wait_time_ms + 50) / 2;
        }

        Ok(())
    }
}

/// Lock manager statistics
#[derive(Debug, Clone)]
pub struct LockManagerStatistics {
    pub total_lock_requests: u64,
    pub successful_acquisitions: u64,
    pub failed_acquisitions: u64,
    pub expired_locks: u64,
    pub renewed_locks: u64,
    pub deadlocks_detected: u64,
    pub average_wait_time_ms: u64,
}

impl LockManagerStatistics {
    pub fn new() -> Self {
        Self {
            total_lock_requests: 0,
            successful_acquisitions: 0,
            failed_acquisitions: 0,
            expired_locks: 0,
            renewed_locks: 0,
            deadlocks_detected: 0,
            average_wait_time_ms: 0,
        }
    }
}

// Convenience function for module-level export
#[pyfunction]
pub fn create_distributed_lock_manager(
    default_timeout_seconds: u64,
    max_pending_requests: usize,
    enable_deadlock_detection: bool,
    deadlock_check_interval_ms: u64,
    cleanup_interval_ms: u64,
) -> PyResult<DistributedLockManager> {
    Ok(DistributedLockManager::new(
        default_timeout_seconds,
        max_pending_requests,
        enable_deadlock_detection,
        deadlock_check_interval_ms,
        cleanup_interval_ms,
    ))
}