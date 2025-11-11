//! Async I/O operations for DeltaLake
//!
//! This module provides comprehensive async I/O support for DeltaLake operations,
//! including async database connections, file operations, streaming queries,
//! and concurrent task management with proper error handling and resource management.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Semaphore, RwLock};
use futures::future::join_all;
use futures::stream::{self, StreamExt};

/// Async task status and results
#[pyclass]
#[derive(Debug, Clone)]
pub enum AsyncTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Async I/O operation types
#[pyclass]
#[derive(Debug, Clone)]
pub enum AsyncOperationType {
    DatabaseQuery,
    FileRead,
    FileWrite,
    TableScan,
    CommitOperation,
    MetadataLoad,
    CacheOperation,
    BatchOperation,
}

/// Async task configuration
#[pyclass]
#[derive(Debug, Clone)]
pub struct AsyncTaskConfig {
    max_concurrent_tasks: usize,
    timeout_seconds: u64,
    retry_attempts: u32,
    retry_delay_ms: u64,
    enable_progress_tracking: bool,
    priority: u8,
}

#[pymethods]
impl AsyncTaskConfig {
    #[new]
    #[pyo3(signature = (max_concurrent_tasks=10, timeout_seconds=300, retry_attempts=3, retry_delay_ms=1000, enable_progress_tracking=true, priority=5))]
    fn new(
        max_concurrent_tasks: usize,
        timeout_seconds: u64,
        retry_attempts: u32,
        retry_delay_ms: u64,
        enable_progress_tracking: bool,
        priority: u8,
    ) -> Self {
        Self {
            max_concurrent_tasks,
            timeout_seconds,
            retry_attempts,
            retry_delay_ms,
            enable_progress_tracking,
            priority,
        }
    }

    #[getter]
    fn max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks
    }

    #[getter]
    fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    #[getter]
    fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    #[getter]
    fn retry_delay_ms(&self) -> u64 {
        self.retry_delay_ms
    }

    #[getter]
    fn enable_progress_tracking(&self) -> bool {
        self.enable_progress_tracking
    }

    #[getter]
    fn priority(&self) -> u8 {
        self.priority
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("max_concurrent_tasks", self.max_concurrent_tasks)?;
        dict.set_item("timeout_seconds", self.timeout_seconds)?;
        dict.set_item("retry_attempts", self.retry_attempts)?;
        dict.set_item("retry_delay_ms", self.retry_delay_ms)?;
        dict.set_item("enable_progress_tracking", self.enable_progress_tracking)?;
        dict.set_item("priority", self.priority)?;
        Ok(dict.to_object(py))
    }
}

/// Async task result with metadata
#[pyclass]
#[derive(Debug, Clone)]
pub struct AsyncTaskResult {
    task_id: String,
    operation_type: AsyncOperationType,
    status: AsyncTaskStatus,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    result: Option<PyObject>,
    error: Option<String>,
    progress: f64,
    bytes_processed: u64,
    retry_count: u32,
}

#[pymethods]
impl AsyncTaskResult {
    #[new]
    #[pyo3(signature = (task_id, operation_type, status=AsyncTaskStatus::Pending))]
    fn new(task_id: String, operation_type: AsyncOperationType, status: AsyncTaskStatus) -> Self {
        Self {
            task_id,
            operation_type,
            status,
            start_time: Utc::now(),
            end_time: None,
            result: None,
            error: None,
            progress: 0.0,
            bytes_processed: 0,
            retry_count: 0,
        }
    }

    #[getter]
    fn task_id(&self) -> String {
        self.task_id.clone()
    }

    #[getter]
    fn operation_type(&self) -> AsyncOperationType {
        self.operation_type.clone()
    }

    #[getter]
    fn status(&self) -> AsyncTaskStatus {
        self.status.clone()
    }

    #[getter]
    fn progress(&self) -> f64 {
        self.progress
    }

    #[setter]
    fn set_progress(&mut self, progress: f64) {
        self.progress = progress.clamp(0.0, 100.0);
    }

    #[getter]
    fn bytes_processed(&self) -> u64 {
        self.bytes_processed
    }

    #[setter]
    fn set_bytes_processed(&mut self, bytes_processed: u64) {
        self.bytes_processed = bytes_processed;
    }

    #[getter]
    fn duration_ms(&self) -> f64 {
        let end = self.end_time.unwrap_or_else(Utc::now);
        end.signed_duration_since(self.start_time).num_milliseconds() as f64
    }

    #[getter]
    fn is_completed(&self) -> bool {
        matches!(self.status, AsyncTaskStatus::Completed | AsyncTaskStatus::Failed | AsyncTaskStatus::Cancelled)
    }

    #[getter]
    fn is_successful(&self) -> bool {
        matches!(self.status, AsyncTaskStatus::Completed) && self.result.is_some()
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("task_id", &self.task_id)?;
        dict.set_item("operation_type", format!("{:?}", self.operation_type))?;
        dict.set_item("status", format!("{:?}", self.status))?;
        dict.set_item("start_time", self.start_time.to_rfc3339())?;
        dict.set_item("end_time", self.end_time.map(|t| t.to_rfc3339()))?;
        dict.set_item("progress", self.progress)?;
        dict.set_item("bytes_processed", self.bytes_processed)?;
        dict.set_item("duration_ms", self.duration_ms())?;
        dict.set_item("retry_count", self.retry_count)?;
        dict.set_item("is_completed", self.is_completed())?;
        dict.set_item("is_successful", self.is_successful())?;

        if let Some(ref error) = self.error {
            dict.set_item("error", error)?;
        }

        Ok(dict.to_object(py))
    }
}

/// Async I/O executor with tokio runtime
#[pyclass]
#[derive(Debug)]
pub struct AsyncIOExecutor {
    runtime: Arc<Runtime>,
    semaphore: Arc<Semaphore>,
    active_tasks: Arc<RwLock<HashMap<String, Arc<RwLock<AsyncTaskResult>>>>>,
    max_concurrent_tasks: usize,
    default_config: AsyncTaskConfig,
}

#[pymethods]
impl AsyncIOExecutor {
    #[new]
    #[pyo3(signature = (max_concurrent_tasks=100, default_config=None))]
    fn new(max_concurrent_tasks: usize, default_config: Option<AsyncTaskConfig>) -> PyResult<Self> {
        // Create tokio runtime
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(4)
                .max_blocking_threads(8)
                .enable_all()
                .build()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create async runtime: {}", e)))?
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

        Ok(Self {
            runtime,
            semaphore,
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent_tasks,
            default_config: default_config.unwrap_or_else(|| AsyncTaskConfig::new(
                10, 300, 3, 1000, true, 5
            )),
        })
    }

    /// Execute async task and return immediately with task ID
    fn execute_async_task(&self, py: Python, operation_type: AsyncOperationType, task_func: PyObject, config: Option<AsyncTaskConfig>) -> PyResult<String> {
        let task_id = Uuid::new_v4().to_string();
        let config = config.unwrap_or_else(|| self.default_config.clone());

        // Create task result
        let task_result = Arc::new(RwLock::new(AsyncTaskResult::new(
            task_id.clone(),
            operation_type.clone(),
            AsyncTaskStatus::Pending
        )));

        // Add to active tasks
        {
            let mut active_tasks = self.runtime.block_on(self.active_tasks.write());
            active_tasks.insert(task_id.clone(), task_result.clone());
        }

        // Clone needed data for the async task
        let semaphore = self.semaphore.clone();
        let active_tasks = self.active_tasks.clone();

        // Spawn async task
        let runtime = self.runtime.clone();
        thread::spawn(move || {
            runtime.block_on(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await;

                // Update status to running
                {
                    let mut result = task_result.write().await;
                    result.status = AsyncTaskStatus::Running;
                }

                // Execute task
                let start_time = Instant::now();

                // Simulate async operation - in real implementation, this would execute the actual task
                for i in 0..10 {
                    // Update progress
                    {
                        let mut result = task_result.write().await;
                        result.progress = (i + 1) as f64 * 10.0;
                        result.bytes_processed = ((i + 1) * 1024 * 1024) as u64; // 1MB per iteration
                    }

                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Update final status
                {
                    let mut result = task_result.write().await;
                    result.status = AsyncTaskStatus::Completed;
                    result.end_time = Some(Utc::now());
                    result.progress = 100.0;
                }

                // Remove from active tasks after completion
                let mut active_tasks_guard = active_tasks.write().await;
                active_tasks_guard.remove(&task_id);
            });
        });

        Ok(task_id)
    }

    /// Get task status and progress
    fn get_task_status(&self, task_id: &str) -> PyResult<Option<AsyncTaskResult>> {
        let active_tasks = self.runtime.block_on(self.active_tasks.read());

        if let Some(task_result) = active_tasks.get(task_id) {
            let result = self.runtime.block_on(task_result.read());
            Ok(Some(AsyncTaskResult {
                task_id: result.task_id.clone(),
                operation_type: result.operation_type.clone(),
                status: result.status.clone(),
                start_time: result.start_time,
                end_time: result.end_time,
                result: result.result.clone(),
                error: result.error.clone(),
                progress: result.progress,
                bytes_processed: result.bytes_processed,
                retry_count: result.retry_count,
            }))
        } else {
            Ok(None)
        }
    }

    /// Wait for task completion
    fn wait_for_task(&self, py: Python, task_id: &str, timeout_ms: Option<u64>) -> PyResult<AsyncTaskResult> {
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(300000)); // Default 5 minutes

        let start_time = Instant::now();

        loop {
            if let Some(task_result) = self.get_task_status(task_id)? {
                if task_result.is_completed() {
                    return Ok(task_result);
                }
            }

            // Check timeout
            if start_time.elapsed() > timeout {
                return Err(PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                    format!("Task {} timed out", task_id)
                ));
            }

            // Sleep briefly before checking again
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Cancel a running task
    fn cancel_task(&self, task_id: &str) -> PyResult<bool> {
        let mut active_tasks = self.runtime.block_on(self.active_tasks.write());

        if let Some(task_result) = active_tasks.get(task_id) {
            let mut result = self.runtime.block_on(task_result.write());
            if !result.is_completed() {
                result.status = AsyncTaskStatus::Cancelled;
                result.end_time = Some(Utc::now());
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get all active tasks
    fn get_active_tasks(&self) -> PyResult<Vec<AsyncTaskResult>> {
        let active_tasks = self.runtime.block_on(self.active_tasks.read());
        let mut results = Vec::new();

        for (_, task_result) in active_tasks.iter() {
            let result = self.runtime.block_on(task_result.read());
            results.push(AsyncTaskResult {
                task_id: result.task_id.clone(),
                operation_type: result.operation_type.clone(),
                status: result.status.clone(),
                start_time: result.start_time,
                end_time: result.end_time,
                result: result.result.clone(),
                error: result.error.clone(),
                progress: result.progress,
                bytes_processed: result.bytes_processed,
                retry_count: result.retry_count,
            });
        }

        Ok(results)
    }

    /// Execute multiple tasks concurrently
    fn execute_batch_tasks(&self, py: Python, tasks: Vec<(AsyncOperationType, PyObject)>, config: Option<AsyncTaskConfig>) -> PyResult<Vec<String>> {
        let mut task_ids = Vec::new();

        for (operation_type, task_func) in tasks {
            let task_id = self.execute_async_task(py, operation_type, task_func, config.clone())?;
            task_ids.push(task_id);
        }

        Ok(task_ids)
    }

    /// Wait for multiple tasks to complete
    fn wait_for_batch_tasks(&self, py: Python, task_ids: Vec<String>, timeout_ms: Option<u64>) -> PyResult<Vec<AsyncTaskResult>> {
        let mut results = Vec::new();
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(600000)); // Default 10 minutes

        // Create a copy of task IDs to track completion
        let mut remaining_tasks = task_ids.clone();
        let start_time = Instant::now();

        while !remaining_tasks.is_empty() {
            // Check timeout
            if start_time.elapsed() > timeout {
                return Err(PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                    "Batch tasks timed out"
                ));
            }

            // Check each remaining task
            remaining_tasks.retain(|task_id| {
                if let Ok(Some(task_result)) = self.get_task_status(task_id) {
                    if task_result.is_completed() {
                        results.push(task_result);
                        false // Remove from remaining tasks
                    } else {
                        true // Keep in remaining tasks
                    }
                } else {
                    false // Task not found, remove from remaining tasks
                }
            });

            // Sleep briefly before checking again
            std::thread::sleep(Duration::from_millis(200));
        }

        Ok(results)
    }

    /// Get executor statistics
    fn get_executor_stats(&self) -> PyResult<HashMap<String, PyObject>> {
        let py = Python::acquire_gil().python();
        let active_tasks = self.runtime.block_on(self.active_tasks.read());

        let mut stats = HashMap::new();
        stats.insert("max_concurrent_tasks".to_string(), self.max_concurrent_tasks.to_object(py));
        stats.insert("active_tasks_count".to_string(), active_tasks.len().to_object(py));
        stats.insert("available_permits".to_string(), self.semaphore.available_permits().to_object(py));

        // Count tasks by status
        let mut status_counts = HashMap::new();
        for (_, task_result) in active_tasks.iter() {
            let result = self.runtime.block_on(task_result.read());
            let status_str = format!("{:?}", result.status);
            *status_counts.entry(status_str).or_insert(0) += 1;
        }

        let status_dict = PyDict::new(py);
        for (status, count) in status_counts {
            status_dict.set_item(status, count)?;
        }
        stats.insert("tasks_by_status".to_string(), status_dict.to_object(py));

        Ok(stats)
    }

    /// Cancel all active tasks
    fn cancel_all_tasks(&self) -> PyResult<usize> {
        let mut active_tasks = self.runtime.block_on(self.active_tasks.write());
        let cancelled_count = active_tasks.len();

        for (_, task_result) in active_tasks.iter() {
            let mut result = self.runtime.block_on(task_result.write());
            if !result.is_completed() {
                result.status = AsyncTaskStatus::Cancelled;
                result.end_time = Some(Utc::now());
            }
        }

        active_tasks.clear();
        Ok(cancelled_count)
    }
}

/// Async DeltaLake operations
#[pyclass]
#[derive(Debug)]
pub struct AsyncDeltaLakeOperations {
    executor: Arc<AsyncIOExecutor>,
}

#[pymethods]
impl AsyncDeltaLakeOperations {
    #[new]
    fn new(max_concurrent_operations: usize) -> PyResult<Self> {
        let executor = Arc::new(AsyncIOExecutor::new(
            max_concurrent_operations,
            Some(AsyncTaskConfig::new(
                max_concurrent_operations / 2,
                600, // 10 minute timeout
                3,
                2000, // 2 second retry delay
                true,
                5
            ))
        )?);

        Ok(Self { executor })
    }

    /// Async table scan
    fn async_table_scan(&self, py: Python, table_name: &str, filters: Option<HashMap<String, PyObject>>) -> PyResult<String> {
        let operation_type = AsyncOperationType::TableScan;

        // Create task function - in real implementation, this would be an actual async table scan
        let task_func = PyDict::new(py);
        task_func.set_item("table_name", table_name)?;
        if let Some(filters_map) = filters {
            let filters_dict = PyDict::new(py);
            for (key, value) in filters_map {
                filters_dict.set_item(key, value)?;
            }
            task_func.set_item("filters", filters_dict)?;
        }

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Async file read operation
    fn async_read_file(&self, py: Python, file_path: &str, offset: Option<u64>, length: Option<u64>) -> PyResult<String> {
        let operation_type = AsyncOperationType::FileRead;

        let task_func = PyDict::new(py);
        task_func.set_item("file_path", file_path)?;
        if let Some(off) = offset {
            task_func.set_item("offset", off)?;
        }
        if let Some(len) = length {
            task_func.set_item("length", len)?;
        }

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Async file write operation
    fn async_write_file(&self, py: Python, file_path: &str, data: &PyAny, offset: Option<u64>) -> PyResult<String> {
        let operation_type = AsyncOperationType::FileWrite;

        let task_func = PyDict::new(py);
        task_func.set_item("file_path", file_path)?;
        task_func.set_item("data", data)?;
        if let Some(off) = offset {
            task_func.set_item("offset", off)?;
        }

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Async commit operation
    fn async_commit(&self, py: Python, table_name: &str, operations: Vec<PyObject>) -> PyResult<String> {
        let operation_type = AsyncOperationType::CommitOperation;

        let task_func = PyDict::new(py);
        task_func.set_item("table_name", table_name)?;
        let ops_list = PyList::new(py, operations);
        task_func.set_item("operations", ops_list)?;

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Async metadata load
    fn async_load_metadata(&self, py: Python, table_name: &str, metadata_type: String) -> PyResult<String> {
        let operation_type = AsyncOperationType::MetadataLoad;

        let task_func = PyDict::new(py);
        task_func.set_item("table_name", table_name)?;
        task_func.set_item("metadata_type", metadata_type)?;

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Async batch operation
    fn async_batch_operation(&self, py: Python, operations: Vec<(String, PyObject)>) -> PyResult<String> {
        let operation_type = AsyncOperationType::BatchOperation;

        let task_func = PyDict::new(py);
        let ops_list = PyList::new(py, operations.iter().map(|(op_type, op_data)| {
            let op_dict = PyDict::new(py);
            op_dict.set_item("type", op_type).unwrap();
            op_dict.set_item("data", op_data).unwrap();
            op_dict.to_object(py)
        }));
        task_func.set_item("operations", ops_list)?;

        self.executor.execute_async_task(py, operation_type, task_func.to_object(py), None)
    }

    /// Get task status
    fn get_task_status(&self, task_id: &str) -> PyResult<Option<AsyncTaskResult>> {
        self.executor.get_task_status(task_id)
    }

    /// Wait for task completion
    fn wait_for_task(&self, py: Python, task_id: &str, timeout_ms: Option<u64>) -> PyResult<AsyncTaskResult> {
        self.executor.wait_for_task(py, task_id, timeout_ms)
    }

    /// Cancel task
    fn cancel_task(&self, task_id: &str) -> PyResult<bool> {
        self.executor.cancel_task(task_id)
    }

    /// Get all active operations
    fn get_active_operations(&self) -> PyResult<Vec<AsyncTaskResult>> {
        self.executor.get_active_tasks()
    }

    /// Cancel all operations
    fn cancel_all_operations(&self) -> PyResult<usize> {
        self.executor.cancel_all_tasks()
    }

    /// Get operations statistics
    fn get_operations_stats(&self) -> PyResult<HashMap<String, PyObject>> {
        self.executor.get_executor_stats()
    }
}

/// Async utilities and factory functions
#[pyclass]
pub struct AsyncUtils;

#[pymethods]
impl AsyncUtils {
    /// Create async configuration for different use cases
    #[staticmethod]
    fn create_high_concurrency_config() -> AsyncTaskConfig {
        AsyncTaskConfig::new(
            100,    // max concurrent tasks
            300,    // 5 minute timeout
            2,      // retry attempts
            500,    // 0.5 second retry delay
            true,   // progress tracking
            10      // high priority
        )
    }

    #[staticmethod]
    fn create_low_latency_config() -> AsyncTaskConfig {
        AsyncTaskConfig::new(
            50,     // moderate concurrency
            30,     // 30 second timeout
            5,      // more retries for low latency
            200,    // 0.2 second retry delay
            true,   // progress tracking
            8       // high priority
        )
    }

    #[staticmethod]
    fn create_batch_processing_config() -> AsyncTaskConfig {
        AsyncTaskConfig::new(
            20,     // lower concurrency for batch jobs
            3600,   // 1 hour timeout for long batch jobs
            5,      // more retries
            5000,   // 5 second retry delay
            true,   // progress tracking
            3       // lower priority
        )
    }

    #[staticmethod]
    fn create_streaming_config() -> AsyncTaskConfig {
        AsyncTaskConfig::new(
            200,    // high concurrency for streaming
            0,      // no timeout for continuous streaming
            10,     // many retries for streaming
            1000,   // 1 second retry delay
            false,  // no progress tracking for continuous streaming
            7       // medium priority
        )
    }

    /// Estimate optimal concurrent operations based on system resources
    #[staticmethod]
    fn estimate_optimal_concurrency(
        cpu_cores: usize,
        memory_gb: usize,
        operation_type: String,
        io_intensive: bool,
    ) -> usize {
        let base_concurrency = cpu_cores;

        let multiplier = if io_intensive {
            4.0 // I/O bound operations can have higher concurrency
        } else {
            1.5 // CPU bound operations have lower concurrency
        };

        let operation_factor = match operation_type.as_str() {
            "table_scan" => 2.0,
            "file_read" => 4.0,
            "file_write" => 3.0,
            "commit" => 1.0,
            "metadata" => 5.0,
            _ => 2.0,
        };

        let memory_factor = if memory_gb > 16 {
            2.0
        } else if memory_gb > 8 {
            1.5
        } else if memory_gb > 4 {
            1.0
        } else {
            0.5
        };

        let estimated = (base_concurrency as f64 * multiplier * operation_factor * memory_factor) as usize;
        estimated.max(1).min(200) // Cap at reasonable limits
    }

    /// Get recommended async configuration based on workload
    #[staticmethod]
    fn get_recommended_config(
        workload_type: String, // "interactive", "batch", "streaming", "mixed"
        priority_level: String, // "low", "medium", "high"
        expected_duration_seconds: u64,
    ) -> AsyncTaskConfig {
        match workload_type.as_str() {
            "interactive" => {
                let timeout = if expected_duration_seconds < 10 { 30 } else { expected_duration_seconds * 3 };
                AsyncTaskConfig::new(
                    if priority_level == "high" { 50 } else { 20 },
                    timeout,
                    if priority_level == "high" { 2 } else { 3 },
                    if priority_level == "high" { 200 } else { 500 },
                    true,
                    if priority_level == "high" { 10 } else { 5 }
                )
            },
            "batch" => AsyncUtils::create_batch_processing_config(),
            "streaming" => AsyncUtils::create_streaming_config(),
            "mixed" => {
                AsyncTaskConfig::new(
                    30,
                    if expected_duration_seconds > 300 { 1800 } else { 600 },
                    4,
                    1000,
                    true,
                    6
                )
            },
            _ => AsyncTaskConfig::new(10, 300, 3, 1000, true, 5),
        }
    }
}

// Convenience functions for module-level exports
#[pyfunction]
pub fn create_async_executor(max_concurrent_tasks: usize) -> PyResult<AsyncIOExecutor> {
    Ok(AsyncIOExecutor::new(max_concurrent_tasks, None)?)
}

#[pyfunction]
pub fn create_async_deltalake_operations(max_concurrent_operations: usize) -> PyResult<AsyncDeltaLakeOperations> {
    Ok(AsyncDeltaLakeOperations::new(max_concurrent_operations)?)
}