//! Asynchronous mirroring pipeline for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::generators::{DeltaFile, DeltaJsonGenerator};
use crate::storage::MirrorStorage;
use crate::config::{MirrorEngineConfig, PerformanceConfig};
use crate::engine::DeltaMirrorEngine;
use crate::recovery::{Reconciler, RecoveryConfig};
use crate::{MirrorStatus, TaskStatus};
use deltalakedb_core::{Table, Commit, Action};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::hash::{Hash, Hasher};

pub mod task;

pub use task::{MirrorTask, TaskStatus};

/// Asynchronous mirroring pipeline for processing Delta Lake log mirroring tasks
pub struct MirrorPipeline {
    config: PerformanceConfig,
    storage: Arc<dyn MirrorStorage>,
    engine: Arc<DeltaMirrorEngine>,
    task_queue: Arc<RwLock<TaskQueue>>,
    reconciler: Option<Arc<Reconciler>>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<RwLock<mpsc::Receiver<()>>>,
}

/// Task queue for mirroring operations
struct TaskQueue {
    pending: VecDeque<MirrorTask>,
    processing: HashMap<Uuid, TaskStatus>,
    completed: HashMap<Uuid, MirrorStatus>,
    max_size: usize,
}

impl MirrorPipeline {
    /// Create a new mirroring pipeline
    pub async fn new(
        config: MirrorEngineConfig,
        engine: Arc<DeltaMirrorEngine>,
        storage: Arc<dyn MirrorStorage>,
    ) -> MirrorResult<Self> {
        let performance = config.performance.clone();
        let max_workers = performance.max_concurrent_tasks;
        let queue_size = performance.task_queue_size;

        let (task_tx, mut task_rx) = mpsc::channel(queue_size);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Initialize task queue
        let task_queue = Arc::new(RwLock::new(TaskQueue {
            pending: VecDeque::new(),
            processing: HashMap::new(),
            completed: HashMap::new(),
            max_size: queue_size,
        }));

        // Start task processor
        let processor_queue = task_queue.clone();
        let processor_engine = engine.clone();
        let processor_storage = storage.clone();
        let processor_reconciler = None; // Will be set later after reconciler is created
        let mut processor_shutdown_rx = shutdown_rx.clone();

        tokio::spawn(async move {
            let mut shutdown = false;

            while !shutdown {
                tokio::select! {
                    task = task_rx.recv() => {
                        match task {
                            Some(task) => {
                                if let Err(e) = Self::process_task(
                                    &processor_engine,
                                    &processor_storage,
                                    &processor_queue,
                                    &processor_reconciler,
                                    task
                                ).await {
                                    tracing::error!("Error processing task: {}", e);
                                }
                            }
                            None => {
                                tracing::info!("Task channel closed, shutting down processor");
                                break;
                            }
                        }
                    }
                    _ = processor_shutdown_rx.recv() => {
                        tracing::info!("Shutdown signal received");
                        shutdown = true;
                    }
                }
            }
        });

        // Initialize reconciler if recovery is enabled
        let reconciler = if config.recovery.enabled {
            let reconciler = Reconciler::new(
                config.recovery.reconciler.clone(),
                engine.clone(),
                storage.clone(),
            ).await?;

            // Start reconciler background task
            reconciler.start().await?;
            Some(Arc::new(reconciler))
        } else {
            None
        };

        Ok(Self {
            config: performance,
            storage,
            engine,
            task_queue,
            reconciler,
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(shutdown_rx)),
        })
    }

    /// Submit a task to the pipeline
    pub async fn submit_task(&self, task: MirrorTask) -> MirrorResult<MirrorStatus> {
        let task_id = task.task_id();

        // Check queue capacity
        {
            let queue = self.task_queue.read().await;
            if queue.pending.len() >= queue.max_size {
                return Err(MirrorError::validation_error("Task queue is full"));
            }
        }

        // Add task to queue
        {
            let mut queue = self.task_queue.write().await;
            queue.pending.push_back(task.clone());

            // Create initial status
            let status = MirrorStatus {
                task_id,
                table_id: task.table_id(),
                status: TaskStatus::Queued,
                progress: 0.0,
                started_at: Utc::now(),
                estimated_completion: None,
                error_message: None,
                files_processed: 0,
                total_files: 1,
                bytes_written: 0,
            };
            queue.processing.insert(task_id, TaskStatus::Queued);
        }

        // Wait for task completion
        self.wait_for_task(task_id).await
    }

    /// Wait for a specific task to complete
    async fn wait_for_task(&self, task_id: Uuid) -> MirrorResult<MirrorStatus> {
        let mut interval = Duration::from_millis(100);
        let mut timeout = Duration::from_secs(self.config.operation_timeout_secs);

        loop {
            {
                let queue = self.task_queue.read().await;
                if let Some(status) = queue.completed.get(&task_id) {
                    return Ok(status.clone());
                }

                // Check if task is still being processed
                if !queue.processing.contains_key(&task_id) {
                    return Err(MirrorError::validation_error("Task not found"));
                }
            }

            if timeout.is_zero() {
                return Err(MirrorError::validation_error("Task timeout"));
            }

            sleep(interval).await;
            timeout = timeout.saturating_sub(interval);
            interval = std::cmp::min(interval * 2, Duration::from_secs(1));
        }
    }

    /// Process a single task
    async fn process_task(
        engine: &DeltaMirrorEngine,
        storage: &Arc<dyn MirrorStorage>,
        queue: &Arc<RwLock<TaskQueue>>,
        reconciler: &Option<Arc<Reconciler>>,
        task: MirrorTask,
    ) -> MirrorResult<()> {
        let task_id = task.task_id();
        let start_time = Instant::now();

        // Update status to running
        {
            let mut q = queue.write().await;
            q.processing.insert(task_id, TaskStatus::Running);
        }

        tracing::debug!("Processing task: {:?}", task);

        let result = match task {
            MirrorTask::CommitMirroring { table_id, version } => {
                Self::process_commit_mirroring(engine, storage, table_id, version).await
            }
            MirrorTask::TableMirroring { table_id } => {
                Self::process_table_mirroring(engine, storage, table_id).await
            }
            MirrorTask::CheckpointGeneration { table_id, version } => {
                Self::process_checkpoint_generation(engine, storage, table_id, version).await
            }
            MirrorTask::BatchMirroring { tasks } => {
                Self::process_batch_mirroring(engine, storage, tasks).await
            }
        };

        let duration = start_time.elapsed();

        // Update final status and handle failed tasks
        let status = match result {
            Ok(bytes_written) => MirrorStatus {
                task_id,
                table_id: task.table_id(),
                status: TaskStatus::Completed,
                progress: 1.0,
                started_at: Utc::now(),
                estimated_completion: None,
                error_message: None,
                files_processed: 1,
                total_files: 1,
                bytes_written,
            },
            Err(error) => {
                // Add failed task to reconciler if available
                if let Some(reconciler) = reconciler {
                    if let Err(reconciler_err) = reconciler.add_failed_task(task.clone(), error.to_string()).await {
                        tracing::error!("Failed to add task to reconciler: {}", reconciler_err);
                    }
                }

                MirrorStatus {
                    task_id,
                    table_id: task.table_id(),
                    status: TaskStatus::Failed,
                    progress: 0.0,
                    started_at: Utc::now(),
                    estimated_completion: None,
                    error_message: Some(error.to_string()),
                    files_processed: 0,
                    total_files: 1,
                    bytes_written: 0,
                }
            },
        };

        // Move task from processing to completed
        {
            let mut q = queue.write().await;
            q.processing.remove(&task_id);
            q.completed.insert(task_id, status);
        }

        tracing::debug!("Task {} completed in {:?}", task_id, duration);
        Ok(())
    }

    /// Process commit mirroring task
    async fn process_commit_mirroring(
        engine: &DeltaMirrorEngine,
        _storage: &Arc<dyn MirrorStorage>,
        table_id: Uuid,
        version: i64,
    ) -> MirrorResult<u64> {
        // This would integrate with the SQL adapter to fetch commit data
        // For now, create a placeholder commit and actions
        let commit = Commit {
            id: Uuid::new_v4(),
            table_id,
            version,
            timestamp: Utc::now(),
            operation_type: "WRITE".to_string(),
            operation_parameters: serde_json::json!({"mode": "Append"}),
            commit_info: serde_json::json!({}),
        };

        let action = Action::AddFile(deltalakedb_core::AddFile {
            path: format!("part-{:05}-{}.parquet", version, Uuid::new_v4()),
            size: 1024,
            modification_time: Utc::now().timestamp_millis(),
            data_change: true,
            stats: None,
            partition_values: None,
            tags: None,
        });

        let table_path = format!("/tables/{}", table_id);
        let actions = &[action];

        let delta_file = engine.mirror_commit(&table_path, version, &commit, actions).await?;
        Ok(delta_file.size)
    }

    /// Process table mirroring task
    async fn process_table_mirroring(
        _engine: &DeltaMirrorEngine,
        _storage: &Arc<dyn MirrorStorage>,
        _table_id: Uuid,
    ) -> MirrorResult<u64> {
        // This would mirror all commits for a table
        // Placeholder implementation
        sleep(Duration::from_millis(100)).await;
        Ok(1024)
    }

    /// Process checkpoint generation task
    async fn process_checkpoint_generation(
        _engine: &DeltaMirrorEngine,
        _storage: &Arc<dyn MirrorStorage>,
        _table_id: Uuid,
        _version: Option<i64>,
    ) -> MirrorResult<u64> {
        // This would generate a checkpoint file
        // Placeholder implementation
        sleep(Duration::from_millis(200)).await;
        Ok(2048)
    }

    /// Process batch mirroring task
    async fn process_batch_mirroring(
        engine: &DeltaMirrorEngine,
        storage: &Arc<dyn MirrorStorage>,
        tasks: Vec<MirrorTask>,
    ) -> MirrorResult<u64> {
        let mut total_bytes = 0;

        for task in tasks {
            let result = match task {
                MirrorTask::CommitMirroring { table_id, version } => {
                    Self::process_commit_mirroring(engine, storage, table_id, version).await
                }
                _ => Ok(0),
            };
            total_bytes += result.unwrap_or(0);
        }

        Ok(total_bytes)
    }

    /// Start the pipeline
    pub async fn start(&self) -> MirrorResult<()> {
        tracing::info!("Starting mirroring pipeline");
        Ok(())
    }

    /// Shutdown the pipeline gracefully
    pub async fn shutdown(&self) -> MirrorResult<()> {
        tracing::info!("Shutting down mirroring pipeline...");

        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        // Shutdown reconciler if enabled
        if let Some(reconciler) = &self.reconciler {
            reconciler.shutdown().await?;
        }

        // Wait for tasks to complete
        sleep(Duration::from_secs(5)).await;

        tracing::info!("Mirroring pipeline shutdown complete");
        Ok(())
    }

    /// Get pipeline statistics
    pub async fn get_stats(&self) -> PipelineStats {
        let queue = self.task_queue.read().await;
        let processing_count = queue.processing.len();
        let completed_count = queue.completed.len();

        PipelineStats {
            pending_tasks: queue.pending.len(),
            processing_tasks: processing_count,
            completed_tasks: completed_count,
            max_queue_size: queue.max_size,
            worker_count: self.config.max_concurrent_tasks,
            is_shutting_down: self.shutdown_rx.read().await.try_recv().is_ok(),
        }
    }

    /// Get recovery statistics
    pub async fn get_recovery_stats(&self) -> Option<crate::recovery::RecoveryStats> {
        if let Some(reconciler) = &self.reconciler {
            Some(reconciler.get_stats().await)
        } else {
            None
        }
    }

    /// Get tasks by status
    pub async fn get_tasks_by_status(&self, status: TaskStatus) -> Vec<MirrorStatus> {
        let queue = self.task_queue.read().await;
        queue.completed
            .values()
            .filter(|s| s.status == status)
            .cloned()
            .collect()
    }

    /// Cancel a task
    pub async fn cancel_task(&self, task_id: Uuid) -> MirrorResult<bool> {
        let mut queue = self.task_queue.write().await;

        // Check if task is in pending queue
        if let Some(pos) = queue.pending.iter().position(|t| t.task_id() == task_id) {
            queue.pending.remove(pos);

            // Mark as cancelled
            let cancelled_status = MirrorStatus {
                task_id,
                table_id: Uuid::new_v4(),
                status: TaskStatus::Cancelled,
                progress: 1.0,
                started_at: Utc::now(),
                estimated_completion: None,
                error_message: Some("Task cancelled".to_string()),
                files_processed: 0,
                total_files: 0,
                bytes_written: 0,
            };

            queue.completed.insert(task_id, cancelled_status);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Pipeline statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    /// Number of pending tasks in queue
    pub pending_tasks: usize,
    /// Number of tasks currently being processed
    pub processing_tasks: usize,
    /// Number of completed tasks
    pub completed_tasks: usize,
    /// Maximum queue size
    pub max_queue_size: usize,
    /// Number of worker threads
    pub worker_count: usize,
    /// Whether the pipeline is shutting down
    pub is_shutting_down: bool,
}

/// Mirroring task definition
#[derive(Debug, Clone)]
pub enum MirrorTask {
    /// Mirror a specific commit
    CommitMirroring {
        table_id: Uuid,
        version: i64,
    },
    /// Mirror all commits for a table
    TableMirroring {
        table_id: Uuid,
    },
    /// Generate checkpoint for a table
    CheckpointGeneration {
        table_id: Uuid,
        version: Option<i64>,
    },
    /// Batch mirroring operations
    BatchMirroring {
        tasks: Vec<MirrorTask>,
    },
}

impl MirrorTask {
    /// Get the unique task ID
    pub fn task_id(&self) -> Uuid {
        let mut hasher = DefaultHasher::new();

        match self {
            MirrorTask::CommitMirroring { table_id, version } => {
                table_id.hash(&mut hasher);
                version.hash(&mut hasher);
                0u8.hash(&mut hasher);
            }
            MirrorTask::TableMirroring { table_id } => {
                table_id.hash(&mut hasher);
                1u8.hash(&mut hasher);
            }
            MirrorTask::CheckpointGeneration { table_id, version } => {
                table_id.hash(&mut hasher);
                version.hash(&mut hasher);
                2u8.hash(&mut hasher);
            }
            MirrorTask::BatchMirroring { tasks } => {
                for task in tasks {
                    task.task_id().hash(&mut hasher);
                }
                3u8.hash(&mut hasher);
            }
        }

        let hash = hasher.finish();
        Uuid::from_u64_pair(hash, 0)
    }

    /// Get the table ID associated with this task
    pub fn table_id(&self) -> Uuid {
        match self {
            MirrorTask::CommitMirroring { table_id, .. } => *table_id,
            MirrorTask::TableMirroring { table_id } => *table_id,
            MirrorTask::CheckpointGeneration { table_id, .. } => *table_id,
            MirrorTask::BatchMirroring { tasks } => {
                tasks.first().map(|t| t.table_id()).unwrap_or_else(Uuid::new_v4)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MirrorEngineConfig;

    #[test]
    fn test_mirror_task_task_id_deterministic() {
        let table_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let task1 = MirrorTask::CommitMirroring {
            table_id,
            version: 1,
        };
        let task2 = MirrorTask::CommitMirroring {
            table_id,
            version: 1,
        };

        // Same tasks should have same IDs
        assert_eq!(task1.task_id(), task2.task_id());

        // Different versions should have different IDs
        let task3 = MirrorTask::CommitMirroring {
            table_id,
            version: 2,
        };
        assert_ne!(task1.task_id(), task3.task_id());
    }

    #[test]
    fn test_pipeline_stats() {
        let stats = PipelineStats {
            pending_tasks: 5,
            processing_tasks: 3,
            completed_tasks: 10,
            max_queue_size: 100,
            worker_count: 4,
            is_shutting_down: false,
        };

        assert_eq!(stats.pending_tasks, 5);
        assert_eq!(stats.processing_tasks, 3);
        assert_eq!(stats.completed_tasks, 10);
        assert_eq!(stats.worker_count, 4);
    }

    #[tokio::test]
    async fn test_task_queue() {
        let queue = TaskQueue {
            pending: VecDeque::new(),
            processing: HashMap::new(),
            completed: HashMap::new(),
            max_size: 10,
        };

        assert_eq!(queue.pending.len(), 0);
        assert_eq!(queue.processing.len(), 0);
        assert_eq!(queue.completed.len(), 0);
    }
}