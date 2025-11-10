//! Parquet checkpoint generation for Delta Lake.
//!
//! Generates Parquet checkpoints following Delta protocol specification.
//! Checkpoints are generated at configurable intervals (default: every 10 commits)
//! to allow readers to avoid replaying the entire transaction log.

use crate::error::{MirrorError, MirrorResult};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use deltalakedb_core::types::Snapshot;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use tracing::debug;

/// Generates a Parquet checkpoint file for a Delta Lake snapshot.
///
/// Checkpoints encode all active files at a version in Parquet format,
/// allowing external readers to start from a checkpoint rather than
/// replaying the entire transaction log from version 0.
///
/// # Arguments
///
/// * `snapshot` - The snapshot to checkpoint
/// * `output_path` - Path to write the checkpoint file
///
/// # Example
///
/// ```ignore
/// let checkpoint_data = generate_checkpoint(&snapshot, "/tmp/checkpoint.parquet")?;
/// ```
pub struct CheckpointWriter;

impl CheckpointWriter {
    /// Generate a Parquet checkpoint from a snapshot.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - Complete table snapshot at a version
    /// * `output_path` - Path where checkpoint file should be written
    ///
    /// # Returns
    ///
    /// Ok(bytes_written) on success, Err on failure
    pub fn write_checkpoint<P: AsRef<Path>>(
        snapshot: &Snapshot,
        output_path: P,
    ) -> MirrorResult<u64> {
        let path = output_path.as_ref();
        debug!("Generating checkpoint for version {} to {:?}", snapshot.version, path);

        // Create record batch with add files from snapshot
        let batch = Self::create_add_files_batch(&snapshot)?;

        // Write to Parquet file with Snappy compression
        let file = File::create(path).map_err(|e| {
            MirrorError::CheckpointError(format!("Failed to create checkpoint file: {}", e))
        })?;

        let writer = BufWriter::new(file);
        let mut arrow_writer = ArrowWriter::try_new(writer, batch.schema(), None)
            .map_err(|e| {
                MirrorError::CheckpointError(format!("Failed to create Parquet writer: {}", e))
            })?;

        arrow_writer.write(&batch).map_err(|e| {
            MirrorError::CheckpointError(format!("Failed to write checkpoint batch: {}", e))
        })?;

        arrow_writer.close().map_err(|e| {
            MirrorError::CheckpointError(format!("Failed to close Parquet writer: {}", e))
        })?;

        // Get file size
        let metadata = std::fs::metadata(path).map_err(|e| {
            MirrorError::CheckpointError(format!("Failed to get checkpoint file size: {}", e))
        })?;

        debug!(
            "Checkpoint written: {} bytes for version {}",
            metadata.len(),
            snapshot.version
        );

        Ok(metadata.len())
    }

    /// Create a RecordBatch with add file actions from snapshot.
    ///
    /// Follows Delta checkpoint schema:
    /// - add: struct with path, size, modificationTime, dataChangeVersion, stats, etc.
    fn create_add_files_batch(snapshot: &Snapshot) -> MirrorResult<RecordBatch> {
        let num_files = snapshot.files.len();

        // Create arrays for each field
        let paths: Vec<&str> = snapshot.files.iter().map(|f| f.path.as_str()).collect();
        let sizes: Vec<i64> = snapshot.files.iter().map(|f| f.size).collect();
        let mod_times: Vec<i64> = snapshot.files.iter().map(|f| f.modification_time).collect();
        let versions: Vec<i64> = snapshot.files.iter().map(|f| f.data_change_version).collect();

        // Build struct arrays for each add file
        let add_fields = vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(sizes)) as ArrayRef,
            Arc::new(Int64Array::from(mod_times)) as ArrayRef,
            Arc::new(Int64Array::from(versions)) as ArrayRef,
        ];

        // Define schema for add struct
        let add_schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modificationTime", DataType::Int64, false),
            Field::new("dataChangeVersion", DataType::Int64, false),
        ]));

        // Create struct array
        let add_struct = Arc::new(
            StructArray::try_new(
                add_schema.clone(),
                add_fields,
                None,
            )
            .map_err(|e| {
                MirrorError::CheckpointError(format!("Failed to create add struct: {}", e))
            })?,
        );

        // Create top-level schema with add column
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", add_schema.data_type(), false),
        ]));

        // Create record batch
        RecordBatch::try_new(
            schema,
            vec![add_struct],
        )
        .map_err(|e| {
            MirrorError::CheckpointError(format!("Failed to create record batch: {}", e))
        })
    }

    /// Get the checkpoint file path for a given version.
    ///
    /// # Arguments
    ///
    /// * `table_location` - Base table location
    /// * `version` - Version number
    ///
    /// # Returns
    ///
    /// Path to checkpoint file: `{table_location}/_delta_log/{version:020}.checkpoint.parquet`
    pub fn checkpoint_path(table_location: &str, version: i64) -> String {
        format!(
            "{}/_delta_log/{:020}.checkpoint.parquet",
            table_location, version
        )
    }

    /// Determine if checkpoint should be generated at this version.
    ///
    /// # Arguments
    ///
    /// * `version` - Version number
    /// * `interval` - Checkpoint interval (generate at 0 and multiples)
    ///
    /// # Returns
    ///
    /// true if checkpoint should be generated
    pub fn should_checkpoint(version: i64, interval: u64) -> bool {
        version == 0 || version % interval as i64 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_core::types::AddFile;

    #[test]
    fn test_checkpoint_path() {
        let path = CheckpointWriter::checkpoint_path("s3://bucket/table", 10);
        assert_eq!(path, "s3://bucket/table/_delta_log/00000000000000000010.checkpoint.parquet");
    }

    #[test]
    fn test_checkpoint_path_large_version() {
        let path = CheckpointWriter::checkpoint_path("s3://bucket/table", 1234567890);
        assert_eq!(
            path,
            "s3://bucket/table/_delta_log/00001234567890.checkpoint.parquet"
        );
    }

    #[test]
    fn test_should_checkpoint() {
        // Version 0 always gets checkpoint
        assert!(CheckpointWriter::should_checkpoint(0, 10));

        // Multiples of interval
        assert!(CheckpointWriter::should_checkpoint(10, 10));
        assert!(CheckpointWriter::should_checkpoint(20, 10));
        assert!(CheckpointWriter::should_checkpoint(100, 10));

        // Non-multiples don't get checkpoint
        assert!(!CheckpointWriter::should_checkpoint(1, 10));
        assert!(!CheckpointWriter::should_checkpoint(9, 10));
        assert!(!CheckpointWriter::should_checkpoint(11, 10));
        assert!(!CheckpointWriter::should_checkpoint(19, 10));
    }

    #[test]
    fn test_should_checkpoint_custom_interval() {
        assert!(CheckpointWriter::should_checkpoint(0, 5));
        assert!(CheckpointWriter::should_checkpoint(5, 5));
        assert!(CheckpointWriter::should_checkpoint(10, 5));

        assert!(!CheckpointWriter::should_checkpoint(1, 5));
        assert!(!CheckpointWriter::should_checkpoint(6, 5));
    }

    #[test]
    fn test_create_add_files_batch() {
        let file1 = AddFile {
            path: "s3://bucket/file1.parquet".to_string(),
            size: 1024,
            modification_time: 1704067200000,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let file2 = AddFile {
            path: "s3://bucket/file2.parquet".to_string(),
            size: 2048,
            modification_time: 1704067200000,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let snapshot = Snapshot {
            version: 1,
            timestamp: 1704067200000,
            files: vec![file1, file2],
            metadata: Default::default(),
            protocol: Default::default(),
        };

        let batch = CheckpointWriter::create_add_files_batch(&snapshot);
        assert!(batch.is_ok());

        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_create_empty_batch() {
        let snapshot = Snapshot {
            version: 0,
            timestamp: 1704067200000,
            files: vec![],
            metadata: Default::default(),
            protocol: Default::default(),
        };

        let batch = CheckpointWriter::create_add_files_batch(&snapshot);
        assert!(batch.is_ok());

        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
