//! Validation logic for migration operations.

use crate::error::{MigrationError, MigrationResult};
use deltalakedb_core::types::Snapshot;
use tracing::{debug, info, warn};

/// Detailed validation results.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether validation passed
    pub passed: bool,
    /// File count matches
    pub file_count_match: bool,
    /// Total file size matches
    pub file_size_match: bool,
    /// Schema matches
    pub schema_match: bool,
    /// Protocol versions match
    pub protocol_match: bool,
    /// Detailed errors if any
    pub errors: Vec<String>,
}

impl ValidationResult {
    /// Create a new validation result.
    pub fn new() -> Self {
        ValidationResult {
            passed: true,
            file_count_match: true,
            file_size_match: true,
            schema_match: true,
            protocol_match: true,
            errors: Vec::new(),
        }
    }

    /// Add an error message.
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.passed = false;
    }

    /// Get human-readable summary.
    pub fn summary(&self) -> String {
        if self.passed {
            "Validation PASSED ✓".to_string()
        } else {
            format!("Validation FAILED ✗ - {} errors", self.errors.len())
        }
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Validates imported Delta table state.
pub struct MigrationValidator;

impl MigrationValidator {
    /// Compare two snapshots and report discrepancies.
    pub fn compare_snapshots(
        sql_snapshot: &Snapshot,
        file_snapshot: &Snapshot,
    ) -> MigrationResult<ValidationResult> {
        let mut result = ValidationResult::new();

        info!(
            "Comparing snapshots - SQL version: {}, File version: {}",
            sql_snapshot.version, file_snapshot.version
        );

        // Check versions match
        if sql_snapshot.version != file_snapshot.version {
            result.add_error(format!(
                "Version mismatch: SQL={}, File={}",
                sql_snapshot.version, file_snapshot.version
            ));
        }

        // Check file counts match
        let sql_file_count = sql_snapshot.files.len();
        let file_file_count = file_snapshot.files.len();
        if sql_file_count != file_file_count {
            result.file_count_match = false;
            result.add_error(format!(
                "File count mismatch: SQL={}, File={}",
                sql_file_count, file_file_count
            ));
        }

        // Check total file size
        let sql_total_size: i64 = sql_snapshot.files.iter().map(|f| f.size).sum();
        let file_total_size: i64 = file_snapshot.files.iter().map(|f| f.size).sum();
        if sql_total_size != file_total_size {
            result.file_size_match = false;
            result.add_error(format!(
                "Total file size mismatch: SQL={} bytes, File={} bytes",
                sql_total_size, file_total_size
            ));
        }

        // Check schema match
        if sql_snapshot.metadata.schema != file_snapshot.metadata.schema {
            result.schema_match = false;
            result.add_error("Schema mismatch between SQL and file-based".to_string());
        }

        // Check protocol match
        if sql_snapshot.protocol.min_reader_version != file_snapshot.protocol.min_reader_version
        {
            result.protocol_match = false;
            result.add_error("Protocol minReaderVersion mismatch".to_string());
        }

        if sql_snapshot.protocol.min_writer_version != file_snapshot.protocol.min_writer_version
        {
            result.protocol_match = false;
            result.add_error("Protocol minWriterVersion mismatch".to_string());
        }

        debug!("Validation result: passed={}", result.passed);
        Ok(result)
    }

    /// Validate file paths are present in both snapshots.
    pub fn validate_file_paths(
        sql_snapshot: &Snapshot,
        file_snapshot: &Snapshot,
    ) -> MigrationResult<ValidationResult> {
        let mut result = ValidationResult::new();

        // Create sets of file paths for comparison
        let sql_paths: std::collections::HashSet<_> =
            sql_snapshot.files.iter().map(|f| &f.path).collect();
        let file_paths: std::collections::HashSet<_> =
            file_snapshot.files.iter().map(|f| &f.path).collect();

        // Find files in SQL but not in file-based
        for path in &sql_paths {
            if !file_paths.contains(path) {
                result.add_error(format!("File only in SQL: {}", path));
            }
        }

        // Find files in file-based but not in SQL
        for path in &file_paths {
            if !sql_paths.contains(path) {
                result.add_error(format!("File only in file-based: {}", path));
            }
        }

        if !result.passed {
            warn!("File path validation found {} discrepancies", result.errors.len());
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_summary_passed() {
        let result = ValidationResult::new();
        assert!(result.summary().contains("PASSED"));
    }

    #[test]
    fn test_validation_result_summary_failed() {
        let mut result = ValidationResult::new();
        result.add_error("Test error".to_string());
        assert!(result.summary().contains("FAILED"));
    }

    #[test]
    fn test_validation_result_passed_flag() {
        let mut result = ValidationResult::new();
        assert!(result.passed);

        result.add_error("Error".to_string());
        assert!(!result.passed);
    }
}
