//! Incremental import support for resuming and tracking progress.

use crate::error::{MigrationError, MigrationResult};
use serde_json::json;
use sqlx::postgres::PgPool;
use uuid::Uuid;

/// Metadata for tracking incremental import progress.
#[derive(Debug, Clone)]
pub struct ImportMetadata {
    /// Last successfully imported version
    pub last_imported_version: i64,
    /// Timestamp of last import
    pub import_timestamp: String,
    /// Source table URI
    pub source_uri: String,
    /// Whether import is complete
    pub import_complete: bool,
}

impl ImportMetadata {
    /// Create new import metadata.
    pub fn new(source_uri: String) -> Self {
        ImportMetadata {
            last_imported_version: -1,
            import_timestamp: chrono::Utc::now().to_rfc3339(),
            source_uri,
            import_complete: false,
        }
    }

    /// Convert to JSON for storage in database.
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "last_imported_version": self.last_imported_version,
            "import_timestamp": self.import_timestamp,
            "source_uri": self.source_uri,
            "import_complete": self.import_complete
        })
    }
}

/// Manages incremental import state and tracking.
pub struct IncrementalImporter {
    table_id: Uuid,
}

impl IncrementalImporter {
    /// Create a new incremental importer.
    pub fn new(table_id: Uuid) -> Self {
        IncrementalImporter { table_id }
    }

    /// Get the last imported version for a table.
    ///
    /// Returns None if table has never been imported.
    pub async fn get_last_imported_version(
        &self,
        _pool: &PgPool,
    ) -> MigrationResult<Option<i64>> {
        // In real implementation, would query dl_tables.properties
        // For now, returning None to indicate no previous import
        Ok(None)
    }

    /// Record successful completion of a version import.
    ///
    /// Updates the last_imported_version in database properties.
    pub async fn record_version_imported(
        &self,
        _pool: &PgPool,
        version: i64,
    ) -> MigrationResult<()> {
        // In real implementation, would update dl_tables.properties
        Ok(())
    }

    /// Mark import as complete.
    pub async fn mark_import_complete(&self, _pool: &PgPool) -> MigrationResult<()> {
        // In real implementation, would update dl_tables.properties
        Ok(())
    }

    /// Get resume start version (last_imported_version + 1).
    pub async fn get_resume_start_version(
        &self,
        pool: &PgPool,
    ) -> MigrationResult<i64> {
        match self.get_last_imported_version(pool).await? {
            Some(version) => Ok(version + 1),
            None => Ok(0),
        }
    }

    /// Check if import was previously interrupted.
    pub async fn check_interrupted(
        &self,
        pool: &PgPool,
    ) -> MigrationResult<bool> {
        match self.get_last_imported_version(pool).await? {
            Some(version) => Ok(version >= 0),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_metadata_creation() {
        let meta = ImportMetadata::new("s3://bucket/table".to_string());
        assert_eq!(meta.source_uri, "s3://bucket/table");
        assert_eq!(meta.last_imported_version, -1);
        assert!(!meta.import_complete);
    }

    #[test]
    fn test_import_metadata_to_json() {
        let meta = ImportMetadata::new("s3://bucket/table".to_string());
        let json = meta.to_json();

        assert_eq!(json["source_uri"], "s3://bucket/table");
        assert_eq!(json["last_imported_version"], -1);
        assert_eq!(json["import_complete"], false);
    }

    #[test]
    fn test_incremental_importer_creation() {
        let importer = IncrementalImporter::new(Uuid::new_v4());
        // Just verify it doesn't panic
        assert!(true);
    }
}
