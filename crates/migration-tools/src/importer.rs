//! Core migration importer logic.

use crate::error::{MigrationError, MigrationResult};
use crate::reader::DeltaLogReader;
use deltalakedb_core::types::Action;
use deltalakedb_sql_metadata_postgres::PostgresWriter;
use object_store::ObjectStore;
use sqlx::postgres::PgPool;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

/// Configuration for table import.
#[derive(Clone, Debug)]
pub struct ImportConfig {
    /// Table ID in SQL database
    pub table_id: Uuid,
    /// Source table location (s3://bucket/path, file:///path, etc.)
    pub source_location: String,
    /// Optional: import only up to this version
    pub up_to_version: Option<i64>,
    /// Skip validation after import
    pub skip_validation: bool,
    /// Dry run mode
    pub dry_run: bool,
}

/// Statistics about the import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    /// Number of versions imported
    pub versions_imported: i64,
    /// Number of add actions imported
    pub add_actions: i64,
    /// Number of remove actions imported
    pub remove_actions: i64,
    /// Number of metadata updates
    pub metadata_updates: i64,
    /// Number of protocol updates
    pub protocol_updates: i64,
    /// Number of transaction actions
    pub txn_actions: i64,
}

impl ImportStats {
    /// Get total actions imported.
    pub fn total_actions(&self) -> i64 {
        self.add_actions + self.remove_actions + self.metadata_updates + self.protocol_updates
            + self.txn_actions
    }

    /// Get human-readable summary.
    pub fn summary(&self) -> String {
        format!(
            "Imported {} versions with {} total actions",
            self.versions_imported, self.total_actions()
        )
    }
}

/// Imports a Delta table into SQL-backed metadata.
pub struct TableImporter {
    reader: DeltaLogReader,
    config: ImportConfig,
    stats: ImportStats,
}

impl TableImporter {
    /// Create a new table importer.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        config: ImportConfig,
    ) -> MigrationResult<Self> {
        let reader = DeltaLogReader::new(object_store, config.source_location.clone());

        Ok(TableImporter {
            reader,
            config,
            stats: ImportStats::default(),
        })
    }

    /// Perform the import operation.
    pub async fn import(&mut self, pool: &PgPool) -> MigrationResult<ImportStats> {
        info!("Starting import for table: {}", self.config.table_id);

        if self.config.dry_run {
            info!("DRY RUN MODE");
        }

        // Find the range of versions to import
        let latest_version = self.reader.find_latest_version().await?;
        let checkpoint_version = self.reader.find_latest_checkpoint().await?;

        info!(
            "Table has {} versions, checkpoint at: {:?}",
            latest_version, checkpoint_version
        );

        let start_version = checkpoint_version.unwrap_or(0);
        let end_version = self.config.up_to_version.unwrap_or(latest_version);

        if end_version < start_version {
            return Err(MigrationError::ConfigError(
                "up_to_version must be >= checkpoint version".to_string(),
            ));
        }

        info!("Importing versions {} to {}", start_version, end_version);

        // Import each version
        for version in start_version..=end_version {
            self.import_version(pool, version).await?;
            self.stats.versions_imported += 1;
        }

        info!("Import complete: {}", self.stats.summary());
        Ok(self.stats.clone())
    }

    /// Import a single version.
    async fn import_version(&mut self, pool: &PgPool, version: i64) -> MigrationResult<()> {
        debug!("Importing version {}", version);

        // Read actions for this version
        let actions = self.reader.read_json_commit(version).await?;

        if actions.is_empty() {
            debug!("No actions in version {}", version);
            return Ok(());
        }

        // Count action types
        for action in &actions {
            match action {
                Action::Add(_) => self.stats.add_actions += 1,
                Action::Remove(_) => self.stats.remove_actions += 1,
                Action::Metadata(_) => self.stats.metadata_updates += 1,
                Action::Protocol(_) => self.stats.protocol_updates += 1,
                Action::Txn(_) => self.stats.txn_actions += 1,
            }
        }

        // Skip database writes in dry-run mode
        if !self.config.dry_run {
            let writer = PostgresWriter::new(pool.clone());

            let handle = writer
                .begin_commit(self.config.table_id, version)
                .await
                .map_err(|e| MigrationError::DatabaseError(format!("Failed to begin commit: {}", e)))?;

            writer
                .write_actions(self.config.table_id, &handle, &actions)
                .await
                .map_err(|e| {
                    MigrationError::DatabaseError(format!("Failed to write actions: {}", e))
                })?;

            writer
                .finalize_commit(self.config.table_id, handle, version)
                .await
                .map_err(|e| {
                    MigrationError::DatabaseError(format!("Failed to finalize commit: {}", e))
                })?;
        }

        info!("Imported version {} ({} actions)", version, actions.len());
        Ok(())
    }

    /// Get current import statistics.
    pub fn stats(&self) -> &ImportStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_stats_summary() {
        let mut stats = ImportStats::default();
        stats.versions_imported = 10;
        stats.add_actions = 100;

        let summary = stats.summary();
        assert!(summary.contains("10 versions"));
    }
}
