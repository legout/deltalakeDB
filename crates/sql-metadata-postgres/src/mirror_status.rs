//! Mirror status tracking for recording mirror progress and failures.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use uuid::Uuid;

/// Mirror status for a specific version.
#[derive(Debug, Clone, PartialEq)]
pub enum MirrorStatus {
    /// Mirror operation is in progress
    Pending,
    /// Mirror completed successfully
    Success,
    /// Mirror failed and needs retry
    Failed,
}

impl MirrorStatus {
    /// Convert to string for database storage.
    pub fn as_str(&self) -> &str {
        match self {
            MirrorStatus::Pending => "PENDING",
            MirrorStatus::Success => "SUCCESS",
            MirrorStatus::Failed => "FAILED",
        }
    }

    /// Parse from database string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "PENDING" => Some(MirrorStatus::Pending),
            "SUCCESS" => Some(MirrorStatus::Success),
            "FAILED" => Some(MirrorStatus::Failed),
            _ => None,
        }
    }
}

/// Mirror status record for a version.
#[derive(Debug, Clone)]
pub struct MirrorStatusRecord {
    /// Table identifier
    pub table_id: Uuid,
    /// Version number
    pub version: i64,
    /// Current status
    pub status: MirrorStatus,
    /// Whether JSON commit was written
    pub json_written: bool,
    /// Whether checkpoint was written
    pub checkpoint_written: bool,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Number of retry attempts
    pub retry_count: i32,
    /// Last attempt timestamp
    pub last_attempt_at: Option<DateTime<Utc>>,
}

/// Mirror status tracker for recording and querying mirror progress.
pub struct MirrorStatusTracker {
    pool: PgPool,
}

impl MirrorStatusTracker {
    /// Create a new mirror status tracker.
    pub fn new(pool: PgPool) -> Self {
        MirrorStatusTracker { pool }
    }

    /// Record that mirroring started for a version.
    pub async fn mark_pending(&self, table_id: Uuid, version: i64) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO dl_mirror_status (table_id, version, status)
             VALUES ($1, $2, $3)
             ON CONFLICT (table_id, version) DO UPDATE
             SET status = $3, updated_at = NOW()"
        )
        .bind(table_id)
        .bind(version)
        .bind(MirrorStatus::Pending.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to mark mirror pending: {}", e))?;

        Ok(())
    }

    /// Record successful mirror completion.
    pub async fn mark_success(
        &self,
        table_id: Uuid,
        version: i64,
        json_written: bool,
        checkpoint_written: bool,
    ) -> Result<(), String> {
        sqlx::query(
            "UPDATE dl_mirror_status
             SET status = $3, json_written = $4, checkpoint_written = $5, 
                 updated_at = NOW()
             WHERE table_id = $1 AND version = $2"
        )
        .bind(table_id)
        .bind(version)
        .bind(MirrorStatus::Success.as_str())
        .bind(json_written)
        .bind(checkpoint_written)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to mark mirror success: {}", e))?;

        Ok(())
    }

    /// Record mirror failure with error details.
    pub async fn mark_failed(
        &self,
        table_id: Uuid,
        version: i64,
        error_message: &str,
    ) -> Result<(), String> {
        sqlx::query(
            "UPDATE dl_mirror_status
             SET status = $3, error_message = $4, retry_count = retry_count + 1,
                 last_attempt_at = NOW(), updated_at = NOW()
             WHERE table_id = $1 AND version = $2"
        )
        .bind(table_id)
        .bind(version)
        .bind(MirrorStatus::Failed.as_str())
        .bind(error_message)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to mark mirror failed: {}", e))?;

        Ok(())
    }

    /// Get mirror status for a version.
    pub async fn get_status(&self, table_id: Uuid, version: i64) -> Result<Option<MirrorStatusRecord>, String> {
        let row = sqlx::query(
            "SELECT status, json_written, checkpoint_written, error_message, 
                    retry_count, last_attempt_at
             FROM dl_mirror_status
             WHERE table_id = $1 AND version = $2"
        )
        .bind(table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("Failed to get mirror status: {}", e))?;

        Ok(row.map(|r| {
            let status_str: String = r.get("status");
            MirrorStatusRecord {
                table_id,
                version,
                status: MirrorStatus::from_str(&status_str).unwrap_or(MirrorStatus::Failed),
                json_written: r.get("json_written"),
                checkpoint_written: r.get("checkpoint_written"),
                error_message: r.get("error_message"),
                retry_count: r.get("retry_count"),
                last_attempt_at: r.get("last_attempt_at"),
            }
        }))
    }

    /// Find all failed mirrors for a table.
    pub async fn find_failed(&self, table_id: Uuid) -> Result<Vec<i64>, String> {
        let rows = sqlx::query("SELECT version FROM dl_mirror_status WHERE table_id = $1 AND status = 'FAILED' ORDER BY version")
            .bind(table_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("Failed to find failed mirrors: {}", e))?;

        Ok(rows.iter().map(|r| r.get::<i64, _>("version")).collect())
    }

    /// Calculate mirror lag (versions not yet successfully mirrored).
    pub async fn get_mirror_lag(&self, table_id: Uuid, latest_version: i64) -> Result<i64, String> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(version), -1) as max_mirrored
             FROM dl_mirror_status
             WHERE table_id = $1 AND status = 'SUCCESS'"
        )
        .bind(table_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("Failed to calculate lag: {}", e))?;

        let max_mirrored: i64 = row.get("max_mirrored");
        Ok((latest_version - max_mirrored).max(0))
    }

    /// Find versions ready for retry (not recently attempted).
    pub async fn find_ready_for_retry(
        &self,
        table_id: Uuid,
        max_attempts: i32,
        min_backoff_secs: i64,
    ) -> Result<Vec<(i64, i32)>, String> {
        let rows = sqlx::query(
            "SELECT version, retry_count
             FROM dl_mirror_status
             WHERE table_id = $1 
             AND status = 'FAILED'
             AND retry_count < $2
             AND (last_attempt_at IS NULL OR last_attempt_at < NOW() - INTERVAL '1 second' * $3)
             ORDER BY retry_count, last_attempt_at"
        )
        .bind(table_id)
        .bind(max_attempts)
        .bind(min_backoff_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to find retry-ready mirrors: {}", e))?;

        Ok(rows
            .iter()
            .map(|r| (r.get::<i64, _>("version"), r.get::<i32, _>("retry_count")))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_status_as_str() {
        assert_eq!(MirrorStatus::Pending.as_str(), "PENDING");
        assert_eq!(MirrorStatus::Success.as_str(), "SUCCESS");
        assert_eq!(MirrorStatus::Failed.as_str(), "FAILED");
    }

    #[test]
    fn test_mirror_status_from_str() {
        assert_eq!(MirrorStatus::from_str("PENDING"), Some(MirrorStatus::Pending));
        assert_eq!(MirrorStatus::from_str("SUCCESS"), Some(MirrorStatus::Success));
        assert_eq!(MirrorStatus::from_str("FAILED"), Some(MirrorStatus::Failed));
        assert_eq!(MirrorStatus::from_str("UNKNOWN"), None);
    }
}
