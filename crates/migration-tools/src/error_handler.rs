//! Error handling and recovery for migration operations.

use crate::error::{MigrationError, MigrationResult};
use tracing::{error, warn};

/// Error recovery strategy.
#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    /// Fail immediately
    FailFast,
    /// Try to continue (may produce partial results)
    ContinueOnError,
    /// Attempt rollback and fail
    Rollback,
}

/// Error context with recovery options.
#[derive(Debug)]
pub struct ErrorContext {
    /// The error that occurred
    pub error: MigrationError,
    /// Version being processed when error occurred
    pub version: i64,
    /// Suggested recovery strategy
    pub recovery: RecoveryStrategy,
    /// Is this error recoverable?
    pub recoverable: bool,
}

impl ErrorContext {
    /// Create new error context.
    pub fn new(error: MigrationError, version: i64) -> Self {
        let (recovery, recoverable) = match &error {
            MigrationError::DeltaLogError(msg) => {
                if msg.contains("not found") || msg.contains("missing") {
                    (RecoveryStrategy::ContinueOnError, false)
                } else {
                    (RecoveryStrategy::FailFast, true)
                }
            }
            MigrationError::ParseError(_) => (RecoveryStrategy::FailFast, false),
            MigrationError::DatabaseError(_) => (RecoveryStrategy::Rollback, true),
            MigrationError::ValidationError(_) => (RecoveryStrategy::ContinueOnError, false),
            _ => (RecoveryStrategy::FailFast, false),
        };

        ErrorContext {
            error,
            version,
            recovery,
            recoverable,
        }
    }

    /// Get human-readable error message with recovery suggestions.
    pub fn detailed_message(&self) -> String {
        let recovery_msg = match self.recovery {
            RecoveryStrategy::FailFast => "This error cannot be automatically recovered. Please fix the issue and retry.",
            RecoveryStrategy::ContinueOnError => "Skipping this version and continuing with the next. Your import may be incomplete.",
            RecoveryStrategy::Rollback => "Attempting to rollback changes from this version.",
        };

        format!(
            "Error at version {}: {}\n\nRecovery: {}\nRecoverable: {}",
            self.version,
            self.error,
            recovery_msg,
            if self.recoverable { "Yes" } else { "No" }
        )
    }
}

/// Error recovery handler.
pub struct ErrorRecovery;

impl ErrorRecovery {
    /// Handle a migration error with context.
    pub fn handle(context: ErrorContext) -> MigrationResult<()> {
        match context.recovery {
            RecoveryStrategy::FailFast => {
                error!("Fatal error: {}", context.detailed_message());
                Err(context.error)
            }
            RecoveryStrategy::ContinueOnError => {
                warn!("Recoverable error: {}", context.detailed_message());
                Ok(())
            }
            RecoveryStrategy::Rollback => {
                error!("Database error, attempting rollback: {}", context.detailed_message());
                Err(context.error)
            }
        }
    }

    /// Check if an error indicates a corrupt checkpoint.
    pub fn is_corrupt_checkpoint(error: &MigrationError) -> bool {
        match error {
            MigrationError::ParseError(msg) => msg.contains("checkpoint"),
            MigrationError::DeltaLogError(msg) => msg.contains("checkpoint") && msg.contains("parse"),
            _ => false,
        }
    }

    /// Check if an error indicates a missing file.
    pub fn is_missing_file(error: &MigrationError) -> bool {
        match error {
            MigrationError::DeltaLogError(msg) => msg.contains("not found") || msg.contains("missing"),
            _ => false,
        }
    }

    /// Get recovery steps for an error type.
    pub fn recovery_steps(error: &MigrationError) -> Vec<String> {
        match error {
            MigrationError::DeltaLogError(msg) => {
                if msg.contains("not found") {
                    vec![
                        "1. Check that the source table path is correct".to_string(),
                        "2. Verify object store credentials and permissions".to_string(),
                        "3. Ensure the table exists at the specified location".to_string(),
                    ]
                } else {
                    vec!["1. Check object store connectivity".to_string()]
                }
            }
            MigrationError::DatabaseError(_) => {
                vec![
                    "1. Check database connection string".to_string(),
                    "2. Verify database is running and accessible".to_string(),
                    "3. Check database user permissions".to_string(),
                    "4. Ensure sufficient disk space".to_string(),
                ]
            }
            MigrationError::ParseError(msg) => {
                if msg.contains("UTF-8") {
                    vec!["1. Source file contains invalid UTF-8 encoding".to_string()]
                } else {
                    vec!["1. Source file is corrupted or malformed".to_string()]
                }
            }
            _ => vec!["1. Check logs for more details".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_context_delta_log_not_found() {
        let error = MigrationError::DeltaLogError("file not found".to_string());
        let ctx = ErrorContext::new(error, 100);

        assert!(!ctx.recoverable);
        matches!(ctx.recovery, RecoveryStrategy::ContinueOnError);
    }

    #[test]
    fn test_error_context_database_error() {
        let error = MigrationError::DatabaseError("connection failed".to_string());
        let ctx = ErrorContext::new(error, 100);

        assert!(ctx.recoverable);
        matches!(ctx.recovery, RecoveryStrategy::Rollback);
    }

    #[test]
    fn test_is_corrupt_checkpoint() {
        let error = MigrationError::ParseError("Failed to parse checkpoint".to_string());
        assert!(ErrorRecovery::is_corrupt_checkpoint(&error));
    }

    #[test]
    fn test_is_missing_file() {
        let error = MigrationError::DeltaLogError("file not found".to_string());
        assert!(ErrorRecovery::is_missing_file(&error));
    }

    #[test]
    fn test_recovery_steps_database_error() {
        let error = MigrationError::DatabaseError("connection failed".to_string());
        let steps = ErrorRecovery::recovery_steps(&error);
        assert!(steps.len() > 0);
        assert!(steps[0].contains("connection"));
    }
}
