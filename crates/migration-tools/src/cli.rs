//! CLI interface for migration tools.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Delta Lake migration tools.
#[derive(Parser, Debug)]
#[command(name = "deltasql")]
#[command(about = "Delta Lake SQL migration tools", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// Available commands.
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Import existing Delta table into SQL-backed metadata
    Import(ImportArgs),
}

/// Arguments for the import command.
#[derive(Parser, Debug)]
pub struct ImportArgs {
    /// Source table location (s3://bucket/path, file:///path, etc.)
    #[arg(value_name = "SOURCE")]
    pub source: String,

    /// Target database URI (deltasql://postgres/dbname/schema/table)
    #[arg(value_name = "TARGET")]
    pub target: String,

    /// Preview import without modifying database
    #[arg(long)]
    pub dry_run: bool,

    /// Import only up to this version
    #[arg(long)]
    pub up_to_version: Option<i64>,

    /// Overwrite existing table in database
    #[arg(long)]
    pub force: bool,

    /// Skip validation after import
    #[arg(long)]
    pub skip_validation: bool,

    /// Resume from last imported version
    #[arg(long)]
    pub resume: bool,

    /// Import only the latest checkpoint (skip history)
    #[arg(long)]
    pub checkpoint_only: bool,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,
}

impl ImportArgs {
    /// Validate arguments and return error if invalid.
    pub fn validate(&self) -> Result<(), String> {
        // Cannot combine --checkpoint-only with --up-to-version
        if self.checkpoint_only && self.up_to_version.is_some() {
            return Err("Cannot use --checkpoint-only with --up-to-version".to_string());
        }

        // Cannot combine --resume with --dry-run when modifying
        if self.resume && self.dry_run {
            return Err("--resume and --dry-run can be combined but won't modify anything".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_args_validation_checkpoint_with_version() {
        let args = ImportArgs {
            source: "s3://bucket/table".to_string(),
            target: "deltasql://postgres/db/public/table".to_string(),
            dry_run: false,
            up_to_version: Some(100),
            force: false,
            skip_validation: false,
            resume: false,
            checkpoint_only: true,
            verbose: false,
            format: "text".to_string(),
        };

        assert!(args.validate().is_err());
    }

    #[test]
    fn test_import_args_validation_valid() {
        let args = ImportArgs {
            source: "s3://bucket/table".to_string(),
            target: "deltasql://postgres/db/public/table".to_string(),
            dry_run: false,
            up_to_version: None,
            force: false,
            skip_validation: false,
            resume: false,
            checkpoint_only: false,
            verbose: false,
            format: "text".to_string(),
        };

        assert!(args.validate().is_ok());
    }
}
