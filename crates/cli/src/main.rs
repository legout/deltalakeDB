use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use deltalakedb_cli::{run_import, ImportConfig};

#[derive(Parser)]
#[command(author, version, about = "deltalakedb command line utilities")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import an existing _delta_log into SQL metadata tables.
    Import(ImportArgs),
}

#[derive(Args)]
struct ImportArgs {
    /// Path to the Delta table root directory.
    table_path: PathBuf,
    /// Database connection string (sqlite://… or postgres://…)
    #[arg(long)]
    dsn: String,
    /// Optional schema name (defaults to public for Postgres)
    #[arg(long)]
    schema: Option<String>,
    /// Optional logical table name (defaults to table directory name)
    #[arg(long)]
    table: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Import(args) => {
            let config = ImportConfig {
                table_path: args.table_path,
                dsn: args.dsn,
                schema: args.schema,
                table: args.table,
            };
            let summary = run_import(config).await?;
            println!(
                "Imported {} commits for table_id={} (current_version={})",
                summary.commits, summary.table_id, summary.current_version
            );
        }
    }
    Ok(())
}
