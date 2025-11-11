"""Command-line interface for deltalakedb migration tools."""

import click
import asyncio
import sys
from pathlib import Path
from typing import Optional

from .migration import MigrationManager, MigrationConfig
from .validation import ValidationManager
from .discovery import TableDiscovery


@click.group()
@click.version_option()
def main():
    """DeltaLakeDB migration tools for SQL-backed metadata."""
    pass


@main.command()
@click.argument("table_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--table-id",
    help="Custom table ID (defaults to directory name)",
)
@click.option(
    "--batch-size",
    default=1000,
    type=int,
    help="Batch size for SQL inserts (default: 1000)",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume a previously interrupted migration",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Perform a dry run without making changes",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def import_table(
    table_path: Path,
    database_url: str,
    table_id: Optional[str],
    batch_size: int,
    resume: bool,
    dry_run: bool,
    verbose: bool,
):
    """Import an existing Delta table to SQL-backed metadata."""

    config = MigrationConfig(
        database_url=database_url,
        table_path=table_path,
        table_id=table_id or table_path.name,
        batch_size=batch_size,
        resume=resume,
        dry_run=dry_run,
        verbose=verbose,
    )

    manager = MigrationManager(config)

    try:
        result = asyncio.run(manager.migrate())
        if result.success:
            click.echo(f"✅ Migration completed successfully!")
            click.echo(f"   Table ID: {result.table_id}")
            click.echo(f"   Version: {result.version}")
            click.echo(f"   Files processed: {result.files_processed}")
            click.echo(f"   Duration: {result.duration:.2f}s")
        else:
            click.echo(f"❌ Migration failed: {result.error}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Migration error: {e}")
        sys.exit(1)


@main.command()
@click.argument("table_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--table-id",
    help="Custom table ID (defaults to directory name)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def validate(
    table_path: Path,
    database_url: str,
    table_id: Optional[str],
    verbose: bool,
):
    """Validate a migrated table against file-based metadata."""

    table_id = table_id or table_path.name
    validator = ValidationManager(database_url, verbose=verbose)

    try:
        result = asyncio.run(validator.validate_table(table_path, table_id))
        if result.is_valid:
            click.echo(f"✅ Table {table_id} is valid")
        else:
            click.echo(f"❌ Table {table_id} has validation issues:")
            for issue in result.issues:
                click.echo(f"   - {issue}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Validation error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--table-id",
    help="Show status for specific table ID",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def migration_status(
    database_url: str,
    table_id: Optional[str],
    verbose: bool,
):
    """Show migration status for tables."""

    manager = MigrationManager(
        MigrationConfig(
            database_url=database_url,
            table_path=Path("."),  # Dummy path for status queries
            verbose=verbose,
        )
    )

    try:
        if table_id:
            status = asyncio.run(manager.get_table_status(table_id))
            if status:
                _print_table_status(status)
            else:
                click.echo(f"No migration status found for table: {table_id}")
        else:
            statuses = asyncio.run(manager.get_all_migration_status())
            if statuses:
                for status in statuses:
                    _print_table_status(status)
                    click.echo()
            else:
                click.echo("No migration status found")
    except Exception as e:
        click.echo(f"❌ Error getting migration status: {e}")
        sys.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--recursive",
    is_flag=True,
    help="Search recursively for Delta tables",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def discover(
    path: Path,
    recursive: bool,
    verbose: bool,
):
    """Discover Delta tables in the given path."""

    discovery = TableDiscovery(verbose=verbose)

    try:
        tables = discovery.discover_tables(path, recursive)
        if tables:
            click.echo(f"Found {len(tables)} Delta table(s):")
            for table in tables:
                click.echo(
                    f"  {table.path} (ID: {table.table_id}, Version: {table.version})"
                )
        else:
            click.echo("No Delta tables found")
    except Exception as e:
        click.echo(f"❌ Error discovering tables: {e}")
        sys.exit(1)


@main.command()
@click.argument("table_id")
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def verify_rollback(
    table_id: str,
    database_url: str,
    verbose: bool,
):
    """Verify rollback capabilities for a migrated table."""

    validator = ValidationManager(database_url, verbose=verbose)

    try:
        result = asyncio.run(validator.verify_rollback(table_id))
        if result.can_rollback:
            click.echo(f"✅ Table {table_id} can be rolled back")
            click.echo(f"   Method: {result.rollback_method}")
            click.echo(f"   Estimated time: {result.estimated_time:.1f}s")
            if result.warnings:
                click.echo("   Warnings:")
                for warning in result.warnings:
                    click.echo(f"     - {warning}")
        else:
            click.echo(f"❌ Table {table_id} cannot be safely rolled back")
            click.echo("   Steps:")
            for step in result.steps:
                click.echo(f"     {step}")
            if result.warnings:
                click.echo("   Warnings:")
                for warning in result.warnings:
                    click.echo(f"     - {warning}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Rollback verification error: {e}")
        sys.exit(1)


@main.command()
@click.argument("table_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--table-id",
    help="Custom table ID (defaults to directory name)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def performance_compare(
    table_path: Path,
    database_url: str,
    table_id: Optional[str],
    verbose: bool,
):
    """Compare performance between file-based and SQL-based metadata access."""

    table_id = table_id or table_path.name
    validator = ValidationManager(database_url, verbose=verbose)

    try:
        result = asyncio.run(validator.compare_performance(table_path, table_id))

        if "error" in result:
            click.echo(
                f"❌ Performance comparison failed: {result['error']['message']}"
            )
            sys.exit(1)

        click.echo(f"Performance comparison for table {table_id}:")
        click.echo()
        click.echo("File-based access:")
        click.echo(f"  Read time: {result['file_based']['read_time']:.3f}s")
        click.echo(f"  Files: {result['file_based']['file_count']}")
        click.echo()
        click.echo("SQL-based access:")
        click.echo(f"  Read time: {result['sql_based']['read_time']:.3f}s")
        click.echo(f"  Files: {result['sql_based']['file_count']}")
        click.echo()
        click.echo(f"Speedup: {result['comparison']['speedup']:.2f}x")
        click.echo(f"Recommendation: {result['comparison']['recommendation']}")

    except Exception as e:
        click.echo(f"❌ Performance comparison error: {e}")
        sys.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--database-url",
    required=True,
    help="Database connection URL (e.g., postgresql://user:pass@host/db)",
)
@click.option(
    "--batch-size",
    default=1000,
    type=int,
    help="Batch size for SQL inserts (default: 1000)",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume previously interrupted migrations",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Perform a dry run without making changes",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
@click.option(
    "--parallel",
    default=1,
    type=int,
    help="Number of parallel migrations (default: 1)",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Exclude table IDs matching these patterns",
)
def batch_import(
    path: Path,
    database_url: str,
    batch_size: int,
    resume: bool,
    dry_run: bool,
    verbose: bool,
    parallel: int,
    exclude: tuple,
):
    """Import multiple Delta tables in batch."""

    discovery = TableDiscovery(verbose=verbose)

    try:
        # Discover all tables
        tables = discovery.discover_tables(path, recursive=True)

        # Filter excluded tables
        if exclude:
            filtered_tables = []
            for table in tables:
                should_exclude = False
                for pattern in exclude:
                    if pattern in table.table_id:
                        should_exclude = True
                        break
                if not should_exclude:
                    filtered_tables.append(table)
            tables = filtered_tables

        if not tables:
            click.echo("No Delta tables found")
            return

        click.echo(f"Found {len(tables)} tables to migrate:")
        for table in tables:
            click.echo(
                f"  {table.path} (ID: {table.table_id}, Version: {table.version})"
            )

        if not dry_run:
            click.echo()
            if not click.confirm("Proceed with batch migration?"):
                click.echo("Batch migration cancelled")
                return

        # Migrate tables
        successful = []
        failed = []

        for i, table in enumerate(tables, 1):
            click.echo(f"\n[{i}/{len(tables)}] Migrating {table.table_id}...")

            config = MigrationConfig(
                database_url=database_url,
                table_path=table.path,
                table_id=table.table_id,
                batch_size=batch_size,
                resume=resume,
                dry_run=dry_run,
                verbose=verbose,
            )

            manager = MigrationManager(config)

            try:
                result = asyncio.run(manager.migrate())
                if result.success:
                    successful.append(table.table_id)
                    click.echo(f"✅ {table.table_id} migrated successfully")
                else:
                    failed.append((table.table_id, result.error))
                    click.echo(f"❌ {table.table_id} failed: {result.error}")
            except Exception as e:
                failed.append((table.table_id, str(e)))
                click.echo(f"❌ {table.table_id} error: {e}")

        # Summary
        click.echo(f"\nBatch migration completed:")
        click.echo(f"  ✅ Successful: {len(successful)}")
        click.echo(f"  ❌ Failed: {len(failed)}")

        if failed:
            click.echo("\nFailed tables:")
            for table_id, error in failed:
                click.echo(f"  - {table_id}: {error}")

    except Exception as e:
        click.echo(f"❌ Batch migration error: {e}")
        sys.exit(1)


def _print_table_status(status):
    """Print formatted table status."""
    click.echo(f"Table: {status.table_id}")
    click.echo(f"  Status: {status.status}")
    click.echo(f"  Version: {status.version}")
    click.echo(f"  Files: {status.files_processed}")
    if status.started_at:
        click.echo(f"  Started: {status.started_at}")
    if status.completed_at:
        click.echo(f"  Completed: {status.completed_at}")
    if status.error_message:
        click.echo(f"  Error: {status.error_message}")


if __name__ == "__main__":
    main()
