"""Command-line interface for DeltaLakeDB."""

import argparse
import sys
from pathlib import Path


def import_command(args: argparse.Namespace) -> None:
    """Import Delta log from an existing table into SQL metadata.

    Usage:
        python -m delkalakedb import /path/to/table \\
            --dsn postgresql://user:pass@localhost/mydb \\
            --schema public \\
            --table my_table
    """
    # TODO: Wire up to the actual Rust CLI implementation
    # For now, this is a stub that will be connected during Phase 5

    try:
        from delkalakedb_cli import import_table
    except ImportError:
        print(
            "Error: CLI bindings not available. "
            "Please ensure you built with: pip install -e .",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        result = import_table(
            table_path=str(args.table_path),
            dsn=args.dsn,
            schema=args.schema,
            table=args.table,
        )
        print(
            f"✓ Import successful: {result.commits} commits, final version {result.current_version}"
        )
    except Exception as e:
        print(f"✗ Import failed: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DeltaLakeDB command-line tools",
        prog="python -m delkalakedb",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # 'import' subcommand
    import_parser = subparsers.add_parser(
        "import",
        help="Import existing Delta table into SQL metadata",
    )
    import_parser.add_argument(
        "table_path",
        type=Path,
        help="Path to Delta table (directory containing _delta_log)",
    )
    import_parser.add_argument(
        "--dsn",
        required=True,
        help="Database connection string (e.g., postgresql://user:pass@localhost/db)",
    )
    import_parser.add_argument(
        "--schema",
        default="public",
        help="Database schema (default: public)",
    )
    import_parser.add_argument(
        "--table",
        help="Table name in SQL (default: inferred from path)",
    )
    import_parser.set_defaults(func=import_command)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
