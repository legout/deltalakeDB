"""Command line tooling for OpenSpec migration workflows."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import uuid

from .migration import (
    DeltaLogReader,
    DriftThresholds,
    ImportConfig,
    MigrationDatabase,
    MigrationError,
    build_log_snapshot,
    diff_snapshots,
)


LOG_FORMAT = "[%(levelname)s] %(message)s"


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    try:
        if args.command == "import":
            return _cmd_import(args)
        if args.command == "diff":
            return _cmd_diff(args)
    except MigrationError as exc:
        logging.error("%s", exc)
        return 1

    parser.print_help()
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dl", description="Delta SQL migration tooling")
    sub = parser.add_subparsers(dest="command", required=True)

    import_parser = sub.add_parser("import", help="Bootstrap SQL tables from a Delta log")
    import_parser.add_argument("--database", required=True, help="Path to SQLite database")
    import_parser.add_argument("--log-dir", required=True, help="Path to _delta_log directory")
    import_parser.add_argument("--table-id", help="Existing table UUID; generated when omitted")
    import_parser.add_argument(
        "--table-name",
        help="Friendly name for the table (defaults to last path component)",
    )
    import_parser.add_argument(
        "--table-location",
        required=True,
        help="Physical storage location for the Delta table",
    )
    import_parser.add_argument(
        "--mode",
        choices=["fast", "full-history"],
        default="fast",
        help="Fast uses checkpoint snapshot; full-history replays all JSON",
    )
    import_parser.add_argument(
        "--json-output",
        help="Write import summary JSON to the provided path",
    )

    diff_parser = sub.add_parser(
        "diff", help="Compare SQL-derived snapshot with _delta_log parity"
    )
    diff_parser.add_argument("--database", required=True, help="Path to SQLite database")
    diff_parser.add_argument("--table-id", required=True, help="Table UUID to validate")
    diff_parser.add_argument("--log-dir", required=True, help="Path to _delta_log directory")
    diff_parser.add_argument(
        "--format",
        choices=["human", "json"],
        default="human",
        help="Output format for the drift summary",
    )
    diff_parser.add_argument(
        "--json-output",
        help="Optional file path to write the drift report JSON",
    )
    diff_parser.add_argument(
        "--lag-threshold-seconds",
        type=float,
        default=5.0,
        help="Maximum acceptable mirror lag before alerts fire",
    )
    diff_parser.add_argument(
        "--max-drift-files",
        type=int,
        default=0,
        help="Allowed number of file differences before failing the run",
    )
    diff_parser.add_argument(
        "--metrics-format",
        choices=["none", "json", "prometheus"],
        default="none",
        help="Emit metrics for dashboards/alerts",
    )
    diff_parser.add_argument(
        "--metrics-output",
        help="Optional path for metrics output (stdout when omitted)",
    )

    return parser


def _default_table_name(location: str) -> str:
    stripped = location.rstrip("/")
    if not stripped:
        return "delta_table"
    if "/" in stripped:
        return stripped.rsplit("/", 1)[-1] or stripped
    return stripped


def _cmd_import(args: argparse.Namespace) -> int:
    db_path = Path(args.database)
    log_dir = Path(args.log_dir)
    table_id = args.table_id or str(uuid.uuid4())
    table_name = args.table_name or _default_table_name(args.table_location)

    db = MigrationDatabase(db_path)
    try:
        reader = DeltaLogReader(log_dir)
        result = db.import_table(
            ImportConfig(
                database_path=db_path,
                log_dir=log_dir,
                table_id=table_id,
                table_name=table_name,
                table_location=args.table_location,
                mode=args.mode,
            ),
            reader,
        )
    finally:
        db.close()

    logging.info(
        "Imported table %s (%s) – %s in %.2fs",
        table_name,
        table_id,
        result.summary,
        result.duration_seconds,
    )
    print(result.summary)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(result.__dict__, indent=2), encoding="utf-8")
    if not args.table_id:
        print(f"Generated table_id={table_id}")
    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    db_path = Path(args.database)
    log_dir = Path(args.log_dir)

    db = MigrationDatabase(db_path)
    db.ensure_schema()
    db_snapshot = db.snapshot_from_db(args.table_id)
    if db_snapshot.version is None:
        db.close()
        raise MigrationError(
            "No imported versions found. Run 'dl import' before diffing parity."
        )

    reader = DeltaLogReader(log_dir)
    log_snapshot = build_log_snapshot(reader)
    db.close()

    thresholds = DriftThresholds(
        max_lag_seconds=args.lag_threshold_seconds,
        max_file_drift=args.max_drift_files,
    )
    report = diff_snapshots(db_snapshot, log_snapshot, thresholds)

    if args.format == "json":
        output = json.dumps(report.to_dict(), indent=2)
    else:
        output = _format_human_report(report, args.table_id)
    print(output)

    if args.json_output:
        Path(args.json_output).write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")

    _emit_metrics(report, args.table_id, args.metrics_format, args.metrics_output)

    if report.status == "drift":
        return 2
    if report.status == "lagging":
        return 3
    return 0


def _format_human_report(report, table_id: str) -> str:
    lines = [
        f"Table {table_id}",
        f"Status: {report.status}",
        f"File drift (db<-log): {len(report.file_missing_in_db)}",
        f"File drift (db->log): {len(report.file_missing_in_log)}",
        f"Metadata matches: {report.metadata_matches}",
        f"Protocol matches: {report.protocol_matches}",
        f"Version delta: {report.version_delta}",
        f"Mirror lag seconds: {report.lag_seconds}",
    ]

    if report.file_missing_in_db:
        preview = ", ".join(report.file_missing_in_db[:5])
        lines.append(f"Files missing in DB: {preview}{'…' if len(report.file_missing_in_db) > 5 else ''}")
    if report.file_missing_in_log:
        preview = ", ".join(report.file_missing_in_log[:5])
        lines.append(
            f"Files missing in log: {preview}{'…' if len(report.file_missing_in_log) > 5 else ''}"
        )
    return "\n".join(lines)


def _emit_metrics(
    report,
    table_id: str,
    metrics_format: str,
    metrics_output: str | None,
) -> None:
    if metrics_format == "none":
        return
    metrics = report.metrics()
    if metrics_format == "json":
        payload = json.dumps({"table_id": table_id, **metrics}, indent=2)
    else:  # prometheus
        payload = "\n".join(
            [
                f"dl_{name}{{table_id=\"{table_id}\"}} {value}"
                for name, value in metrics.items()
            ]
        )
    if metrics_output:
        Path(metrics_output).write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
