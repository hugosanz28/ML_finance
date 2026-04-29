"""Import canonical DEGIRO CSV exports into normalized parquet datasets."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import get_settings
from src.degiro_exports.importer import import_degiro_exports
from src.degiro_exports.warehouse import load_normalized_degiro_to_duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--incoming-dir",
        type=Path,
        help="Directory with canonical DEGIRO CSV exports. Defaults to DEGIRO_EXPORTS_DIR/incoming.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for normalized parquet datasets. Defaults to DATA_DIR/normalized/degiro.",
    )
    parser.add_argument("--base-currency", help="Override base currency for parsed exports.")
    parser.add_argument("--account-id", help="Optional account identifier stored in normalized rows.")
    parser.add_argument(
        "--source-root",
        type=Path,
        help="Root used to store relative source paths. Defaults to DEGIRO_EXPORTS_DIR.",
    )
    parser.add_argument(
        "--ignore-unknown",
        action="store_true",
        help="Skip CSV files that do not match a supported canonical DEGIRO filename.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be imported without parsing or writing parquet outputs.",
    )
    parser.add_argument(
        "--skip-duckdb-load",
        action="store_true",
        help="Do not load normalized parquet datasets into the local DuckDB warehouse after import.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    summary = import_degiro_exports(
        settings=settings,
        incoming_dir=args.incoming_dir,
        output_dir=args.output_dir,
        base_currency=args.base_currency,
        account_id=args.account_id,
        source_root=args.source_root,
        ignore_unknown=args.ignore_unknown,
        dry_run=args.dry_run,
    )

    print(f"Incoming: {summary.incoming_dir}")
    print(f"Output: {summary.output_dir}")
    print(
        "Summary: "
        f"imported={summary.imported_count}, "
        f"would_import={summary.would_import_count}, "
        f"skipped={summary.skipped_count}, "
        f"failed={summary.failed_count}"
    )
    if not summary.outcomes:
        print("No CSV files found.")
        return 1

    for outcome in summary.outcomes:
        print(f"- {outcome.source_path.name}: {outcome.status} ({outcome.kind})")
        if outcome.message:
            print(f"  {outcome.message}")
        for output_path in outcome.output_paths:
            print(f"  -> {output_path}")

    if not args.dry_run and not args.skip_duckdb_load and not summary.failed_count:
        warehouse_summary = load_normalized_degiro_to_duckdb(
            settings=settings,
            normalized_degiro_dir=summary.output_dir,
        )
        print(
            "DuckDB load: "
            f"assets={warehouse_summary.assets}, "
            f"transactions={warehouse_summary.transactions}, "
            f"cash_movements={warehouse_summary.cash_movements}, "
            f"portfolio_snapshots={warehouse_summary.portfolio_snapshots}"
        )

    return 1 if summary.failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
