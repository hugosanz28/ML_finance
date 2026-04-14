"""Generate the monthly portfolio report in Markdown."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.reports import generate_monthly_report, get_latest_monthly_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        help="Reference date for the report in YYYY-MM-DD format. Defaults to the latest valuation date.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Override the output directory. Defaults to REPORTS_DIR.",
    )
    parser.add_argument(
        "--normalized-degiro-dir",
        type=Path,
        help="Override the normalized DEGIRO parquet directory.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print the Markdown content to stdout after generating the file.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Show the latest persisted monthly report metadata and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.latest:
        latest = get_latest_monthly_report()
        if latest is None:
            print("No monthly report metadata found.")
            return 1
        print(f"Latest monthly report: {latest.report_id}")
        print(f"Generated at: {latest.generated_at}")
        print(f"As of date: {latest.as_of_date}")
        print(f"Path: {latest.report_path}")
        return 0

    result = generate_monthly_report(
        as_of_date=args.as_of_date,
        output_dir=args.output_dir,
        normalized_degiro_dir=args.normalized_degiro_dir,
        persist=True,
    )

    print(f"Monthly report generated for {result.as_of_date.isoformat()}.")
    if result.output_path is not None:
        print(f"Output: {result.output_path}")
    if args.stdout:
        print("\n---\n")
        print(result.content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
