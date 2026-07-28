"""Refresh daily market prices for assets sourced from normalized DEGIRO data."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.application.market_data import RefreshMarketDataRequest, RefreshMarketDataUseCase


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, help="Refresh start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", type=date.fromisoformat, help="Refresh end date in YYYY-MM-DD format.")
    parser.add_argument("--asset-id", action="append", dest="asset_ids", help="Refresh only the selected asset_id.")
    parser.add_argument("--provider", help="Override the configured price provider for this run.")
    parser.add_argument(
        "--no-bootstrap-degiro",
        action="store_true",
        help="Skip syncing assets from src/data/local/normalized/degiro before refreshing prices.",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive assets in the refresh selection.",
    )
    parser.add_argument(
        "--no-write-overrides-template",
        action="store_true",
        help="Do not create/update asset_overrides.csv for skipped assets.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    use_case_result = RefreshMarketDataUseCase().execute(
        RefreshMarketDataRequest(
            start_date=args.start_date,
            end_date=args.end_date,
            asset_ids=tuple(args.asset_ids or ()),
            provider=args.provider,
            bootstrap_degiro_assets=not args.no_bootstrap_degiro,
            include_inactive=args.include_inactive,
            write_overrides_template=not args.no_write_overrides_template,
        )
    )
    if not args.no_bootstrap_degiro:
        print(f"Synced {use_case_result.synced_assets} assets from normalized DEGIRO data into assets_master.")

    summary = use_case_result.summary
    if summary is None:
        print(use_case_result.result.message)
        return 1

    start_date = use_case_result.result.artifacts["start_date"]
    end_date = use_case_result.result.artifacts["end_date"]
    print(f"Provider: {summary.provider_name}")
    print(f"Window: {start_date} -> {end_date}")
    print(f"Assets updated: {summary.updated_assets}")
    print(f"Assets skipped: {summary.skipped_assets}")
    print(f"Rows written: {summary.total_records}")

    if summary.outcomes:
        print("\nPer-asset outcomes:")
        for outcome in summary.outcomes:
            detail = outcome.resolved_symbol or "-"
            note = f" | {outcome.message}" if outcome.message else ""
            print(
                f"- {outcome.asset_id}: {outcome.status} | rows={outcome.records_written} | symbol={detail}{note}"
            )

    if use_case_result.override_template_path is not None:
        print(f"\nOverride template updated: {use_case_result.override_template_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
