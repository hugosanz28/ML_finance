"""Refresh daily market prices for assets sourced from normalized DEGIRO data."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import get_settings
from src.market_data import (
    DuckDBMarketDataRepository,
    PriceRefreshService,
    build_price_provider,
    sync_market_assets_from_normalized_degiro,
    write_asset_overrides_template,
)


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
    settings = get_settings()
    repository = DuckDBMarketDataRepository(settings=settings)

    if not args.no_bootstrap_degiro:
        synced_count = sync_market_assets_from_normalized_degiro(repository=repository, settings=settings)
        print(f"Synced {synced_count} assets from normalized DEGIRO data into assets_master.")

    assets = repository.list_assets(asset_ids=args.asset_ids, active_only=not args.include_inactive)
    if not assets:
        print("No assets available for refresh.")
        return 1

    start_date = args.start_date or _derive_start_date(assets)
    end_date = args.end_date or date.today()
    provider = None
    if args.provider:
        provider = build_price_provider(
            args.provider,
            cache_dir=settings.market_data_dir / "yfinance_cache",
        )
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(
        start_date=start_date,
        end_date=end_date,
        asset_ids=args.asset_ids,
        active_only=not args.include_inactive,
        bootstrap_degiro_assets=False,
    )

    print(f"Provider: {summary.provider_name}")
    print(f"Window: {start_date.isoformat()} -> {end_date.isoformat()}")
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

    skipped_asset_ids = [outcome.asset_id for outcome in summary.outcomes if outcome.status == "skipped"]
    if skipped_asset_ids and not args.no_write_overrides_template:
        override_path = write_asset_overrides_template(
            skipped_asset_ids,
            repository=repository,
            settings=settings,
        )
        print(f"\nOverride template updated: {override_path}")

    return 0


def _derive_start_date(assets) -> date:
    dated_assets = [asset.first_seen_date for asset in assets if asset.first_seen_date is not None]
    if dated_assets:
        return min(dated_assets)
    return date.today()


if __name__ == "__main__":
    raise SystemExit(main())
