"""Bootstrap the public synthetic demo workspace without private data or network calls."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.market_data import DailyPriceRecord, DuckDBMarketDataRepository

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEMO_ENV_FILE = REPO_ROOT / "demo" / "synthetic_config" / ".env.demo"
DEMO_AS_OF_DATE = date(2026, 4, 30)
SYNTHETIC_PROVIDER = "synthetic"

SYNTHETIC_PRICE_SERIES = {
    "degiro:isin:IE00SYNCORE1": {
        date(2026, 1, 15): 80.00,
        date(2026, 2, 15): 81.50,
        date(2026, 3, 15): 84.00,
        date(2026, 4, 15): 84.00,
        date(2026, 4, 30): 86.50,
    },
    "degiro:isin:IE00SYNBOND1": {
        date(2026, 2, 15): 50.00,
        date(2026, 3, 15): 50.40,
        date(2026, 4, 15): 50.60,
        date(2026, 4, 30): 50.80,
    },
    "degiro:isin:NL00SYNTECH1": {
        date(2026, 3, 15): 95.00,
        date(2026, 4, 15): 99.00,
        date(2026, 4, 30): 102.00,
    },
}


def main() -> int:
    print("Loading project modules...")
    from src.config import clear_settings_cache, get_settings
    from src.degiro_exports.importer import import_degiro_exports
    from src.degiro_exports.warehouse import load_normalized_degiro_to_duckdb
    from src.market_data import DuckDBMarketDataRepository
    from src.reports import generate_monthly_report

    os.environ["ML_FINANCE_ENV_FILE"] = str(DEMO_ENV_FILE)
    clear_settings_cache()
    settings = get_settings()

    print(f"Demo env: {settings.env_file}")
    print(f"Demo data dir: {settings.data_dir}")
    print(f"Demo exports: {settings.degiro_exports_dir}")

    summary = import_degiro_exports(
        settings=settings,
        incoming_dir=settings.degiro_exports_dir / "incoming",
        output_dir=settings.normalized_data_dir / "degiro",
        base_currency=settings.default_currency,
        source_root=settings.degiro_exports_dir,
        ignore_unknown=True,
    )
    if summary.failed_count:
        for outcome in summary.outcomes:
            if outcome.status == "failed":
                print(f"Failed import: {outcome.source_path.name}: {outcome.message}")
        return 1
    print(f"Imported synthetic exports: {summary.imported_count}")

    warehouse = load_normalized_degiro_to_duckdb(settings=settings)
    print(
        "Loaded DuckDB: "
        f"assets={warehouse.assets}, transactions={warehouse.transactions}, "
        f"cash_movements={warehouse.cash_movements}, snapshots={warehouse.portfolio_snapshots}"
    )

    repository = DuckDBMarketDataRepository(settings=settings)
    prices_written = _upsert_synthetic_prices(repository)
    print(f"Synthetic market data: prices={prices_written}, fx=0")

    report = generate_monthly_report(settings=settings, as_of_date=DEMO_AS_OF_DATE, persist=True)
    print(f"Monthly report generated: {report.output_path}")
    return 0


def _upsert_synthetic_prices(repository: DuckDBMarketDataRepository) -> int:
    total = 0
    for asset_id, points in SYNTHETIC_PRICE_SERIES.items():
        total += repository.upsert_daily_prices(
            asset_id=asset_id,
            provider_name=SYNTHETIC_PROVIDER,
            prices=_daily_records(points),
        )
    return total


def _daily_records(points: dict[date, float]) -> tuple[DailyPriceRecord, ...]:
    from src.market_data import DailyPriceRecord

    ordered_dates = sorted(points)
    start_date = ordered_dates[0]
    end_date = ordered_dates[-1]
    records: list[DailyPriceRecord] = []
    current_date = start_date
    last_price = points[start_date]
    while current_date <= end_date:
        if current_date in points:
            last_price = points[current_date]
        records.append(
            DailyPriceRecord(
                price_date=current_date,
                price_currency="EUR",
                close_price=last_price,
                adjusted_close_price=last_price,
            )
        )
        current_date += timedelta(days=1)
    return tuple(records)


if __name__ == "__main__":
    raise SystemExit(main())
