"""Load normalized DEGIRO parquet datasets into the local DuckDB warehouse."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.config import Settings, get_settings
from src.market_data import DuckDBMarketDataRepository, sync_market_assets_from_normalized_degiro


@dataclass(frozen=True)
class DegiroWarehouseLoadSummary:
    """Row counts written from normalized DEGIRO datasets into DuckDB."""

    assets: int
    transactions: int
    cash_movements: int
    portfolio_snapshots: int

    @property
    def total_rows(self) -> int:
        return self.assets + self.transactions + self.cash_movements + self.portfolio_snapshots


def load_normalized_degiro_to_duckdb(
    *,
    repository: DuckDBMarketDataRepository | None = None,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> DegiroWarehouseLoadSummary:
    """Upsert normalized DEGIRO assets and event datasets into DuckDB."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_repository = repository or DuckDBMarketDataRepository(settings=resolved_settings)
    base_dir = (
        resolved_settings.normalized_data_dir / "degiro"
        if normalized_degiro_dir is None
        else Path(normalized_degiro_dir).expanduser().resolve()
    )

    assets_count = sync_market_assets_from_normalized_degiro(
        repository=resolved_repository,
        settings=resolved_settings,
        normalized_degiro_dir=base_dir,
    )
    transactions_count = _upsert_transactions(
        resolved_repository,
        _load_parquet_collection(base_dir / "transactions"),
    )
    cash_count = _upsert_cash_movements(
        resolved_repository,
        _load_parquet_collection(base_dir / "cash_movements"),
    )
    snapshots_count = _upsert_portfolio_snapshots(
        resolved_repository,
        _load_parquet_collection(base_dir / "portfolio_snapshots"),
    )
    return DegiroWarehouseLoadSummary(
        assets=assets_count,
        transactions=transactions_count,
        cash_movements=cash_count,
        portfolio_snapshots=snapshots_count,
    )


def _upsert_transactions(repository: DuckDBMarketDataRepository, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    rows = [
        (
            _none_if_missing(row.get("transaction_id")),
            _none_if_missing(row.get("broker")) or "DEGIRO",
            _none_if_missing(row.get("account_id")),
            _none_if_missing(row.get("asset_id")),
            _none_if_missing(row.get("external_reference")),
            _date_or_none(row.get("trade_date")),
            _date_or_none(row.get("settlement_date")),
            _none_if_missing(row.get("transaction_type")),
            _float_or_none(row.get("quantity")),
            _float_or_none(row.get("unit_price")),
            _float_or_none(row.get("gross_amount")),
            _float_or_zero(row.get("fees_amount")),
            _float_or_zero(row.get("taxes_amount")),
            _float_or_none(row.get("net_cash_amount_local")),
            _none_if_missing(row.get("transaction_currency")),
            _none_if_missing(row.get("base_currency")),
            _float_or_none(row.get("fx_rate_to_base")),
            _float_or_none(row.get("net_cash_amount_base")),
            _none_if_missing(row.get("notes")),
            _none_if_missing(row.get("source_file")),
            _int_or_none(row.get("source_row")),
        )
        for row in frame.to_dict(orient="records")
    ]
    with repository.connection() as connection:
        connection.executemany(
            """
            INSERT INTO transactions (
                transaction_id,
                broker,
                account_id,
                asset_id,
                external_reference,
                trade_date,
                settlement_date,
                transaction_type,
                quantity,
                unit_price,
                gross_amount,
                fees_amount,
                taxes_amount,
                net_cash_amount,
                transaction_currency,
                base_currency,
                fx_rate_to_base,
                net_cash_amount_base,
                notes,
                source_file,
                source_row
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO UPDATE SET
                broker = EXCLUDED.broker,
                account_id = EXCLUDED.account_id,
                asset_id = EXCLUDED.asset_id,
                external_reference = EXCLUDED.external_reference,
                trade_date = EXCLUDED.trade_date,
                settlement_date = EXCLUDED.settlement_date,
                transaction_type = EXCLUDED.transaction_type,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                gross_amount = EXCLUDED.gross_amount,
                fees_amount = EXCLUDED.fees_amount,
                taxes_amount = EXCLUDED.taxes_amount,
                net_cash_amount = EXCLUDED.net_cash_amount,
                transaction_currency = EXCLUDED.transaction_currency,
                base_currency = EXCLUDED.base_currency,
                fx_rate_to_base = EXCLUDED.fx_rate_to_base,
                net_cash_amount_base = EXCLUDED.net_cash_amount_base,
                notes = EXCLUDED.notes,
                source_file = EXCLUDED.source_file,
                source_row = EXCLUDED.source_row
            """,
            rows,
        )
    return len(rows)


def _upsert_cash_movements(repository: DuckDBMarketDataRepository, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    rows = [
        (
            _none_if_missing(row.get("cash_movement_id")),
            _none_if_missing(row.get("broker")) or "DEGIRO",
            _none_if_missing(row.get("account_id")),
            _none_if_missing(row.get("asset_id")),
            _none_if_missing(row.get("external_reference")),
            _date_or_none(row.get("movement_date")),
            _valid_value_date(row.get("value_date"), row.get("movement_date")),
            _none_if_missing(row.get("movement_type")),
            _none_if_missing(row.get("description")),
            _float_or_none(row.get("amount")),
            _none_if_missing(row.get("movement_currency")),
            _none_if_missing(row.get("base_currency")),
            _float_or_none(row.get("fx_rate_to_base")),
            _float_or_none(row.get("amount_base")),
            _none_if_missing(row.get("source_file")),
            _int_or_none(row.get("source_row")),
        )
        for row in frame.to_dict(orient="records")
    ]
    with repository.connection() as connection:
        connection.executemany(
            """
            INSERT INTO cash_movements (
                cash_movement_id,
                broker,
                account_id,
                asset_id,
                external_reference,
                movement_date,
                value_date,
                movement_type,
                description,
                amount,
                movement_currency,
                base_currency,
                fx_rate_to_base,
                amount_base,
                source_file,
                source_row
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (cash_movement_id) DO UPDATE SET
                broker = EXCLUDED.broker,
                account_id = EXCLUDED.account_id,
                asset_id = EXCLUDED.asset_id,
                external_reference = EXCLUDED.external_reference,
                movement_date = EXCLUDED.movement_date,
                value_date = EXCLUDED.value_date,
                movement_type = EXCLUDED.movement_type,
                description = EXCLUDED.description,
                amount = EXCLUDED.amount,
                movement_currency = EXCLUDED.movement_currency,
                base_currency = EXCLUDED.base_currency,
                fx_rate_to_base = EXCLUDED.fx_rate_to_base,
                amount_base = EXCLUDED.amount_base,
                source_file = EXCLUDED.source_file,
                source_row = EXCLUDED.source_row
            """,
            rows,
        )
    return len(rows)


def _upsert_portfolio_snapshots(repository: DuckDBMarketDataRepository, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    rows = [
        (
            _none_if_missing(row.get("snapshot_id")),
            _none_if_missing(row.get("broker")) or "DEGIRO",
            _none_if_missing(row.get("account_id")),
            _date_or_none(row.get("snapshot_date")),
            _none_if_missing(row.get("asset_id")),
            _none_if_missing(row.get("snapshot_source")) or "broker_export",
            _float_or_none(row.get("quantity")),
            _float_or_none(row.get("average_cost")),
            _float_or_none(row.get("market_price")),
            _float_or_none(row.get("market_value")),
            _none_if_missing(row.get("position_currency")),
            _none_if_missing(row.get("base_currency")),
            _float_or_none(row.get("fx_rate_to_base")),
            _float_or_none(row.get("market_value_base")),
            _float_or_none(row.get("unrealized_pnl_base")),
            _none_if_missing(row.get("source_file")),
            _int_or_none(row.get("source_row")),
        )
        for row in frame.to_dict(orient="records")
    ]
    with repository.connection() as connection:
        connection.executemany(
            """
            INSERT INTO portfolio_snapshots (
                snapshot_id,
                broker,
                account_id,
                snapshot_date,
                asset_id,
                snapshot_source,
                quantity,
                average_cost,
                market_price,
                market_value,
                position_currency,
                base_currency,
                fx_rate_to_base,
                market_value_base,
                unrealized_pnl_base,
                source_file,
                source_row
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                broker = EXCLUDED.broker,
                account_id = EXCLUDED.account_id,
                snapshot_date = EXCLUDED.snapshot_date,
                asset_id = EXCLUDED.asset_id,
                snapshot_source = EXCLUDED.snapshot_source,
                quantity = EXCLUDED.quantity,
                average_cost = EXCLUDED.average_cost,
                market_price = EXCLUDED.market_price,
                market_value = EXCLUDED.market_value,
                position_currency = EXCLUDED.position_currency,
                base_currency = EXCLUDED.base_currency,
                fx_rate_to_base = EXCLUDED.fx_rate_to_base,
                market_value_base = EXCLUDED.market_value_base,
                unrealized_pnl_base = EXCLUDED.unrealized_pnl_base,
                source_file = EXCLUDED.source_file,
                source_row = EXCLUDED.source_row
            """,
            rows,
        )
    return len(rows)


def _load_parquet_collection(directory: Path) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in sorted(directory.glob("*.parquet"))] if directory.exists() else []
    populated = [frame for frame in frames if not frame.empty]
    if not populated:
        return pd.DataFrame()
    return pd.concat(populated, ignore_index=True, sort=False)


def _none_if_missing(value: object | None) -> object | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _date_or_none(value: object | None):
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date()


def _valid_value_date(value: object | None, movement_date: object | None):
    parsed_value_date = _date_or_none(value)
    parsed_movement_date = _date_or_none(movement_date)
    if parsed_value_date is None or parsed_movement_date is None:
        return parsed_value_date
    if parsed_value_date < parsed_movement_date:
        return None
    return parsed_value_date


def _float_or_none(value: object | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _float_or_zero(value: object | None) -> float:
    parsed = _float_or_none(value)
    return 0.0 if parsed is None else parsed


def _int_or_none(value: object | None) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)
