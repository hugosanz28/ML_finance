"""DuckDB persistence helpers for market data."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import duckdb

from src.config import Settings, default_repo_root, ensure_local_directories, get_settings
from src.market_data.models import DailyPriceRecord, FxRateRecord, MarketAsset


ASSET_COLUMNS = [
    "asset_id",
    "asset_type",
    "asset_name",
    "asset_similar",
    "isin",
    "ticker",
    "broker_symbol",
    "exchange_mic",
    "trading_currency",
    "first_seen_date",
    "last_seen_date",
    "is_active",
]


def _order_assets_for_upsert(assets: Sequence[MarketAsset]) -> list[MarketAsset]:
    """Insert proxy targets before assets that reference them."""
    pending = {asset.asset_id: asset for asset in assets}
    ordered: list[MarketAsset] = []

    while pending:
        ready_asset_ids = [
            asset_id
            for asset_id, asset in pending.items()
            if asset.asset_similar is None or asset.asset_similar not in pending
        ]
        if not ready_asset_ids:
            ordered.extend(pending.values())
            break

        for asset_id in sorted(ready_asset_ids):
            ordered.append(pending.pop(asset_id))

    return ordered


class DuckDBMarketDataRepository:
    """Read and write market data entities from the local DuckDB database."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.settings = get_settings() if settings is None else settings
        self.db_path = (
            self.settings.portfolio_db_path
            if db_path is None
            else Path(db_path).expanduser().resolve()
        )
        schema_path = self.settings.initial_schema_path
        if not schema_path.exists():
            schema_path = default_repo_root() / "src" / "data" / "sql" / "001_initial_schema.sql"
        self._schema_sql = schema_path.read_text(encoding="utf-8")

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Open a connection with the project schema applied."""
        ensure_local_directories(self.settings)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        connection = duckdb.connect(str(self.db_path))
        try:
            connection.execute(self._schema_sql)
            yield connection
        finally:
            connection.close()

    def ensure_schema(self) -> Path:
        """Create the database file and apply the versioned schema."""
        with self.connection():
            pass
        return self.db_path

    def upsert_assets(self, assets: Sequence[MarketAsset]) -> int:
        """Insert or update assets used by the market data workflow."""
        if not assets:
            return 0

        ordered_assets = _order_assets_for_upsert(assets)
        rows = [
            (
                asset.asset_id,
                asset.asset_type,
                asset.asset_name,
                asset.asset_similar,
                asset.isin,
                asset.ticker,
                asset.broker_symbol,
                asset.exchange_mic,
                asset.trading_currency,
                asset.first_seen_date,
                asset.last_seen_date,
                asset.is_active,
            )
            for asset in ordered_assets
        ]

        with self.connection() as connection:
            asset_ids = [asset.asset_id for asset in ordered_assets]
            placeholders = ", ".join("?" for _ in asset_ids)
            existing_asset_ids = set()
            if asset_ids:
                existing_rows = connection.execute(
                    f"SELECT asset_id FROM assets_master WHERE asset_id IN ({placeholders})",
                    asset_ids,
                ).fetchall()
                existing_asset_ids = {row[0] for row in existing_rows}

            new_rows = [row for row in rows if row[0] not in existing_asset_ids]
            existing_rows = [row for row in rows if row[0] in existing_asset_ids]

            if new_rows:
                connection.executemany(
                    """
                    INSERT INTO assets_master (
                        asset_id,
                        asset_type,
                        asset_name,
                        asset_similar,
                        isin,
                        ticker,
                        broker_symbol,
                        exchange_mic,
                        trading_currency,
                        first_seen_date,
                        last_seen_date,
                        is_active
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    new_rows,
                )

            update_sql = """
                UPDATE assets_master
                SET
                    asset_type = ?,
                    asset_name = ?,
                    asset_similar = ?,
                    isin = ?,
                    ticker = ?,
                    broker_symbol = ?,
                    exchange_mic = ?,
                    trading_currency = ?,
                    first_seen_date = ?,
                    last_seen_date = ?,
                    is_active = ?,
                    updated_at = now()
                WHERE asset_id = ?
                """
            for row in existing_rows:
                update_parameters = (
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[0],
                )
                try:
                    connection.execute(update_sql, update_parameters)
                except duckdb.ConstraintException:
                    # DuckDB can reject updates on parent rows referenced by FK
                    # tables even when the primary key is unchanged. Keep the
                    # existing referenced row rather than breaking downstream
                    # prices, transactions or snapshots.
                    continue

        return len(rows)

    def get_asset(self, asset_id: str) -> MarketAsset | None:
        """Return a single asset by identifier."""
        with self.connection() as connection:
            result = connection.execute(
                f"""
                SELECT {", ".join(ASSET_COLUMNS)}
                FROM assets_master
                WHERE asset_id = ?
                """,
                [asset_id],
            )
            row = result.fetchone()
            if row is None:
                return None

            column_names = [description[0] for description in result.description]
            return MarketAsset.from_mapping(dict(zip(column_names, row)))

    def list_assets(
        self,
        *,
        asset_ids: Sequence[str] | None = None,
        active_only: bool = True,
    ) -> list[MarketAsset]:
        """List assets available for price refresh."""
        where_clauses: list[str] = []
        parameters: list[object] = []

        if active_only:
            where_clauses.append("is_active = TRUE")

        if asset_ids:
            placeholders = ", ".join("?" for _ in asset_ids)
            where_clauses.append(f"asset_id IN ({placeholders})")
            parameters.extend(asset_ids)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        with self.connection() as connection:
            result = connection.execute(
                f"""
                SELECT {", ".join(ASSET_COLUMNS)}
                FROM assets_master
                {where_sql}
                ORDER BY asset_id
                """,
                parameters,
            )
            rows = result.fetchall()
            column_names = [description[0] for description in result.description]

        return [MarketAsset.from_mapping(dict(zip(column_names, row))) for row in rows]

    def upsert_daily_prices(
        self,
        *,
        asset_id: str,
        provider_name: str,
        prices: Sequence[DailyPriceRecord],
    ) -> int:
        """Insert or update daily prices for one asset and provider."""
        if not prices:
            return 0

        rows = [
            (
                asset_id,
                price.price_date,
                provider_name,
                price.price_currency,
                price.open_price,
                price.high_price,
                price.low_price,
                price.close_price,
                price.adjusted_close_price,
                price.volume,
                price.source_updated_at,
            )
            for price in prices
        ]

        with self.connection() as connection:
            connection.executemany(
                """
                INSERT INTO prices_daily (
                    asset_id,
                    price_date,
                    price_provider,
                    price_currency,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjusted_close_price,
                    volume,
                    source_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (asset_id, price_date, price_provider) DO UPDATE SET
                    price_currency = EXCLUDED.price_currency,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    adjusted_close_price = EXCLUDED.adjusted_close_price,
                    volume = EXCLUDED.volume,
                    source_updated_at = EXCLUDED.source_updated_at,
                    ingested_at = now()
                """,
                rows,
            )

        return len(rows)

    def upsert_fx_rates(
        self,
        *,
        base_currency: str,
        quote_currency: str,
        provider_name: str,
        rates: Sequence[FxRateRecord],
    ) -> int:
        """Insert or update daily FX rates for one pair and provider."""
        if not rates:
            return 0

        normalized_base = base_currency.strip().upper()
        normalized_quote = quote_currency.strip().upper()
        rows = [
            (
                normalized_base,
                normalized_quote,
                rate.rate_date,
                provider_name,
                rate.rate,
                rate.source_updated_at,
            )
            for rate in rates
        ]

        with self.connection() as connection:
            connection.executemany(
                """
                INSERT INTO fx_rates (
                    base_currency,
                    quote_currency,
                    rate_date,
                    rate_provider,
                    rate,
                    source_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (base_currency, quote_currency, rate_date, rate_provider) DO UPDATE SET
                    rate = EXCLUDED.rate,
                    source_updated_at = EXCLUDED.source_updated_at,
                    ingested_at = now()
                """,
                rows,
            )

        return len(rows)

    def list_fx_rates(
        self,
        *,
        base_currency: str | None = None,
        quote_currency: str | None = None,
        provider_name: str | None = None,
    ) -> list[tuple[str, str, object, str, float]]:
        """List persisted FX rates ordered by pair and date."""
        where_clauses: list[str] = []
        parameters: list[object] = []

        if base_currency:
            where_clauses.append("base_currency = ?")
            parameters.append(base_currency.strip().upper())
        if quote_currency:
            where_clauses.append("quote_currency = ?")
            parameters.append(quote_currency.strip().upper())
        if provider_name:
            where_clauses.append("rate_provider = ?")
            parameters.append(provider_name)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        with self.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT base_currency, quote_currency, rate_date, rate_provider, rate
                FROM fx_rates
                {where_sql}
                ORDER BY base_currency, quote_currency, rate_date, rate_provider
                """,
                parameters,
            ).fetchall()

        return [(row[0], row[1], row[2], row[3], float(row[4])) for row in rows]
