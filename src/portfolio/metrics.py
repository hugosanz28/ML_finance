"""Portfolio valuation and aggregate metrics from positions and market data."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import Settings, ensure_local_directories, get_settings
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio.positions import (
    ReconstructedPositionHistory,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
    reconstruct_positions_by_date,
    reconstruct_positions_from_normalized_degiro,
)


POSITION_METRICS_COLUMNS = [
    "valuation_date",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "quantity",
    "price_date",
    "price_currency",
    "close_price",
    "market_value_local",
    "fx_rate_to_base",
    "market_value_base",
    "cost_basis_base",
    "unrealized_pnl_base",
    "unrealized_return_pct",
    "weight",
    "valuation_status",
]

PORTFOLIO_DAILY_METRICS_COLUMNS = [
    "valuation_date",
    "total_positions_count",
    "valued_positions_count",
    "missing_price_positions_count",
    "missing_fx_positions_count",
    "valuation_coverage_ratio",
    "return_coverage_ratio",
    "total_market_value_base",
    "total_cost_basis_base",
    "total_unrealized_pnl_base",
    "portfolio_return_pct",
    "daily_change_base",
    "daily_return_pct",
    "running_peak_value_base",
    "drawdown_pct",
]

_POSITION_REQUIRED_COLUMNS = ("position_date", "asset_id", "quantity")
_POSITION_OPTIONAL_COLUMNS = ("asset_name", "asset_type", "isin", "anchor_snapshot_date")
_PRICE_REQUIRED_COLUMNS = ("asset_id", "price_date", "price_currency", "close_price")
_PRICE_OPTIONAL_COLUMNS = ("adjusted_close_price", "price_provider")
_TRANSACTION_REQUIRED_COLUMNS = (
    "asset_id",
    "trade_date",
    "transaction_type",
    "quantity",
    "gross_amount_base",
)
_TRANSACTION_OPTIONAL_COLUMNS = ("fees_amount_base", "taxes_amount_base", "source_row")
_FX_REQUIRED_COLUMNS = ("base_currency", "quote_currency", "rate_date", "rate")
_FX_OPTIONAL_COLUMNS = ("rate_provider",)


@dataclass(frozen=True)
class PortfolioMetricsResult:
    """Reusable valuation outputs for reporting and Streamlit."""

    start_date: date
    end_date: date
    base_currency: str
    position_metrics: pd.DataFrame
    portfolio_daily_metrics: pd.DataFrame
    position_metrics_output_path: Path | None = None
    portfolio_daily_output_path: Path | None = None


def calculate_portfolio_metrics(
    positions: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    transactions: pd.DataFrame | None = None,
    fx_rates: pd.DataFrame | None = None,
    base_currency: str = "EUR",
    use_adjusted_close: bool = False,
) -> PortfolioMetricsResult:
    """Value positions by date and derive aggregate portfolio metrics."""
    positions_frame = _prepare_positions_frame(positions)
    prices_frame = _prepare_prices_frame(prices, use_adjusted_close=use_adjusted_close)
    transactions_frame = _prepare_transactions_frame(transactions)
    fx_rates_frame = _prepare_fx_rates_frame(fx_rates)

    if positions_frame.empty:
        raise ValueError("positions cannot be empty when calculating portfolio metrics.")

    position_metrics = _build_position_metrics(
        positions_frame,
        prices_frame,
        transactions_frame=transactions_frame,
        fx_rates_frame=fx_rates_frame,
        base_currency=base_currency.upper(),
    )
    portfolio_daily_metrics = _build_portfolio_daily_metrics(position_metrics)

    return PortfolioMetricsResult(
        start_date=positions_frame["position_date"].min(),
        end_date=positions_frame["position_date"].max(),
        base_currency=base_currency.upper(),
        position_metrics=position_metrics,
        portfolio_daily_metrics=portfolio_daily_metrics,
    )


def calculate_portfolio_metrics_from_normalized_degiro(
    *,
    repository: DuckDBMarketDataRepository | None = None,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    include_zero_quantity_days: bool = False,
    zero_tolerance: float = 1e-8,
    price_provider: str | None = None,
    fx_provider: str | None = None,
    persist: bool = False,
    output_dir: str | Path | None = None,
) -> PortfolioMetricsResult:
    """Run the full portfolio-metrics pipeline from normalized DEGIRO data and DuckDB prices."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_repository = repository or DuckDBMarketDataRepository(settings=resolved_settings)

    reconstructed = reconstruct_positions_from_normalized_degiro(
        settings=resolved_settings,
        normalized_degiro_dir=normalized_degiro_dir,
        start_date=start_date,
        end_date=end_date,
        include_zero_quantity_days=include_zero_quantity_days,
        zero_tolerance=zero_tolerance,
        persist=False,
    )
    transactions = load_normalized_degiro_transactions(
        settings=resolved_settings,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    prices = load_prices_daily_from_duckdb(
        repository=resolved_repository,
        asset_ids=reconstructed.positions["asset_id"].dropna().unique().tolist(),
        end_date=reconstructed.end_date,
        provider_name=price_provider or resolved_settings.price_provider,
    )
    fx_rates = load_fx_rates_from_duckdb(
        repository=resolved_repository,
        end_date=reconstructed.end_date,
        provider_name=fx_provider,
    )
    metrics = calculate_portfolio_metrics(
        reconstructed.positions,
        prices,
        transactions=transactions,
        fx_rates=fx_rates,
        base_currency=resolved_settings.default_currency,
    )
    if not persist:
        return metrics
    return persist_portfolio_metrics(metrics, settings=resolved_settings, output_dir=output_dir)


def persist_portfolio_metrics(
    metrics: PortfolioMetricsResult,
    *,
    settings: Settings | None = None,
    output_dir: str | Path | None = None,
) -> PortfolioMetricsResult:
    """Persist valuation outputs as parquet datasets."""
    resolved_settings = get_settings() if settings is None else settings
    ensure_local_directories(resolved_settings)

    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.curated_data_dir / "portfolio" / "metrics"
    )
    base_output_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"{metrics.start_date.isoformat()}_{metrics.end_date.isoformat()}"
    position_metrics_output_path = (base_output_dir / f"position_metrics_{suffix}.parquet").resolve()
    portfolio_daily_output_path = (base_output_dir / f"portfolio_daily_metrics_{suffix}.parquet").resolve()

    positions_ready = metrics.position_metrics.copy()
    positions_ready["valuation_date"] = pd.to_datetime(positions_ready["valuation_date"])
    positions_ready["price_date"] = pd.to_datetime(positions_ready["price_date"])
    positions_ready.to_parquet(position_metrics_output_path, index=False)

    daily_ready = metrics.portfolio_daily_metrics.copy()
    daily_ready["valuation_date"] = pd.to_datetime(daily_ready["valuation_date"])
    daily_ready.to_parquet(portfolio_daily_output_path, index=False)

    return replace(
        metrics,
        position_metrics_output_path=position_metrics_output_path,
        portfolio_daily_output_path=portfolio_daily_output_path,
    )


def load_prices_daily_from_duckdb(
    *,
    repository: DuckDBMarketDataRepository,
    asset_ids: list[str],
    end_date: date,
    provider_name: str | None = None,
) -> pd.DataFrame:
    """Load daily prices up to the requested end date for selected assets."""
    if not asset_ids:
        return pd.DataFrame(columns=[*_PRICE_REQUIRED_COLUMNS, *_PRICE_OPTIONAL_COLUMNS])

    placeholders = ", ".join("?" for _ in asset_ids)
    where_clauses = [f"asset_id IN ({placeholders})", "price_date <= ?"]
    parameters: list[object] = [*asset_ids, end_date]

    if provider_name:
        where_clauses.append("price_provider = ?")
        parameters.append(provider_name)

    with repository.connection() as connection:
        frame = connection.execute(
            f"""
            SELECT
                asset_id,
                price_date,
                price_currency,
                close_price,
                adjusted_close_price,
                price_provider
            FROM prices_daily
            WHERE {" AND ".join(where_clauses)}
            ORDER BY asset_id, price_date, price_provider
            """,
            parameters,
        ).fetchdf()
    return frame


def load_fx_rates_from_duckdb(
    *,
    repository: DuckDBMarketDataRepository,
    end_date: date,
    provider_name: str | None = None,
) -> pd.DataFrame:
    """Load FX rates up to the requested end date."""
    where_clauses = ["rate_date <= ?"]
    parameters: list[object] = [end_date]

    if provider_name:
        where_clauses.append("rate_provider = ?")
        parameters.append(provider_name)

    with repository.connection() as connection:
        frame = connection.execute(
            f"""
            SELECT
                base_currency,
                quote_currency,
                rate_date,
                rate,
                rate_provider
            FROM fx_rates
            WHERE {" AND ".join(where_clauses)}
            ORDER BY base_currency, quote_currency, rate_date, rate_provider
            """,
            parameters,
        ).fetchdf()
    return frame


def _prepare_positions_frame(positions: pd.DataFrame) -> pd.DataFrame:
    frame = positions.copy()
    _require_columns(frame, _POSITION_REQUIRED_COLUMNS, frame_name="positions")
    for column in _POSITION_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    frame["position_date"] = pd.to_datetime(frame["position_date"], errors="raise").dt.date
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="raise")
    frame["asset_name"] = frame["asset_name"].map(_normalize_optional_text)
    frame["asset_type"] = frame["asset_type"].map(_normalize_optional_text)
    frame["isin"] = frame["isin"].map(_normalize_optional_text)
    frame["anchor_snapshot_date"] = pd.to_datetime(frame["anchor_snapshot_date"], errors="coerce").dt.date
    return frame.loc[:, [*_POSITION_REQUIRED_COLUMNS, *_POSITION_OPTIONAL_COLUMNS]]


def _prepare_prices_frame(prices: pd.DataFrame, *, use_adjusted_close: bool) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[*_PRICE_REQUIRED_COLUMNS, *_PRICE_OPTIONAL_COLUMNS])
    if prices is not None and not prices.empty:
        frame = prices.copy()
    _require_columns(frame, _PRICE_REQUIRED_COLUMNS, frame_name="prices")
    for column in _PRICE_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    if frame.empty:
        return frame.loc[:, [*_PRICE_REQUIRED_COLUMNS, *_PRICE_OPTIONAL_COLUMNS, "effective_close_price"]]

    frame["price_date"] = pd.to_datetime(frame["price_date"], errors="raise").dt.date
    frame["close_price"] = pd.to_numeric(frame["close_price"], errors="raise")
    frame["adjusted_close_price"] = pd.to_numeric(frame["adjusted_close_price"], errors="coerce")
    frame["effective_close_price"] = frame["adjusted_close_price"].where(
        use_adjusted_close & frame["adjusted_close_price"].notna(),
        frame["close_price"],
    )
    frame["price_currency"] = frame["price_currency"].astype("string").str.upper()
    frame["price_provider"] = frame["price_provider"].map(_normalize_optional_text)
    return frame.loc[:, [*_PRICE_REQUIRED_COLUMNS, *_PRICE_OPTIONAL_COLUMNS, "effective_close_price"]]


def _prepare_transactions_frame(transactions: pd.DataFrame | None) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS])
    if transactions is not None and not transactions.empty:
        frame = transactions.copy()

    _require_columns(frame, _TRANSACTION_REQUIRED_COLUMNS, frame_name="transactions")
    for column in _TRANSACTION_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = 0

    if frame.empty:
        return frame.loc[:, [*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS]]

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
    frame["transaction_type"] = frame["transaction_type"].astype("string").str.upper()
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="raise")
    frame["gross_amount_base"] = pd.to_numeric(frame["gross_amount_base"], errors="raise")
    frame["fees_amount_base"] = pd.to_numeric(frame["fees_amount_base"], errors="coerce").fillna(0.0)
    frame["taxes_amount_base"] = pd.to_numeric(frame["taxes_amount_base"], errors="coerce").fillna(0.0)
    frame["source_row"] = pd.to_numeric(frame["source_row"], errors="coerce").fillna(0).astype(int)
    return frame.loc[:, [*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS]]


def _prepare_fx_rates_frame(fx_rates: pd.DataFrame | None) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[*_FX_REQUIRED_COLUMNS, *_FX_OPTIONAL_COLUMNS])
    if fx_rates is not None and not fx_rates.empty:
        frame = fx_rates.copy()

    _require_columns(frame, _FX_REQUIRED_COLUMNS, frame_name="fx_rates")
    for column in _FX_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    if frame.empty:
        return frame.loc[:, [*_FX_REQUIRED_COLUMNS, *_FX_OPTIONAL_COLUMNS]]

    frame["base_currency"] = frame["base_currency"].astype("string").str.upper()
    frame["quote_currency"] = frame["quote_currency"].astype("string").str.upper()
    frame["rate_date"] = pd.to_datetime(frame["rate_date"], errors="raise").dt.date
    frame["rate"] = pd.to_numeric(frame["rate"], errors="raise")
    return frame.loc[:, [*_FX_REQUIRED_COLUMNS, *_FX_OPTIONAL_COLUMNS]]


def _build_position_metrics(
    positions: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    transactions_frame: pd.DataFrame,
    fx_rates_frame: pd.DataFrame,
    base_currency: str,
) -> pd.DataFrame:
    price_lookup = _build_price_lookup(prices)
    cost_basis_lookup = _build_cost_basis_lookup(transactions_frame)
    fx_lookup = _build_fx_lookup(fx_rates_frame)

    rows: list[dict[str, object]] = []
    for row in positions.sort_values(["asset_id", "position_date"]).to_dict(orient="records"):
        valuation = _value_position_row(
            row,
            price_lookup=price_lookup,
            cost_basis_lookup=cost_basis_lookup,
            fx_lookup=fx_lookup,
            base_currency=base_currency,
        )
        rows.append(valuation)

    frame = pd.DataFrame(rows, columns=POSITION_METRICS_COLUMNS)
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"])
    frame["price_date"] = pd.to_datetime(frame["price_date"])
    frame["market_value_base"] = pd.to_numeric(frame["market_value_base"], errors="coerce")
    frame["cost_basis_base"] = pd.to_numeric(frame["cost_basis_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame["unrealized_pnl_base"], errors="coerce")
    frame["weight"] = 0.0

    totals = (
        frame.groupby("valuation_date", as_index=False)
        .agg(total_market_value_base=("market_value_base", "sum"))
        .rename(columns={"total_market_value_base": "_total_market_value_base"})
    )
    frame = frame.merge(totals, on="valuation_date", how="left")
    non_zero_totals = frame["_total_market_value_base"].fillna(0.0)
    frame.loc[non_zero_totals > 0, "weight"] = (
        frame.loc[non_zero_totals > 0, "market_value_base"].fillna(0.0)
        / non_zero_totals.loc[non_zero_totals > 0]
    ).round(8)
    frame = frame.drop(columns=["_total_market_value_base"])

    return frame.sort_values(["valuation_date", "asset_id"]).reset_index(drop=True)


def _build_portfolio_daily_metrics(position_metrics: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        position_metrics.groupby("valuation_date", as_index=False)
        .agg(
            total_positions_count=("asset_id", "size"),
            valued_positions_count=("valuation_status", lambda values: int(sum(value.startswith("valued") for value in values))),
            missing_price_positions_count=("valuation_status", lambda values: int(sum(value == "missing_price" for value in values))),
            missing_fx_positions_count=("valuation_status", lambda values: int(sum(value == "missing_fx" for value in values))),
            total_market_value_base=("market_value_base", "sum"),
            total_cost_basis_base=("cost_basis_base", "sum"),
            total_unrealized_pnl_base=("unrealized_pnl_base", "sum"),
            known_cost_basis_positions_count=("cost_basis_base", lambda values: int(pd.Series(values).notna().sum())),
        )
    )

    grouped["valuation_coverage_ratio"] = (
        grouped["valued_positions_count"] / grouped["total_positions_count"]
    ).fillna(0.0).round(8)
    grouped["return_coverage_ratio"] = pd.to_numeric(
        grouped["known_cost_basis_positions_count"] / grouped["valued_positions_count"].replace(0, pd.NA),
        errors="coerce",
    ).fillna(0.0).round(8)
    grouped["portfolio_return_pct"] = pd.to_numeric(
        grouped["total_unrealized_pnl_base"] / grouped["total_cost_basis_base"].replace(0, pd.NA),
        errors="coerce",
    ).round(8)
    grouped["daily_change_base"] = grouped["total_market_value_base"].diff().round(8)
    grouped["daily_return_pct"] = grouped["total_market_value_base"].pct_change().round(8)
    grouped["running_peak_value_base"] = grouped["total_market_value_base"].cummax().round(8)
    grouped["drawdown_pct"] = pd.to_numeric(
        grouped["total_market_value_base"] / grouped["running_peak_value_base"].replace(0, pd.NA) - 1
    , errors="coerce").round(8)
    grouped["drawdown_pct"] = grouped["drawdown_pct"].fillna(0.0)

    return grouped.loc[:, PORTFOLIO_DAILY_METRICS_COLUMNS].sort_values("valuation_date").reset_index(drop=True)


def _build_price_lookup(prices: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if prices.empty:
        return {}

    lookups: dict[str, pd.DataFrame] = {}
    for asset_id, group in prices.sort_values(["asset_id", "price_date", "price_provider"]).groupby("asset_id", sort=True):
        current = group.drop_duplicates(subset=["asset_id", "price_date"], keep="last").copy()
        current["price_date"] = pd.to_datetime(current["price_date"])
        lookups[str(asset_id)] = current.loc[:, ["price_date", "price_currency", "effective_close_price"]]
    return lookups


def _build_cost_basis_lookup(transactions: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if transactions.empty:
        return {}

    lookups: dict[str, pd.DataFrame] = {}
    for asset_id, group in transactions.sort_values(["asset_id", "trade_date", "source_row"]).groupby("asset_id", sort=True):
        running_quantity = 0.0
        running_cost_basis = 0.0
        rows: list[dict[str, object]] = []
        for row in group.to_dict(orient="records"):
            quantity = float(row["quantity"])
            trade_date = row["trade_date"]
            transaction_type = str(row["transaction_type"])
            total_buy_cost = float(row["gross_amount_base"]) + float(row["fees_amount_base"]) + float(row["taxes_amount_base"])

            if transaction_type == "BUY":
                running_quantity = round(running_quantity + quantity, 8)
                running_cost_basis = round(running_cost_basis + total_buy_cost, 8)
            elif transaction_type == "SELL":
                if running_quantity < quantity - 1e-8:
                    raise ValueError(
                        f"Cannot compute moving-average cost basis for {asset_id}: sell quantity exceeds holdings on "
                        f"{trade_date.isoformat()}."
                    )
                average_cost_per_unit = running_cost_basis / running_quantity if running_quantity > 0 else 0.0
                running_quantity = round(running_quantity - quantity, 8)
                running_cost_basis = round(running_cost_basis - average_cost_per_unit * quantity, 8)
                if abs(running_quantity) <= 1e-8:
                    running_quantity = 0.0
                    running_cost_basis = 0.0
            else:
                raise ValueError(f"Unsupported transaction type for cost basis: {transaction_type}")

            rows.append(
                {
                    "valuation_date": pd.Timestamp(trade_date),
                    "cost_basis_base": running_cost_basis,
                }
            )

        lookups[str(asset_id)] = pd.DataFrame(rows)
    return lookups


def _build_fx_lookup(fx_rates: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
    if fx_rates.empty:
        return {}

    lookup: dict[tuple[str, str], pd.DataFrame] = {}
    ordered = fx_rates.sort_values(["base_currency", "quote_currency", "rate_date", "rate_provider"])
    for (base_currency, quote_currency), group in ordered.groupby(["base_currency", "quote_currency"], sort=True):
        current = group.drop_duplicates(subset=["base_currency", "quote_currency", "rate_date"], keep="last").copy()
        current["rate_date"] = pd.to_datetime(current["rate_date"])
        lookup[(str(base_currency), str(quote_currency))] = current.loc[:, ["rate_date", "rate"]]
    return lookup


def _value_position_row(
    row: dict[str, object],
    *,
    price_lookup: dict[str, pd.DataFrame],
    cost_basis_lookup: dict[str, pd.DataFrame],
    fx_lookup: dict[tuple[str, str], pd.DataFrame],
    base_currency: str,
) -> dict[str, object]:
    valuation_date = row["position_date"]
    asset_id = str(row["asset_id"])
    quantity = float(row["quantity"])
    asset_type = _normalize_optional_text(row.get("asset_type"))

    if asset_type == "cash" or asset_id.startswith("degiro:cash:"):
        cash_currency = asset_id.split(":")[-1].upper()
        fx_rate_to_base = 1.0 if cash_currency == base_currency else _resolve_fx_rate(
            valuation_date,
            from_currency=cash_currency,
            to_currency=base_currency,
            fx_lookup=fx_lookup,
        )
        if fx_rate_to_base is None and cash_currency != base_currency:
            market_value_base = None
            valuation_status = "missing_fx"
        else:
            market_value_base = quantity if cash_currency == base_currency else round(quantity / fx_rate_to_base, 8)
            valuation_status = "valued_cash"

        return {
            "valuation_date": valuation_date,
            "asset_id": asset_id,
            "asset_name": row.get("asset_name"),
            "asset_type": asset_type,
            "isin": row.get("isin"),
            "quantity": quantity,
            "price_date": valuation_date,
            "price_currency": cash_currency,
            "close_price": 1.0,
            "market_value_local": quantity,
            "fx_rate_to_base": fx_rate_to_base if cash_currency != base_currency else 1.0,
            "market_value_base": market_value_base,
            "cost_basis_base": quantity if cash_currency == base_currency else None,
            "unrealized_pnl_base": 0.0 if cash_currency == base_currency else None,
            "unrealized_return_pct": 0.0 if cash_currency == base_currency else None,
            "weight": 0.0,
            "valuation_status": valuation_status,
        }

    price_row = _resolve_latest_row(price_lookup.get(asset_id), date_column="price_date", as_of_date=valuation_date)
    if price_row is None:
        return _build_unvalued_row(row, valuation_status="missing_price")

    price_currency = str(price_row["price_currency"])
    close_price = float(price_row["effective_close_price"])
    market_value_local = round(quantity * close_price, 8)

    if price_currency == base_currency:
        fx_rate_to_base = 1.0
        market_value_base = market_value_local
        valuation_status = "valued"
    else:
        fx_rate_to_base = _resolve_fx_rate(
            valuation_date,
            from_currency=price_currency,
            to_currency=base_currency,
            fx_lookup=fx_lookup,
        )
        if fx_rate_to_base is None:
            return {
                **_build_unvalued_row(row, valuation_status="missing_fx"),
                "price_date": price_row["price_date"].date(),
                "price_currency": price_currency,
                "close_price": close_price,
                "market_value_local": market_value_local,
            }
        market_value_base = round(market_value_local / fx_rate_to_base, 8)
        valuation_status = "valued"

    cost_basis_row = _resolve_latest_row(cost_basis_lookup.get(asset_id), date_column="valuation_date", as_of_date=valuation_date)
    cost_basis_base = None if cost_basis_row is None else round(float(cost_basis_row["cost_basis_base"]), 8)
    unrealized_pnl_base = None
    unrealized_return_pct = None
    if cost_basis_base is not None:
        unrealized_pnl_base = round(market_value_base - cost_basis_base, 8)
        if cost_basis_base != 0:
            unrealized_return_pct = round(unrealized_pnl_base / cost_basis_base, 8)

    return {
        "valuation_date": valuation_date,
        "asset_id": asset_id,
        "asset_name": row.get("asset_name"),
        "asset_type": asset_type,
        "isin": row.get("isin"),
        "quantity": quantity,
        "price_date": price_row["price_date"].date(),
        "price_currency": price_currency,
        "close_price": close_price,
        "market_value_local": market_value_local,
        "fx_rate_to_base": fx_rate_to_base,
        "market_value_base": market_value_base,
        "cost_basis_base": cost_basis_base,
        "unrealized_pnl_base": unrealized_pnl_base,
        "unrealized_return_pct": unrealized_return_pct,
        "weight": 0.0,
        "valuation_status": valuation_status,
    }


def _build_unvalued_row(row: dict[str, object], *, valuation_status: str) -> dict[str, object]:
    return {
        "valuation_date": row["position_date"],
        "asset_id": row["asset_id"],
        "asset_name": row.get("asset_name"),
        "asset_type": row.get("asset_type"),
        "isin": row.get("isin"),
        "quantity": float(row["quantity"]),
        "price_date": None,
        "price_currency": None,
        "close_price": None,
        "market_value_local": None,
        "fx_rate_to_base": None,
        "market_value_base": None,
        "cost_basis_base": None,
        "unrealized_pnl_base": None,
        "unrealized_return_pct": None,
        "weight": 0.0,
        "valuation_status": valuation_status,
    }


def _resolve_latest_row(frame: pd.DataFrame | None, *, date_column: str, as_of_date: date) -> pd.Series | None:
    if frame is None or frame.empty:
        return None
    eligible = frame.loc[frame[date_column] <= pd.Timestamp(as_of_date)]
    if eligible.empty:
        return None
    return eligible.iloc[-1]


def _resolve_fx_rate(
    valuation_date: date,
    *,
    from_currency: str,
    to_currency: str,
    fx_lookup: dict[tuple[str, str], pd.DataFrame],
) -> float | None:
    if from_currency == to_currency:
        return 1.0

    direct = _resolve_latest_row(
        fx_lookup.get((to_currency, from_currency)),
        date_column="rate_date",
        as_of_date=valuation_date,
    )
    if direct is not None:
        return round(float(direct["rate"]), 10)

    inverse = _resolve_latest_row(
        fx_lookup.get((from_currency, to_currency)),
        date_column="rate_date",
        as_of_date=valuation_date,
    )
    if inverse is None:
        return None
    rate = float(inverse["rate"])
    if rate == 0:
        return None
    return round(1 / rate, 10)


def _require_columns(frame: pd.DataFrame, required_columns: tuple[str, ...], *, frame_name: str) -> None:
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in {frame_name}: {', '.join(missing_columns)}")


def _normalize_optional_text(value: object | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None
