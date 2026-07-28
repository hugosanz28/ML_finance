"""Reusable portfolio read projections shared by reports and interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd


_BROKER_POSITION_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "quantity",
    "market_value_base",
    "weight",
    "cost_basis_base",
    "unrealized_pnl_base",
    "unrealized_return_pct",
    "valuation_status",
]
_COST_POSITION_COLUMNS = [
    "asset_id",
    "cost_basis_base",
    "unrealized_pnl_base",
    "unrealized_return_pct",
]
_STATE_POSITION_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "quantity",
    "market_value_base",
    "weight",
    "cost_basis_base",
    "unrealized_pnl_base",
    "unrealized_return_pct",
    "valuation_status",
]
_STATE_HISTORY_COLUMNS = [
    "valuation_date",
    "total_market_value_base",
    "drawdown_pct",
    "valuation_coverage_ratio",
]
_STATE_SUMMARY_COLUMNS = [
    "total_market_value_base",
    "total_unrealized_pnl_base",
    "portfolio_return_pct",
    "drawdown_pct",
    "valuation_coverage_ratio",
]


@dataclass(frozen=True)
class PortfolioStateProjection:
    """Domain read projection before application-layer JSON normalization."""

    as_of_date: date
    summary: dict[str, float | None]
    broker_snapshot: dict[str, object] | None
    positions: tuple[dict[str, object], ...]
    history: tuple[dict[str, object], ...]
    warnings: tuple[str, ...]


def latest_broker_snapshot_view(
    snapshots: pd.DataFrame,
    *,
    include_isin: bool = False,
) -> dict[str, Any] | None:
    """Project the latest valid broker snapshot and its position-level PnL."""
    if snapshots is None or snapshots.empty:
        return None

    frame = snapshots.copy()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce").dt.date
    frame["market_value_base"] = pd.to_numeric(frame["market_value_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame.get("unrealized_pnl_base"), errors="coerce")
    frame["quantity"] = pd.to_numeric(frame.get("quantity"), errors="coerce")
    frame["asset_name"] = frame["asset_name"].fillna(frame["asset_id"]).astype("string")
    frame["asset_type"] = frame["asset_type"].fillna("unknown").astype("string")
    frame = frame.dropna(subset=["snapshot_date", "market_value_base"])
    if frame.empty:
        return None

    latest_date = max(frame["snapshot_date"])
    latest = frame.loc[frame["snapshot_date"] == latest_date].copy()
    total_value = float(latest["market_value_base"].sum())
    has_snapshot_unrealized = latest["unrealized_pnl_base"].notna().any()
    if has_snapshot_unrealized:
        total_unrealized = float(latest["unrealized_pnl_base"].fillna(0.0).sum())
        total_cost = total_value - total_unrealized
        portfolio_return_pct = None if abs(total_cost) < 1e-9 else total_unrealized / total_cost
    else:
        total_unrealized = None
        portfolio_return_pct = None

    latest["weight"] = 0.0 if abs(total_value) < 1e-9 else latest["market_value_base"] / total_value
    latest["cost_basis_base"] = pd.NA
    rows_with_unrealized = latest["unrealized_pnl_base"].notna()
    latest.loc[rows_with_unrealized, "cost_basis_base"] = (
        latest.loc[rows_with_unrealized, "market_value_base"]
        - latest.loc[rows_with_unrealized, "unrealized_pnl_base"]
    )
    latest["unrealized_return_pct"] = pd.to_numeric(
        latest["unrealized_pnl_base"]
        / pd.to_numeric(latest["cost_basis_base"], errors="coerce").replace(0, pd.NA),
        errors="coerce",
    )
    latest["valuation_status"] = "broker_snapshot"

    position_columns = list(_BROKER_POSITION_COLUMNS)
    if include_isin:
        if "isin" not in latest.columns:
            latest["isin"] = pd.NA
        position_columns.insert(3, "isin")
    positions = latest.loc[:, position_columns].copy()

    return {
        "snapshot_date": latest_date,
        "positions": positions,
        "total_market_value_base": total_value,
        "total_unrealized_pnl_base": total_unrealized,
        "portfolio_return_pct": portfolio_return_pct,
    }


def broker_snapshot_view_for_date(
    snapshots: pd.DataFrame,
    *,
    as_of_date: date,
    include_isin: bool = False,
) -> dict[str, Any] | None:
    """Return the latest broker snapshot available on or before a date."""
    if snapshots is None or snapshots.empty:
        return None

    frame = snapshots.copy()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["snapshot_date"])
    eligible = frame.loc[frame["snapshot_date"] <= as_of_date].copy()
    if eligible.empty:
        return None
    return latest_broker_snapshot_view(eligible, include_isin=include_isin)


def build_broker_snapshot_projection(
    snapshots: pd.DataFrame,
    *,
    position_metrics: pd.DataFrame,
    portfolio_daily_metrics: pd.DataFrame,
    as_of_date: date,
    aggregate_cost_basis_date: date | None = None,
    include_isin: bool = False,
) -> dict[str, Any] | None:
    """Combine broker valuation with the best available external cost basis."""
    broker = broker_snapshot_view_for_date(
        snapshots,
        as_of_date=as_of_date,
        include_isin=include_isin,
    )
    if broker is None:
        return None

    positions = overlay_external_cost_metrics(
        broker["positions"],
        position_metrics,
        target_date=broker["snapshot_date"],
    )
    total_value = float(broker["total_market_value_base"])
    total_unrealized, total_return = derive_broker_pnl_with_external_cost_basis(
        portfolio_daily_metrics,
        target_date=aggregate_cost_basis_date or broker["snapshot_date"],
        total_market_value_base=total_value,
    )
    if total_unrealized is None or total_return is None:
        total_unrealized, total_return = derive_totals_from_positions(
            positions,
            total_market_value_base=total_value,
        )

    return {
        "snapshot_date": broker["snapshot_date"],
        "positions": positions,
        "total_market_value_base": total_value,
        "total_unrealized_pnl_base": total_unrealized,
        "portfolio_return_pct": total_return,
    }


def overlay_external_cost_metrics(
    broker_positions: pd.DataFrame,
    position_metrics: pd.DataFrame,
    *,
    target_date: date,
) -> pd.DataFrame:
    """Fill broker cost/PnL gaps with the latest eligible calculated metrics."""
    if broker_positions.empty:
        return broker_positions

    enriched = broker_positions.copy()
    for column in ("cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"):
        if column not in enriched.columns:
            enriched[column] = pd.NA

    external = _external_positions_for_date(position_metrics, target_date=target_date)
    if external.empty:
        return enriched

    merged = enriched.merge(
        external.rename(
            columns={
                "cost_basis_base": "cost_basis_external",
                "unrealized_pnl_base": "unrealized_external",
                "unrealized_return_pct": "return_external",
            }
        ),
        on="asset_id",
        how="left",
    )
    merged["cost_basis_base"] = pd.to_numeric(merged["cost_basis_base"], errors="coerce").fillna(
        pd.to_numeric(merged["cost_basis_external"], errors="coerce")
    )
    merged["unrealized_pnl_base"] = pd.to_numeric(merged["unrealized_pnl_base"], errors="coerce").fillna(
        pd.to_numeric(merged["unrealized_external"], errors="coerce")
    )
    merged["unrealized_return_pct"] = pd.to_numeric(
        merged["unrealized_return_pct"],
        errors="coerce",
    ).fillna(pd.to_numeric(merged["return_external"], errors="coerce"))
    return merged.drop(columns=["cost_basis_external", "unrealized_external", "return_external"])


def derive_totals_from_positions(
    positions: pd.DataFrame,
    *,
    total_market_value_base: float,
) -> tuple[float | None, float | None]:
    """Derive aggregate PnL and return from known position cost bases."""
    if positions.empty:
        return None, None
    cost = pd.to_numeric(positions.get("cost_basis_base"), errors="coerce")
    if cost is None or cost.notna().sum() == 0:
        return None, None
    total_cost = float(cost.fillna(0.0).sum())
    total_unrealized = float(total_market_value_base - total_cost)
    total_return = None if abs(total_cost) < 1e-9 else total_unrealized / total_cost
    return total_unrealized, total_return


def derive_broker_pnl_with_external_cost_basis(
    portfolio_daily_metrics: pd.DataFrame,
    *,
    target_date: date,
    total_market_value_base: float,
) -> tuple[float | None, float | None]:
    """Apply the latest eligible aggregate cost basis to a broker valuation."""
    frame = portfolio_daily_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["total_cost_basis_base"] = pd.to_numeric(frame["total_cost_basis_base"], errors="coerce")
    frame = frame.dropna(subset=["valuation_date"]).sort_values("valuation_date")
    if frame.empty:
        return None, None

    if target_date in set(frame["valuation_date"].tolist()):
        row = frame.loc[frame["valuation_date"] == target_date].iloc[-1]
    else:
        candidates = frame.loc[frame["valuation_date"] <= target_date]
        if candidates.empty:
            return None, None
        row = candidates.iloc[-1]

    total_cost = float(row["total_cost_basis_base"]) if pd.notna(row["total_cost_basis_base"]) else None
    if total_cost is None or abs(total_cost) < 1e-9:
        return None, None
    total_unrealized = float(total_market_value_base - total_cost)
    total_return = total_unrealized / total_cost
    return total_unrealized, total_return


def project_portfolio_state(
    *,
    position_metrics: pd.DataFrame,
    portfolio_daily_metrics: pd.DataFrame,
    snapshots: pd.DataFrame,
    as_of_date: date | None,
    include_positions: bool,
    include_history: bool,
) -> PortfolioStateProjection:
    """Build the domain read model consumed by the application boundary."""
    if portfolio_daily_metrics is None or portfolio_daily_metrics.empty:
        raise ValueError("portfolio_daily_metrics cannot be empty.")

    daily = portfolio_daily_metrics.copy()
    daily["valuation_date"] = pd.to_datetime(daily["valuation_date"], errors="coerce").dt.date
    daily = daily.dropna(subset=["valuation_date"]).sort_values("valuation_date")
    if daily.empty:
        raise ValueError("portfolio_daily_metrics has no valid valuation dates.")

    requested_date = as_of_date or max(daily["valuation_date"])
    eligible_daily = daily.loc[daily["valuation_date"] <= requested_date].copy()
    if eligible_daily.empty:
        raise ValueError(f"No portfolio metrics available on or before {requested_date.isoformat()}.")

    daily_row = eligible_daily.iloc[-1]
    resolved_date = daily_row["valuation_date"]
    summary = {column: _optional_float(daily_row.get(column)) for column in _STATE_SUMMARY_COLUMNS}

    broker = broker_snapshot_view_for_date(snapshots, as_of_date=resolved_date)
    broker_summary = None
    if broker is not None:
        broker_summary = {
            "snapshot_date": broker["snapshot_date"],
            "total_market_value_base": float(broker["total_market_value_base"]),
        }

    position_records: tuple[dict[str, object], ...] = ()
    if include_positions and position_metrics is not None and not position_metrics.empty:
        positions = position_metrics.copy()
        if "valuation_date" not in positions.columns:
            raise ValueError("position_metrics must include valuation_date.")
        positions["valuation_date"] = pd.to_datetime(positions["valuation_date"], errors="coerce").dt.date
        positions = positions.loc[positions["valuation_date"] == resolved_date].copy()
        for column in _STATE_POSITION_COLUMNS:
            if column not in positions.columns:
                positions[column] = pd.NA
        positions = positions.loc[:, _STATE_POSITION_COLUMNS].sort_values(
            ["weight", "asset_name"],
            ascending=[False, True],
        )
        position_records = tuple(positions.to_dict(orient="records"))

    history_records: tuple[dict[str, object], ...] = ()
    if include_history:
        history = eligible_daily.copy()
        for column in _STATE_HISTORY_COLUMNS:
            if column not in history.columns:
                history[column] = pd.NA
        history_records = tuple(history.loc[:, _STATE_HISTORY_COLUMNS].to_dict(orient="records"))

    return PortfolioStateProjection(
        as_of_date=resolved_date,
        summary=summary,
        broker_snapshot=broker_summary,
        positions=position_records,
        history=history_records,
        warnings=_state_warnings(daily_row),
    )


def _external_positions_for_date(position_metrics: pd.DataFrame, *, target_date: date) -> pd.DataFrame:
    frame = position_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["cost_basis_base"] = pd.to_numeric(frame["cost_basis_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame["unrealized_pnl_base"], errors="coerce")
    frame["unrealized_return_pct"] = pd.to_numeric(frame["unrealized_return_pct"], errors="coerce")
    frame = frame.dropna(subset=["valuation_date", "asset_id"])
    if frame.empty:
        return pd.DataFrame(columns=_COST_POSITION_COLUMNS)

    dates = sorted(frame["valuation_date"].dropna().unique().tolist())
    if not dates:
        return pd.DataFrame(columns=_COST_POSITION_COLUMNS)
    chosen_date = (
        target_date
        if target_date in set(dates)
        else max((date_value for date_value in dates if date_value <= target_date), default=None)
    )
    if chosen_date is None:
        return pd.DataFrame(columns=_COST_POSITION_COLUMNS)
    return frame.loc[frame["valuation_date"] == chosen_date, _COST_POSITION_COLUMNS].copy()


def _state_warnings(daily_row: pd.Series) -> tuple[str, ...]:
    warnings: list[str] = []
    missing_prices = _optional_int(daily_row.get("missing_price_positions_count"))
    missing_fx = _optional_int(daily_row.get("missing_fx_positions_count"))
    if missing_prices:
        warnings.append(f"missing_price_positions:{missing_prices}")
    if missing_fx:
        warnings.append(f"missing_fx_positions:{missing_fx}")
    return tuple(warnings)


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _optional_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


__all__ = [
    "PortfolioStateProjection",
    "broker_snapshot_view_for_date",
    "build_broker_snapshot_projection",
    "derive_broker_pnl_with_external_cost_basis",
    "derive_totals_from_positions",
    "latest_broker_snapshot_view",
    "overlay_external_cost_metrics",
    "project_portfolio_state",
]
