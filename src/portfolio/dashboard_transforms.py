"""Data transforms and chart builders for the Streamlit dashboard."""

from __future__ import annotations

from datetime import date
import json
from typing import Any

import altair as alt
import pandas as pd

from src.agents import build_portfolio_metrics_snapshot
from src.config import Settings
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio import PortfolioMetricsResult


def _daily_metrics(metrics: PortfolioMetricsResult) -> pd.DataFrame:
    frame = metrics.portfolio_daily_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    return frame.sort_values("valuation_date")


def _positions_for_date(metrics: PortfolioMetricsResult, valuation_date: date) -> pd.DataFrame:
    frame = metrics.position_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    return frame.loc[frame["valuation_date"] == valuation_date].copy()


def _latest_broker_snapshot_view(snapshots: pd.DataFrame) -> dict[str, Any] | None:
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
        latest.loc[rows_with_unrealized, "market_value_base"] - latest.loc[rows_with_unrealized, "unrealized_pnl_base"]
    )
    latest["unrealized_return_pct"] = pd.to_numeric(
        latest["unrealized_pnl_base"] / pd.to_numeric(latest["cost_basis_base"], errors="coerce").replace(0, pd.NA),
        errors="coerce",
    )
    latest["valuation_status"] = "broker_snapshot"
    positions = latest.loc[
        :,
        [
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
        ],
    ].copy()

    return {
        "snapshot_date": latest_date,
        "positions": positions,
        "total_market_value_base": total_value,
        "total_unrealized_pnl_base": total_unrealized,
        "portfolio_return_pct": portfolio_return_pct,
    }


def _broker_snapshot_view_for_date(snapshots: pd.DataFrame, *, as_of_date: date) -> dict[str, Any] | None:
    if snapshots is None or snapshots.empty:
        return None
    frame = snapshots.copy()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["snapshot_date"])
    eligible = frame.loc[frame["snapshot_date"] <= as_of_date].copy()
    if eligible.empty:
        return None
    return _latest_broker_snapshot_view(eligible)


def _build_broker_anchored_daily_series(daily: pd.DataFrame, snapshots: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    ready = daily.copy().sort_values("valuation_date")
    ready["total_market_value_base"] = pd.to_numeric(ready["total_market_value_base"], errors="coerce")
    ready["valuation_date"] = pd.to_datetime(ready["valuation_date"], errors="coerce").dt.date
    ready = ready.dropna(subset=["valuation_date", "total_market_value_base"])
    if ready.empty:
        return ready, ""

    broker = _latest_broker_snapshot_view(snapshots)
    if broker is None:
        ready["total_market_value_broker_anchored"] = ready["total_market_value_base"]
        return ready, "No hay snapshot DEGIRO disponible; se muestra valoracion externa."

    anchor_date = broker["snapshot_date"]
    anchor_total = float(broker["total_market_value_base"])
    anchor_rows = ready.loc[ready["valuation_date"] == anchor_date]
    if anchor_rows.empty:
        ready["total_market_value_broker_anchored"] = ready["total_market_value_base"]
        return ready, (
            f"No hay serie externa en la fecha del snapshot ({anchor_date.isoformat()}); "
            "se muestra valoracion externa sin ancla."
        )

    external_anchor = float(anchor_rows.iloc[-1]["total_market_value_base"])
    if abs(external_anchor) < 1e-9:
        ready["total_market_value_broker_anchored"] = ready["total_market_value_base"]
        return ready, "Ancla externa no valida (0); se muestra valoracion externa."

    scale = anchor_total / external_anchor
    ready["total_market_value_broker_anchored"] = (ready["total_market_value_base"] * scale).round(8)
    return ready, (
        f"Ancla DEGIRO aplicada en {anchor_date.isoformat()} "
        f"({anchor_total:,.2f} EUR)."
    )


def _external_positions_for_date(metrics: PortfolioMetricsResult, *, target_date: date) -> pd.DataFrame:
    frame = metrics.position_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["cost_basis_base"] = pd.to_numeric(frame["cost_basis_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame["unrealized_pnl_base"], errors="coerce")
    frame["unrealized_return_pct"] = pd.to_numeric(frame["unrealized_return_pct"], errors="coerce")
    frame = frame.dropna(subset=["valuation_date", "asset_id"])
    if frame.empty:
        return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])

    dates = sorted(frame["valuation_date"].dropna().unique().tolist())
    if not dates:
        return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])
    if target_date in set(dates):
        chosen_date = target_date
    else:
        fallback_dates = [date_value for date_value in dates if date_value <= target_date]
        if not fallback_dates:
            return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])
        chosen_date = max(fallback_dates)
    current = frame.loc[frame["valuation_date"] == chosen_date, ["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"]].copy()
    return current


def _overlay_external_cost_metrics(
    broker_positions: pd.DataFrame,
    metrics: PortfolioMetricsResult,
    *,
    target_date: date,
) -> pd.DataFrame:
    if broker_positions.empty:
        return broker_positions
    enriched = broker_positions.copy()
    for column in ("cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"):
        if column not in enriched.columns:
            enriched[column] = pd.NA

    external = _external_positions_for_date(metrics, target_date=target_date)
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
    merged["unrealized_return_pct"] = pd.to_numeric(merged["unrealized_return_pct"], errors="coerce").fillna(
        pd.to_numeric(merged["return_external"], errors="coerce")
    )
    return merged.drop(columns=["cost_basis_external", "unrealized_external", "return_external"])


def _derive_totals_from_positions(
    positions: pd.DataFrame,
    *,
    total_market_value_base: float,
) -> tuple[float | None, float | None]:
    if positions.empty:
        return None, None
    cost = pd.to_numeric(positions.get("cost_basis_base"), errors="coerce")
    if cost is None or cost.notna().sum() == 0:
        return None, None
    total_cost = float(cost.fillna(0.0).sum())
    total_unrealized = float(total_market_value_base - total_cost)
    total_return = None if abs(total_cost) < 1e-9 else total_unrealized / total_cost
    return total_unrealized, total_return


def _derive_broker_pnl_with_external_cost_basis(
    daily: pd.DataFrame,
    *,
    target_date: date,
    total_market_value_base: float,
) -> tuple[float | None, float | None]:
    frame = daily.copy()
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


def _build_agent_snapshot_for_dashboard(
    metrics: PortfolioMetricsResult,
    *,
    snapshots: pd.DataFrame,
    as_of_date: date,
) -> dict[str, Any]:
    base_snapshot = build_portfolio_metrics_snapshot(metrics, as_of_date=as_of_date)
    broker = _broker_snapshot_view_for_date(snapshots, as_of_date=as_of_date)
    if broker is None:
        return base_snapshot

    positions = _overlay_external_cost_metrics(
        broker["positions"],
        metrics,
        target_date=broker["snapshot_date"],
    )
    total_value = float(broker["total_market_value_base"])
    total_unrealized, total_return = _derive_broker_pnl_with_external_cost_basis(
        _daily_metrics(metrics),
        target_date=broker["snapshot_date"],
        total_market_value_base=total_value,
    )
    if total_unrealized is None or total_return is None:
        total_unrealized, total_return = _derive_totals_from_positions(
            positions,
            total_market_value_base=total_value,
        )

    daily_payload = dict(base_snapshot.get("daily") or {})
    daily_payload["valuation_date"] = broker["snapshot_date"].isoformat()
    daily_payload["total_market_value_base"] = round(total_value, 8)
    if total_unrealized is not None:
        daily_payload["total_unrealized_pnl_base"] = round(float(total_unrealized), 8)
    if total_return is not None:
        daily_payload["portfolio_return_pct"] = round(float(total_return), 8)

    selected_columns = [
        "asset_id",
        "asset_name",
        "asset_type",
        "isin",
        "quantity",
        "market_value_base",
        "cost_basis_base",
        "unrealized_pnl_base",
        "unrealized_return_pct",
        "weight",
        "valuation_status",
    ]
    for column in selected_columns:
        if column not in positions.columns:
            positions[column] = pd.NA
    positions_ready = positions.loc[:, selected_columns].sort_values(["weight", "asset_name"], ascending=[False, True])

    return {
        "as_of_date": broker["snapshot_date"].isoformat(),
        "base_currency": metrics.base_currency,
        "daily": _json_ready_value(daily_payload),
        "positions": _json_ready_value(positions_ready.to_dict(orient="records")),
    }


def _parse_target_weights_input(raw_text: str) -> dict[str, Any]:
    text = (raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_ready_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready_value(item) for item in value]
    if isinstance(value, (date, pd.Timestamp)):
        return value.isoformat()
    if value is None or pd.isna(value):
        return None
    return value


def _net_external_contributions_until(settings: Settings, *, as_of_date: date) -> float | None:
    repository = DuckDBMarketDataRepository(settings=settings)
    query = """
        SELECT SUM(
            CASE
                WHEN UPPER(movement_type) = 'DEPOSIT' THEN ABS(amount_base)
                WHEN UPPER(movement_type) = 'WITHDRAWAL' THEN -ABS(amount_base)
                ELSE 0
            END
        ) AS net_external
        FROM cash_movements
        WHERE amount_base IS NOT NULL
          AND UPPER(movement_type) IN ('DEPOSIT', 'WITHDRAWAL')
          AND COALESCE(value_date, movement_date) <= ?
    """
    with repository.connection() as connection:
        row = connection.execute(query, [as_of_date]).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _build_net_trade_flow_by_day(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions is None or transactions.empty:
        return pd.DataFrame(columns=["valuation_date", "flujo_operativo_dia"])

    frame = transactions.copy()
    frame["trade_date"] = pd.to_datetime(frame.get("trade_date"), errors="coerce").dt.date
    frame["transaction_type"] = frame["transaction_type"].fillna("").astype("string").str.upper()
    for column in ("gross_amount_base", "fees_amount_base", "taxes_amount_base"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce").fillna(0.0)

    frame["flujo_operativo"] = 0.0
    buy_mask = frame["transaction_type"] == "BUY"
    sell_mask = frame["transaction_type"] == "SELL"
    frame.loc[buy_mask, "flujo_operativo"] = -(
        frame.loc[buy_mask, "gross_amount_base"]
        + frame.loc[buy_mask, "fees_amount_base"]
        + frame.loc[buy_mask, "taxes_amount_base"]
    )
    frame.loc[sell_mask, "flujo_operativo"] = (
        frame.loc[sell_mask, "gross_amount_base"]
        - frame.loc[sell_mask, "fees_amount_base"]
        - frame.loc[sell_mask, "taxes_amount_base"]
    )
    frame = frame.loc[frame["transaction_type"].isin(["BUY", "SELL"])].dropna(subset=["trade_date"])
    if frame.empty:
        return pd.DataFrame(columns=["valuation_date", "flujo_operativo_dia"])
    return (
        frame.groupby("trade_date", as_index=False)
        .agg(flujo_operativo_dia=("flujo_operativo", "sum"))
        .rename(columns={"trade_date": "valuation_date"})
    )


def _build_value_with_trades_chart(
    *,
    daily_frame: pd.DataFrame,
    value_column: str,
    transactions: pd.DataFrame,
    base_currency: str,
) -> alt.LayerChart | None:
    if daily_frame.empty:
        return None

    ready_daily = daily_frame.copy()
    ready_daily["valuation_date"] = pd.to_datetime(ready_daily["valuation_date"], errors="coerce").dt.date
    ready_daily[value_column] = pd.to_numeric(ready_daily[value_column], errors="coerce")
    ready_daily = ready_daily.dropna(subset=["valuation_date", value_column]).sort_values("valuation_date")
    if ready_daily.empty:
        return None

    flows_by_day = _build_net_trade_flow_by_day(transactions)

    merged = ready_daily.loc[:, ["valuation_date", value_column]].merge(
        flows_by_day,
        on="valuation_date",
        how="left",
    )
    merged["flujo_operativo_dia"] = pd.to_numeric(merged["flujo_operativo_dia"], errors="coerce")
    merged["tipo_operativa"] = "Sin operativa"
    merged.loc[merged["flujo_operativo_dia"] < 0, "tipo_operativa"] = "Compra neta"
    merged.loc[merged["flujo_operativo_dia"] > 0, "tipo_operativa"] = "Venta neta"

    line = (
        alt.Chart(merged)
        .mark_line(color="#1d4ed8", strokeWidth=2.2)
        .encode(
            x=alt.X("valuation_date:T", title="Fecha"),
            y=alt.Y(f"{value_column}:Q", title=f"Valor ({base_currency})"),
            tooltip=[
                alt.Tooltip("valuation_date:T", title="Fecha"),
                alt.Tooltip(f"{value_column}:Q", title=f"Valor cartera ({base_currency})", format=",.2f"),
            ],
        )
    )

    markers_source = merged.loc[merged["flujo_operativo_dia"].notna() & (merged["flujo_operativo_dia"] != 0)].copy()
    if markers_source.empty:
        return line.properties(height=300)

    markers = (
        alt.Chart(markers_source)
        .mark_circle(size=85, opacity=0.95)
        .encode(
            x=alt.X("valuation_date:T"),
            y=alt.Y(f"{value_column}:Q"),
            color=alt.Color(
                "tipo_operativa:N",
                scale=alt.Scale(domain=["Compra neta", "Venta neta"], range=["#0f766e", "#dc2626"]),
                legend=alt.Legend(title="Operativa"),
            ),
            tooltip=[
                alt.Tooltip("valuation_date:T", title="Fecha"),
                alt.Tooltip(f"{value_column}:Q", title=f"Valor cartera ({base_currency})", format=",.2f"),
                alt.Tooltip("flujo_operativo_dia:Q", title=f"Flujo operativo dia ({base_currency})", format=",.2f"),
                alt.Tooltip("tipo_operativa:N", title="Tipo"),
            ],
        )
    )
    return alt.layer(line, markers).properties(height=300)


def _build_asset_evolution_frame(
    metrics: PortfolioMetricsResult,
    *,
    include_cash: bool,
    top_n: int,
    transactions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    positions = metrics.position_metrics.copy()
    if positions.empty:
        return pd.DataFrame()

    positions["valuation_date"] = pd.to_datetime(positions["valuation_date"], errors="coerce").dt.date
    positions["market_value_base"] = pd.to_numeric(positions["market_value_base"], errors="coerce")
    positions["quantity"] = pd.to_numeric(positions["quantity"], errors="coerce")
    positions["asset_name"] = positions["asset_name"].fillna(positions["asset_id"]).astype("string")
    if not include_cash:
        positions = positions.loc[positions["asset_type"].fillna("").astype(str).str.lower() != "cash"].copy()
    positions = positions.dropna(subset=["valuation_date", "asset_name", "market_value_base", "quantity"])
    positions = positions.loc[positions["quantity"] > 0].copy()
    first_buy_dates = _first_buy_dates_by_asset(transactions)
    if first_buy_dates:
        positions["first_buy_date"] = positions["asset_id"].astype(str).map(first_buy_dates)
        positions = positions.loc[
            positions["first_buy_date"].isna()
            | (positions["valuation_date"] >= positions["first_buy_date"])
        ].copy()
    if positions.empty:
        return pd.DataFrame()
    positions["unit_price_base"] = positions["market_value_base"] / positions["quantity"]
    positions = positions.dropna(subset=["unit_price_base"])
    positions = positions.loc[positions["unit_price_base"] > 0].copy()
    if positions.empty:
        return pd.DataFrame()

    latest_date = positions["valuation_date"].max()
    latest = (
        positions.loc[positions["valuation_date"] == latest_date, ["asset_name", "market_value_base"]]
        .groupby("asset_name", as_index=False)
        .agg(latest_value=("market_value_base", "sum"))
        .sort_values("latest_value", ascending=False)
        .head(top_n)
    )
    selected_assets = set(latest["asset_name"].tolist())
    filtered = positions.loc[positions["asset_name"].isin(selected_assets), ["valuation_date", "asset_name", "unit_price_base"]].copy()
    series = (
        filtered.groupby(["valuation_date", "asset_name"], as_index=False)
        .agg(value=("unit_price_base", "mean"))
        .pivot(index="valuation_date", columns="asset_name", values="value")
        .sort_index()
    )
    if series.empty:
        return pd.DataFrame()

    normalized = pd.DataFrame(index=series.index)
    for column in series.columns:
        current = pd.to_numeric(series[column], errors="coerce")
        first_valid = current.loc[current.notna() & (current != 0)]
        if first_valid.empty:
            continue
        base_value = float(first_valid.iloc[0])
        normalized[column] = ((current / base_value) - 1.0).mul(100.0).round(8)
    series = normalized

    series = series.dropna(axis=1, how="all")
    if series.empty:
        return pd.DataFrame()
    return series.reset_index()


def _first_buy_dates_by_asset(transactions: pd.DataFrame | None) -> dict[str, date]:
    if transactions is None or transactions.empty:
        return {}
    required_columns = {"asset_id", "trade_date", "transaction_type"}
    if not required_columns.issubset(transactions.columns):
        return {}

    frame = transactions.loc[:, ["asset_id", "trade_date", "transaction_type"]].copy()
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    frame["transaction_type"] = frame["transaction_type"].fillna("").astype("string").str.upper()
    frame = frame.loc[
        frame["asset_id"].notna()
        & frame["trade_date"].notna()
        & (frame["transaction_type"] == "BUY")
    ].copy()
    if frame.empty:
        return {}
    return frame.groupby("asset_id")["trade_date"].min().to_dict()


def _build_asset_evolution_chart(
    frame: pd.DataFrame,
    *,
    base_currency: str,
) -> alt.Chart | None:
    if frame.empty:
        return None

    long_frame = (
        frame.melt(id_vars=["valuation_date"], var_name="asset_name", value_name="value")
        .dropna(subset=["valuation_date", "asset_name", "value"])
        .copy()
    )
    if long_frame.empty:
        return None

    y_title = "Rentabilidad (%)"
    value_format = ".2f"
    line = (
        alt.Chart(long_frame)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("valuation_date:T", title="Fecha"),
            y=alt.Y("value:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=alt.Color("asset_name:N", title="Activo"),
            tooltip=[
                alt.Tooltip("valuation_date:T", title="Fecha"),
                alt.Tooltip("asset_name:N", title="Activo"),
                alt.Tooltip("value:Q", title=y_title, format=value_format),
            ],
        )
        .properties(height=320)
    )
    baseline = alt.Chart(pd.DataFrame({"y": [0.0]})).mark_rule(color="#94a3b8", strokeDash=[4, 4]).encode(y="y:Q")
    return alt.layer(baseline, line)
