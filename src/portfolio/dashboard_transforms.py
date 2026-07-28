"""Data transforms and chart builders for the Streamlit dashboard."""

from __future__ import annotations

from datetime import date
import json
from typing import Any

import altair as alt
import pandas as pd

from src.portfolio import PortfolioMetricsResult
from src.portfolio.state_projection import latest_broker_snapshot_view as _latest_broker_snapshot_view


def _daily_metrics(metrics: PortfolioMetricsResult) -> pd.DataFrame:
    frame = metrics.portfolio_daily_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    return frame.sort_values("valuation_date")


def _positions_for_date(metrics: PortfolioMetricsResult, valuation_date: date) -> pd.DataFrame:
    frame = metrics.position_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    return frame.loc[frame["valuation_date"] == valuation_date].copy()


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


def _parse_target_weights_input(raw_text: str) -> dict[str, Any]:
    text = (raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


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
