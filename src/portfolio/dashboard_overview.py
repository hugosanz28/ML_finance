"""Overview and historical evolution Streamlit tabs."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from src.application import (
    GetNetExternalContributionsRequest,
    GetNetExternalContributionsUseCase,
    RefreshFxRequest,
    RefreshFxUseCase,
    RefreshMarketDataRequest,
    RefreshMarketDataUseCase,
)
from src.config import Settings
from src.portfolio.dashboard_common import (
    clear_dashboard_caches,
    dashboard_data_fingerprint,
    format_currency,
    format_pct,
    load_metrics,
    load_snapshots,
    load_transactions,
    metric_card,
    render_beginner_explainer,
    render_pending_import_warning,
    render_quality_warnings,
    section_header,
    show_metrics_error,
)
from src.portfolio.dashboard_transforms import (
    _build_asset_evolution_chart,
    _build_asset_evolution_frame,
    _build_value_with_trades_chart,
    _daily_metrics,
    _latest_broker_snapshot_view,
    _positions_for_date,
)


def render_portfolio_tab(settings: Settings) -> None:
    section_header(
        "Vista general",
        "Foto actual de la cartera: valor broker DEGIRO, reparto por activo y calidad de datos analiticos.",
    )
    render_market_refresh_control(settings)
    render_pending_import_warning(settings)

    data_fingerprint = dashboard_data_fingerprint(settings)
    metrics = load_metrics(settings, data_fingerprint)
    if metrics is None:
        show_metrics_error()
        return
    snapshots = load_snapshots(settings, data_fingerprint)

    daily = _daily_metrics(metrics)
    current_date = daily["valuation_date"].max()
    latest_external = daily.loc[daily["valuation_date"] == current_date].iloc[-1]
    broker_snapshot = _latest_broker_snapshot_view(snapshots)
    positions = _positions_for_date(metrics, current_date)
    total_value = float(latest_external["total_market_value_base"])
    total_unrealized = float(latest_external["total_unrealized_pnl_base"])
    total_return = float(latest_external["portfolio_return_pct"]) if pd.notna(latest_external["portfolio_return_pct"]) else None

    st.caption(f"Fecha de referencia: {current_date.isoformat()}")
    if broker_snapshot is not None:
        st.caption(f"Ultimo snapshot DEGIRO usado como ancla: {broker_snapshot['snapshot_date'].isoformat()}")
    net_contributions = GetNetExternalContributionsUseCase(settings=settings).execute(
        GetNetExternalContributionsRequest(as_of_date=current_date)
    ).net_external
    total_realized = None
    if net_contributions is not None and total_unrealized is not None and not pd.isna(total_unrealized):
        total_result_vs_contributions = float(total_value) - float(net_contributions)
        total_realized = total_result_vs_contributions - float(total_unrealized)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        metric_card("Valor total", format_currency(total_value, metrics.base_currency), "Valoracion actualizada con precio local DEGIRO anclado y variacion diaria de market data.")
    with c2:
        metric_card("PnL no realizado", format_currency(total_unrealized, metrics.base_currency), "Ganancia o perdida latente frente al coste conocido.")
    with c3:
        metric_card("PnL realizado", format_currency(total_realized, metrics.base_currency), "Resultado ya consolidado fuera de posiciones abiertas.")
    with c4:
        metric_card("Rentabilidad", format_pct(total_return), "PnL dividido entre el coste base conocido.")
    with c5:
        metric_card("Drawdown", format_pct(latest_external["drawdown_pct"]), "Caida desde el maximo historico de valor observado.")
    with c6:
        metric_card("Cobertura", format_pct(latest_external["valuation_coverage_ratio"]), "Porcentaje de posiciones con precio y divisa disponibles.")

    render_quality_warnings(latest_external)
    render_beginner_explainer()
    _render_allocation(metrics, positions)


def render_evolution_tab(settings: Settings) -> None:
    section_header(
        "Evolucion historica",
        "Evolucion del valor de cartera anclado a DEGIRO y dinamica de mercado entre snapshots.",
    )
    data_fingerprint = dashboard_data_fingerprint(settings)
    metrics = load_metrics(settings, data_fingerprint)
    if metrics is None:
        show_metrics_error()
        return

    daily = _daily_metrics(metrics).copy()
    daily["total_market_value_broker_anchored"] = daily["total_market_value_base"]
    daily_indexed = daily.set_index("valuation_date")
    transactions = load_transactions(settings)
    st.markdown("#### Valor de la cartera")
    st.caption(
        "Valor calculado por activo: DEGIRO fija el precio local de referencia en cada snapshot y market data "
        "aporta la variacion relativa diaria."
    )
    value_chart = _build_value_with_trades_chart(
        daily_frame=daily,
        value_column="total_market_value_broker_anchored",
        transactions=transactions,
        base_currency=metrics.base_currency,
    )
    if value_chart is None:
        st.line_chart(daily_indexed[["total_market_value_broker_anchored"]])
    else:
        st.altair_chart(value_chart, width="stretch")

    st.markdown("#### Evolucion por activo")
    include_cash = st.checkbox("Incluir posiciones de efectivo", value=False)
    top_n = st.slider("Numero maximo de activos", min_value=3, max_value=20, value=10, step=1)
    per_asset = _build_asset_evolution_frame(metrics, include_cash=include_cash, top_n=top_n, transactions=transactions)
    if per_asset.empty:
        st.info("No hay series de activos suficientes para esta vista.")
    else:
        st.caption("Cada linea muestra rentabilidad (%) desde la primera fecha en cartera de ese activo.")
        per_asset_chart = _build_asset_evolution_chart(per_asset, base_currency=metrics.base_currency)
        if per_asset_chart is None:
            st.line_chart(per_asset.set_index("valuation_date"))
        else:
            st.altair_chart(per_asset_chart, width="stretch")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Drawdown")
        st.caption("Cuanto cae la cartera desde su maximo anterior. Cuanto mas negativo, peor fue la caida.")
        st.line_chart(daily_indexed[["drawdown_pct"]])
    with c2:
        st.markdown("#### Cobertura de valoracion")
        st.caption("Proporcion de posiciones que se pudieron valorar con precio y FX.")
        st.line_chart(daily_indexed[["valuation_coverage_ratio"]])

    st.markdown("#### Datos diarios")
    st.dataframe(daily.sort_values("valuation_date", ascending=False), width="stretch", hide_index=True)


def render_market_refresh_control(settings: Settings) -> None:
    st.caption("La vista usa datos locales. Actualiza FX y precios para extender la valoracion hasta hoy.")
    left, right = st.columns([1, 4])
    with left:
        refresh_clicked = st.button("Actualizar a hoy", type="primary")
    with right:
        st.caption("No importa nuevos CSVs de DEGIRO; usa el ultimo snapshot como ancla.")

    if not refresh_clicked:
        return

    target_date = date.today()
    try:
        with st.spinner(f"Actualizando FX y precios hasta {target_date.isoformat()}..."):
            result = refresh_market_data_to_date(settings=settings, target_date=target_date)
    except Exception as exc:
        st.error(f"No se pudieron actualizar los datos: {exc}")
        return

    clear_dashboard_caches()
    fx_summary = result["fx_summary"]
    price_summary = result["price_summary"]
    st.success(
        "Actualizacion completada: "
        f"FX pares={fx_summary.updated_pairs}, fx_filas={fx_summary.total_records}; "
        f"activos={price_summary.updated_assets}, precio_filas={price_summary.total_records}."
    )


def refresh_market_data_to_date(*, settings: Settings, target_date: date) -> dict[str, Any]:
    fx_summary = RefreshFxUseCase(settings=settings).execute(
        RefreshFxRequest(end_date=target_date, only_missing_base=False)
    ).summary
    market_result = RefreshMarketDataUseCase(settings=settings).execute(
        RefreshMarketDataRequest(end_date=target_date)
    )
    if market_result.summary is None:
        raise RuntimeError(market_result.result.message)
    return {"target_date": target_date, "fx_summary": fx_summary, "price_summary": market_result.summary}


def _render_allocation(metrics, positions: pd.DataFrame) -> None:
    allocation = positions.loc[
        :,
        [
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
    allocation["weight_pct"] = allocation["weight"] * 100
    allocation = allocation.sort_values("weight", ascending=False)

    left, right = st.columns([2, 1])
    with left:
        st.markdown("#### Posiciones")
        st.caption("El peso indica que parte de la cartera representa cada activo.")
        st.dataframe(
            allocation,
            width="stretch",
            hide_index=True,
            column_config={
                "asset_name": "Activo",
                "asset_type": "Tipo",
                "quantity": st.column_config.NumberColumn("Cantidad", format="%.8f"),
                "market_value_base": st.column_config.NumberColumn(f"Valor ({metrics.base_currency})", format="%.2f"),
                "weight": st.column_config.ProgressColumn("Peso", format="%.2f", min_value=0.0, max_value=1.0),
                "cost_basis_base": st.column_config.NumberColumn(f"Coste ({metrics.base_currency})", format="%.2f"),
                "unrealized_pnl_base": st.column_config.NumberColumn(f"PnL ({metrics.base_currency})", format="%.2f"),
                "unrealized_return_pct": st.column_config.NumberColumn("Rentab", format="%.2f"),
                "valuation_status": "Estado",
                "weight_pct": None,
            },
        )
    with right:
        st.markdown("#### Concentracion")
        st.caption("Valor de las principales posiciones. Sirve para detectar dependencias excesivas de pocos activos.")
        chart_data = allocation.set_index("asset_name")["market_value_base"].head(12)
        st.bar_chart(chart_data)
