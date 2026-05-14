"""Initial Streamlit dashboard for portfolio review and monthly agents."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re
import sys
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents import load_investment_brief, run_monthly_agent_pipeline
from src.config import Settings, get_settings
from src.degiro_exports import import_degiro_exports, load_normalized_degiro_to_duckdb
from src.market_data import (
    DuckDBMarketDataRepository,
    FxRefreshService,
    PriceRefreshService,
    load_market_assets_from_normalized_degiro,
)
from src.portfolio import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
)
from src.reports import generate_monthly_report, get_latest_monthly_report
from src.portfolio.dashboard_uploads import (
    _canonical_degiro_upload_name,
    _detect_degiro_upload_kind,
    _extract_dates_from_filename,
    _friendly_degiro_kind,
    _save_uploaded_degiro_files,
)
from src.portfolio.dashboard_transforms import (
    _build_agent_snapshot_for_dashboard,
    _build_asset_evolution_chart,
    _build_asset_evolution_frame,
    _build_net_trade_flow_by_day,
    _build_value_with_trades_chart,
    _daily_metrics,
    _latest_broker_snapshot_view,
    _net_external_contributions_until,
    _parse_target_weights_input,
    _positions_for_date,
)


def _apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --mf-bg: #f7f8fb;
            --mf-panel: #ffffff;
            --mf-border: #d9dde7;
            --mf-text: #1b2430;
            --mf-muted: #5d6675;
            --mf-accent: #0f766e;
            --mf-accent-soft: #e6f4f1;
            --mf-warning: #8a5a00;
            --mf-warning-soft: #fff6df;
        }
        .stApp {
            background: var(--mf-bg);
            color: var(--mf-text);
        }
        .block-container {
            padding-top: 1.25rem;
            padding-bottom: 3rem;
            max-width: 1380px;
        }
        [data-testid="stSidebar"] {
            background: #ffffff;
            border-right: 1px solid var(--mf-border);
        }
        h1, h2, h3 {
            letter-spacing: 0;
        }
        div[data-testid="stMetric"] {
            background: var(--mf-panel);
            border: 1px solid var(--mf-border);
            border-radius: 8px;
            padding: 14px 16px;
            min-height: 112px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        div[data-testid="stMetricLabel"] {
            color: var(--mf-muted);
            font-size: 0.78rem;
        }
        div[data-testid="stMetricValue"] {
            font-size: 1.35rem;
            color: var(--mf-text);
        }
        .mf-hero {
            background: #ffffff;
            border: 1px solid var(--mf-border);
            border-radius: 8px;
            padding: 22px 24px;
            margin-bottom: 18px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .mf-hero h1 {
            margin: 0 0 6px 0;
            font-size: 2rem;
            line-height: 1.15;
        }
        .mf-hero p {
            margin: 0;
            color: var(--mf-muted);
            font-size: 1rem;
            max-width: 860px;
        }
        .mf-section {
            margin: 6px 0 18px 0;
        }
        .mf-section h2 {
            margin-bottom: 4px;
        }
        .mf-section p {
            color: var(--mf-muted);
            margin-top: 0;
        }
        .mf-help {
            background: var(--mf-accent-soft);
            border: 1px solid #b7ddd6;
            border-radius: 8px;
            padding: 14px 16px;
            color: #164e46;
            margin: 12px 0 18px 0;
        }
        .mf-help strong {
            color: #0f3f39;
        }
        .mf-step {
            background: #ffffff;
            border: 1px solid var(--mf-border);
            border-radius: 8px;
            padding: 12px 14px;
            min-height: 112px;
        }
        .mf-step p {
            color: var(--mf-muted);
            margin: 4px 0 0 0;
            font-size: 0.88rem;
        }
        .mf-pill {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 999px;
            background: #eef2f7;
            color: #334155;
            font-size: 0.78rem;
            margin-right: 6px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_hero() -> None:
    st.markdown(
        """
        <div class="mf-hero">
            <h1>ML_finance</h1>
            <p>
                Panel local para entender la cartera, actualizar datos de DEGIRO,
                revisar informes mensuales y ejecutar agentes de apoyo. La app
                no opera en el broker: solo organiza datos y genera recomendaciones para revision manual.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section_header(title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="mf-section">
            <h2>{title}</h2>
            <p>{description}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _metric_card(title: str, value: str, help_text: str) -> None:
    st.metric(title, value)
    st.caption(help_text)


def _render_beginner_explainer() -> None:
    with st.expander("Como leer esta pantalla", expanded=False):
        st.markdown(
            """
            - **Valor total**: ultima valoracion calculada con snapshots DEGIRO como ancla y market data como variacion.
            - **Peso**: porcentaje que representa cada activo. Ayuda a ver concentracion.
            - **PnL no realizado**: ganancia o perdida teorica frente al coste base conocido.
            - **PnL realizado**: resultado ya consolidado (ventas cerradas, dividendos y otros flujos no abiertos).
            - **Drawdown**: caida desde el maximo registrado. Es una forma simple de ver riesgo vivido.
            - **Cobertura**: refleja si la serie externa de precios/FX esta completa para analitica.
            """
        )


def main() -> None:
    """Render the dashboard."""
    st.set_page_config(page_title="ML_finance", layout="wide")
    _apply_theme()

    settings = get_settings()
    _render_hero()
    _render_sidebar(settings)

    tabs = st.tabs(["Vista general", "Evolucion", "Informes", "Actualizar datos", "Agentes"])
    with tabs[0]:
        _render_portfolio_tab(settings)
    with tabs[1]:
        _render_evolution_tab(settings)
    with tabs[2]:
        _render_reports_tab(settings)
    with tabs[3]:
        _render_update_tab(settings)
    with tabs[4]:
        _render_agents_tab(settings)


def _render_sidebar(settings: Settings) -> None:
    st.sidebar.markdown("### Estado local")
    st.sidebar.caption("Datos disponibles en la bodega local.")
    st.sidebar.caption(f"`{settings.portfolio_db_path}`")
    data_fingerprint = _dashboard_data_fingerprint(settings)
    counts = _warehouse_counts(settings, data_fingerprint)
    for key, value in counts.items():
        st.sidebar.metric(_friendly_table_name(key), value)

    if st.sidebar.button("Limpiar cache de dashboard"):
        st.cache_data.clear()
        st.rerun()


def _render_portfolio_tab(settings: Settings) -> None:
    _section_header(
        "Vista general",
        "Foto actual de la cartera: valor broker DEGIRO, reparto por activo y calidad de datos analiticos.",
    )
    _render_market_refresh_control(settings)

    data_fingerprint = _dashboard_data_fingerprint(settings)
    metrics = _load_metrics(settings, data_fingerprint)
    if metrics is None:
        _show_metrics_error()
        return
    snapshots = _load_snapshots(settings, data_fingerprint)

    daily = _daily_metrics(metrics)
    current_date = daily["valuation_date"].max()
    latest_external = daily.loc[daily["valuation_date"] == current_date].iloc[-1]
    broker_snapshot = _latest_broker_snapshot_view(snapshots)
    positions = _positions_for_date(metrics, current_date)
    total_value = float(latest_external["total_market_value_base"])
    total_unrealized = float(latest_external["total_unrealized_pnl_base"])
    total_return = float(latest_external["portfolio_return_pct"]) if pd.notna(latest_external["portfolio_return_pct"]) else None
    value_help = "Valoracion actualizada con precio local DEGIRO anclado y variacion diaria de market data."

    st.caption(f"Fecha de referencia: {current_date.isoformat()}")
    if broker_snapshot is not None:
        st.caption(f"Ultimo snapshot DEGIRO usado como ancla: {broker_snapshot['snapshot_date'].isoformat()}")
    net_contributions = _net_external_contributions_until(settings, as_of_date=current_date)
    total_realized = None
    if net_contributions is not None and total_unrealized is not None and not pd.isna(total_unrealized):
        total_result_vs_contributions = float(total_value) - float(net_contributions)
        total_realized = total_result_vs_contributions - float(total_unrealized)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        _metric_card(
            "Valor total",
            _format_currency(total_value, metrics.base_currency),
            value_help,
        )
    with c2:
        _metric_card(
            "PnL no realizado",
            _format_currency(total_unrealized, metrics.base_currency),
            "Ganancia o perdida latente frente al coste conocido.",
        )
    with c3:
        _metric_card(
            "PnL realizado",
            _format_currency(total_realized, metrics.base_currency),
            "Resultado ya consolidado fuera de posiciones abiertas.",
        )
    with c4:
        _metric_card(
            "Rentabilidad",
            _format_pct(total_return),
            "PnL dividido entre el coste base conocido.",
        )
    with c5:
        _metric_card(
            "Drawdown",
            _format_pct(latest_external["drawdown_pct"]),
            "Caida desde el maximo historico de valor observado.",
        )
    with c6:
        _metric_card(
            "Cobertura",
            _format_pct(latest_external["valuation_coverage_ratio"]),
            "Porcentaje de posiciones con precio y divisa disponibles.",
        )

    _render_quality_warnings(latest_external)
    _render_beginner_explainer()

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


def _render_evolution_tab(settings: Settings) -> None:
    _section_header(
        "Evolucion historica",
        "Evolucion del valor de cartera anclado a DEGIRO y dinamica de mercado entre snapshots.",
    )
    data_fingerprint = _dashboard_data_fingerprint(settings)
    metrics = _load_metrics(settings, data_fingerprint)
    if metrics is None:
        _show_metrics_error()
        return

    daily = _daily_metrics(metrics)
    daily = daily.copy()
    daily["total_market_value_broker_anchored"] = daily["total_market_value_base"]
    daily_indexed = daily.set_index("valuation_date")
    transactions = _load_transactions(settings)
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
    per_asset = _build_asset_evolution_frame(
        metrics,
        include_cash=include_cash,
        top_n=top_n,
        transactions=transactions,
    )
    if per_asset.empty:
        st.info("No hay series de activos suficientes para esta vista.")
    else:
        st.caption("Cada linea muestra rentabilidad (%) desde la primera fecha en cartera de ese activo.")
        per_asset_chart = _build_asset_evolution_chart(
            per_asset,
            base_currency=metrics.base_currency,
        )
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
    st.dataframe(
        daily.sort_values("valuation_date", ascending=False),
        width="stretch",
        hide_index=True,
    )


def _render_reports_tab(settings: Settings) -> None:
    _section_header(
        "Informes",
        "Resumen mensual en Markdown: cartera, cambios por periodo, actividad y notas de cobertura.",
    )
    reports = _list_reports(settings)
    if not reports:
        st.info("No hay informes generados todavia.")
        if st.button("Generar informe mensual"):
            _generate_report_action(settings)
        return

    selected = st.selectbox(
        "Informe",
        options=reports,
        format_func=lambda item: f"{item['label']} - {item['path'].name}",
    )
    report_path = selected["path"]
    st.caption(str(report_path))

    if st.button("Generar nuevo informe mensual"):
        _generate_report_action(settings)
        st.rerun()

    with st.container(border=True):
        st.markdown(report_path.read_text(encoding="utf-8"))


def _render_update_tab(settings: Settings) -> None:
    _section_header(
        "Actualizar datos",
        "Sube exportaciones de DEGIRO y ejecuta el pipeline local. Ninguna operacion se envia al broker.",
    )

    st.markdown("#### 1. Entrada de CSVs")
    st.caption(
        "Puedes subirlos aqui o copiarlos manualmente a `src/degiro_exports/local/incoming/`. "
        "Al subirlos desde la UI se renombran al formato que exige el importador."
    )
    uploaded_files = st.file_uploader(
        "Subir CSVs de DEGIRO",
        type=["csv"],
        accept_multiple_files=True,
    )
    if uploaded_files and st.button("Guardar CSVs en incoming"):
        outcomes = _save_uploaded_degiro_files(uploaded_files, settings=settings, uploaded_at=date.today())
        saved_count = sum(outcome["status"] == "guardado" for outcome in outcomes)
        skipped_count = sum(outcome["status"] == "omitido" for outcome in outcomes)
        st.dataframe(pd.DataFrame(outcomes), hide_index=True, width="stretch")
        if saved_count:
            st.success(f"Guardados {saved_count} CSVs normalizados en {settings.degiro_exports_dir / 'incoming'}")
        if skipped_count:
            st.warning("Algunos CSVs no se guardaron porque el tipo no se pudo detectar por el nombre.")

    st.divider()
    st.markdown("#### 2. Pipeline paso a paso")
    st.caption("Ejecuta los pasos en orden si quieres revisar cada etapa.")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown("**Importar**")
        st.caption("Convierte CSVs a parquets normalizados y carga DuckDB.")
        if st.button("1. Importar DEGIRO"):
            with st.spinner("Importando CSVs y cargando DuckDB..."):
                summary = import_degiro_exports(settings=settings)
                warehouse = load_normalized_degiro_to_duckdb(settings=settings)
            st.success(
                f"Importados={summary.imported_count}; DuckDB tx={warehouse.transactions}, "
                f"cash={warehouse.cash_movements}, snapshots={warehouse.portfolio_snapshots}"
            )
            st.cache_data.clear()
    with c2:
        st.markdown("**FX**")
        st.caption("Descarga tipos de cambio para valorar posiciones no EUR.")
        only_missing = st.checkbox("Solo huecos FX", value=True)
        if st.button("2. Refrescar FX"):
            with st.spinner("Consultando FX..."):
                fx_summary = FxRefreshService(settings=settings).refresh_rates(only_missing_base=only_missing)
            st.success(f"Pares actualizados={fx_summary.updated_pairs}; filas={fx_summary.total_records}")
            st.cache_data.clear()
    with c3:
        st.markdown("**Precios**")
        st.caption("Actualiza precios diarios usando tickers y overrides.")
        if st.button("3. Refrescar precios"):
            with st.spinner("Consultando market data..."):
                repository = DuckDBMarketDataRepository(settings=settings)
                assets = _load_refresh_assets(settings=settings, repository=repository)
                start_date = _derive_start_date(assets)
                price_summary = PriceRefreshService(repository=repository, settings=settings).refresh_prices(
                    start_date=start_date,
                    end_date=date.today(),
                )
            st.success(f"Activos actualizados={price_summary.updated_assets}; filas={price_summary.total_records}")
            st.cache_data.clear()
    with c4:
        st.markdown("**Informe**")
        st.caption("Genera el informe mensual que consumen los agentes.")
        if st.button("4. Generar informe"):
            _generate_report_action(settings)
            st.cache_data.clear()

    st.divider()
    st.markdown("#### Flujo rapido")
    st.caption("Lanza datos, FX, precios e informe en una sola accion. Los agentes se ejecutan despues desde su pestaña.")
    if st.button("Ejecutar flujo mensual basico"):
        with st.spinner("Ejecutando importacion, FX, precios e informe..."):
            import_summary = import_degiro_exports(settings=settings)
            warehouse = load_normalized_degiro_to_duckdb(settings=settings)
            fx_summary = FxRefreshService(settings=settings).refresh_rates(only_missing_base=True)
            assets = DuckDBMarketDataRepository(settings=settings).list_assets(active_only=True)
            price_summary = PriceRefreshService(settings=settings).refresh_prices(
                start_date=_derive_start_date(assets),
                end_date=date.today(),
            )
            report = generate_monthly_report(settings=settings)
        st.success(
            "Flujo completado: "
            f"imported={import_summary.imported_count}, tx={warehouse.transactions}, "
            f"fx_rows={fx_summary.total_records}, price_rows={price_summary.total_records}, "
            f"report={report.output_path.name if report.output_path else '-'}"
        )
        st.cache_data.clear()


def _render_market_refresh_control(settings: Settings) -> None:
    st.caption(
        "La vista usa datos locales. Actualiza FX y precios para extender la valoracion hasta hoy."
    )
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
            result = _refresh_market_data_to_date(settings=settings, target_date=target_date)
    except Exception as exc:
        st.error(f"No se pudieron actualizar los datos: {exc}")
        return

    st.cache_data.clear()
    fx_summary = result["fx_summary"]
    price_summary = result["price_summary"]
    st.success(
        "Actualizacion completada: "
        f"FX pares={fx_summary.updated_pairs}, fx_filas={fx_summary.total_records}; "
        f"activos={price_summary.updated_assets}, precio_filas={price_summary.total_records}."
    )


def _refresh_market_data_to_date(*, settings: Settings, target_date: date) -> dict[str, Any]:
    repository = DuckDBMarketDataRepository(settings=settings)
    fx_summary = FxRefreshService(repository=repository, settings=settings).refresh_rates(
        end_date=target_date,
        only_missing_base=False,
    )
    assets = _load_refresh_assets(settings=settings, repository=repository)
    price_summary = PriceRefreshService(repository=repository, settings=settings).refresh_prices(
        start_date=_derive_start_date(assets),
        end_date=target_date,
    )
    return {
        "target_date": target_date,
        "fx_summary": fx_summary,
        "price_summary": price_summary,
    }


def _load_refresh_assets(*, settings: Settings, repository: DuckDBMarketDataRepository):
    assets = list(repository.list_assets(active_only=True))
    known_asset_ids = {asset.asset_id for asset in assets}
    for asset in load_market_assets_from_normalized_degiro(settings=settings):
        if asset.is_active and asset.asset_id not in known_asset_ids:
            assets.append(asset)
            known_asset_ids.add(asset.asset_id)
    return assets


def _render_agents_tab(settings: Settings) -> None:
    _section_header(
        "Agentes",
        "Pipeline mensual: contexto tematico -> juicio por activo -> recomendacion mensual.",
    )
    data_fingerprint = _dashboard_data_fingerprint(settings)
    metrics = _load_metrics(settings, data_fingerprint)
    if metrics is None:
        _show_metrics_error()
        return
    snapshots = _load_snapshots(settings, data_fingerprint)

    st.markdown(
        """
        **Flujo entre agentes**
        1. `monitor_tematico`: extrae contexto externo relevante para tu cartera.
        2. `analista_activos`: evalua encaje y riesgo de cada activo con el mandato.
        3. `asistente_aportacion_mensual`: sintetiza y propone accion mensual.
        """
    )

    left, right = st.columns([2, 1])
    with left:
        default_brief = _read_default_brief(settings)
        investment_brief = st.text_area("Investment brief", value=default_brief, height=240)
        st.caption("Este texto es el mandato de la cuenta: objetivo, horizonte, tolerancia al riesgo y reglas personales.")
        if st.button("Guardar investment brief"):
            settings.investment_brief_path.parent.mkdir(parents=True, exist_ok=True)
            settings.investment_brief_path.write_text(investment_brief, encoding="utf-8")
            st.success(f"Guardado en {settings.investment_brief_path}")
    with right:
        st.markdown("#### Configuracion")
        user_interest = st.text_input("Idea puntual de satellite")
        monthly_budget = st.number_input(
            "Monthly budget (EUR)",
            min_value=0.0,
            value=float(settings.monthly_contribution_eur),
            step=50.0,
            help="Presupuesto mensual que recibira `asistente_aportacion_mensual` para proponer acciones del mes.",
        )
        send_target_weights = st.checkbox(
            "Enviar target_weights al pipeline",
            value=True,
            help="Si lo desactivas, los agentes no reciben pesos objetivo y evaluan sin esa restriccion.",
        )
        target_weights_text = st.text_area(
            "Pesos objetivo (JSON opcional)",
            value='{"core": 0.80, "satellite": 0.20}',
            height=110,
            disabled=not send_target_weights,
            help="Se pasa a `asistente_aportacion_mensual` para evaluar rebalanceo con criterio cuantitativo.",
        )
        llm_provider = st.selectbox("LLM provider", options=["static", "openai"], index=0)
        search_provider = st.selectbox("Search provider", options=["null", "duckduckgo"], index=0)
        st.caption("Usa `static/null` para demo sin coste ni red. Usa `openai/duckduckgo` para una ejecucion real.")
    target_weights = _parse_target_weights_input(target_weights_text) if send_target_weights else {}
    target_weights_invalid = send_target_weights and target_weights_text.strip() and not target_weights
    if target_weights_invalid:
        st.warning("`Pesos objetivo` no se pudo parsear como JSON valido; se ejecutara sin pesos objetivo.")

    reports = _list_reports(settings)
    report_option = None
    report_text_input = ""
    report_as_of = metrics.end_date
    if reports:
        report_option = st.selectbox(
            "latest_monthly_report (fuente)",
            options=reports,
            format_func=lambda item: f"{item['label']} - {item['path'].name}",
        )
        report_path = Path(report_option["path"])
        report_text_input = st.text_area(
            "latest_monthly_report (editable)",
            value=report_path.read_text(encoding="utf-8"),
            height=240,
        )
        report_as_of = report_option.get("as_of_date") or metrics.end_date
        st.caption(f"Fuente seleccionada: {report_path}")
    else:
        st.warning("No hay informes `.md` detectados en reports_history/reports. Genera uno primero.")

    snapshot_default = _build_agent_snapshot_for_dashboard(
        metrics,
        snapshots=snapshots,
        as_of_date=report_as_of,
    )
    snapshot_text_input = st.text_area(
        "portfolio_metrics_snapshot (JSON editable)",
        value=json.dumps(snapshot_default, ensure_ascii=False, indent=2),
        height=240,
    )

    with st.expander("Inputs que recibiran los agentes", expanded=True):
        st.markdown("#### Resumen de inputs")
        st.markdown(
            "- `investment_brief`\n"
            "- `latest_monthly_report` (editable arriba)\n"
            "- `portfolio_metrics_snapshot` (JSON editable arriba)\n"
            "- `monthly_budget` (EUR, editable en configuracion)\n"
            "- `target_weights` (opcional)\n"
            "- `user_satellite_interest` (opcional)"
        )
        st.markdown("#### Flujo de ejecucion")
        st.markdown(
            "1. `monitor_tematico` lee brief+informe+snapshot y genera contexto.\n"
            "2. `analista_activos` consume lo anterior y juzga encaje por activo.\n"
            "3. `asistente_aportacion_mensual` sintetiza ambos y propone accion mensual."
        )
        st.markdown("#### Target weights (opcional)")
        if not send_target_weights:
            st.json({"_status": "desactivado_por_usuario"})
        else:
            st.json(target_weights if target_weights else {"_status": "no definido"})

    if st.button("Ejecutar red de agentes"):
        if report_option is None:
            st.error("No hay `latest_monthly_report` seleccionado. Genera o selecciona un informe primero.")
            return
        try:
            portfolio_metrics_snapshot = json.loads(snapshot_text_input)
            if not isinstance(portfolio_metrics_snapshot, dict):
                raise ValueError("snapshot_no_dict")
        except Exception:
            st.error("`portfolio_metrics_snapshot` no es JSON valido.")
            return
        if target_weights_invalid:
            st.error("`target_weights` esta activado pero no es un JSON valido.")
            return
        override_dir = settings.data_dir / "agents" / "input_overrides"
        override_dir.mkdir(parents=True, exist_ok=True)
        report_override_path = override_dir / "latest_monthly_report_override.md"
        report_override_path.write_text(report_text_input, encoding="utf-8")

        request_parameters: dict[str, Any] = {}
        if send_target_weights and target_weights:
            request_parameters["target_weights"] = target_weights
        with st.spinner("Ejecutando agentes..."):
            result = run_monthly_agent_pipeline(
                settings=settings,
                investment_brief_text=investment_brief,
                monthly_report_path=report_override_path,
                user_satellite_interest=user_interest or None,
                monthly_budget=float(monthly_budget),
                llm_provider=llm_provider,
                search_provider=search_provider,
                persist=True,
                request_parameters=request_parameters,
                portfolio_metrics_snapshot=portfolio_metrics_snapshot,
            )
        st.success(f"Run {result.run_id} guardado en {result.output_dir}")
        _render_agent_result("monitor_tematico", result.monitor_tematico)
        _render_agent_result("analista_activos", result.analista_activos)
        _render_agent_result("asistente_aportacion_mensual", result.asistente_aportacion_mensual)


def _render_agent_result(name: str, result) -> None:
    with st.expander(f"{name}: {result.status}", expanded=True):
        st.write(result.summary)
        if result.warnings:
            st.warning("\n".join(f"- {warning}" for warning in result.warnings))
        if result.errors:
            st.error("\n".join(f"- {error}" for error in result.errors))
        if result.findings:
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "title": finding.title,
                            "category": finding.category,
                            "severity": finding.severity,
                            "asset_id": finding.asset_id,
                            "detail": finding.detail,
                        }
                        for finding in result.findings
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
        st.json(
            {
                "metadata": dict(result.metadata),
                "sources": [source.location for source in result.sources],
                "artifacts": [artifact.title for artifact in result.artifacts],
            }
        )


@st.cache_data(show_spinner=False)
def _load_metrics(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> PortfolioMetricsResult | None:
    try:
        return calculate_portfolio_metrics_from_normalized_degiro(settings=_settings)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def _load_snapshots(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> pd.DataFrame:
    return load_normalized_degiro_snapshots(settings=_settings)


@st.cache_data(show_spinner=False)
def _warehouse_counts(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> dict[str, int]:
    repository = DuckDBMarketDataRepository(settings=_settings)
    tables = ("assets_master", "transactions", "cash_movements", "portfolio_snapshots", "prices_daily", "fx_rates")
    counts: dict[str, int] = {}
    with repository.connection() as connection:
        for table in tables:
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return counts


def _dashboard_data_fingerprint(settings: Settings) -> tuple[tuple[str, int, int], ...]:
    paths: list[Path] = [settings.portfolio_db_path]
    normalized_degiro_dir = settings.normalized_data_dir / "degiro"
    if normalized_degiro_dir.exists():
        paths.extend(sorted(normalized_degiro_dir.rglob("*.parquet")))
    return tuple(_file_fingerprint(path) for path in paths if path.exists())


def _file_fingerprint(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return (str(path), stat.st_mtime_ns, stat.st_size)


def _load_transactions(_settings: Settings) -> pd.DataFrame:
    return load_normalized_degiro_transactions(settings=_settings)


def _render_quality_warnings(latest: pd.Series) -> None:
    warnings: list[str] = []
    if int(latest["missing_price_positions_count"]) > 0:
        warnings.append(f"Faltan precios para {int(latest['missing_price_positions_count'])} posiciones.")
    if int(latest["missing_fx_positions_count"]) > 0:
        warnings.append(f"Faltan FX para {int(latest['missing_fx_positions_count'])} posiciones.")
    if float(latest["valuation_coverage_ratio"]) < 1.0:
        warnings.append("La cobertura de valoracion no es completa.")
    if warnings:
        st.warning("\n".join(f"- {warning}" for warning in warnings))


def _list_reports(settings: Settings) -> list[dict[str, Any]]:
    by_path: dict[Path, dict[str, Any]] = {}
    latest = get_latest_monthly_report(settings=settings)
    if latest is not None:
        path = Path(latest.report_path).expanduser().resolve()
        if path.exists():
            by_path[path] = {
                "label": "history latest",
                "path": path,
                "as_of_date": latest.as_of_date,
                "source": "history",
            }

    if settings.reports_dir.exists():
        for path in sorted(settings.reports_dir.glob("*.md"), reverse=True):
            resolved = path.resolve()
            as_of_date = _extract_report_as_of_date_from_path(resolved)
            if resolved in by_path:
                if by_path[resolved].get("as_of_date") is None:
                    by_path[resolved]["as_of_date"] = as_of_date
                continue
            by_path[resolved] = {
                "label": "file",
                "path": resolved,
                "as_of_date": as_of_date,
                "source": "reports_dir",
            }

    reports = list(by_path.values())
    reports.sort(
        key=lambda item: (
            item.get("as_of_date") or date.min,
            item["path"].stat().st_mtime if item.get("path") and item["path"].exists() else 0.0,
        ),
        reverse=True,
    )
    for item in reports:
        as_of = item.get("as_of_date")
        as_of_label = as_of.isoformat() if isinstance(as_of, date) else "sin_fecha"
        item["label"] = f"{as_of_label} | {item.get('source', 'file')}"
    return reports


def _extract_report_as_of_date_from_path(path: Path) -> date | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", path.stem)
    if match is None:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _generate_report_action(settings: Settings) -> None:
    with st.spinner("Generando informe mensual..."):
        report = generate_monthly_report(settings=settings)
    st.success(f"Informe generado: {report.output_path}")


def _derive_start_date(assets) -> date:
    dates = [asset.first_seen_date for asset in assets if asset.first_seen_date is not None]
    return min(dates) if dates else date.today()


def _read_default_brief(settings: Settings) -> str:
    try:
        return load_investment_brief(settings=settings)
    except FileNotFoundError:
        return ""


def _show_metrics_error() -> None:
    st.error("No se pudieron calcular metricas. Importa DEGIRO, refresca FX/precios o revisa los normalizados.")


def _format_currency(value: object, currency: str) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f} {currency}"


def _format_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def _friendly_table_name(table_name: str) -> str:
    return {
        "assets_master": "Activos",
        "transactions": "Transacciones",
        "cash_movements": "Mov. efectivo",
        "portfolio_snapshots": "Snapshots",
        "prices_daily": "Precios",
        "fx_rates": "FX",
    }.get(table_name, table_name)


if __name__ == "__main__":
    main()
