"""Initial Streamlit dashboard for portfolio review and monthly agents."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re
import sys
import unicodedata
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents import build_portfolio_metrics_snapshot, load_investment_brief, run_monthly_agent_pipeline
from src.config import Settings, get_settings
from src.degiro_exports import import_degiro_exports, load_normalized_degiro_to_duckdb
from src.market_data import DuckDBMarketDataRepository, FxRefreshService, PriceRefreshService
from src.portfolio import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
)
from src.reports import generate_monthly_report, get_latest_monthly_report


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
            - **Valor total**: ultimo valor del broker (DEGIRO) cuando hay snapshot disponible.
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
    counts = _warehouse_counts(settings)
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
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return
    snapshots = _load_snapshots(settings)

    daily = _daily_metrics(metrics)
    current_date = daily["valuation_date"].max()
    latest_external = daily.loc[daily["valuation_date"] == current_date].iloc[-1]
    broker_snapshot = _latest_broker_snapshot_view(snapshots)
    value_help = "Valor de cartera segun el ultimo snapshot de DEGIRO disponible."
    if broker_snapshot is not None:
        current_date = broker_snapshot["snapshot_date"]
        positions = broker_snapshot["positions"]
        positions = _overlay_external_cost_metrics(positions, metrics, target_date=current_date)
        total_value = broker_snapshot["total_market_value_base"]
        total_unrealized, total_return = _derive_broker_pnl_with_external_cost_basis(
            daily,
            target_date=current_date,
            total_market_value_base=total_value,
        )
        if total_unrealized is None or total_return is None:
            total_unrealized, total_return = _derive_totals_from_positions(
                positions,
                total_market_value_base=total_value,
            )
    else:
        positions = _positions_for_date(metrics, current_date)
        total_value = float(latest_external["total_market_value_base"])
        total_unrealized = float(latest_external["total_unrealized_pnl_base"])
        total_return = float(latest_external["portfolio_return_pct"]) if pd.notna(latest_external["portfolio_return_pct"]) else None
        value_help = "No hay snapshot DEGIRO; se muestra valoracion externa."

    st.caption(f"Fecha de referencia: {current_date.isoformat()}")
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
            use_container_width=True,
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
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return

    daily = _daily_metrics(metrics)
    snapshots = _load_snapshots(settings)
    anchored_daily, anchor_note = _build_broker_anchored_daily_series(daily, snapshots)
    daily_indexed = anchored_daily.set_index("valuation_date")
    transactions = _load_transactions(settings)
    st.markdown("#### Valor de la cartera")
    st.caption(
        "Linea anclada al ultimo snapshot DEGIRO: el nivel absoluto viene del broker y la variacion diaria "
        "la aporta market data."
    )
    if anchor_note:
        st.caption(anchor_note)
    value_chart = _build_value_with_trades_chart(
        daily_frame=anchored_daily,
        value_column="total_market_value_broker_anchored",
        transactions=transactions,
        base_currency=metrics.base_currency,
    )
    if value_chart is None:
        st.line_chart(daily_indexed[["total_market_value_broker_anchored"]])
    else:
        st.altair_chart(value_chart, use_container_width=True)

    st.markdown("#### Evolucion por activo")
    include_cash = st.checkbox("Incluir posiciones de efectivo", value=False)
    top_n = st.slider("Numero maximo de activos", min_value=3, max_value=20, value=10, step=1)
    per_asset = _build_asset_evolution_frame(metrics, include_cash=include_cash, top_n=top_n)
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
            st.altair_chart(per_asset_chart, use_container_width=True)

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
        anchored_daily.sort_values("valuation_date", ascending=False),
        use_container_width=True,
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
        st.dataframe(pd.DataFrame(outcomes), hide_index=True, use_container_width=True)
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
                assets = DuckDBMarketDataRepository(settings=settings).list_assets(active_only=True)
                start_date = _derive_start_date(assets)
                price_summary = PriceRefreshService(settings=settings).refresh_prices(
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


def _render_agents_tab(settings: Settings) -> None:
    _section_header(
        "Agentes",
        "Pipeline mensual: contexto tematico -> juicio por activo -> recomendacion mensual.",
    )
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return
    snapshots = _load_snapshots(settings)

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
                use_container_width=True,
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
def _load_metrics(_settings: Settings) -> PortfolioMetricsResult | None:
    try:
        return calculate_portfolio_metrics_from_normalized_degiro(settings=_settings)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def _load_snapshots(_settings: Settings) -> pd.DataFrame:
    return load_normalized_degiro_snapshots(settings=_settings)


@st.cache_data(show_spinner=False)
def _warehouse_counts(_settings: Settings) -> dict[str, int]:
    repository = DuckDBMarketDataRepository(settings=_settings)
    tables = ("assets_master", "transactions", "cash_movements", "portfolio_snapshots", "prices_daily", "fx_rates")
    counts: dict[str, int] = {}
    with repository.connection() as connection:
        for table in tables:
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return counts


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


@st.cache_data(show_spinner=False)
def _load_transactions(_settings: Settings) -> pd.DataFrame:
    return load_normalized_degiro_transactions(settings=_settings)


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


def _save_uploaded_degiro_files(uploaded_files: list[Any], *, settings: Settings, uploaded_at: date) -> list[dict[str, str]]:
    incoming_dir = settings.degiro_exports_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    outcomes: list[dict[str, str]] = []
    for uploaded_file in uploaded_files:
        canonical_name = _canonical_degiro_upload_name(uploaded_file.name, fallback_date=uploaded_at)
        if canonical_name is None:
            outcomes.append(
                {
                    "archivo_original": uploaded_file.name,
                    "tipo_detectado": "desconocido",
                    "guardado_como": "",
                    "status": "omitido",
                    "detalle": "Renombra el archivo incluyendo cartera/portfolio, transacciones/transactions o cuenta/account.",
                }
            )
            continue

        target_path = incoming_dir / canonical_name
        existed = target_path.exists()
        target_path.write_bytes(uploaded_file.getbuffer())
        outcomes.append(
            {
                "archivo_original": uploaded_file.name,
                "tipo_detectado": _friendly_degiro_kind(canonical_name),
                "guardado_como": canonical_name,
                "status": "guardado",
                "detalle": "Sobrescrito" if existed else "Nuevo",
            }
        )

    return outcomes


def _canonical_degiro_upload_name(filename: str, *, fallback_date: date) -> str | None:
    kind = _detect_degiro_upload_kind(filename)
    if kind is None:
        return None

    dates = _extract_dates_from_filename(filename)
    if kind == "portfolio":
        snapshot_date = max(dates) if dates else fallback_date
        return f"portfolio_{snapshot_date.isoformat()}.csv"

    date_from, date_to = _date_range_from_filename_dates(dates, fallback_date=fallback_date)
    if kind == "transactions":
        return f"transactions_{date_from.isoformat()}_{date_to.isoformat()}.csv"
    return f"account_{date_from.isoformat()}_{date_to.isoformat()}.csv"


def _detect_degiro_upload_kind(filename: str) -> str | None:
    normalized = _normalize_filename_text(filename)
    portfolio_tokens = ("portfolio", "cartera", "posiciones", "positions", "snapshot")
    transaction_tokens = ("transactions", "transaction", "transacciones", "transaccion", "operaciones", "ordenes", "orders")
    account_tokens = ("account", "cuenta", "cash", "efectivo", "movimientos", "actividad", "activity")

    if any(token in normalized for token in portfolio_tokens):
        return "portfolio"
    if any(token in normalized for token in transaction_tokens):
        return "transactions"
    if any(token in normalized for token in account_tokens):
        return "account"
    return None


def _extract_dates_from_filename(filename: str) -> list[date]:
    dates: list[date] = []
    seen: set[date] = set()
    normalized = _normalize_filename_text(filename)

    patterns = (
        (re.compile(r"(?<!\d)(\d{4})[-_.](\d{1,2})[-_.](\d{1,2})(?!\d)"), "ymd"),
        (re.compile(r"(?<!\d)(\d{1,2})[-_.](\d{1,2})[-_.](\d{4})(?!\d)"), "dmy"),
        (re.compile(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)"), "compact_ymd"),
        (re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)"), "compact_dmy"),
    )
    for pattern, order in patterns:
        for match in pattern.finditer(normalized):
            parsed = _parse_filename_date(match.groups(), order)
            if parsed is not None and parsed not in seen:
                dates.append(parsed)
                seen.add(parsed)
    return sorted(dates)


def _parse_filename_date(parts: tuple[str, ...], order: str) -> date | None:
    try:
        if order in {"ymd", "compact_ymd"}:
            year, month, day = (int(part) for part in parts)
        else:
            day, month, year = (int(part) for part in parts)
        return date(year, month, day)
    except ValueError:
        return None


def _date_range_from_filename_dates(dates: list[date], *, fallback_date: date) -> tuple[date, date]:
    if not dates:
        return fallback_date, fallback_date
    return min(dates), max(dates)


def _normalize_filename_text(filename: str) -> str:
    without_accents = unicodedata.normalize("NFKD", filename).encode("ascii", "ignore").decode("ascii")
    return without_accents.lower()


def _friendly_degiro_kind(canonical_name: str) -> str:
    if canonical_name.startswith("transactions_"):
        return "transacciones"
    if canonical_name.startswith("account_"):
        return "cuenta / efectivo"
    if canonical_name.startswith("portfolio_"):
        return "cartera"
    return "desconocido"


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
