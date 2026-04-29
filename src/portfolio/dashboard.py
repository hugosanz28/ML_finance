"""Initial Streamlit dashboard for portfolio review and monthly agents."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re
import sys
import unicodedata
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents import build_portfolio_metrics_snapshot, load_investment_brief, run_monthly_agent_pipeline
from src.config import Settings, get_settings
from src.degiro_exports import import_degiro_exports, load_normalized_degiro_to_duckdb
from src.market_data import DuckDBMarketDataRepository, FxRefreshService, PriceRefreshService
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
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
            - **Valor total**: estimacion actual de la cartera en la moneda base.
            - **Peso**: porcentaje que representa cada activo. Ayuda a ver concentracion.
            - **PnL no realizado**: ganancia o perdida teorica si se vendiera al precio usado.
            - **Drawdown**: caida desde el maximo registrado. Es una forma simple de ver riesgo vivido.
            - **Cobertura**: si esta por debajo de 100%, faltan precios o tipos de cambio para valorar algo.
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
        "Foto actual de la cartera: cuanto vale, como se reparte y si los datos usados para valorar son completos.",
    )
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return

    daily = _daily_metrics(metrics)
    current_date = daily["valuation_date"].max()
    latest = daily.loc[daily["valuation_date"] == current_date].iloc[-1]
    positions = _positions_for_date(metrics, current_date)

    st.caption(f"Fecha de referencia: {current_date.isoformat()}")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        _metric_card(
            "Valor total",
            _format_currency(latest["total_market_value_base"], metrics.base_currency),
            "Valor estimado de todas las posiciones en moneda base.",
        )
    with c2:
        _metric_card(
            "PnL no realizado",
            _format_currency(latest["total_unrealized_pnl_base"], metrics.base_currency),
            "Ganancia o perdida latente frente al coste conocido.",
        )
    with c3:
        _metric_card(
            "Rentabilidad",
            _format_pct(latest["portfolio_return_pct"]),
            "PnL dividido entre el coste base conocido.",
        )
    with c4:
        _metric_card(
            "Drawdown",
            _format_pct(latest["drawdown_pct"]),
            "Caida desde el maximo historico de valor observado.",
        )
    with c5:
        _metric_card(
            "Cobertura",
            _format_pct(latest["valuation_coverage_ratio"]),
            "Porcentaje de posiciones con precio y divisa disponibles.",
        )

    _render_quality_warnings(latest)
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
        "Como ha cambiado el valor estimado de la cartera y cuanto se ha alejado de sus maximos.",
    )
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return

    daily = _daily_metrics(metrics).set_index("valuation_date")
    st.markdown("#### Valor de la cartera")
    st.caption("Serie diaria valorada con el ultimo precio disponible para cada activo.")
    st.line_chart(daily[["total_market_value_base"]])

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Drawdown")
        st.caption("Cuanto cae la cartera desde su maximo anterior. Cuanto mas negativo, peor fue la caida.")
        st.line_chart(daily[["drawdown_pct"]])
    with c2:
        st.markdown("#### Cobertura de valoracion")
        st.caption("Proporcion de posiciones que se pudieron valorar con precio y FX.")
        st.line_chart(daily[["valuation_coverage_ratio"]])

    st.markdown("#### Datos diarios")
    st.dataframe(
        daily.reset_index().sort_values("valuation_date", ascending=False),
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
        "Revisa exactamente que informacion reciben los agentes y ejecuta la red mensual de analisis.",
    )
    metrics = _load_metrics(settings)
    if metrics is None:
        _show_metrics_error()
        return

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
        llm_provider = st.selectbox("LLM provider", options=["static", "openai"], index=0)
        search_provider = st.selectbox("Search provider", options=["null", "duckduckgo"], index=0)
        st.caption("Usa `static/null` para demo sin coste ni red. Usa `openai/duckduckgo` para una ejecucion real.")

    latest_report = get_latest_monthly_report(settings=settings)
    if latest_report is None:
        st.warning("No hay informe mensual en reports_history. Genera uno primero.")
    else:
        report_path = Path(latest_report.report_path)
        snapshot = build_portfolio_metrics_snapshot(metrics, as_of_date=latest_report.as_of_date or metrics.end_date)
        with st.expander("Inputs que recibiran los agentes", expanded=True):
            st.markdown("#### Investment brief")
            st.code(investment_brief[:4000], language="markdown")
            st.markdown("#### Latest monthly report")
            st.caption(str(report_path))
            st.code(report_path.read_text(encoding="utf-8")[:4000], language="markdown")
            st.markdown("#### Portfolio metrics snapshot")
            st.caption("Resumen estructurado de pesos, valoraciones y estado de cobertura.")
            st.json(snapshot)

    if st.button("Ejecutar red de agentes"):
        with st.spinner("Ejecutando agentes..."):
            result = run_monthly_agent_pipeline(
                settings=settings,
                investment_brief_text=investment_brief,
                user_satellite_interest=user_interest or None,
                llm_provider=llm_provider,
                search_provider=search_provider,
                persist=True,
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
    reports: list[dict[str, Any]] = []
    latest = get_latest_monthly_report(settings=settings)
    if latest is not None:
        path = Path(latest.report_path).expanduser().resolve()
        if path.exists():
            reports.append({"label": f"latest {latest.as_of_date}", "path": path})

    if settings.reports_dir.exists():
        for path in sorted(settings.reports_dir.glob("*.md"), reverse=True):
            if not any(item["path"] == path.resolve() for item in reports):
                reports.append({"label": "file", "path": path.resolve()})
    return reports


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
