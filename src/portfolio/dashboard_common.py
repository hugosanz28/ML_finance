"""Shared Streamlit dashboard helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.application import (
    GenerateMonthlyReportRequest,
    GenerateMonthlyReportUseCase,
    GetWarehouseCountsUseCase,
    ListDashboardReportsUseCase,
    LoadPortfolioMetricsRequest,
    LoadPortfolioMetricsUseCase,
    LoadPortfolioSnapshotsUseCase,
    LoadPortfolioTransactionsUseCase,
    ReadInvestmentBriefUseCase,
    ReadTargetWeightsUseCase,
)
from src.application.dashboard import GetPendingDegiroImportStatusUseCase
from src.config import Settings
from src.portfolio import PortfolioMetricsResult


def apply_theme() -> None:
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
        @media (prefers-color-scheme: dark) {
            :root {
                --mf-bg: #101419;
                --mf-panel: #171d24;
                --mf-border: #303946;
                --mf-text: #eef2f7;
                --mf-muted: #b8c0cc;
                --mf-accent: #2dd4bf;
                --mf-accent-soft: #143f3a;
                --mf-warning: #f7c56b;
                --mf-warning-soft: #342814;
            }
        }
        .stApp { background: var(--mf-bg); color: var(--mf-text); }
        .block-container { padding-top: 1.25rem; padding-bottom: 3rem; max-width: 1380px; }
        [data-testid="stSidebar"] { background: var(--mf-panel); border-right: 1px solid var(--mf-border); }
        [data-testid="stSidebar"] * { color: var(--mf-text); }
        h1, h2, h3 { color: var(--mf-text); letter-spacing: 0; }
        div[data-testid="stMetric"] {
            background: var(--mf-panel);
            border: 1px solid var(--mf-border);
            border-radius: 8px;
            padding: 14px 16px;
            min-height: 112px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        div[data-testid="stMetricLabel"] { color: var(--mf-muted); font-size: 0.78rem; }
        div[data-testid="stMetricValue"] { font-size: 1.35rem; color: var(--mf-text); }
        .mf-hero {
            background: var(--mf-panel);
            border: 1px solid var(--mf-border);
            border-radius: 8px;
            padding: 22px 24px;
            margin-bottom: 18px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .mf-hero h1 { margin: 0 0 6px 0; color: var(--mf-text); font-size: 2rem; line-height: 1.15; }
        .mf-hero p { margin: 0; color: var(--mf-muted); font-size: 1rem; max-width: 860px; }
        .mf-section { margin: 6px 0 18px 0; }
        .mf-section h2 { margin-bottom: 4px; color: var(--mf-text); }
        .mf-section p { color: var(--mf-muted); margin-top: 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def clear_dashboard_caches() -> None:
    st.cache_data.clear()
    st.cache_resource.clear()


def render_hero() -> None:
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


def section_header(title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="mf-section">
            <h2>{title}</h2>
            <p>{description}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pending_import_warning(settings: Settings) -> None:
    status = GetPendingDegiroImportStatusUseCase(settings=settings).execute()
    if not status.pending_portfolio_files:
        return

    latest_incoming = (
        status.latest_incoming_portfolio_date.isoformat()
        if status.latest_incoming_portfolio_date is not None
        else "sin fecha"
    )
    latest_loaded = (
        status.latest_normalized_portfolio_date.isoformat()
        if status.latest_normalized_portfolio_date is not None
        else "ninguno"
    )
    st.warning(
        "Hay exportaciones de cartera pendientes de importar. "
        f"Ultimo CSV en incoming: {latest_incoming}; ultimo snapshot cargado: {latest_loaded}. "
        "En `Actualizar datos`, pulsa `1. Importar DEGIRO` para que el efectivo y las posiciones del nuevo snapshot entren en el dashboard."
    )


def metric_card(title: str, value: str, help_text: str) -> None:
    st.metric(title, value)
    st.caption(help_text)


def render_beginner_explainer() -> None:
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


@st.cache_resource(show_spinner=False)
def load_metrics(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> PortfolioMetricsResult | None:
    try:
        return LoadPortfolioMetricsUseCase(settings=_settings).execute(
            LoadPortfolioMetricsRequest()
        ).metrics
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def load_snapshots(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> pd.DataFrame:
    return LoadPortfolioSnapshotsUseCase(settings=_settings).execute().snapshots


@st.cache_data(show_spinner=False)
def warehouse_counts(_settings: Settings, data_fingerprint: tuple[tuple[str, int, int], ...]) -> dict[str, int]:
    return GetWarehouseCountsUseCase(settings=_settings).execute().counts


def dashboard_data_fingerprint(settings: Settings) -> tuple[tuple[str, int, int], ...]:
    paths: list[Path] = [settings.portfolio_db_path]
    normalized_degiro_dir = settings.normalized_data_dir / "degiro"
    if normalized_degiro_dir.exists():
        paths.extend(sorted(normalized_degiro_dir.rglob("*.parquet")))
    return tuple(file_fingerprint(path) for path in paths if path.exists())


def file_fingerprint(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return (str(path), stat.st_mtime_ns, stat.st_size)


def load_transactions(settings: Settings) -> pd.DataFrame:
    return LoadPortfolioTransactionsUseCase(settings=settings).execute().transactions


def render_quality_warnings(latest: pd.Series) -> None:
    warnings: list[str] = []
    if int(latest["missing_price_positions_count"]) > 0:
        warnings.append(f"Faltan precios para {int(latest['missing_price_positions_count'])} posiciones.")
    if int(latest["missing_fx_positions_count"]) > 0:
        warnings.append(f"Faltan FX para {int(latest['missing_fx_positions_count'])} posiciones.")
    if float(latest["valuation_coverage_ratio"]) < 1.0:
        warnings.append("La cobertura de valoracion no es completa.")
    if warnings:
        st.warning("\n".join(f"- {warning}" for warning in warnings))


def list_reports(settings: Settings) -> list[dict[str, Any]]:
    return ListDashboardReportsUseCase(settings=settings).execute().reports


def generate_report_action(settings: Settings) -> None:
    with st.spinner("Generando informe mensual..."):
        report = GenerateMonthlyReportUseCase(settings=settings).execute(
            GenerateMonthlyReportRequest()
        ).report
    st.success(f"Informe generado: {report.output_path}")


def read_default_brief(settings: Settings) -> str:
    return ReadInvestmentBriefUseCase(settings=settings).execute().content


def read_default_target_weights(settings: Settings) -> dict[str, Any]:
    return ReadTargetWeightsUseCase(settings=settings).execute().target_weights


def show_metrics_error() -> None:
    st.error("No se pudieron calcular metricas. Importa DEGIRO, refresca FX/precios o revisa los normalizados.")


def format_currency(value: object, currency: str) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f} {currency}"


def format_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def friendly_table_name(table_name: str) -> str:
    return {
        "assets_master": "Activos",
        "transactions": "Transacciones",
        "cash_movements": "Mov. efectivo",
        "portfolio_snapshots": "Snapshots",
        "prices_daily": "Precios",
        "fx_rates": "FX",
    }.get(table_name, table_name)
