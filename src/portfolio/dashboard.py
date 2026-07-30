"""Streamlit dashboard entrypoint and navigation."""

from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import Settings, get_settings
from src.portfolio.dashboard_agents import render_agents_tab
from src.portfolio.dashboard_common import (
    apply_theme,
    clear_dashboard_caches,
    dashboard_data_fingerprint,
    friendly_table_name,
    render_hero,
    warehouse_counts,
)
from src.portfolio.dashboard_contribution_lab import render_contribution_lab_tab
from src.portfolio.dashboard_data_update import render_update_tab
from src.portfolio.dashboard_overview import render_evolution_tab, render_portfolio_tab
from src.portfolio.dashboard_reports import render_reports_tab


def main() -> None:
    """Render the dashboard."""
    st.set_page_config(page_title="ML_finance", layout="wide")
    apply_theme()

    settings = get_settings()
    render_hero()
    render_sidebar(settings)

    tabs = st.tabs(
        [
            "Vista general",
            "Aportacion",
            "Evolucion",
            "Informes",
            "Actualizar datos",
            "Agentes",
        ]
    )
    with tabs[0]:
        render_portfolio_tab(settings)
    with tabs[1]:
        render_contribution_lab_tab(settings)
    with tabs[2]:
        render_evolution_tab(settings)
    with tabs[3]:
        render_reports_tab(settings)
    with tabs[4]:
        render_update_tab(settings)
    with tabs[5]:
        render_agents_tab(settings)


def render_sidebar(settings: Settings) -> None:
    st.sidebar.markdown("### Estado local")
    st.sidebar.caption("Datos disponibles en la bodega local.")
    st.sidebar.caption(f"`{settings.portfolio_db_path}`")
    data_fingerprint = dashboard_data_fingerprint(settings)
    counts = warehouse_counts(settings, data_fingerprint)
    for key, value in counts.items():
        st.sidebar.metric(friendly_table_name(key), value)

    if st.sidebar.button("Limpiar cache de dashboard"):
        clear_dashboard_caches()
        st.rerun()


if __name__ == "__main__":
    main()
