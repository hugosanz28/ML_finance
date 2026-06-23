"""Reports Streamlit tab."""

from __future__ import annotations

import streamlit as st

from src.config import Settings
from src.portfolio.dashboard_common import generate_report_action, list_reports, section_header


def render_reports_tab(settings: Settings) -> None:
    section_header(
        "Informes",
        "Resumen mensual en Markdown: cartera, cambios por periodo, actividad y notas de cobertura.",
    )
    reports = list_reports(settings)
    if not reports:
        st.info("No hay informes generados todavia.")
        if st.button("Generar informe mensual"):
            generate_report_action(settings)
        return

    selected = st.selectbox(
        "Informe",
        options=reports,
        format_func=lambda item: f"{item['label']} - {item['path'].name}",
    )
    report_path = selected["path"]
    st.caption(str(report_path))

    if st.button("Generar nuevo informe mensual"):
        generate_report_action(settings)
        st.rerun()

    with st.container(border=True):
        st.markdown(report_path.read_text(encoding="utf-8"))
