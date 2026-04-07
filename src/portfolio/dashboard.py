"""Minimal Streamlit scaffold for the portfolio dashboard."""

import streamlit as st


def main() -> None:
    """Render the initial project dashboard placeholder."""
    st.set_page_config(page_title="ML_finance", layout="wide")
    st.title("ML_finance")
    st.caption("Seguimiento de cartera, análisis y agentes sobre exportaciones oficiales de DEGIRO.")

    st.info(
        "Esta es una base inicial. El dashboard crecerá cuando estén listos "
        "el modelo de datos local y el importador de exportaciones DEGIRO."
    )

    left, right = st.columns(2)
    left.subheader("Primeras fases")
    left.write("- Modelo de datos local")
    left.write("- Importador DEGIRO")
    left.write("- Histórico de precios y cartera")

    right.subheader("Documentación")
    right.write("- docs/roadmap.md")
    right.write("- docs/architecture.md")
    right.write("- docs/decisions.md")


if __name__ == "__main__":
    main()
