"""Data import and refresh Streamlit tab."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from src.application import (
    GenerateMonthlyReportUseCase,
    ImportDegiroUseCase,
    RefreshFxRequest,
    RefreshFxUseCase,
    RefreshMarketDataRequest,
    RefreshMarketDataUseCase,
)
from src.config import Settings
from src.portfolio.dashboard_common import (
    clear_dashboard_caches,
    generate_report_action,
    render_pending_import_warning,
    section_header,
)
from src.portfolio.dashboard_uploads import _save_uploaded_degiro_files


def render_update_tab(settings: Settings) -> None:
    section_header(
        "Actualizar datos",
        "Sube exportaciones de DEGIRO y ejecuta el pipeline local. Ninguna operacion se envia al broker.",
    )
    render_pending_import_warning(settings)

    st.markdown("#### 1. Entrada de CSVs")
    st.caption(
        "Puedes subirlos aqui o copiarlos manualmente a `src/degiro_exports/local/incoming/`. "
        "Al subirlos desde la UI se renombran al formato que exige el importador."
    )
    uploaded_files = st.file_uploader("Subir CSVs de DEGIRO", type=["csv"], accept_multiple_files=True)
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
                import_result = ImportDegiroUseCase(settings=settings).execute()
                summary = import_result.import_summary
                warehouse = import_result.warehouse_summary
            if warehouse is None:
                st.info(import_result.result.message)
            else:
                st.success(
                    f"Importados={summary.imported_count}; DuckDB tx={warehouse.transactions}, "
                    f"cash={warehouse.cash_movements}, snapshots={warehouse.portfolio_snapshots}"
                )
            clear_dashboard_caches()
    with c2:
        st.markdown("**FX**")
        st.caption("Descarga tipos de cambio para valorar posiciones no EUR.")
        only_missing = st.checkbox("Solo huecos FX", value=True)
        if st.button("2. Refrescar FX"):
            with st.spinner("Consultando FX..."):
                fx_summary = RefreshFxUseCase(settings=settings).execute(
                    RefreshFxRequest(only_missing_base=only_missing)
                ).summary
            st.success(f"Pares actualizados={fx_summary.updated_pairs}; filas={fx_summary.total_records}")
            clear_dashboard_caches()
    with c3:
        st.markdown("**Precios**")
        st.caption("Actualiza precios diarios usando tickers y overrides.")
        if st.button("3. Refrescar precios"):
            with st.spinner("Consultando market data..."):
                market_result = RefreshMarketDataUseCase(settings=settings).execute(
                    RefreshMarketDataRequest(end_date=date.today())
                )
            if market_result.summary is None:
                st.error(market_result.result.message)
            else:
                price_summary = market_result.summary
                st.success(f"Activos actualizados={price_summary.updated_assets}; filas={price_summary.total_records}")
            clear_dashboard_caches()
    with c4:
        st.markdown("**Informe**")
        st.caption("Genera el informe mensual que consumen los agentes.")
        if st.button("4. Generar informe"):
            generate_report_action(settings)
            clear_dashboard_caches()

    st.divider()
    st.markdown("#### Flujo rapido")
    st.caption("Lanza datos, FX, precios e informe en una sola accion. Los agentes se ejecutan despues desde su pestana.")
    if st.button("Ejecutar flujo mensual basico"):
        with st.spinner("Ejecutando importacion, FX, precios e informe..."):
            import_result = ImportDegiroUseCase(settings=settings).execute()
            import_summary = import_result.import_summary
            warehouse = import_result.warehouse_summary
            fx_summary = RefreshFxUseCase(settings=settings).execute(
                RefreshFxRequest(only_missing_base=True)
            ).summary
            market_result = RefreshMarketDataUseCase(settings=settings).execute(
                RefreshMarketDataRequest(end_date=date.today())
            )
            report = GenerateMonthlyReportUseCase(settings=settings).execute().report
        price_rows = market_result.summary.total_records if market_result.summary else 0
        st.success(
            "Flujo completado: "
            f"imported={import_summary.imported_count}, tx={warehouse.transactions if warehouse else 0}, "
            f"fx_rows={fx_summary.total_records}, price_rows={price_rows}, "
            f"report={report.output_path.name if report.output_path else '-'}"
        )
        clear_dashboard_caches()
