"""Deterministic monthly contribution simulator for Streamlit."""

from __future__ import annotations

from typing import Any, Mapping

import streamlit as st

from src.application import (
    ReadPortfolioTargetsUseCase,
    SimulateContributionRequest,
    SimulateContributionUseCase,
)
from src.config import Settings
from src.portfolio.dashboard_common import (
    format_currency,
    format_pct,
    section_header,
)


def render_contribution_lab_tab(settings: Settings) -> None:
    """Render controls and results without duplicating planning logic."""
    section_header(
        "Laboratorio de aportacion",
        "Simula como repartir una aportacion entre posiciones actuales sin vender ni ejecutar ordenes.",
    )
    st.info(
        "Es una simulacion determinista y offline sobre las posiciones actuales. "
        "Nunca vende ni ejecuta ordenes."
    )

    targets_state = ReadPortfolioTargetsUseCase(settings=settings).execute()
    targets = targets_state.portfolio_targets
    targets_available = targets is not None and targets_state.validation_error is None
    mapping_available = (
        targets_available
        and isinstance(targets.get("asset_bucket_mapping"), Mapping)
        and bool(targets["asset_bucket_mapping"])
    )
    if targets_state.validation_error:
        st.error(f"Los portfolio targets no son validos: {targets_state.validation_error}")
    elif targets is None:
        st.warning(
            "No hay portfolio targets configurados. Guardalos primero desde la pestana Agentes."
        )
    elif not mapping_available:
        st.warning(
            "Falta asset_bucket_mapping en los portfolio targets. Asigna cada "
            "posicion activa a un bucket antes de simular."
        )

    target_values = targets or {}
    base_currency = str(target_values.get("base_currency") or settings.default_currency)
    configured_contribution = target_values.get("monthly_contribution")
    if configured_contribution is None:
        configured_contribution = settings.monthly_contribution_eur

    with st.form("contribution_simulation_form"):
        left, right = st.columns(2)
        with left:
            contribution_amount = st.number_input(
                f"Aportacion ({base_currency})",
                min_value=0.0,
                value=float(configured_contribution),
                step=50.0,
                help="Efectivo nuevo disponible para esta simulacion.",
            )
            allow_fractional_units = st.checkbox(
                "Permitir unidades fraccionarias",
                value=False,
            )
        with right:
            minimum_order_value = st.number_input(
                f"Orden minima ({base_currency})",
                min_value=0.0,
                value=25.0,
                step=5.0,
            )
            max_orders = st.number_input(
                "Maximo de ordenes",
                min_value=1,
                max_value=100,
                value=4,
                step=1,
            )
        submitted = st.form_submit_button(
            "Simular aportacion",
            type="primary",
            disabled=not mapping_available,
            width="stretch",
        )

    if not submitted:
        return

    result = SimulateContributionUseCase(settings=settings).execute(
        SimulateContributionRequest(
            contribution_amount=float(contribution_amount),
            allow_fractional_units=bool(allow_fractional_units),
            minimum_order_value=float(minimum_order_value),
            max_orders=int(max_orders),
            as_of_date=None,
        )
    )
    _render_simulation_result(result.to_dict())


def _render_simulation_result(payload: Mapping[str, Any]) -> None:
    status = str(payload.get("status") or "failed")
    message = str(payload.get("message") or "No se pudo completar la simulacion.")
    if status == "succeeded":
        st.success(message)
    elif status == "partial":
        st.warning(message)
    else:
        st.error(message)

    _render_message_list("Avisos", payload.get("warnings"))
    _render_message_list("Supuestos", payload.get("assumptions"))

    simulation = payload.get("simulation")
    if not isinstance(simulation, Mapping):
        return

    base_currency = str(simulation.get("base_currency") or "")
    as_of_date = simulation.get("as_of_date")
    if as_of_date:
        st.caption(f"Estado de cartera usado: {as_of_date}")

    metrics = st.columns(5)
    metrics[0].metric(
        "Presupuesto",
        format_currency(simulation.get("budget"), base_currency),
    )
    metrics[1].metric(
        "Compras propuestas",
        format_currency(simulation.get("invested_amount"), base_currency),
    )
    metrics[2].metric(
        "Efectivo restante",
        format_currency(simulation.get("remaining_cash"), base_currency),
    )
    metrics[3].metric(
        "Desviacion antes",
        format_pct(simulation.get("deviation_before")),
    )
    metrics[4].metric(
        "Desviacion despues",
        format_pct(simulation.get("deviation_after")),
    )
    st.caption(
        '"Antes" es la cartera actual antes de compras. "Despues" incorpora solo '
        "las compras propuestas; el efectivo que no se invierte queda fuera de "
        "esos pesos y se muestra por separado."
    )

    orders = simulation.get("orders")
    st.markdown("#### Compras propuestas")
    if isinstance(orders, list) and orders:
        st.dataframe(orders, width="stretch", hide_index=True)
    else:
        st.info("No hay ninguna compra que mejore la desviacion respetando las restricciones.")

    bucket_allocations = simulation.get("bucket_allocations")
    st.markdown("#### Pesos y desviacion por bucket")
    if isinstance(bucket_allocations, list) and bucket_allocations:
        st.dataframe(bucket_allocations, width="stretch", hide_index=True)
    else:
        st.info("No hay detalle de buckets disponible.")

    constraints = simulation.get("constraint_events")
    if constraints is None:
        constraints = simulation.get("constraints")
    st.markdown("#### Restricciones activadas")
    if isinstance(constraints, list) and constraints:
        if all(isinstance(item, Mapping) for item in constraints):
            st.dataframe(constraints, width="stretch", hide_index=True)
        else:
            _render_message_list(None, constraints)
    else:
        st.caption("Ninguna restriccion adicional activada.")


def _render_message_list(title: str | None, values: Any) -> None:
    if not isinstance(values, (list, tuple)) or not values:
        return
    if title:
        st.markdown(f"**{title}**")
    st.markdown("\n".join(f"- {value}" for value in values))


__all__ = ["render_contribution_lab_tab"]
