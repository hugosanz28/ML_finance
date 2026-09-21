"""Strict schemas exchanged by the monthly graph supervisor."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


SpecialistName = Literal[
    "monitor_tematico",
    "analista_activos",
    "asistente_aportacion_mensual",
]
SupervisorTarget = Literal[
    "monitor_tematico",
    "analista_activos",
    "asistente_aportacion_mensual",
    "finish",
]


class SupervisorDecision(BaseModel):
    """One bounded routing decision; it never contains a financial decision."""

    model_config = ConfigDict(extra="forbid", strict=True)

    next_agent: SupervisorTarget
    instruction: str = Field(min_length=1, max_length=2000)
    expected_output: str = Field(min_length=1, max_length=1000)
    reason_code: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")


__all__ = ["SpecialistName", "SupervisorDecision", "SupervisorTarget"]
