"""Supervisor providers for the bounded monthly workflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol

from dotenv import dotenv_values
from pydantic import ValidationError

from src.agents.langchain_provider import StructuredOutputError, call_openai_structured
from src.agents.orchestration.schemas import SupervisorDecision
from src.agents.orchestration.state import MonthlyAgentState
from src.agents.prompts import load_prompt
from src.agents.provider_audit import record_provider_failure


class SupervisorProvider(Protocol):
    @property
    def name(self) -> str: ...

    def decide(self, state: MonthlyAgentState) -> SupervisorDecision: ...


class StaticSupervisorProvider:
    """Deterministic offline policy used by demos and tests."""

    @property
    def name(self) -> str:
        return "static_llm"

    def decide(self, state: MonthlyAgentState) -> SupervisorDecision:
        results = state.get("results", {})
        if "monitor_tematico" not in results:
            return SupervisorDecision(
                next_agent="monitor_tematico",
                instruction="Revisa el contexto tematico relevante para esta ejecucion mensual.",
                expected_output="Hallazgos tematicos trazables o cobertura parcial explicita.",
                reason_code="baseline_monitor",
            )
        if "analista_activos" not in results:
            return SupervisorDecision(
                next_agent="analista_activos",
                instruction="Evalua posiciones y candidatos con los datos validados disponibles.",
                expected_output="Evaluaciones por activo alineadas con brief y analitica.",
                reason_code="baseline_analysis",
            )
        return SupervisorDecision(
            next_agent="asistente_aportacion_mensual",
            instruction="Sintetiza una decision mensual dentro de presupuesto y restricciones.",
            expected_output="Decision mensual estructurada y validada.",
            reason_code="baseline_decision",
        )


class OpenAISupervisorProvider:
    """LangChain/OpenAI supervisor that only selects the next specialist."""

    def __init__(self, *, model: str | None = None, api_key: str | None = None) -> None:
        repo_env = _repo_env_values()
        self.model = model or os.environ.get("OPENAI_MODEL") or repo_env.get("OPENAI_MODEL") or "gpt-4.1-mini"
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY") or repo_env.get("OPENAI_API_KEY")
        self._langchain_model: Any | None = None

    @property
    def name(self) -> str:
        return "openai"

    def decide(self, state: MonthlyAgentState) -> SupervisorDecision:
        payload = {
            "run": {
                "as_of_date": state.get("as_of_date"),
                "base_currency": state.get("base_currency"),
            },
            "available_inputs": state.get("input_summary", {}),
            "attempts": state.get("attempts", {}),
            "delegations_used": state.get("total_delegations", 0),
            "results": {
                name: {
                    "status": result.get("status"),
                    "summary": result.get("summary"),
                    "warnings": result.get("warnings", []),
                    "errors": result.get("errors", []),
                }
                for name, result in state.get("results", {}).items()
            },
        }
        last_error: Exception | None = None
        for repair in range(2):
            try:
                data = call_openai_structured(
                    self,
                    system_prompt=_SUPERVISOR_PROMPT,
                    user_payload={**payload, "repair_attempt": repair},
                    schema_name="monthly_agent_supervisor_decision",
                    schema=SupervisorDecision.model_json_schema(),
                )
                return SupervisorDecision.model_validate(data)
            except (StructuredOutputError, ValidationError) as exc:
                last_error = exc
                record_provider_failure(self, operation="monthly_agent_supervisor_decision")
            except Exception:
                record_provider_failure(self, operation="monthly_agent_supervisor_decision")
                raise
        raise RuntimeError("OpenAI supervisor did not return a valid decision.") from last_error


_SUPERVISOR_PROMPT = load_prompt("monthly_supervisor.routing")


def _repo_env_values() -> dict[str, str]:
    env_file = Path(__file__).resolve().parents[3] / ".env"
    if not env_file.exists():
        return {}
    values = dotenv_values(env_file)
    return {key: value for key, value in values.items() if isinstance(value, str) and value}


__all__ = ["OpenAISupervisorProvider", "StaticSupervisorProvider", "SupervisorProvider"]
