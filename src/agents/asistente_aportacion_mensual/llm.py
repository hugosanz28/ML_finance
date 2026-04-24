"""LLM providers for `asistente_aportacion_mensual`."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from typing import Any, Mapping, Protocol

from dotenv import dotenv_values

from src.agents.asistente_aportacion_mensual._types import (
    MonthlyDecision,
    MonthlyRecommendation,
    PriorAgentFinding,
)


class ContributionLLMProvider(Protocol):
    """Protocol for the LLM brain used by the monthly contribution assistant."""

    @property
    def name(self) -> str:
        """Stable provider identifier."""

    def decide(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        user_satellite_interest: str | None,
        monthly_budget: float,
        target_weights: Mapping[str, Any],
        current_allocation: tuple[Mapping[str, Any], ...],
        upstream_findings: tuple[PriorAgentFinding, ...],
        max_recommendations: int,
    ) -> MonthlyDecision:
        """Synthesize one monthly portfolio decision."""


class ContributionLLMProviderError(RuntimeError):
    """Raised when the LLM provider cannot complete a request."""


class StaticContributionLLMProvider:
    """Deterministic LLM provider for tests and local fixtures."""

    def __init__(self, decision: MonthlyDecision | None = None) -> None:
        self._decision = decision or MonthlyDecision(
            summary="Sin recomendacion mensual sintetizada.",
            primary_action="hold",
            monthly_budget=0.0,
        )

    @property
    def name(self) -> str:
        return "static_llm"

    def decide(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        user_satellite_interest: str | None,
        monthly_budget: float,
        target_weights: Mapping[str, Any],
        current_allocation: tuple[Mapping[str, Any], ...],
        upstream_findings: tuple[PriorAgentFinding, ...],
        max_recommendations: int,
    ) -> MonthlyDecision:
        return MonthlyDecision(
            summary=self._decision.summary,
            primary_action=self._decision.primary_action,
            monthly_budget=monthly_budget if self._decision.monthly_budget == 0.0 else self._decision.monthly_budget,
            recommendations=self._decision.recommendations[:max_recommendations],
            assumptions=self._decision.assumptions,
            warnings=self._decision.warnings,
        )


class OpenAIContributionLLMProvider:
    """OpenAI-backed monthly decision provider using Structured Outputs."""

    def __init__(self, *, model: str | None = None, api_key: str | None = None) -> None:
        repo_env = _repo_env_values()
        self.model = model or os.environ.get("OPENAI_MODEL") or repo_env.get("OPENAI_MODEL") or "gpt-4.1-mini"
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY") or repo_env.get("OPENAI_API_KEY")
        self._client: Any | None = None

    @property
    def name(self) -> str:
        return "openai"

    def decide(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        user_satellite_interest: str | None,
        monthly_budget: float,
        target_weights: Mapping[str, Any],
        current_allocation: tuple[Mapping[str, Any], ...],
        upstream_findings: tuple[PriorAgentFinding, ...],
        max_recommendations: int,
    ) -> MonthlyDecision:
        payload = {
            "investment_brief": _truncate_text(investment_brief),
            "latest_monthly_report": _truncate_text(latest_monthly_report),
            "portfolio_metrics_snapshot": _truncate_text(portfolio_metrics_snapshot or ""),
            "user_satellite_interest": _truncate_text(user_satellite_interest or ""),
            "monthly_budget": monthly_budget,
            "target_weights": dict(target_weights),
            "current_allocation": [dict(item) for item in current_allocation],
            "upstream_findings": [asdict(finding) for finding in upstream_findings],
            "max_recommendations": max_recommendations,
        }
        data = self._call_structured(
            system_prompt=_DECISION_SYSTEM_PROMPT,
            user_payload=payload,
            schema_name="asistente_aportacion_mensual_decision",
            schema=_decision_schema(),
        )
        return MonthlyDecision(
            summary=str(data["summary"]),
            primary_action=str(data["primary_action"]),
            monthly_budget=float(data["monthly_budget"]),
            recommendations=tuple(
                _recommendation_from_payload(item)
                for item in data.get("recommendations", [])[:max_recommendations]
            ),
            assumptions=tuple(str(item) for item in data.get("assumptions", [])),
            warnings=tuple(str(item) for item in data.get("warnings", [])),
        )

    def _client_instance(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ContributionLLMProviderError(
                "The OpenAI Python package is not installed. Run `pip install -r requirements.txt`."
            ) from exc
        self._client = OpenAI(api_key=self.api_key or None)
        return self._client

    def _call_structured(
        self,
        *,
        system_prompt: str,
        user_payload: dict[str, Any],
        schema_name: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            response = self._client_instance().responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": (
                            "Return JSON that matches the provided schema. "
                            f"Input payload:\n{json.dumps(user_payload, ensure_ascii=False)}"
                        ),
                    },
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": schema_name,
                        "strict": True,
                        "schema": schema,
                    }
                },
            )
        except Exception as exc:
            raise ContributionLLMProviderError(f"OpenAI request failed: {exc}") from exc

        text = getattr(response, "output_text", None)
        if not text:
            raise ContributionLLMProviderError("OpenAI response did not include output_text.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ContributionLLMProviderError("OpenAI response was not valid JSON.") from exc
        if not isinstance(parsed, dict):
            raise ContributionLLMProviderError("OpenAI response JSON root was not an object.")
        return parsed


_DECISION_SYSTEM_PROMPT = """
Eres `asistente_aportacion_mensual`, el sintetizador de decision mensual de una cartera personal.
Debes proponer una decision accionable: buy, no_buy, reduce, sell_partial, rebalance, hold o watch.
Puedes repartir el presupuesto mensual si hay conviccion suficiente, pero no ejecutes operaciones ni asumas integracion con broker.
La decision debe respetar una cuenta para entrada de vivienda en 3-4 anos: preservacion de capital, volatilidad moderada,
core diversificado y satellites minoritarios.
Usa `monitor_tematico` como contexto de riesgos/catalizadores y `analista_activos` como criterio por activo.
No conviertas ninguna senal individual en decision automatica; justifica por mandato, pesos, desviaciones, horizonte y riesgos.
Si faltan datos, explicita supuestos y limitaciones.
Si hay una idea puntual del usuario, deja claro si encaja como satellite pequeno, si debe vigilarse o si no encaja ahora.
""".strip()


def _recommendation_from_payload(item: Mapping[str, Any]) -> MonthlyRecommendation:
    return MonthlyRecommendation(
        target=str(item["target"]),
        action=str(item["action"]),
        recommendation_type=str(item["recommendation_type"]),
        suggested_amount=float(item["suggested_amount"]),
        priority=str(item["priority"]),
        rationale=str(item["rationale"]),
        role=str(item.get("role") or ""),
        source_signal_ids=tuple(str(value) for value in item.get("source_signal_ids", [])),
        conditions=tuple(str(value) for value in item.get("conditions", [])),
        warnings=tuple(str(value) for value in item.get("warnings", [])),
        tags=tuple(str(value) for value in item.get("tags", [])),
    )


def _decision_schema() -> dict[str, Any]:
    recommendation_properties: dict[str, Any] = {
        "target": {"type": "string"},
        "action": {
            "type": "string",
            "enum": ["buy", "no_buy", "reduce", "sell_partial", "rebalance", "hold", "watch"],
        },
        "recommendation_type": {
            "type": "string",
            "enum": ["contribution", "risk_control", "rebalance", "candidate_decision", "hold"],
        },
        "suggested_amount": {"type": "number"},
        "priority": {"type": "string", "enum": ["high", "medium", "low", "info"]},
        "rationale": {"type": "string"},
        "role": {"type": "string"},
        "source_signal_ids": {"type": "array", "items": {"type": "string"}},
        "conditions": {"type": "array", "items": {"type": "string"}},
        "warnings": {"type": "array", "items": {"type": "string"}},
        "tags": {"type": "array", "items": {"type": "string"}},
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["summary", "primary_action", "monthly_budget", "recommendations", "assumptions", "warnings"],
        "properties": {
            "summary": {"type": "string"},
            "primary_action": {
                "type": "string",
                "enum": ["buy", "no_buy", "reduce", "sell_partial", "rebalance", "hold", "watch", "mixed"],
            },
            "monthly_budget": {"type": "number"},
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": list(recommendation_properties),
                    "properties": recommendation_properties,
                },
            },
            "assumptions": {"type": "array", "items": {"type": "string"}},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
    }


def _truncate_text(value: str, *, max_chars: int = 8000) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n...[truncated]"


def _repo_env_values() -> dict[str, str]:
    env_file = Path(__file__).resolve().parents[3] / ".env"
    if not env_file.exists():
        return {}
    values = dotenv_values(env_file)
    return {key: value for key, value in values.items() if isinstance(value, str) and value}
