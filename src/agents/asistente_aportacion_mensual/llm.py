"""LLM providers for `asistente_aportacion_mensual`."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from typing import Any, Mapping, Protocol

from dotenv import dotenv_values

from src.agents.prompts import load_prompt
from src.agents.asistente_aportacion_mensual._types import (
    MonthlyDecision,
    MonthlyRecommendation,
    MonthlyScenario,
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
        self._decision = decision

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
        if self._decision is None:
            return _static_monthly_decision(
                monthly_budget=monthly_budget,
                target_weights=target_weights,
                current_allocation=current_allocation,
                upstream_findings=upstream_findings,
                max_recommendations=max_recommendations,
            )
        return MonthlyDecision(
            summary=self._decision.summary,
            primary_action=self._decision.primary_action,
            monthly_budget=monthly_budget if self._decision.monthly_budget == 0.0 else self._decision.monthly_budget,
            recommendations=self._decision.recommendations[:max_recommendations],
            scenarios=_limit_scenario_recommendations(self._decision.scenarios, max_recommendations=max_recommendations),
            assumptions=self._decision.assumptions,
            warnings=self._decision.warnings,
        )


def _static_monthly_decision(
    *,
    monthly_budget: float,
    target_weights: Mapping[str, Any],
    current_allocation: tuple[Mapping[str, Any], ...],
    upstream_findings: tuple[PriorAgentFinding, ...],
    max_recommendations: int,
) -> MonthlyDecision:
    core_target = _first_target_weight(target_weights, ("core_global_equity", "core", "ETFs"), default=0.55)
    satellite_target = _first_target_weight(target_weights, ("satellites", "satellite", "stocks"), default=0.10)
    core_amount = round(monthly_budget * min(max(core_target, 0.0), 0.85), 2)
    reserve_amount = round(max(monthly_budget - core_amount, 0.0), 2)
    core_name = _first_asset_name(current_allocation, preferred_types=("etf",), fallback="Synthetic Global Core UCITS ETF")
    satellite_name = _first_asset_name(current_allocation, preferred_types=("stock", "crypto"), fallback="satellite watchlist")
    main = MonthlyRecommendation(
        target=core_name,
        action="buy",
        recommendation_type="contribution",
        suggested_amount=core_amount,
        priority="high",
        role="core",
        rationale=(
            "Propuesta sintetica local: reforzar el nucleo diversificado porque encaja mejor "
            "con el horizonte de vivienda y reduce dependencia de satellites."
        ),
        source_signal_ids=tuple(finding.title for finding in upstream_findings[:3]),
        tags=("demo", "core", "contribution"),
    )
    reserve = MonthlyRecommendation(
        target="liquidez",
        action="hold",
        recommendation_type="hold",
        suggested_amount=reserve_amount,
        priority="medium",
        role="cash",
        rationale="Mantener una parte como liquidez defensiva para preservar flexibilidad.",
        tags=("demo", "cash"),
    )
    satellite = MonthlyRecommendation(
        target=satellite_name,
        action="watch",
        recommendation_type="candidate_decision",
        suggested_amount=round(monthly_budget * min(max(satellite_target, 0.0), 0.15), 2),
        priority="low",
        role="satellite",
        rationale="Vigilar como satellite pequeno, sin convertirlo en compra automatica.",
        conditions=("Solo ejecutar si el peso satellite sigue bajo el objetivo y no aumenta concentracion.",),
        warnings=("Mayor volatilidad que el nucleo defensivo.",),
        tags=("demo", "satellite", "watch"),
    )
    scenarios = (
        MonthlyScenario(
            name="conservador",
            summary="Invertir una parte reducida y mantener mas liquidez defensiva.",
            recommended_action="mixed",
            budget_to_invest=round(monthly_budget * 0.50, 2),
            recommendations=(main, reserve),
            conditions=("Usar si se prioriza estabilidad o hay incertidumbre de mercado.",),
            risk_notes=("Menor participacion si el mercado sube.",),
        ),
        MonthlyScenario(
            name="neutral",
            summary="Aportar principalmente al core diversificado y no ampliar satellites.",
            recommended_action="buy",
            budget_to_invest=core_amount,
            recommendations=(main,),
            conditions=("Usar como escenario base de demo con revision manual.",),
            risk_notes=("No elimina riesgo de mercado del core global.",),
        ),
        MonthlyScenario(
            name="oportunista",
            summary="Aportar al core y reservar una parte pequena para satellite condicionado.",
            recommended_action="mixed",
            budget_to_invest=monthly_budget,
            recommendations=(main, satellite),
            conditions=("Usar solo si el satellite sigue dentro de limites de concentracion.",),
            risk_notes=("Aumenta volatilidad y riesgo tematico frente al escenario neutral.",),
        ),
    )
    recommendations = (main, reserve, satellite)[:max_recommendations]
    return MonthlyDecision(
        summary="Recomendacion sintetica local: priorizar aportacion al core y revisar satellites manualmente.",
        primary_action="mixed",
        monthly_budget=monthly_budget,
        recommendations=recommendations,
        scenarios=scenarios,
        assumptions=("Modo static: salida sintetica para demo, no recomendacion financiera real.",),
    )


def _first_target_weight(target_weights: Mapping[str, Any], keys: tuple[str, ...], *, default: float) -> float:
    for key in keys:
        value = target_weights.get(key)
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _first_asset_name(
    current_allocation: tuple[Mapping[str, Any], ...],
    *,
    preferred_types: tuple[str, ...],
    fallback: str,
) -> str:
    for item in current_allocation:
        asset_type = str(item.get("asset_type") or "").lower()
        if asset_type in preferred_types:
            return str(item.get("asset_name") or item.get("asset_id") or fallback)
    return fallback


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
            scenarios=tuple(
                _scenario_from_payload(item, max_recommendations=max_recommendations)
                for item in data.get("scenarios", [])
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


_DECISION_SYSTEM_PROMPT = load_prompt("asistente_aportacion_mensual.decision")


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


def _scenario_from_payload(item: Mapping[str, Any], *, max_recommendations: int) -> MonthlyScenario:
    return MonthlyScenario(
        name=str(item["name"]),
        summary=str(item["summary"]),
        recommended_action=str(item["recommended_action"]),
        budget_to_invest=float(item["budget_to_invest"]),
        recommendations=tuple(
            _recommendation_from_payload(recommendation)
            for recommendation in item.get("recommendations", [])[:max_recommendations]
        ),
        conditions=tuple(str(value) for value in item.get("conditions", [])),
        risk_notes=tuple(str(value) for value in item.get("risk_notes", [])),
    )


def _limit_scenario_recommendations(
    scenarios: tuple[MonthlyScenario, ...],
    *,
    max_recommendations: int,
) -> tuple[MonthlyScenario, ...]:
    return tuple(
        MonthlyScenario(
            name=scenario.name,
            summary=scenario.summary,
            recommended_action=scenario.recommended_action,
            budget_to_invest=scenario.budget_to_invest,
            recommendations=scenario.recommendations[:max_recommendations],
            conditions=scenario.conditions,
            risk_notes=scenario.risk_notes,
        )
        for scenario in scenarios
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
    recommendation_schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": list(recommendation_properties),
        "properties": recommendation_properties,
    }
    scenario_properties: dict[str, Any] = {
        "name": {"type": "string", "enum": ["conservador", "neutral", "oportunista"]},
        "summary": {"type": "string"},
        "recommended_action": {
            "type": "string",
            "enum": ["buy", "no_buy", "reduce", "sell_partial", "rebalance", "hold", "watch", "mixed"],
        },
        "budget_to_invest": {"type": "number"},
        "recommendations": {"type": "array", "items": recommendation_schema},
        "conditions": {"type": "array", "items": {"type": "string"}},
        "risk_notes": {"type": "array", "items": {"type": "string"}},
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "summary",
            "primary_action",
            "monthly_budget",
            "recommendations",
            "scenarios",
            "assumptions",
            "warnings",
        ],
        "properties": {
            "summary": {"type": "string"},
            "primary_action": {
                "type": "string",
                "enum": ["buy", "no_buy", "reduce", "sell_partial", "rebalance", "hold", "watch", "mixed"],
            },
            "monthly_budget": {"type": "number"},
            "recommendations": {
                "type": "array",
                "items": recommendation_schema,
            },
            "scenarios": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": list(scenario_properties),
                    "properties": scenario_properties,
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
