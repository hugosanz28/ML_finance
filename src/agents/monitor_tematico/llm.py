"""LLM providers for `monitor_tematico`.

The LLM has two jobs in this agent:
1. generate focused web-search queries from the portfolio context;
2. synthesize search results into structured findings.

The production provider uses OpenAI Structured Outputs when available. Tests use
`StaticThemeLLMProvider`, so the test suite never depends on network access or
an API key.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
import json
import os
from pathlib import Path
from typing import Any, Protocol

from dotenv import dotenv_values

from src.agents.monitor_tematico._types import (
    LLMSearchQuery,
    ObservedTopic,
    SearchResultBundle,
    SynthesizedFinding,
    ThemeSynthesis,
)


class ThemeLLMProvider(Protocol):
    """Protocol for the LLM brain used by the thematic monitor."""

    @property
    def name(self) -> str:
        """Stable provider identifier."""

    def generate_queries(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        start_date: date,
        end_date: date,
        max_queries: int,
    ) -> tuple[LLMSearchQuery, ...]:
        """Generate search queries from portfolio context."""

    def synthesize(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        search_bundles: tuple[SearchResultBundle, ...],
        start_date: date,
        end_date: date,
        max_findings: int,
    ) -> ThemeSynthesis:
        """Summarize and classify search results into actionable findings."""


class ThemeLLMProviderError(RuntimeError):
    """Raised when the LLM provider cannot complete a request."""


class StaticThemeLLMProvider:
    """Deterministic LLM provider for tests and local fixtures."""

    def __init__(
        self,
        *,
        queries: tuple[LLMSearchQuery, ...] = (),
        synthesis: ThemeSynthesis | None = None,
    ) -> None:
        self._queries = queries
        self._synthesis = synthesis or ThemeSynthesis(summary="Sin hallazgos sintetizados.")

    @property
    def name(self) -> str:
        return "static_llm"

    def generate_queries(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        start_date: date,
        end_date: date,
        max_queries: int,
    ) -> tuple[LLMSearchQuery, ...]:
        return self._queries[:max_queries]

    def synthesize(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        search_bundles: tuple[SearchResultBundle, ...],
        start_date: date,
        end_date: date,
        max_findings: int,
    ) -> ThemeSynthesis:
        return ThemeSynthesis(
            summary=self._synthesis.summary,
            findings=self._synthesis.findings[:max_findings],
            warnings=self._synthesis.warnings,
        )


class OpenAIThemeLLMProvider:
    """OpenAI-backed LLM provider using Structured Outputs.

    `OPENAI_API_KEY` can come from the process environment or the repo `.env`.
    The model is configurable through `OPENAI_MODEL` and defaults to a small,
    structured-output capable model.
    """

    def __init__(self, *, model: str | None = None, api_key: str | None = None) -> None:
        repo_env = _repo_env_values()
        self.model = model or os.environ.get("OPENAI_MODEL") or repo_env.get("OPENAI_MODEL") or "gpt-4.1-mini"
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY") or repo_env.get("OPENAI_API_KEY")
        self._client: Any | None = None

    @property
    def name(self) -> str:
        return "openai"

    def generate_queries(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        start_date: date,
        end_date: date,
        max_queries: int,
    ) -> tuple[LLMSearchQuery, ...]:
        schema = _query_plan_schema()
        payload = {
            "investment_brief": _truncate_text(investment_brief),
            "latest_monthly_report": _truncate_text(latest_monthly_report),
            "watchlist_candidates": _truncate_text(watchlist_candidates or ""),
            "user_satellite_interest": _truncate_text(user_satellite_interest or ""),
            "observed_topics": [_topic_payload(topic) for topic in observed_topics],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "max_queries": max_queries,
        }
        data = self._call_structured(
            system_prompt=_QUERY_SYSTEM_PROMPT,
            user_payload=payload,
            schema_name="monitor_tematico_query_plan",
            schema=schema,
        )
        queries = data.get("queries", [])
        return tuple(
            LLMSearchQuery(
                query_id=str(item["query_id"]),
                query=str(item["query"]),
                topic_name=str(item["topic_name"]),
                impact_scope=str(item["impact_scope"]),
                priority=str(item["priority"]),
                rationale=str(item["rationale"]),
            )
            for item in queries[:max_queries]
        )

    def synthesize(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        watchlist_candidates: str | None,
        user_satellite_interest: str | None,
        observed_topics: tuple[ObservedTopic, ...],
        search_bundles: tuple[SearchResultBundle, ...],
        start_date: date,
        end_date: date,
        max_findings: int,
    ) -> ThemeSynthesis:
        schema = _synthesis_schema()
        payload = {
            "investment_brief": _truncate_text(investment_brief),
            "latest_monthly_report": _truncate_text(latest_monthly_report),
            "watchlist_candidates": _truncate_text(watchlist_candidates or ""),
            "user_satellite_interest": _truncate_text(user_satellite_interest or ""),
            "observed_topics": [_topic_payload(topic) for topic in observed_topics],
            "search_bundles": [_bundle_payload(bundle) for bundle in search_bundles],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "max_findings": max_findings,
        }
        data = self._call_structured(
            system_prompt=_SYNTHESIS_SYSTEM_PROMPT,
            user_payload=payload,
            schema_name="monitor_tematico_synthesis",
            schema=schema,
        )
        return ThemeSynthesis(
            summary=str(data["summary"]),
            findings=tuple(
                SynthesizedFinding(
                    title=str(item["title"]),
                    detail=str(item["detail"]),
                    category=str(item["category"]),
                    severity=str(item["severity"]),
                    impact_scope=str(item["impact_scope"]),
                    change_type=str(item["change_type"]),
                    time_horizon=str(item["time_horizon"]),
                    novelty=str(item["novelty"]),
                    affected_exposure=str(item["affected_exposure"]),
                    potential_decision_relevance=str(item["potential_decision_relevance"]),
                    downstream_hint=str(item["downstream_hint"]),
                    source_urls=tuple(str(url) for url in item.get("source_urls", [])),
                    tags=tuple(str(tag) for tag in item.get("tags", [])),
                )
                for item in data.get("findings", [])[:max_findings]
            ),
            warnings=tuple(str(warning) for warning in data.get("warnings", [])),
        )

    def _client_instance(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ThemeLLMProviderError(
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
            raise ThemeLLMProviderError(f"OpenAI request failed: {exc}") from exc

        text = getattr(response, "output_text", None)
        if not text:
            raise ThemeLLMProviderError("OpenAI response did not include output_text.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ThemeLLMProviderError("OpenAI response was not valid JSON.") from exc
        if not isinstance(parsed, dict):
            raise ThemeLLMProviderError("OpenAI response JSON root was not an object.")
        return parsed


_QUERY_SYSTEM_PROMPT = """
Eres el cerebro de `monitor_tematico`, un agente de apoyo a decision mensual de cartera.
Genera queries de busqueda web concretas y acotadas. No recomiendes compras ni ventas.
Prioriza cambios relevantes para una cuenta cuyo objetivo es una entrada de vivienda en 3-4 anos.
Incluye queries para core, satellites y candidatos solo si hay motivo.
""".strip()


_SYNTHESIS_SYSTEM_PROMPT = """
Eres el cerebro de `monitor_tematico`, un agente de contexto de mercado.
Resume noticias y resultados de busqueda en hallazgos estructurados.
Clasifica cada hallazgo como fact, risk, catalyst, macro, regulation, product_change o coverage.
Asigna severidad high, medium, low o info segun impacto potencial sobre la cuenta.
Distingue impacto core, satellite, candidate, portfolio o mixed.
No propongas importes ni recomendaciones directas de compra o venta.
""".strip()


def _topic_payload(topic: ObservedTopic) -> dict[str, Any]:
    return {
        "name": topic.name,
        "role": topic.role,
        "query_terms": list(topic.query_terms),
        "priority": topic.priority,
        "source_key": topic.source_key or "",
        "asset_id": topic.asset_id or "",
        "metadata": dict(topic.metadata),
    }


def _bundle_payload(bundle: SearchResultBundle) -> dict[str, Any]:
    return {
        "search_query": asdict(bundle.search_query),
        "results": [
            {
                "title": result.title,
                "url": result.url,
                "snippet": result.snippet,
                "query": result.query,
                "published_date": result.published_date.isoformat() if result.published_date else "",
                "metadata": dict(result.metadata),
            }
            for result in bundle.results
        ],
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


def _query_plan_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["queries"],
        "properties": {
            "queries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["query_id", "query", "topic_name", "impact_scope", "priority", "rationale"],
                    "properties": {
                        "query_id": {"type": "string"},
                        "query": {"type": "string"},
                        "topic_name": {"type": "string"},
                        "impact_scope": {
                            "type": "string",
                            "enum": ["core", "satellite", "candidate", "portfolio", "mixed"],
                        },
                        "priority": {"type": "string", "enum": ["high", "medium", "low", "info"]},
                        "rationale": {"type": "string"},
                    },
                },
            }
        },
    }


def _synthesis_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["summary", "findings", "warnings"],
        "properties": {
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "title",
                        "detail",
                        "category",
                        "severity",
                        "impact_scope",
                        "change_type",
                        "time_horizon",
                        "novelty",
                        "affected_exposure",
                        "potential_decision_relevance",
                        "downstream_hint",
                        "source_urls",
                        "tags",
                    ],
                    "properties": {
                        "title": {"type": "string"},
                        "detail": {"type": "string"},
                        "category": {
                            "type": "string",
                            "enum": ["fact", "risk", "catalyst", "macro", "regulation", "product_change", "coverage"],
                        },
                        "severity": {"type": "string", "enum": ["high", "medium", "low", "info"]},
                        "impact_scope": {
                            "type": "string",
                            "enum": ["core", "satellite", "candidate", "portfolio", "mixed"],
                        },
                        "change_type": {"type": "string", "enum": ["fact", "risk", "catalyst"]},
                        "time_horizon": {"type": "string", "enum": ["immediate", "near_term", "medium_term"]},
                        "novelty": {"type": "string", "enum": ["new", "ongoing", "resolved"]},
                        "affected_exposure": {"type": "string"},
                        "potential_decision_relevance": {
                            "type": "string",
                            "enum": ["buy", "do_not_buy", "reduce", "sell", "rebalance", "watch", "analysis_needed"],
                        },
                        "downstream_hint": {
                            "type": "string",
                            "enum": [
                                "review_fit",
                                "watch_weight",
                                "consider_rebalance",
                                "candidate_needs_analysis",
                                "no_action_context",
                            ],
                        },
                        "source_urls": {"type": "array", "items": {"type": "string"}},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
    }
