"""Thematic monitor agent implementation.

This module is the orchestration layer: it builds context, asks the LLM to plan
searches, retrieves external results, asks the LLM to synthesize them, and then
returns the common `AgentResult` contract used by the rest of the project.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from src.agents.base import BaseAgent
from src.agents.models import AgentContext, AgentFinding, AgentRequest, AgentResult, AgentSource
from src.agents.monitor_tematico._types import (
    SearchResult,
    SearchResultBundle,
    SynthesizedFinding,
)
from src.agents.monitor_tematico.llm import (
    OpenAIThemeLLMProvider,
    ThemeLLMProvider,
    ThemeLLMProviderError,
)
from src.agents.monitor_tematico.providers import (
    DuckDuckGoHtmlSearchProvider,
    SearchProvider,
    SearchProviderError,
)
from src.agents.monitor_tematico.topic_builder import build_observed_topics


class MonitorTematicoAgent(BaseAgent):
    """Monitor external context relevant to the monthly portfolio review.

    The agent deliberately stays below the decision layer: it produces context,
    sources, and prioritized findings, but it never proposes amounts to buy,
    sell, or rebalance.
    """

    def __init__(
        self,
        *,
        search_provider: SearchProvider | None = None,
        llm_provider: ThemeLLMProvider | None = None,
    ) -> None:
        # The provider is injected so tests can run without network access and
        # future providers like Tavily can be added without changing agent logic.
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()
        self.llm_provider = llm_provider or OpenAIThemeLLMProvider()

    @property
    def name(self) -> str:
        return "monitor_tematico"

    @property
    def description(self) -> str:
        return "Detecta eventos, riesgos y catalizadores relevantes para la revision mensual de cartera."

    def required_inputs(self) -> tuple[str, ...]:
        return ("investment_brief", "latest_monthly_report")

    def supports(self, request: AgentRequest) -> bool:
        cadence = request.scope.get("cadence") or request.parameters.get("cadence")
        return cadence in {None, "monthly", "mensual"}

    def run(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        # Phase 1: determine the monthly analysis window and execution limits.
        # These parameters keep v1 bounded and avoid unbounded web searches.
        window_start, window_end = _resolve_window(request, context)
        max_topics = int(request.parameters.get("max_topics", 8))
        max_queries = int(request.parameters.get("max_queries", max_topics))
        max_results_per_query = int(request.parameters.get("max_results_per_query", 2))
        max_findings = int(request.parameters.get("max_findings", 10))

        # Phase 2: translate internal inputs into a concrete monitoring universe.
        # This is where current positions, optional watchlist items, and optional
        # user satellite interests become queryable topics.
        observed_topics = build_observed_topics(request, context)[:max_topics]
        input_texts = _collect_input_texts(context)
        if not observed_topics:
            return AgentResult(
                status="failed",
                summary="No se pudo construir un universo tematico minimo para monitorizar.",
                errors=("No observed topics were built from the available inputs.",),
                metadata={
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "search_provider": self.search_provider.name,
                },
            )

        warnings: list[str] = []
        sources: list[AgentSource] = _input_sources(context)
        searched_queries: list[str] = []
        search_bundles: list[SearchResultBundle] = []
        web_sources_by_url: dict[str, AgentSource] = {}

        try:
            llm_queries = self.llm_provider.generate_queries(
                investment_brief=input_texts["investment_brief"],
                latest_monthly_report=input_texts["latest_monthly_report"],
                watchlist_candidates=input_texts.get("watchlist_candidates"),
                user_satellite_interest=input_texts.get("user_satellite_interest"),
                observed_topics=observed_topics,
                start_date=window_start,
                end_date=window_end,
                max_queries=max_queries,
            )
        except ThemeLLMProviderError as exc:
            warnings.append(str(exc))
            llm_queries = ()

        if not llm_queries:
            warnings.append("El LLM no genero queries de busqueda para el universo observado.")
            return AgentResult(
                status="partial",
                summary="Monitor tematico ejecutado con cobertura parcial: no se pudieron generar queries con LLM.",
                findings=(),
                sources=tuple(_deduplicate_sources(sources)),
                warnings=tuple(warnings),
                metadata={
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "search_provider": self.search_provider.name,
                    "llm_provider": self.llm_provider.name,
                    "observed_topics": _observed_topics_metadata(observed_topics),
                    "searched_queries": (),
                    "findings_count": 0,
                },
            )

        # Phase 3: run one focused search per LLM-generated query. Provider failures are
        # warnings, not fatal errors, because partial output is still useful.
        for llm_query in llm_queries:
            query = llm_query.query
            searched_queries.append(query)
            try:
                results = self.search_provider.search(
                    query,
                    start_date=window_start,
                    end_date=window_end,
                    max_results=max_results_per_query,
                )
            except SearchProviderError as exc:
                warnings.append(str(exc))
                continue

            normalized_results: list[SearchResult] = []
            for result in results:
                normalized_result = _result_with_query(result, query)
                source = _source_from_search_result(normalized_result, context, self.search_provider.name)
                sources.append(source)
                web_sources_by_url[normalized_result.url] = source
                normalized_results.append(normalized_result)
            search_bundles.append(SearchResultBundle(search_query=llm_query, results=tuple(normalized_results)))

        if not any(bundle.results for bundle in search_bundles):
            warnings.append("No se encontraron resultados externos relevantes para el universo observado.")
            return AgentResult(
                status="partial",
                summary="Monitor tematico ejecutado con cobertura parcial: no hubo resultados externos que sintetizar.",
                findings=(),
                sources=tuple(_deduplicate_sources(sources)),
                warnings=tuple(warnings),
                metadata={
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "search_provider": self.search_provider.name,
                    "llm_provider": self.llm_provider.name,
                    "observed_topics": _observed_topics_metadata(observed_topics),
                    "llm_queries": _llm_queries_metadata(llm_queries),
                    "searched_queries": tuple(searched_queries),
                    "findings_count": 0,
                },
            )

        # Phase 4: ask the LLM to summarize, classify, and prioritize the actual
        # search results. This is the "AI brain" of the agent.
        try:
            synthesis = self.llm_provider.synthesize(
                investment_brief=input_texts["investment_brief"],
                latest_monthly_report=input_texts["latest_monthly_report"],
                watchlist_candidates=input_texts.get("watchlist_candidates"),
                user_satellite_interest=input_texts.get("user_satellite_interest"),
                observed_topics=observed_topics,
                search_bundles=tuple(search_bundles),
                start_date=window_start,
                end_date=window_end,
                max_findings=max_findings,
            )
        except ThemeLLMProviderError as exc:
            warnings.append(str(exc))
            synthesis = None

        if synthesis is None:
            return AgentResult(
                status="partial",
                summary="Monitor tematico ejecutado con cobertura parcial: hubo busquedas, pero fallo la sintesis LLM.",
                findings=(),
                sources=tuple(_deduplicate_sources(sources)),
                warnings=tuple(warnings),
                metadata={
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "search_provider": self.search_provider.name,
                    "llm_provider": self.llm_provider.name,
                    "observed_topics": _observed_topics_metadata(observed_topics),
                    "llm_queries": _llm_queries_metadata(llm_queries),
                    "searched_queries": tuple(searched_queries),
                    "findings_count": 0,
                },
            )

        warnings.extend(synthesis.warnings)
        findings = tuple(_finding_from_synthesis(finding, web_sources_by_url) for finding in synthesis.findings)
        status = "success" if findings else "partial"
        if not findings and not warnings:
            warnings.append("El LLM no sintetizo hallazgos relevantes a partir de los resultados externos.")

        return AgentResult(
            status=status,
            summary=synthesis.summary,
            findings=findings,
            sources=tuple(_deduplicate_sources(sources)),
            warnings=tuple(warnings),
            metadata={
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "search_provider": self.search_provider.name,
                "llm_provider": self.llm_provider.name,
                "observed_topics": _observed_topics_metadata(observed_topics),
                "llm_queries": _llm_queries_metadata(llm_queries),
                "searched_queries": tuple(searched_queries),
                "findings_count": len(findings),
            },
        )


def _resolve_window(request: AgentRequest, context: AgentContext) -> tuple[date, date]:
    """Resolve the period to monitor.

    Prefer explicit request dates, then the date of the latest monthly report,
    and finally a conservative 45-day lookback.
    """
    end_date = _coerce_date(request.parameters.get("end_date")) or context.as_of_date
    explicit_start = _coerce_date(request.parameters.get("start_date"))
    if explicit_start is not None:
        return explicit_start, end_date

    monthly_input = context.get_input("latest_monthly_report")
    if monthly_input.as_of_date is not None and monthly_input.as_of_date < end_date:
        return monthly_input.as_of_date, end_date

    lookback_days = int(request.parameters.get("lookback_days", 45))
    return end_date - timedelta(days=lookback_days), end_date


def _finding_from_synthesis(finding: SynthesizedFinding, sources_by_url: dict[str, AgentSource]) -> AgentFinding:
    matched_sources = tuple(
        source
        for url in finding.source_urls
        for source in (sources_by_url.get(url),)
        if source is not None
    )
    return AgentFinding(
        title=finding.title,
        detail=finding.detail,
        category=finding.category,
        severity=finding.severity,
        tags=finding.tags,
        sources=matched_sources,
        metadata={
            "impact_scope": finding.impact_scope,
            "change_type": finding.change_type,
            "time_horizon": finding.time_horizon,
            "novelty": finding.novelty,
            "affected_exposure": finding.affected_exposure,
            "potential_decision_relevance": finding.potential_decision_relevance,
            "downstream_hint": finding.downstream_hint,
            "source_urls": finding.source_urls,
        },
    )


def _result_with_query(result: SearchResult, query: str) -> SearchResult:
    if result.query:
        return result
    return SearchResult(
        title=result.title,
        url=result.url,
        snippet=result.snippet,
        query=query,
        published_date=result.published_date,
        metadata=result.metadata,
    )


def _source_from_search_result(result: SearchResult, context: AgentContext, provider_name: str) -> AgentSource:
    return AgentSource(
        source_type="web",
        label=result.title,
        location=result.url,
        retrieved_at=context.generated_at,
        effective_date=result.published_date,
        metadata={"provider": provider_name, "query": result.query, **dict(result.metadata)},
    )


def _input_sources(context: AgentContext) -> list[AgentSource]:
    return [
        AgentSource(
            source_type=input_ref.source_type,
            label=input_ref.label,
            location=input_ref.location,
            retrieved_at=context.generated_at,
            effective_date=input_ref.as_of_date,
            metadata={"input_key": input_ref.key, **dict(input_ref.metadata)},
        )
        for input_ref in context.input_refs
        if input_ref.key in {"investment_brief", "latest_monthly_report", "watchlist_candidates", "user_satellite_interest"}
    ]


def _deduplicate_sources(sources: list[AgentSource]) -> tuple[AgentSource, ...]:
    seen: set[tuple[str, str]] = set()
    deduped: list[AgentSource] = []
    for source in sources:
        key = (source.source_type, source.location)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(source)
    return tuple(deduped)


def _coerce_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value)
    return None


def _collect_input_texts(context: AgentContext) -> dict[str, str]:
    values = {
        "investment_brief": _read_input_text(context.get_input("investment_brief"), context),
        "latest_monthly_report": _read_input_text(context.get_input("latest_monthly_report"), context),
    }
    for optional_key in ("watchlist_candidates", "user_satellite_interest"):
        if context.has_input(optional_key):
            values[optional_key] = _read_input_text(context.get_input(optional_key), context)
    return values


def _read_input_text(input_ref, context: AgentContext) -> str:
    inline = input_ref.metadata.get("content") or input_ref.metadata.get("text")
    if inline:
        return str(inline)
    if input_ref.metadata:
        return str(dict(input_ref.metadata))
    path = Path(input_ref.location).expanduser()
    if not path.is_absolute():
        path = context.settings.repo_root / path
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return input_ref.description or ""


def _observed_topics_metadata(observed_topics) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "name": topic.name,
            "role": topic.role,
            "priority": topic.priority,
            "source_key": topic.source_key,
        }
        for topic in observed_topics
    )


def _llm_queries_metadata(llm_queries) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "query_id": query.query_id,
            "query": query.query,
            "topic_name": query.topic_name,
            "impact_scope": query.impact_scope,
            "priority": query.priority,
            "rationale": query.rationale,
        }
        for query in llm_queries
    )
