"""Application use case for running the thematic monitor independently."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.agents import AgentInputRef, AgentRequest, AgentResult, build_agent_context
from src.agents.monitor_tematico import (
    CachedSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    MonitorTematicoAgent,
    NullSearchProvider,
    OpenAIThemeLLMProvider,
    SearchProvider,
    StaticSearchProvider,
    StaticThemeLLMProvider,
    TavilySearchProvider,
    ThemeLLMProvider,
    build_observed_topics,
)
from src.application.types import ApplicationResult, ApplicationStatus
from src.config import Settings, get_settings
from src.reports import get_latest_monthly_report


@dataclass(frozen=True)
class RunMonitorTematicoRequest:
    investment_brief_text: str | None = None
    investment_brief_path: Path | None = None
    monthly_report_path: Path | None = None
    watchlist_path: Path | None = None
    user_satellite_interest: str | None = None
    llm_provider: str = "static"
    search_provider: str = "null"
    disable_cache: bool = False
    cache_dir: Path | None = None
    max_topics: int = 8
    max_queries: int = 8
    max_results_per_query: int = 2
    max_findings: int = 10
    dry_run: bool = False


@dataclass(frozen=True)
class RunMonitorTematicoResult:
    result: ApplicationResult
    payload: dict[str, Any]


class RunMonitorTematicoUseCase:
    """Resolve monitor inputs and execute it with explicitly selected providers."""

    name = "run_monitor_tematico"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(
        self,
        request: RunMonitorTematicoRequest | None = None,
    ) -> RunMonitorTematicoResult:
        resolved_request = request or RunMonitorTematicoRequest()
        investment_brief = _resolve_investment_brief(resolved_request)
        monthly_report_path = _resolve_monthly_report_path(resolved_request, self.settings)
        monthly_report_text = monthly_report_path.read_text(encoding="utf-8")
        now = datetime.now().astimezone()

        context = build_agent_context(
            agent_name="monitor_tematico",
            as_of_date=now.date(),
            generated_at=now,
            base_currency=self.settings.default_currency,
            settings=self.settings,
            input_refs=_build_input_refs(
                investment_brief=investment_brief,
                monthly_report_path=monthly_report_path,
                monthly_report_text=monthly_report_text,
                watchlist_path=resolved_request.watchlist_path,
                user_satellite_interest=resolved_request.user_satellite_interest,
            ),
        )
        agent_request = AgentRequest(
            parameters={
                "max_topics": resolved_request.max_topics,
                "max_queries": resolved_request.max_queries,
                "max_results_per_query": resolved_request.max_results_per_query,
                "max_findings": resolved_request.max_findings,
            }
        )

        if resolved_request.dry_run:
            topics = build_observed_topics(agent_request, context)
            payload = {
                "mode": "dry_run",
                "monthly_report": str(monthly_report_path),
                "input_keys": list(context.available_input_keys),
                "observed_topics": [
                    {
                        "name": topic.name,
                        "role": topic.role,
                        "priority": topic.priority,
                        "query_terms": list(topic.query_terms),
                    }
                    for topic in topics
                ],
                "llm_provider": resolved_request.llm_provider,
                "search_provider": resolved_request.search_provider,
            }
            return RunMonitorTematicoResult(
                result=ApplicationResult(
                    name=self.name,
                    status="succeeded",
                    message=f"Resolved {len(topics)} observed topic(s) without external calls.",
                    artifacts={"monthly_report": monthly_report_path, "topic_count": len(topics)},
                ),
                payload=payload,
            )

        agent = MonitorTematicoAgent(
            search_provider=_build_search_provider(resolved_request, self.settings),
            llm_provider=_build_llm_provider(resolved_request.llm_provider),
        )
        agent_result = agent.execute(agent_request, context)
        payload = _serialize_agent_result(agent_result)
        return RunMonitorTematicoResult(
            result=ApplicationResult(
                name=self.name,
                status=_application_status(agent_result.status),
                message=agent_result.summary,
                warnings=agent_result.warnings,
                artifacts={
                    "monthly_report": monthly_report_path,
                    "finding_count": len(agent_result.findings),
                    "llm_provider": resolved_request.llm_provider,
                    "search_provider": resolved_request.search_provider,
                },
            ),
            payload=payload,
        )


def _resolve_investment_brief(request: RunMonitorTematicoRequest) -> str:
    if request.investment_brief_text:
        return request.investment_brief_text
    if request.investment_brief_path is not None:
        return request.investment_brief_path.expanduser().resolve().read_text(encoding="utf-8")
    raise ValueError("Provide investment_brief_text or investment_brief_path.")


def _resolve_monthly_report_path(request: RunMonitorTematicoRequest, settings: Settings) -> Path:
    if request.monthly_report_path is not None:
        return request.monthly_report_path.expanduser().resolve()
    latest = get_latest_monthly_report(settings=settings)
    if latest is None:
        raise FileNotFoundError("No monthly report found. Generate one or provide monthly_report_path.")
    return Path(latest.report_path).expanduser().resolve()


def _build_input_refs(
    *,
    investment_brief: str,
    monthly_report_path: Path,
    monthly_report_text: str,
    watchlist_path: Path | None,
    user_satellite_interest: str | None,
) -> tuple[AgentInputRef, ...]:
    refs = [
        AgentInputRef(
            key="investment_brief",
            label="Investment brief",
            location="manual://investment-brief",
            source_type="manual",
            metadata={"content": investment_brief},
        ),
        AgentInputRef(
            key="latest_monthly_report",
            label="Latest monthly report",
            location=str(monthly_report_path),
            source_type="report",
            metadata={"content": monthly_report_text},
        ),
    ]
    if watchlist_path is not None:
        refs.append(
            AgentInputRef(
                key="watchlist_candidates",
                label="Watchlist candidates",
                location=str(watchlist_path.expanduser().resolve()),
                source_type="manual",
            )
        )
    if user_satellite_interest:
        refs.append(
            AgentInputRef(
                key="user_satellite_interest",
                label="User satellite interest",
                location="manual://user-satellite-interest",
                source_type="manual",
                metadata={"text": user_satellite_interest},
            )
        )
    return tuple(refs)


def _build_search_provider(
    request: RunMonitorTematicoRequest,
    settings: Settings,
) -> SearchProvider:
    if request.search_provider == "null":
        return NullSearchProvider()
    if request.search_provider == "static":
        return StaticSearchProvider()
    if request.search_provider == "tavily":
        provider: SearchProvider = TavilySearchProvider()
    elif request.search_provider == "duckduckgo":
        provider = DuckDuckGoHtmlSearchProvider()
    else:
        raise ValueError(f"Unsupported search provider: {request.search_provider}")

    if request.disable_cache:
        return provider
    cache_dir = request.cache_dir or (settings.data_dir / "agents" / "monitor_tematico" / "search_cache")
    return CachedSearchProvider(provider, cache_dir=cache_dir)


def _build_llm_provider(provider_name: str) -> ThemeLLMProvider:
    if provider_name == "static":
        return StaticThemeLLMProvider()
    if provider_name == "openai":
        return OpenAIThemeLLMProvider()
    raise ValueError(f"Unsupported LLM provider: {provider_name}")


def _serialize_agent_result(result: AgentResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "summary": result.summary,
        "warnings": list(result.warnings),
        "errors": list(result.errors),
        "metadata": dict(result.metadata),
        "findings": [
            {
                "title": finding.title,
                "detail": finding.detail,
                "category": finding.category,
                "severity": finding.severity,
                "asset_id": finding.asset_id,
                "tags": list(finding.tags),
                "metadata": dict(finding.metadata),
                "sources": [
                    {
                        "label": source.label,
                        "location": source.location,
                        "source_type": source.source_type,
                        "effective_date": source.effective_date.isoformat() if source.effective_date else None,
                    }
                    for source in finding.sources
                ],
            }
            for finding in result.findings
        ],
    }


def _application_status(agent_status: str) -> ApplicationStatus:
    if agent_status == "success":
        return "succeeded"
    if agent_status == "partial":
        return "partial"
    return "failed"


__all__ = [
    "RunMonitorTematicoRequest",
    "RunMonitorTematicoResult",
    "RunMonitorTematicoUseCase",
]
