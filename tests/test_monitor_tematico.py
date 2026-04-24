from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.agents import AgentInputRef, AgentRequest, AgentValidationError, build_agent_context
from src.agents.monitor_tematico import (
    CachedSearchProvider,
    LLMSearchQuery,
    MonitorTematicoAgent,
    NullSearchProvider,
    SearchResult,
    StaticSearchProvider,
    StaticThemeLLMProvider,
    SynthesizedFinding,
    ThemeSynthesis,
    build_observed_topics,
)
from src.config import default_repo_root, load_settings


@pytest.fixture
def workspace_tmp_path() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)

    temp_dir = base_dir / uuid4().hex
    temp_dir.mkdir()

    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _context(workspace_tmp_path: Path, *, include_watchlist: bool = False, include_interest: bool = False):
    settings = load_settings(repo_root=workspace_tmp_path)
    generated_at = datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc)
    input_refs = [
        AgentInputRef(
            key="investment_brief",
            label="Investment brief",
            location="manual://investment-brief",
            source_type="manual",
            as_of_date=date(2026, 4, 20),
            metadata={
                "content": (
                    "Cuenta para entrada de vivienda en 3-4 anos. "
                    "Core diversificado y satellites minoritarios."
                )
            },
        ),
        AgentInputRef(
            key="latest_monthly_report",
            label="Latest monthly report",
            location="reports/monthly-001.md",
            source_type="report",
            as_of_date=date(2026, 3, 31),
            generated_at=generated_at,
            metadata={
                "positions": (
                    {
                        "asset_id": "IWDA",
                        "asset_name": "iShares Core MSCI World UCITS ETF",
                        "ticker": "IWDA.AS",
                        "weight": 0.42,
                        "role": "core",
                    },
                    {
                        "asset_id": "BTC",
                        "asset_name": "Bitcoin",
                        "ticker": "BTC-EUR",
                        "weight": 0.04,
                        "role": "satellite",
                    },
                )
            },
        ),
    ]
    if include_watchlist:
        input_refs.append(
            AgentInputRef(
                key="watchlist_candidates",
                label="Watchlist",
                location="manual://watchlist",
                source_type="manual",
                metadata={
                    "candidates": (
                        {
                            "name": "iShares Physical Gold ETC",
                            "ticker": "IGLN",
                            "intended_role": "core",
                            "priority": "low",
                            "theme": "gold defensive diversifier",
                        },
                    )
                },
            )
        )
    if include_interest:
        input_refs.append(
            AgentInputRef(
                key="user_satellite_interest",
                label="Satellite interest",
                location="manual://satellite-interest",
                source_type="manual",
                metadata={"text": "ETF de semiconductores"},
            )
        )

    return build_agent_context(
        agent_name="monitor_tematico",
        as_of_date=date(2026, 4, 20),
        generated_at=generated_at,
        base_currency="EUR",
        settings=settings,
        input_refs=tuple(input_refs),
    )


def test_monitor_tematico_requires_only_brief_and_monthly_report(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)
    llm_provider = StaticThemeLLMProvider(
        queries=(
            LLMSearchQuery(
                query_id="q1",
                query="IWDA monthly market risk",
                topic_name="iShares Core MSCI World UCITS ETF",
                impact_scope="core",
                priority="high",
                rationale="Core position.",
            ),
        )
    )
    result = MonitorTematicoAgent(
        search_provider=NullSearchProvider(),
        llm_provider=llm_provider,
    ).execute(AgentRequest(), context)

    assert result.status == "partial"
    assert MonitorTematicoAgent(
        search_provider=NullSearchProvider(),
        llm_provider=llm_provider,
    ).required_inputs() == (
        "investment_brief",
        "latest_monthly_report",
    )
    assert any("No se encontraron resultados externos" in warning for warning in result.warnings)
    assert result.metadata["search_provider"] == "null"


def test_monitor_tematico_rejects_missing_required_inputs(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    context = build_agent_context(
        agent_name="monitor_tematico",
        as_of_date=date(2026, 4, 20),
        generated_at=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
        base_currency="EUR",
        settings=settings,
    )

    with pytest.raises(AgentValidationError, match="investment_brief"):
        MonitorTematicoAgent(
            search_provider=NullSearchProvider(),
            llm_provider=StaticThemeLLMProvider(),
        ).execute(AgentRequest(), context)


def test_build_observed_topics_includes_optional_watchlist_and_user_interest(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path, include_watchlist=True, include_interest=True)

    topics = build_observed_topics(AgentRequest(), context)
    topic_names = {topic.name for topic in topics}

    assert "iShares Core MSCI World UCITS ETF" in topic_names
    assert "Bitcoin" in topic_names
    assert "iShares Physical Gold ETC" in topic_names
    assert "ETF de semiconductores" in topic_names


def test_monitor_tematico_turns_search_results_into_prioritized_findings(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path, include_interest=True)
    search_provider = StaticSearchProvider(
        {
            "IWDA monthly market risk": (
                SearchResult(
                    title="ECB rate risk weighs on global equities",
                    url="https://example.com/ecb-rate-risk",
                    snippet="Central bank rates and inflation remain a risk for broad equity ETFs.",
                ),
            ),
            "semiconductor ETF earnings catalyst": (
                SearchResult(
                    title="Semiconductor ETF sees earnings catalyst",
                    url="https://example.com/semiconductor-etf-catalyst",
                    snippet="Upcoming earnings could become a catalyst for semiconductor exposure.",
                ),
            ),
        }
    )
    llm_provider = StaticThemeLLMProvider(
        queries=(
            LLMSearchQuery(
                query_id="q1",
                query="IWDA monthly market risk",
                topic_name="iShares Core MSCI World UCITS ETF",
                impact_scope="core",
                priority="high",
                rationale="Core exposure with macro sensitivity.",
            ),
            LLMSearchQuery(
                query_id="q2",
                query="semiconductor ETF earnings catalyst",
                topic_name="ETF de semiconductores",
                impact_scope="candidate",
                priority="high",
                rationale="User satellite interest.",
            ),
        ),
        synthesis=ThemeSynthesis(
            summary="El LLM detecta un riesgo macro para el core y un catalizador para el candidato satelite.",
            findings=(
                SynthesizedFinding(
                    title="ECB rate risk weighs on global equities",
                    detail="Los tipos e inflacion siguen siendo riesgo para ETFs globales amplios.",
                    category="macro",
                    severity="high",
                    impact_scope="core",
                    change_type="risk",
                    time_horizon="near_term",
                    novelty="new",
                    affected_exposure="iShares Core MSCI World UCITS ETF",
                    potential_decision_relevance="rebalance",
                    downstream_hint="consider_rebalance",
                    source_urls=("https://example.com/ecb-rate-risk",),
                    tags=("core", "macro", "risk"),
                ),
                SynthesizedFinding(
                    title="Semiconductor ETF sees earnings catalyst",
                    detail="Resultados proximos pueden afectar la tesis tactica de semiconductores.",
                    category="catalyst",
                    severity="medium",
                    impact_scope="candidate",
                    change_type="catalyst",
                    time_horizon="near_term",
                    novelty="new",
                    affected_exposure="ETF de semiconductores",
                    potential_decision_relevance="analysis_needed",
                    downstream_hint="candidate_needs_analysis",
                    source_urls=("https://example.com/semiconductor-etf-catalyst",),
                    tags=("candidate", "catalyst"),
                ),
            ),
        ),
    )

    result = MonitorTematicoAgent(search_provider=search_provider, llm_provider=llm_provider).execute(
        AgentRequest(parameters={"max_topics": 6, "max_results_per_query": 1}),
        context,
    )

    assert result.status == "success"
    assert len(result.findings) == 2
    assert result.findings[0].severity == "high"
    assert result.findings[0].metadata["impact_scope"] == "core"
    assert result.findings[0].metadata["downstream_hint"] == "consider_rebalance"
    assert result.findings[1].metadata["impact_scope"] == "candidate"
    assert result.metadata["llm_provider"] == "static_llm"
    assert result.metadata["llm_queries"][0]["query"] == "IWDA monthly market risk"
    assert result.sources[0].source_type == "manual"
    assert any(source.location == "https://example.com/ecb-rate-risk" for source in result.sources)


def test_cached_search_provider_reuses_local_results(workspace_tmp_path: Path) -> None:
    class CountingProvider:
        def __init__(self) -> None:
            self.calls = 0

        @property
        def name(self) -> str:
            return "counting"

        def search(self, query: str, *, start_date: date, end_date: date, max_results: int):
            self.calls += 1
            return (
                SearchResult(
                    title="Cached headline",
                    url="https://example.com/cached",
                    snippet="Cached snippet.",
                    query=query,
                ),
            )

    provider = CountingProvider()
    cached = CachedSearchProvider(provider, cache_dir=workspace_tmp_path / "cache")

    first = cached.search(
        "query 1",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 20),
        max_results=2,
    )
    second = cached.search(
        "query 1",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 20),
        max_results=2,
    )

    assert provider.calls == 1
    assert first == second
