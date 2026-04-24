"""Internal types used by the thematic monitor agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping


@dataclass(frozen=True)
class ObservedTopic:
    """Asset, exposure, candidate, or theme to monitor during one run."""

    name: str
    role: str
    query_terms: tuple[str, ...]
    priority: str = "medium"
    source_key: str | None = None
    asset_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SearchResult:
    """Normalized result returned by one external search provider."""

    title: str
    url: str
    snippet: str = ""
    query: str = ""
    published_date: date | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LLMSearchQuery:
    """Search query proposed by the LLM for one monitored theme."""

    query_id: str
    query: str
    topic_name: str
    impact_scope: str
    priority: str = "medium"
    rationale: str = ""


@dataclass(frozen=True)
class SearchResultBundle:
    """Search results grouped under the LLM query that produced them."""

    search_query: LLMSearchQuery
    results: tuple[SearchResult, ...]


@dataclass(frozen=True)
class SynthesizedFinding:
    """Finding synthesized by the LLM from search results and portfolio context."""

    title: str
    detail: str
    category: str
    severity: str
    impact_scope: str
    change_type: str
    time_horizon: str
    novelty: str
    affected_exposure: str
    potential_decision_relevance: str
    downstream_hint: str
    source_urls: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class ThemeSynthesis:
    """Structured LLM synthesis returned to the agent."""

    summary: str
    findings: tuple[SynthesizedFinding, ...] = ()
    warnings: tuple[str, ...] = ()
