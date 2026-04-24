"""Thematic monitor agent."""

from src.agents.monitor_tematico._types import (
    LLMSearchQuery,
    ObservedTopic,
    SearchResult,
    SearchResultBundle,
    SynthesizedFinding,
    ThemeSynthesis,
)
from src.agents.monitor_tematico.agent import MonitorTematicoAgent
from src.agents.monitor_tematico.llm import (
    OpenAIThemeLLMProvider,
    StaticThemeLLMProvider,
    ThemeLLMProvider,
    ThemeLLMProviderError,
)
from src.agents.monitor_tematico.providers import (
    CachedSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    NullSearchProvider,
    SearchProvider,
    SearchProviderError,
    StaticSearchProvider,
)
from src.agents.monitor_tematico.topic_builder import build_observed_topics

__all__ = [
    "CachedSearchProvider",
    "DuckDuckGoHtmlSearchProvider",
    "LLMSearchQuery",
    "MonitorTematicoAgent",
    "NullSearchProvider",
    "ObservedTopic",
    "OpenAIThemeLLMProvider",
    "SearchProvider",
    "SearchProviderError",
    "SearchResult",
    "SearchResultBundle",
    "StaticSearchProvider",
    "StaticThemeLLMProvider",
    "SynthesizedFinding",
    "ThemeLLMProvider",
    "ThemeLLMProviderError",
    "ThemeSynthesis",
    "build_observed_topics",
]
