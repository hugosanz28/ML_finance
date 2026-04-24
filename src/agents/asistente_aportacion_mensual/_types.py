"""Internal types used by the monthly contribution assistant."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class PriorAgentFinding:
    """Finding imported from an upstream agent result."""

    title: str
    detail: str
    source_agent: str
    category: str = "general"
    severity: str = "info"
    asset_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MonthlyRecommendation:
    """One actionable monthly portfolio recommendation."""

    target: str
    action: str
    recommendation_type: str
    suggested_amount: float
    priority: str
    rationale: str
    role: str = ""
    source_signal_ids: tuple[str, ...] = ()
    conditions: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class MonthlyDecision:
    """Structured decision returned by the contribution assistant LLM."""

    summary: str
    primary_action: str
    monthly_budget: float
    recommendations: tuple[MonthlyRecommendation, ...] = ()
    assumptions: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
