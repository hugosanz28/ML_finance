"""Internal types used by the asset analyst agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class AssetUnderReview:
    """Current holding or candidate that should be evaluated by the agent."""

    name: str
    asset_type: str = "other"
    role: str = "portfolio"
    ticker: str | None = None
    asset_id: str | None = None
    current_weight: float | None = None
    source_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MonitorContextFinding:
    """Lightweight finding imported from `monitor_tematico` output."""

    title: str
    detail: str
    category: str = "general"
    severity: str = "info"
    asset_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AssetAssessment:
    """Structured LLM assessment for one asset."""

    asset_name: str
    asset_type: str
    portfolio_fit: str
    explicit_judgement: str
    horizon_fit: str
    risk_level: str
    valuation_signal: str
    rationale: str
    business_summary: str = ""
    fundamentals_view: str = ""
    valuation_view: str = ""
    main_risks: tuple[str, ...] = ()
    etf_provider: str = ""
    etf_index: str = ""
    top_holdings: tuple[str, ...] = ()
    sector_exposure: str = ""
    geographic_bias: str = ""
    concentration_view: str = ""
    portfolio_role_view: str = ""
    volatility_view: str = ""
    liquidity_view: str = ""
    monitor_context_used: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class AssetAnalysis:
    """Structured analysis returned by the asset LLM provider."""

    summary: str
    assessments: tuple[AssetAssessment, ...] = ()
    warnings: tuple[str, ...] = ()
