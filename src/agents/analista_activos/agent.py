"""Asset analyst agent implementation."""

from __future__ import annotations

from src.agents.analista_activos._types import AssetAssessment
from src.agents.analista_activos.asset_builder import (
    build_assets_under_review,
    build_monitor_context,
    collect_input_texts,
)
from src.agents.analista_activos.llm import (
    AssetLLMProvider,
    AssetLLMProviderError,
    OpenAIAssetLLMProvider,
)
from src.agents.base import BaseAgent
from src.agents.models import AgentContext, AgentFinding, AgentRequest, AgentResult, AgentSource


class AnalistaActivosAgent(BaseAgent):
    """Evaluate current holdings and candidates against the account mandate."""

    def __init__(self, *, llm_provider: AssetLLMProvider | None = None) -> None:
        self.llm_provider = llm_provider or OpenAIAssetLLMProvider()

    @property
    def name(self) -> str:
        return "analista_activos"

    @property
    def description(self) -> str:
        return "Evalua posiciones y candidatos segun mandato, horizonte, riesgo y encaje core/satellite."

    def required_inputs(self) -> tuple[str, ...]:
        return ("investment_brief", "latest_monthly_report")

    def supports(self, request: AgentRequest) -> bool:
        scope_type = request.scope.get("type") or request.parameters.get("scope_type")
        return scope_type in {None, "assets", "activos", "portfolio_review"}

    def run(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        max_assets = int(request.parameters.get("max_assets", 12))
        input_texts = collect_input_texts(context)
        assets = build_assets_under_review(request, context)[:max_assets]
        monitor_findings = build_monitor_context(context)
        sources = _input_sources(context)
        warnings: list[str] = []

        if not context.has_input("portfolio_metrics_snapshot"):
            warnings.append("No se recibio portfolio_metrics_snapshot; el analisis de pesos y sobreextension queda limitado.")
        if not monitor_findings:
            warnings.append("No se recibieron hallazgos de monitor_tematico; el analisis usa solo inputs internos disponibles.")

        if not assets:
            return AgentResult(
                status="failed",
                summary="No se pudo construir un universo de activos para analizar.",
                sources=tuple(sources),
                warnings=tuple(warnings),
                errors=("No assets were built from monthly report, metrics, watchlist, user interest, or request scope.",),
                metadata={
                    "llm_provider": self.llm_provider.name,
                    "assets_count": 0,
                    "monitor_findings_count": len(monitor_findings),
                },
            )

        try:
            analysis = self.llm_provider.analyze(
                investment_brief=input_texts["investment_brief"],
                latest_monthly_report=input_texts["latest_monthly_report"],
                portfolio_metrics_snapshot=input_texts.get("portfolio_metrics_snapshot"),
                assets=tuple(assets),
                monitor_findings=monitor_findings,
                max_assets=max_assets,
            )
        except AssetLLMProviderError as exc:
            return AgentResult(
                status="partial",
                summary="Analista de activos ejecutado con cobertura parcial: fallo la evaluacion LLM.",
                sources=tuple(sources),
                warnings=tuple([*warnings, str(exc)]),
                metadata={
                    "llm_provider": self.llm_provider.name,
                    "assets": _assets_metadata(tuple(assets)),
                    "assets_count": len(assets),
                    "monitor_findings_count": len(monitor_findings),
                    "findings_count": 0,
                },
            )

        warnings.extend(analysis.warnings)
        findings = tuple(_finding_from_assessment(assessment, sources) for assessment in analysis.assessments)
        if not findings:
            warnings.append("El LLM no genero fichas de analisis para los activos revisados.")

        status = "success" if findings else "partial"
        return AgentResult(
            status=status,
            summary=analysis.summary,
            findings=findings,
            sources=tuple(_deduplicate_sources(sources)),
            warnings=tuple(warnings),
            metadata={
                "llm_provider": self.llm_provider.name,
                "assets": _assets_metadata(tuple(assets)),
                "assets_count": len(assets),
                "monitor_findings_count": len(monitor_findings),
                "findings_count": len(findings),
            },
        )


def _finding_from_assessment(assessment: AssetAssessment, sources: list[AgentSource]) -> AgentFinding:
    severity = _severity_from_assessment(assessment)
    return AgentFinding(
        title=f"{assessment.asset_name}: {assessment.explicit_judgement}",
        detail=assessment.rationale,
        category="asset_fit",
        severity=severity,
        asset_id=assessment.asset_name,
        tags=assessment.tags,
        sources=tuple(sources),
        metadata={
            "asset_type": assessment.asset_type,
            "portfolio_fit": assessment.portfolio_fit,
            "explicit_judgement": assessment.explicit_judgement,
            "horizon_fit": assessment.horizon_fit,
            "risk_level": assessment.risk_level,
            "valuation_signal": assessment.valuation_signal,
            "business_summary": assessment.business_summary,
            "fundamentals_view": assessment.fundamentals_view,
            "valuation_view": assessment.valuation_view,
            "main_risks": assessment.main_risks,
            "etf_provider": assessment.etf_provider,
            "etf_index": assessment.etf_index,
            "top_holdings": assessment.top_holdings,
            "sector_exposure": assessment.sector_exposure,
            "geographic_bias": assessment.geographic_bias,
            "concentration_view": assessment.concentration_view,
            "portfolio_role_view": assessment.portfolio_role_view,
            "volatility_view": assessment.volatility_view,
            "liquidity_view": assessment.liquidity_view,
            "monitor_context_used": assessment.monitor_context_used,
            "warnings": assessment.warnings,
        },
    )


def _severity_from_assessment(assessment: AssetAssessment) -> str:
    if assessment.explicit_judgement == "reduce" or assessment.portfolio_fit == "not_fit":
        return "high"
    if assessment.risk_level in {"high", "very_high"} or assessment.valuation_signal == "overextended":
        return "medium"
    if assessment.explicit_judgement in {"watch", "do_not_incorporate"}:
        return "low"
    return "info"


def _input_sources(context: AgentContext) -> list[AgentSource]:
    relevant_keys = {
        "investment_brief",
        "latest_monthly_report",
        "portfolio_metrics_snapshot",
        "watchlist_candidates",
        "user_satellite_interest",
        "monitor_tematico_result",
        "monitor_tematico",
        "theme_monitor_result",
    }
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
        if input_ref.key in relevant_keys
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


def _assets_metadata(assets) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "name": asset.name,
            "asset_type": asset.asset_type,
            "role": asset.role,
            "ticker": asset.ticker,
            "asset_id": asset.asset_id,
            "current_weight": asset.current_weight,
            "source_key": asset.source_key,
        }
        for asset in assets
    )
