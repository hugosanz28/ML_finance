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
from src.agents.autonomy import autonomy_metadata, skipped_action
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
        monitor_findings = build_monitor_context(context)
        all_assets = build_assets_under_review(request, context)
        assets = _prioritize_assets(all_assets, monitor_findings)[:max_assets]
        sources = _input_sources(context)
        warnings: list[str] = []
        skipped_assets = _skipped_assets(all_assets, assets)

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
                    **_asset_autonomy_metadata(
                        selected_actions=("prioritize_assets", "declare_insufficient_universe"),
                        skipped_actions=(skipped_action("analyze_assets", "No assets were available to review."),),
                        max_assets=max_assets,
                        assets=assets,
                        skipped_assets=skipped_assets,
                        monitor_findings_count=len(monitor_findings),
                    ),
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
                    **_asset_autonomy_metadata(
                        selected_actions=("prioritize_assets",),
                        skipped_actions=(skipped_action("analyze_assets", "LLM asset analysis failed."), *skipped_assets),
                        max_assets=max_assets,
                        assets=assets,
                        skipped_assets=skipped_assets,
                        monitor_findings_count=len(monitor_findings),
                    ),
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
                **_asset_autonomy_metadata(
                    selected_actions=("prioritize_assets", "analyze_assets"),
                    skipped_actions=skipped_assets,
                    max_assets=max_assets,
                    assets=assets,
                    skipped_assets=skipped_assets,
                    monitor_findings_count=len(monitor_findings),
                ),
                "llm_provider": self.llm_provider.name,
                "assets": _assets_metadata(tuple(assets)),
                "assets_count": len(assets),
                "monitor_findings_count": len(monitor_findings),
                "findings_count": len(findings),
            },
        )


def _asset_autonomy_metadata(
    *,
    selected_actions: tuple[str, ...],
    skipped_actions: tuple[dict[str, str], ...],
    max_assets: int,
    assets,
    skipped_assets: tuple[dict[str, str], ...],
    monitor_findings_count: int,
) -> dict[str, object]:
    return autonomy_metadata(
        agent_plan=(
            "Construir universo de posiciones y candidatos.",
            "Priorizar activos por peso, senales del monitor, rol y riesgo potencial.",
            "Analizar solo el subconjunto que cabe en max_assets.",
            "Marcar activos omitidos por limite de cobertura.",
        ),
        allowed_actions=(
            "prioritize_assets",
            "analyze_assets",
            "compare_against_brief",
            "use_monitor_findings",
            "skip_low_priority_assets",
        ),
        selected_actions=selected_actions,
        skipped_actions=skipped_actions,
        applied_constraints=(f"max_assets={max_assets}", "no_monthly_allocation_decision", "no_trade_execution"),
        decision_basis=(
            "investment_brief",
            "latest_monthly_report",
            "portfolio_metrics_snapshot",
            f"monitor_findings_count={monitor_findings_count}",
            f"reviewed_assets={len(tuple(assets))}",
            f"skipped_assets={len(skipped_assets)}",
        ),
    )


def _prioritize_assets(assets, monitor_findings) -> tuple:
    monitor_text = " ".join(
        " ".join(str(value or "") for value in (finding.title, finding.detail, finding.asset_id)).lower()
        for finding in monitor_findings
    )
    return tuple(
        sorted(
            assets,
            key=lambda asset: (
                _asset_priority(asset, monitor_text),
                asset.name.lower(),
            ),
            reverse=False,
        )
    )


def _asset_priority(asset, monitor_text: str) -> tuple[int, float]:
    name = str(asset.name or "").lower()
    asset_id = str(asset.asset_id or "").lower()
    monitor_hit = bool(monitor_text and (name in monitor_text or (asset_id and asset_id in monitor_text)))
    role = str(asset.role or "").lower()
    asset_type = str(asset.asset_type or "").lower()
    weight = float(asset.current_weight or 0.0)
    bucket = 0
    if monitor_hit:
        bucket = -3
    elif weight >= 0.10:
        bucket = -2
    elif role == "candidate" or asset_type in {"stock", "crypto"}:
        bucket = -1
    return (bucket, -weight)


def _skipped_assets(all_assets, selected_assets) -> tuple[dict[str, str], ...]:
    selected_keys = {_asset_metadata_key(asset) for asset in selected_assets}
    return tuple(
        skipped_action(f"skip_asset:{asset.name}", "Omitted after prioritization because max_assets was reached.")
        for asset in all_assets
        if _asset_metadata_key(asset) not in selected_keys
    )


def _asset_metadata_key(asset) -> str:
    return str(asset.asset_id or asset.ticker or asset.name).lower()


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
