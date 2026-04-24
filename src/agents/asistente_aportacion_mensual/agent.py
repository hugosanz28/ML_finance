"""Monthly contribution assistant agent implementation."""

from __future__ import annotations

from src.agents.asistente_aportacion_mensual._types import MonthlyRecommendation
from src.agents.asistente_aportacion_mensual.context_builder import (
    collect_input_texts,
    extract_current_allocation,
    extract_prior_findings,
    resolve_monthly_budget,
    resolve_target_weights,
)
from src.agents.asistente_aportacion_mensual.llm import (
    ContributionLLMProvider,
    ContributionLLMProviderError,
    OpenAIContributionLLMProvider,
)
from src.agents.base import BaseAgent
from src.agents.models import AgentArtifact, AgentContext, AgentFinding, AgentRequest, AgentResult, AgentSource


class AsistenteAportacionMensualAgent(BaseAgent):
    """Synthesize the monthly portfolio action from reports and upstream agents."""

    def __init__(self, *, llm_provider: ContributionLLMProvider | None = None) -> None:
        self.llm_provider = llm_provider or OpenAIContributionLLMProvider()

    @property
    def name(self) -> str:
        return "asistente_aportacion_mensual"

    @property
    def description(self) -> str:
        return "Sintetiza la decision mensual de compra, no compra, reduccion, venta o rebalanceo."

    def required_inputs(self) -> tuple[str, ...]:
        return ("investment_brief", "latest_monthly_report")

    def supports(self, request: AgentRequest) -> bool:
        scope_type = request.scope.get("type") or request.parameters.get("scope_type")
        return scope_type in {None, "monthly_contribution", "aportacion_mensual", "monthly_decision"}

    def run(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        max_recommendations = int(request.parameters.get("max_recommendations", 8))
        input_texts = collect_input_texts(context)
        monthly_budget = resolve_monthly_budget(request, context)
        target_weights = resolve_target_weights(request, context)
        current_allocation = extract_current_allocation(context)
        upstream_findings = extract_prior_findings(context)
        sources = _input_sources(context)
        warnings: list[str] = []

        if monthly_budget <= 0:
            warnings.append("El presupuesto mensual resuelto no es positivo; la propuesta no puede repartir aportacion nueva.")
        if not target_weights:
            warnings.append("No se recibieron pesos objetivo; el rebalanceo se evalua solo de forma cualitativa.")
        if not context.has_input("portfolio_metrics_snapshot"):
            warnings.append("No se recibio portfolio_metrics_snapshot; la asignacion actual y desviaciones quedan limitadas.")
        if not any(finding.source_agent == "monitor_tematico" for finding in upstream_findings):
            warnings.append("No se recibieron resultados de monitor_tematico; falta contexto externo reciente.")
        if not any(finding.source_agent == "analista_activos" for finding in upstream_findings):
            warnings.append("No se recibieron resultados de analista_activos; falta criterio estructurado por activo.")

        try:
            decision = self.llm_provider.decide(
                investment_brief=input_texts["investment_brief"],
                latest_monthly_report=input_texts["latest_monthly_report"],
                portfolio_metrics_snapshot=input_texts.get("portfolio_metrics_snapshot"),
                user_satellite_interest=input_texts.get("user_satellite_interest"),
                monthly_budget=monthly_budget,
                target_weights=target_weights,
                current_allocation=current_allocation,
                upstream_findings=upstream_findings,
                max_recommendations=max_recommendations,
            )
        except ContributionLLMProviderError as exc:
            return AgentResult(
                status="partial",
                summary="Asistente de aportacion mensual ejecutado con cobertura parcial: fallo la decision LLM.",
                sources=tuple(sources),
                warnings=tuple([*warnings, str(exc)]),
                metadata={
                    "llm_provider": self.llm_provider.name,
                    "monthly_budget": monthly_budget,
                    "target_weights": dict(target_weights),
                    "current_allocation_count": len(current_allocation),
                    "upstream_findings_count": len(upstream_findings),
                    "recommendations_count": 0,
                },
            )

        warnings.extend(decision.warnings)
        findings = tuple(_finding_from_recommendation(recommendation, sources) for recommendation in decision.recommendations)
        if not findings:
            warnings.append("El LLM no genero recomendaciones mensuales estructuradas.")

        status = "success" if findings and _has_expected_context(context, upstream_findings) else "partial"
        return AgentResult(
            status=status,
            summary=decision.summary,
            findings=findings,
            artifacts=(_recommendation_artifact(decision.recommendations),) if findings else (),
            sources=tuple(_deduplicate_sources(sources)),
            warnings=tuple(warnings),
            metadata={
                "llm_provider": self.llm_provider.name,
                "primary_action": decision.primary_action,
                "monthly_budget": decision.monthly_budget,
                "target_weights": dict(target_weights),
                "assumptions": decision.assumptions,
                "current_allocation_count": len(current_allocation),
                "upstream_findings_count": len(upstream_findings),
                "recommendations_count": len(findings),
            },
        )


def _finding_from_recommendation(recommendation: MonthlyRecommendation, sources: list[AgentSource]) -> AgentFinding:
    return AgentFinding(
        title=f"{recommendation.target}: {recommendation.action}",
        detail=recommendation.rationale,
        category="monthly_decision",
        severity=_severity_from_recommendation(recommendation),
        asset_id=recommendation.target,
        tags=recommendation.tags,
        sources=tuple(sources),
        metadata={
            "target": recommendation.target,
            "action": recommendation.action,
            "recommendation_type": recommendation.recommendation_type,
            "suggested_amount": recommendation.suggested_amount,
            "priority": recommendation.priority,
            "role": recommendation.role,
            "source_signal_ids": recommendation.source_signal_ids,
            "conditions": recommendation.conditions,
            "warnings": recommendation.warnings,
        },
    )


def _severity_from_recommendation(recommendation: MonthlyRecommendation) -> str:
    if recommendation.action in {"reduce", "sell_partial", "rebalance"}:
        return "high" if recommendation.priority == "high" else "medium"
    if recommendation.action in {"no_buy", "watch"}:
        return "low"
    return "info"


def _recommendation_artifact(recommendations: tuple[MonthlyRecommendation, ...]) -> AgentArtifact:
    rows = [
        "| Objetivo | Accion | Importe | Prioridad | Rol |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for recommendation in recommendations:
        rows.append(
            "| "
            + " | ".join(
                [
                    recommendation.target,
                    recommendation.action,
                    f"{recommendation.suggested_amount:.2f}",
                    recommendation.priority,
                    recommendation.role or "-",
                ]
            )
            + " |"
        )
    return AgentArtifact(
        artifact_type="recommendation",
        title="Propuesta mensual estructurada",
        content="\n".join(rows),
        metadata={"rows": len(recommendations)},
    )


def _has_expected_context(context: AgentContext, upstream_findings) -> bool:
    return (
        context.has_input("portfolio_metrics_snapshot")
        and any(finding.source_agent == "monitor_tematico" for finding in upstream_findings)
        and any(finding.source_agent == "analista_activos" for finding in upstream_findings)
    )


def _input_sources(context: AgentContext) -> list[AgentSource]:
    relevant_keys = {
        "investment_brief",
        "latest_monthly_report",
        "portfolio_metrics_snapshot",
        "target_weights",
        "watchlist_candidates",
        "user_satellite_interest",
        "monitor_tematico_result",
        "monitor_tematico",
        "theme_monitor_result",
        "analista_activos_result",
        "analista_activos",
        "asset_analysis_result",
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
