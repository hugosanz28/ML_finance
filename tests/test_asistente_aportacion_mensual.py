from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.agents import AgentFinding, AgentInputRef, AgentRequest, AgentValidationError, build_agent_context
from src.agents.asistente_aportacion_mensual import (
    AsistenteAportacionMensualAgent,
    MonthlyDecision,
    MonthlyRecommendation,
    StaticContributionLLMProvider,
    extract_current_allocation,
    extract_prior_findings,
    resolve_monthly_budget,
    resolve_target_weights,
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


def _context(
    workspace_tmp_path: Path,
    *,
    include_metrics: bool = True,
    include_monitor: bool = True,
    include_asset_analysis: bool = True,
    include_interest: bool = True,
):
    settings = load_settings(repo_root=workspace_tmp_path, env={"MONTHLY_CONTRIBUTION_EUR": "1000"})
    generated_at = datetime(2026, 4, 24, 8, 30, tzinfo=timezone.utc)
    input_refs = [
        AgentInputRef(
            key="investment_brief",
            label="Investment brief",
            location="manual://investment-brief",
            source_type="manual",
            as_of_date=date(2026, 4, 24),
            metadata={
                "content": (
                    "Cuenta para entrada de vivienda en 3-4 anos. "
                    "Priorizar preservacion de capital, volatilidad moderada, core diversificado "
                    "y satellites minoritarios."
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
                "content": (
                    "## Asignacion actual\n\n"
                    "| Activo | Cantidad | Peso | Valor |\n"
                    "| --- | --- | --- | --- |\n"
                    "| iShares Core MSCI World UCITS ETF | 10 | 42.00% | 4200 EUR |\n"
                    "| NVIDIA | 2 | 9.00% | 900 EUR |\n"
                    "| Bitcoin | 0.02 | 4.00% | 400 EUR |\n"
                )
            },
        ),
        AgentInputRef(
            key="target_weights",
            label="Target weights",
            location="manual://target-weights",
            source_type="manual",
            metadata={"weights": {"core_global_equity": 0.55, "defensive_liquidity": 0.30, "satellites": 0.15}},
        ),
    ]
    if include_metrics:
        input_refs.append(
            AgentInputRef(
                key="portfolio_metrics_snapshot",
                label="Portfolio metrics",
                location="manual://portfolio-metrics",
                source_type="dataset",
                as_of_date=date(2026, 4, 24),
                metadata={
                    "positions": (
                        {
                            "asset_id": "IWDA",
                            "asset_name": "iShares Core MSCI World UCITS ETF",
                            "asset_type": "etf",
                            "weight": 0.42,
                            "role": "core",
                        },
                        {
                            "asset_id": "NVDA",
                            "asset_name": "NVIDIA",
                            "asset_type": "stock",
                            "weight": 0.09,
                            "role": "satellite",
                        },
                        {
                            "asset_id": "BTC",
                            "asset_name": "Bitcoin",
                            "asset_type": "crypto",
                            "weight": 0.04,
                            "role": "satellite",
                        },
                    )
                },
            )
        )
    if include_monitor:
        input_refs.append(
            AgentInputRef(
                key="monitor_tematico_result",
                label="Monitor tematico result",
                location="manual://monitor-tematico",
                source_type="derived",
                as_of_date=date(2026, 4, 24),
                metadata={
                    "findings": (
                        AgentFinding(
                            title="ECB rate risk weighs on global equities",
                            detail="Tipos e inflacion siguen condicionando el core global.",
                            category="macro",
                            severity="medium",
                            metadata={"downstream_hint": "review_fit"},
                        ),
                    )
                },
            )
        )
    if include_asset_analysis:
        input_refs.append(
            AgentInputRef(
                key="analista_activos_result",
                label="Analista activos result",
                location="manual://analista-activos",
                source_type="derived",
                as_of_date=date(2026, 4, 24),
                metadata={
                    "findings": (
                        AgentFinding(
                            title="IWDA: maintain",
                            detail="Encaja como core diversificado.",
                            category="asset_fit",
                            severity="info",
                            asset_id="IWDA",
                            metadata={"portfolio_fit": "core", "explicit_judgement": "maintain"},
                        ),
                        AgentFinding(
                            title="NVIDIA: watch",
                            detail="Satelite de calidad pero valoracion exigente.",
                            category="asset_fit",
                            severity="medium",
                            asset_id="NVDA",
                            metadata={"portfolio_fit": "satellite", "explicit_judgement": "watch"},
                        ),
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
        agent_name="asistente_aportacion_mensual",
        as_of_date=date(2026, 4, 24),
        generated_at=generated_at,
        base_currency="EUR",
        settings=settings,
        input_refs=tuple(input_refs),
    )


def test_asistente_aportacion_requires_brief_and_monthly_report(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path, include_metrics=False, include_monitor=False, include_asset_analysis=False)
    result = AsistenteAportacionMensualAgent(llm_provider=StaticContributionLLMProvider()).execute(
        AgentRequest(),
        context,
    )

    assert result.status == "partial"
    assert AsistenteAportacionMensualAgent(llm_provider=StaticContributionLLMProvider()).required_inputs() == (
        "investment_brief",
        "latest_monthly_report",
    )
    assert any("portfolio_metrics_snapshot" in warning for warning in result.warnings)
    assert any("monitor_tematico" in warning for warning in result.warnings)
    assert any("analista_activos" in warning for warning in result.warnings)


def test_asistente_aportacion_rejects_missing_required_inputs(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    context = build_agent_context(
        agent_name="asistente_aportacion_mensual",
        as_of_date=date(2026, 4, 24),
        generated_at=datetime(2026, 4, 24, 8, 30, tzinfo=timezone.utc),
        base_currency="EUR",
        settings=settings,
    )

    with pytest.raises(AgentValidationError, match="investment_brief"):
        AsistenteAportacionMensualAgent(llm_provider=StaticContributionLLMProvider()).execute(AgentRequest(), context)


def test_context_builder_resolves_budget_targets_allocation_and_upstream_findings(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)
    request = AgentRequest(parameters={"monthly_budget": 1200})

    assert resolve_monthly_budget(request, context) == 1200
    assert resolve_target_weights(request, context)["satellites"] == 0.15
    assert len(extract_current_allocation(context)) == 3
    findings = extract_prior_findings(context)
    assert {finding.source_agent for finding in findings} == {"monitor_tematico", "analista_activos"}
    assert any(finding.asset_id == "NVDA" for finding in findings)


def test_asistente_aportacion_returns_actionable_monthly_recommendation(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)
    decision = MonthlyDecision(
        summary="Aportar al core global y no ampliar satelites este mes.",
        primary_action="mixed",
        monthly_budget=1000.0,
        recommendations=(
            MonthlyRecommendation(
                target="iShares Core MSCI World UCITS ETF",
                action="buy",
                recommendation_type="contribution",
                suggested_amount=800.0,
                priority="high",
                role="core",
                rationale="Encaja con el mandato y ayuda a reforzar el nucleo diversificado.",
                source_signal_ids=("IWDA: maintain",),
                tags=("core", "buy"),
            ),
            MonthlyRecommendation(
                target="ETF de semiconductores",
                action="no_buy",
                recommendation_type="candidate_decision",
                suggested_amount=0.0,
                priority="medium",
                role="satellite",
                rationale="Idea tematica interesante pero demasiado volatil para ampliar satelites ahora.",
                source_signal_ids=("NVIDIA: watch",),
                conditions=("Revisar si baja concentracion o mejora margen de seguridad.",),
                tags=("candidate", "satellite", "no_buy"),
            ),
            MonthlyRecommendation(
                target="NVIDIA",
                action="watch",
                recommendation_type="risk_control",
                suggested_amount=0.0,
                priority="medium",
                role="satellite",
                rationale="Mantener vigilancia por valoracion exigente sin vender automaticamente.",
                source_signal_ids=("NVIDIA: watch",),
                tags=("satellite", "watch"),
            ),
        ),
        assumptions=("Presupuesto mensual disponible: 1000 EUR.",),
    )

    result = AsistenteAportacionMensualAgent(llm_provider=StaticContributionLLMProvider(decision)).execute(
        AgentRequest(parameters={"monthly_budget": 1000}),
        context,
    )

    assert result.status == "success"
    assert result.metadata["primary_action"] == "mixed"
    assert result.metadata["monthly_budget"] == 1000
    assert result.metadata["upstream_findings_count"] == 3
    assert len(result.findings) == 3
    assert result.findings[0].metadata["action"] == "buy"
    assert result.findings[0].metadata["suggested_amount"] == 800.0
    assert result.findings[1].metadata["action"] == "no_buy"
    assert result.findings[1].metadata["role"] == "satellite"
    assert result.findings[2].metadata["recommendation_type"] == "risk_control"
    assert result.artifacts[0].artifact_type == "recommendation"
    assert "iShares Core MSCI World UCITS ETF" in (result.artifacts[0].content or "")
