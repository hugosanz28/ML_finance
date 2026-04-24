from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.agents import AgentFinding, AgentInputRef, AgentRequest, AgentValidationError, build_agent_context
from src.agents.analista_activos import (
    AnalistaActivosAgent,
    AssetAnalysis,
    AssetAssessment,
    StaticAssetLLMProvider,
    build_assets_under_review,
    build_monitor_context,
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
    include_watchlist: bool = True,
    include_interest: bool = True,
):
    settings = load_settings(repo_root=workspace_tmp_path)
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
                    "Preservacion de capital, volatilidad moderada, core diversificado "
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
                "positions": (
                    {
                        "asset_id": "IWDA",
                        "asset_name": "iShares Core MSCI World UCITS ETF",
                        "ticker": "IWDA.AS",
                        "asset_type": "etf",
                        "weight": 0.42,
                        "role": "core",
                    },
                    {
                        "asset_id": "NVDA",
                        "asset_name": "NVIDIA",
                        "ticker": "NVDA",
                        "asset_type": "stock",
                        "weight": 0.08,
                        "role": "satellite",
                    },
                    {
                        "asset_id": "BTC",
                        "asset_name": "Bitcoin",
                        "ticker": "BTC-EUR",
                        "asset_type": "crypto",
                        "weight": 0.04,
                        "role": "satellite",
                    },
                )
            },
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
                            "asset_id": "NVDA",
                            "asset_name": "NVIDIA",
                            "ticker": "NVDA",
                            "asset_type": "stock",
                            "weight": 0.09,
                            "recent_return": 0.55,
                            "drawdown": -0.12,
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
                            title="NVIDIA valuation risk",
                            detail="Subida reciente eleva el riesgo de sobreextension.",
                            category="risk",
                            severity="medium",
                            asset_id="NVDA",
                            metadata={"potential_decision_relevance": "watch"},
                        ),
                    )
                },
            )
        )
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
                            "asset_type": "metal",
                            "intended_role": "satellite",
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
        agent_name="analista_activos",
        as_of_date=date(2026, 4, 24),
        generated_at=generated_at,
        base_currency="EUR",
        settings=settings,
        input_refs=tuple(input_refs),
    )


def test_analista_activos_requires_brief_and_monthly_report(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path, include_metrics=False, include_monitor=False)
    result = AnalistaActivosAgent(llm_provider=StaticAssetLLMProvider()).execute(AgentRequest(), context)

    assert result.status == "partial"
    assert AnalistaActivosAgent(llm_provider=StaticAssetLLMProvider()).required_inputs() == (
        "investment_brief",
        "latest_monthly_report",
    )
    assert any("portfolio_metrics_snapshot" in warning for warning in result.warnings)
    assert any("monitor_tematico" in warning for warning in result.warnings)


def test_analista_activos_rejects_missing_required_inputs(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    context = build_agent_context(
        agent_name="analista_activos",
        as_of_date=date(2026, 4, 24),
        generated_at=datetime(2026, 4, 24, 8, 30, tzinfo=timezone.utc),
        base_currency="EUR",
        settings=settings,
    )

    with pytest.raises(AgentValidationError, match="investment_brief"):
        AnalistaActivosAgent(llm_provider=StaticAssetLLMProvider()).execute(AgentRequest(), context)


def test_build_assets_includes_positions_metrics_watchlist_and_user_interest(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)

    assets = build_assets_under_review(AgentRequest(), context)
    names = {asset.name for asset in assets}

    assert "iShares Core MSCI World UCITS ETF" in names
    assert "NVIDIA" in names
    assert "Bitcoin" in names
    assert "iShares Physical Gold ETC" in names
    assert "ETF de semiconductores" in names
    assert next(asset for asset in assets if asset.name == "NVIDIA").metadata["metrics_snapshot"]["recent_return"] == 0.55


def test_build_monitor_context_reads_thematic_findings(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)

    findings = build_monitor_context(context)

    assert len(findings) == 1
    assert findings[0].title == "NVIDIA valuation risk"
    assert findings[0].metadata["potential_decision_relevance"] == "watch"


def test_analista_activos_returns_structured_findings_by_asset_type(workspace_tmp_path: Path) -> None:
    context = _context(workspace_tmp_path)
    analysis = AssetAnalysis(
        summary="IWDA encaja como core, NVIDIA queda como satelite vigilado, BTC debe ser minoritario.",
        assessments=(
            AssetAssessment(
                asset_name="iShares Core MSCI World UCITS ETF",
                asset_type="etf",
                portfolio_fit="core",
                explicit_judgement="maintain",
                horizon_fit="aligned",
                risk_level="medium",
                valuation_signal="reasonable",
                rationale="ETF global diversificado compatible con el core de una cuenta a 3-4 anos.",
                etf_provider="iShares",
                etf_index="MSCI World",
                top_holdings=("Apple", "Microsoft", "NVIDIA"),
                sector_exposure="Sesgo a tecnologia por peso del indice.",
                geographic_bias="Alta exposicion a Estados Unidos.",
                concentration_view="Concentracion moderada en mega caps.",
                tags=("core", "etf"),
            ),
            AssetAssessment(
                asset_name="NVIDIA",
                asset_type="stock",
                portfolio_fit="satellite",
                explicit_judgement="watch",
                horizon_fit="borderline",
                risk_level="high",
                valuation_signal="overextended",
                rationale="Negocio de calidad, pero la subida reciente y valoracion exigen cautela.",
                business_summary="Proveedor dominante de aceleradores para IA.",
                fundamentals_view="Crecimiento y margenes altos, dependientes del ciclo de demanda IA.",
                valuation_view="Multiples exigentes frente al riesgo de expectativas elevadas.",
                main_risks=("valoracion", "concentracion", "ciclo de semiconductores"),
                monitor_context_used=("NVIDIA valuation risk",),
                tags=("satellite", "stock", "watch"),
            ),
            AssetAssessment(
                asset_name="Bitcoin",
                asset_type="crypto",
                portfolio_fit="satellite",
                explicit_judgement="maintain",
                horizon_fit="borderline",
                risk_level="very_high",
                valuation_signal="unknown",
                rationale="Puede mantenerse solo como satellite pequeno por volatilidad.",
                portfolio_role_view="Satellite no defensivo.",
                volatility_view="Volatilidad alta para un horizonte de vivienda.",
                liquidity_view="Liquidez amplia, pero riesgo de caidas bruscas.",
                tags=("satellite", "crypto"),
            ),
            AssetAssessment(
                asset_name="ETF de semiconductores",
                asset_type="etf",
                portfolio_fit="watch_only",
                explicit_judgement="do_not_incorporate",
                horizon_fit="misaligned",
                risk_level="high",
                valuation_signal="demanding",
                rationale="Candidato tematico demasiado volatil para ampliar riesgo ahora.",
                etf_provider="unknown",
                etf_index="unknown",
                concentration_view="Probable concentracion sectorial elevada.",
                tags=("candidate", "watch_only"),
            ),
        ),
    )

    result = AnalistaActivosAgent(llm_provider=StaticAssetLLMProvider(analysis)).execute(
        AgentRequest(parameters={"max_assets": 8}),
        context,
    )

    assert result.status == "success"
    assert len(result.findings) == 4
    iwda = result.findings[0]
    nvda = result.findings[1]
    btc = result.findings[2]
    semis = result.findings[3]
    assert iwda.metadata["asset_type"] == "etf"
    assert iwda.metadata["portfolio_fit"] == "core"
    assert iwda.metadata["etf_provider"] == "iShares"
    assert iwda.metadata["top_holdings"] == ("Apple", "Microsoft", "NVIDIA")
    assert nvda.metadata["business_summary"]
    assert nvda.metadata["valuation_signal"] == "overextended"
    assert nvda.metadata["monitor_context_used"] == ("NVIDIA valuation risk",)
    assert btc.metadata["portfolio_role_view"] == "Satellite no defensivo."
    assert btc.metadata["fundamentals_view"] == ""
    assert semis.metadata["explicit_judgement"] == "do_not_incorporate"
    assert semis.metadata["portfolio_fit"] == "watch_only"
    assert result.metadata["monitor_findings_count"] == 1
    assert any(source.metadata["input_key"] == "portfolio_metrics_snapshot" for source in result.sources)
