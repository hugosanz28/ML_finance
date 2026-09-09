from copy import deepcopy
import json

import pytest

from src.agents.analytics import ANALYTICS_INPUT_KEY, analytics_for_agent, analytics_quality_issues
from src.agents.pipeline import run_monthly_agent_pipeline
from src.agents.analista_activos.llm import StaticAssetLLMProvider, OpenAIAssetLLMProvider
from src.agents.analista_activos._types import AssetUnderReview
from src.agents.asistente_aportacion_mensual.llm import StaticContributionLLMProvider, OpenAIContributionLLMProvider
from src.application import (
    BuildPortfolioAnalyticsSnapshotRequest, BuildPortfolioAnalyticsSnapshotUseCase,
    RunMonthlyAgentsRequest, RunMonthlyAgentsUseCase,
)
from src.application.analytics_snapshot import BuildPortfolioAnalyticsSnapshotResult
from src.config import default_repo_root, load_settings


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    from scripts.bootstrap_demo import _upsert_synthetic_prices, DEMO_AS_OF_DATE
    from src.degiro_exports.importer import import_degiro_exports
    from src.degiro_exports.warehouse import load_normalized_degiro_to_duckdb
    from src.market_data import DuckDBMarketDataRepository
    from src.portfolio import calculate_portfolio_metrics_from_normalized_degiro
    from src.reports import generate_monthly_report

    root = tmp_path_factory.mktemp("agent-analytics")
    public = default_repo_root() / "demo"
    settings = load_settings(repo_root=root, env_file=root / "absent.env", env={
        "PRICE_PROVIDER": "synthetic",
        "PORTFOLIO_TARGETS_PATH": str(public / "synthetic_config" / "portfolio_targets.yaml"),
        "BENCHMARK_SELECTION_PATH": str(public / "synthetic_config" / "benchmark_selection.json"),
    })
    source = public / "synthetic_degiro_exports"
    import_degiro_exports(settings=settings, incoming_dir=source / "incoming",
                         output_dir=settings.normalized_data_dir / "degiro", base_currency="EUR", source_root=source)
    load_normalized_degiro_to_duckdb(settings=settings)
    _upsert_synthetic_prices(DuckDBMarketDataRepository(settings=settings))
    metrics = calculate_portfolio_metrics_from_normalized_degiro(settings=settings, end_date=DEMO_AS_OF_DATE)
    report = generate_monthly_report(settings=settings, as_of_date=DEMO_AS_OF_DATE, persist=True)
    snapshot = BuildPortfolioAnalyticsSnapshotUseCase(settings=settings).execute(
        BuildPortfolioAnalyticsSnapshotRequest(DEMO_AS_OF_DATE.isoformat()),
    ).snapshot
    return settings, metrics, report.output_path, snapshot


def test_snapshot_is_compact_strict_json_with_explicit_coverage(demo):
    settings, metrics, _, snapshot = demo
    encoded = json.dumps(snapshot, allow_nan=False)
    assert len(encoded.encode()) < 65536
    assert "daily_returns" not in encoded and '"growth"' not in encoded
    assert str(settings.repo_root) not in encoded
    assert snapshot["portfolio"]["performance"]["twr"]["value"] is not None
    assert snapshot["portfolio"]["target_deviations"]
    assert snapshot["assets"] and snapshot["correlations"]
    assert snapshot["asset_series_kind"] == "valuation_price_proxy"
    issues = analytics_quality_issues(snapshot, as_of_date=metrics.end_date, base_currency="EUR")
    assert not any(item.blocks_agents for item in issues)
    assert any(item.severity == "warning" for item in issues)


@pytest.mark.parametrize("mutation,expected", [
    (lambda s: s.update(as_of_date="2026-05-01"), "analytics_date_mismatch"),
    (lambda s: s.update(base_currency="USD"), "analytics_currency_mismatch"),
    (lambda s: s["portfolio"]["risk"]["volatility_annualized"].update(value=float("nan")), "analytics_invalid_contract"),
    (lambda s: s["portfolio"]["risk"]["volatility_annualized"].update(observations=2), "analytics_insufficient_sample_value"),
    (lambda s: s.update(schema_version=99), "analytics_invalid_contract"),
])
def test_invalid_analytics_blocks_before_any_provider_and_audits(demo, tmp_path, monkeypatch, mutation, expected):
    settings, metrics, report, original = demo
    snapshot = deepcopy(original)
    mutation(snapshot)
    monkeypatch.setattr(BuildPortfolioAnalyticsSnapshotUseCase, "execute", lambda *args: BuildPortfolioAnalyticsSnapshotResult(snapshot))
    def unexpected(*args, **kwargs):
        raise AssertionError("Invalid analytics reached provider construction")
    for builder in ("_build_search_provider", "_build_monitor_llm_provider", "_build_asset_llm_provider", "_build_contribution_llm_provider"):
        monkeypatch.setattr("src.agents.pipeline." + builder, unexpected)
    result = RunMonthlyAgentsUseCase(settings=settings).execute(RunMonthlyAgentsRequest(
        metrics=metrics, monthly_report_path=report, investment_brief_text="Synthetic mandate.",
        output_dir=tmp_path / "blocked", persist=True,
    ))
    assert result.pipeline_result is None
    assert expected in result.result.artifacts["blocking_issue_codes"]
    directory = result.result.artifacts["output_dir"]
    assert (directory / "preflight.json").exists()
    assert not (directory / "agents").exists()
    json.loads((directory / "input_payload.json").read_text())


def test_partial_snapshot_runs_offline_and_has_preflight_hash(demo, tmp_path):
    settings, metrics, report, _ = demo
    result = RunMonthlyAgentsUseCase(settings=settings).execute(RunMonthlyAgentsRequest(
        metrics=metrics, monthly_report_path=report, investment_brief_text="Synthetic mandate.",
        output_dir=tmp_path / "run", persist=True,
    ))
    assert result.pipeline_result is not None
    assert result.result.status == "partial"
    assert result.quality_result.to_dict()["inputs"]["analytics_snapshot_hash"].startswith("sha256:")
    contexts = result.pipeline_result.agent_contexts
    assert ANALYTICS_INPUT_KEY not in result.pipeline_result.agent_requests["monitor_tematico"].input_refs
    for agent_name in ("analista_activos", "asistente_aportacion_mensual"):
        assert ANALYTICS_INPUT_KEY in result.pipeline_result.agent_requests[agent_name].input_refs
        view = next(item for item in contexts[agent_name]["input_refs"] if item["key"] == ANALYTICS_INPUT_KEY)["metadata"]["snapshot"]
        assert ("assets" in view) == (agent_name == "analista_activos")
    assert all(agent.status != "failed" for agent in (result.pipeline_result.analista_activos, result.pipeline_result.asistente_aportacion_mensual))


def run_snapshot(demo, snapshot, directory):
    settings, metrics, report, _ = demo
    return run_monthly_agent_pipeline(settings=settings, metrics=metrics, monthly_report_path=report,
        investment_brief_text="Synthetic mandate.", portfolio_analytics_snapshot=snapshot,
        persist=True, output_dir=directory, request_metadata={"api_key": "test-placeholder"})  # pragma: allowlist secret - synthetic redaction fixture


def test_analytics_changes_hashes_and_static_reason_codes_but_not_monitor(demo, tmp_path):
    _, _, _, original = demo
    first = deepcopy(original)
    second = deepcopy(original)
    first["assets"][0]["metrics"]["max_drawdown"]["value"] = 0.0
    second["assets"][0]["metrics"]["max_drawdown"]["value"] = -.15
    run_snapshot(demo, first, tmp_path / "a")
    run_snapshot(demo, second, tmp_path / "b")
    def audit(folder, agent, filename):
        return json.loads((tmp_path / folder / "agents" / agent / filename).read_text(encoding="utf-8"))
    for agent in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"):
        a = audit("a", agent, "audit_metadata.json")
        b = audit("b", agent, "audit_metadata.json")
        assert (a["input_hash"] == b["input_hash"]) == (agent == "monitor_tematico")
    assert "analytics_drawdown_observed" in json.dumps(audit("b", "analista_activos", "parsed_output.json"))
    inputs = json.loads((tmp_path / "a" / "input_payload.json").read_text(encoding="utf-8"))
    assert any(item["key"] == ANALYTICS_INPUT_KEY for item in inputs["inputs"])
    assert "test-placeholder" not in "".join(p.read_text(encoding="utf-8") for p in tmp_path.rglob("*.json"))


def test_static_monthly_provider_waits_when_performance_becomes_unavailable(demo):
    view = analytics_for_agent(demo[3], "asistente_aportacion_mensual")
    kwargs = dict(investment_brief="demo", latest_monthly_report="demo", portfolio_metrics_snapshot="{}",
                  user_satellite_interest=None, monthly_budget=500, target_weights={}, current_allocation=(), upstream_findings=(), max_recommendations=3)
    provider = StaticContributionLLMProvider()
    baseline = provider.decide(**kwargs, portfolio_analytics_snapshot=view)
    view["portfolio"]["performance"]["twr"].update(value=None, status="unavailable", reason_code="cash_flow_data_missing")
    changed = provider.decide(**kwargs, portfolio_analytics_snapshot=view)
    assert baseline.primary_action != changed.primary_action
    assert changed.primary_action == "hold"
    assert all(scenario.budget_to_invest == 0 for scenario in changed.scenarios)
    assert "analytics_manual_review_required" in changed.recommendations[0].tags


def test_provider_payloads_use_structured_snapshot_and_v2_prompts_without_network(demo, monkeypatch):
    payloads = []
    def capture(**kwargs):
        payloads.append(kwargs)
        return {"summary": "test", "primary_action": "hold", "monthly_budget": 500, "recommendations": [], "assessments": [], "scenarios": []}
    analyst = OpenAIAssetLLMProvider(api_key="placeholder")
    assistant = OpenAIContributionLLMProvider(api_key="placeholder")
    monkeypatch.setattr(analyst, "_call_structured", capture)
    monkeypatch.setattr(assistant, "_call_structured", capture)
    analyst.analyze(investment_brief="demo", latest_monthly_report="demo", portfolio_metrics_snapshot="{}",
                    assets=(), monitor_findings=(), max_assets=12,
                    portfolio_analytics_snapshot=analytics_for_agent(demo[3], "analista_activos"))
    assistant.decide(investment_brief="demo", latest_monthly_report="demo", portfolio_metrics_snapshot="{}",
                     user_satellite_interest=None, monthly_budget=500, target_weights={}, current_allocation=(),
                     upstream_findings=(), max_recommendations=3,
                     portfolio_analytics_snapshot=analytics_for_agent(demo[3], "asistente_aportacion_mensual"))
    for payload in payloads:
        assert isinstance(payload["user_payload"][ANALYTICS_INPUT_KEY], dict)
        assert "Python" in payload["system_prompt"]
        assert "reason_code" in payload["system_prompt"]
        json.dumps(payload["user_payload"], allow_nan=False)


def test_static_asset_reason_codes_follow_the_provided_metric(demo):
    snapshot = analytics_for_agent(demo[3], "analista_activos")
    item = snapshot["assets"][0]
    asset = AssetUnderReview(name="Synthetic asset", asset_id=item["asset_id"])
    provider = StaticAssetLLMProvider()
    def analyze():
        return provider.analyze(investment_brief="demo", latest_monthly_report="demo", portfolio_metrics_snapshot="{}",
            assets=(asset,), monitor_findings=(), max_assets=12, portfolio_analytics_snapshot=snapshot).assessments[0]
    item["metrics"]["max_drawdown"].update(value=0, coverage_ratio=1)
    assert "analytics_no_drawdown_observed" in analyze().tags
    item["metrics"]["max_drawdown"]["value"] = -.1
    assert "analytics_drawdown_observed" in analyze().tags


def test_snapshot_prunes_large_inputs_deterministically(demo, monkeypatch):
    from dataclasses import replace
    from src.application.analytics import GetAnalyticsSummaryUseCase, AnalyticsRequest
    settings = demo[0]
    summary = GetAnalyticsSummaryUseCase(settings=settings).execute(AnalyticsRequest(as_of_date=demo[1].end_date.isoformat()))
    data = deepcopy(summary.data)
    risk = data["risk"]["positions"]["asset_risk"]
    template = next(iter(risk.values()))
    data["risk"]["positions"]["asset_risk"] = {f"asset-{i:03}": deepcopy(template) for i in range(40)}
    monkeypatch.setattr(GetAnalyticsSummaryUseCase, "execute", lambda *args: replace(summary, data=data))
    builder = BuildPortfolioAnalyticsSnapshotUseCase(settings=settings)
    request = BuildPortfolioAnalyticsSnapshotRequest(demo[1].end_date.isoformat())
    first = builder.execute(request).snapshot
    assert first == builder.execute(request).snapshot
    assert len(first["assets"]) == 12
    assert first["omitted"]["assets"] == 28
    assert "analytics_snapshot_truncated" in first["warnings"]


def test_quality_rejects_non_json_or_oversized_snapshot(demo):
    snapshot = deepcopy(demo[3])
    snapshot["warnings"] = ("not-a-JSON-array",)
    assert analytics_quality_issues(snapshot, as_of_date=demo[1].end_date, base_currency="EUR")[0].blocks_agents
    snapshot["warnings"] = ["x" * 65536]
    assert analytics_quality_issues(snapshot, as_of_date=demo[1].end_date, base_currency="EUR")[0].code == "analytics_snapshot_too_large"
