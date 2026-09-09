from dataclasses import asdict, replace
from datetime import date, timedelta
import hashlib
import json

import duckdb
import pandas as pd
import pytest

import src.application.analytics as application
from src.application import (
    AnalyticsRequest, GetAnalyticsSummaryUseCase, GetPortfolioPerformanceUseCase,
    GetPortfolioRiskUseCase, GetBenchmarkComparisonUseCase, GetMetricDefinitionsUseCase,
)
from src.analytics.portfolio import analyze_positions
from src.config import load_settings, default_repo_root
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio.metrics_models import PortfolioMetricsResult


START = date(2026, 1, 1)
END = START + timedelta(days=65)


def settings_for(path, provider="synthetic"):
    return load_settings(repo_root=path, env_file=path / "absent.env", env={"PRICE_PROVIDER": provider})


def sample_metrics():
    daily, positions = [], []
    price = 100
    for i in range(66):
        day = START + timedelta(days=i)
        if i:
            price *= 1.01 if i % 2 else .995
        daily.append({"valuation_date": day, "total_market_value_base": price * 10, "valuation_coverage_ratio": 1})
        positions.append({
            "valuation_date": day, "asset_id": "a", "isin": "ISIN_A", "quantity": 10,
            "close_price": price, "fx_rate_to_base": 1, "market_value_base": price * 10,
            "valuation_status": "valued", "price_currency": "EUR", "asset_type": "ETF",
        })
    return PortfolioMetricsResult(START, END, "EUR", pd.DataFrame(positions), pd.DataFrame(daily))


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    settings = settings_for(tmp_path)
    settings.portfolio_db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(settings.portfolio_db_path)):
        pass
    metrics = sample_metrics()

    def load(**kwargs):
        assert kwargs["settings"] == settings
        assert kwargs["repository"].read_only is True
        assert kwargs["persist"] is False
        return metrics

    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", load)
    monkeypatch.setattr(application, "load_normalized_degiro_cash_movements", lambda **kw: pd.DataFrame([{
        "movement_date": START, "movement_type": "DEPOSIT", "amount_base": 1000, "base_currency": "EUR",
    }]))
    return settings


def assert_json_tree(value):
    if isinstance(value, dict):
        assert all(type(key) is str for key in value)
        for item in value.values():
            assert_json_tree(item)
    elif isinstance(value, list):
        for item in value:
            assert_json_tree(item)
    else:
        assert value is None or type(value) in (bool, int, float, str)


@pytest.mark.parametrize("use_case", [GetAnalyticsSummaryUseCase, GetPortfolioPerformanceUseCase, GetPortfolioRiskUseCase, GetBenchmarkComparisonUseCase])
def test_use_cases_return_only_strict_json_and_do_not_persist(workspace, use_case):
    before = hashlib.sha256(workspace.portfolio_db_path.read_bytes()).hexdigest()
    result = use_case(settings=workspace).execute(AnalyticsRequest(benchmark_id="msci_world", risk_free_rate_annual=0))
    assert result.period["actual_start"] == START.isoformat()
    assert result.period["end_date"] == END.isoformat()
    assert_json_tree(asdict(result))
    json.dumps(result.to_dict(), allow_nan=False)
    assert str(workspace.repo_root) not in json.dumps(result.to_dict())
    assert before == hashlib.sha256(workspace.portfolio_db_path.read_bytes()).hexdigest()
    assert not workspace.curated_data_dir.exists()


def test_summary_and_individual_views_share_period_and_calculations(workspace):
    request = AnalyticsRequest(period="last_month", benchmark_id="sp500", risk_free_rate_annual=.02)
    summary = GetAnalyticsSummaryUseCase(settings=workspace).execute(request)
    for cls, key in [(GetPortfolioPerformanceUseCase, "performance"), (GetPortfolioRiskUseCase, "risk"), (GetBenchmarkComparisonUseCase, "benchmarks")]:
        result = cls(settings=workspace).execute(request)
        assert result.period == summary.period
        assert result.data[key] == summary.data[key]
    start = summary.period["actual_start"]
    assert start == "2026-02-07"
    assert all(row["previous_valuation_date"] >= start for row in summary.data["performance"]["daily_returns"])


def test_missing_cash_data_cannot_report_fictitious_returns(workspace, monkeypatch):
    monkeypatch.setattr(application, "load_normalized_degiro_cash_movements", lambda **kw: pd.DataFrame())
    result = GetPortfolioPerformanceUseCase(settings=workspace).execute()
    assert result.status == "unavailable"
    assert result.reason_code == "cash_flow_data_missing"
    assert result.data["performance"]["period"]["twr"]["value"] is None
    assert result.data["performance"]["period"]["mwr"]["value"] is None


def test_benchmark_request_overrides_saved_selection_without_writing(workspace):
    workspace.benchmark_selection_path.write_text('{"primary_benchmark_id":"estr_cash"}', encoding="utf-8")
    uc = GetBenchmarkComparisonUseCase(settings=workspace)
    assert uc.execute().data["benchmarks"]["selection"]["primary_benchmark_id"] == "estr_cash"
    assert uc.execute(AnalyticsRequest(benchmark_id="sp500")).data["benchmarks"]["selection"]["primary_benchmark_id"] == "sp500"
    assert json.loads(workspace.benchmark_selection_path.read_text())["primary_benchmark_id"] == "estr_cash"


def test_real_workspace_never_falls_back_to_synthetic(workspace, monkeypatch):
    settings = replace(workspace, price_provider="yfinance")
    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", lambda **kw: sample_metrics())
    result = GetBenchmarkComparisonUseCase(settings=settings).execute(AnalyticsRequest(benchmark_id="sp500"))
    assert result.status == "unavailable"
    assert result.reason_code == "benchmark_provider_unavailable"
    assert result.data["benchmarks"]["comparison"] is None


def test_local_loaded_benchmark_is_supported_without_network(workspace, monkeypatch):
    from src.market_data.benchmarks import LoadedBenchmarkProvider

    settings = replace(workspace, price_provider="yfinance")
    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", lambda **kw: sample_metrics())
    frame = pd.DataFrame([{
        "benchmark_id": "msci_world", "previous_observation_date": START + timedelta(days=i),
        "observation_date": START + timedelta(days=i + 1), "return_decimal": .001, "currency": "EUR",
    } for i in range(65)])
    result = GetBenchmarkComparisonUseCase(
        settings=settings, benchmark_provider=LoadedBenchmarkProvider(frame, provider_name="loaded_fixture"),
    ).execute(AnalyticsRequest(benchmark_id="msci_world"))
    comparison = result.data["benchmarks"]["comparison"]["comparisons"][0]
    assert comparison["provider_name"] == "loaded_fixture"
    assert comparison["observations"] > 0


@pytest.mark.parametrize("analytics_request", [
    AnalyticsRequest(period="made_up"), AnalyticsRequest(as_of_date="2026/01/01"),
    AnalyticsRequest(as_of_date="2099-01-01"), AnalyticsRequest(benchmark_id="../private"),
    AnalyticsRequest(risk_free_rate_annual=float("nan")),
])
def test_invalid_requests_fail_before_loading(tmp_path, monkeypatch, analytics_request):
    def unexpected(**kwargs):
        raise AssertionError("Invalid request accessed portfolio")
    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", unexpected)
    with pytest.raises(ValueError):
        GetAnalyticsSummaryUseCase(settings=settings_for(tmp_path)).execute(analytics_request)


def test_empty_workspace_returns_unavailable_without_creating_files(tmp_path):
    settings = settings_for(tmp_path)
    result = GetAnalyticsSummaryUseCase(settings=settings).execute()
    assert result.reason_code == "portfolio_data_unavailable"
    assert not settings.data_dir.exists()


def test_initialized_database_without_imports_is_unavailable(tmp_path):
    settings = settings_for(tmp_path)
    DuckDBMarketDataRepository(settings=settings).ensure_schema()
    result = GetAnalyticsSummaryUseCase(settings=settings).execute()
    assert result.reason_code == "portfolio_data_unavailable"


def test_single_valuation_and_short_history_remain_serializable(workspace, monkeypatch):
    metrics = sample_metrics()
    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", lambda **kw: replace(
        metrics, portfolio_daily_metrics=metrics.portfolio_daily_metrics.iloc[:1],
        position_metrics=metrics.position_metrics.iloc[:1],
    ))
    result = GetAnalyticsSummaryUseCase(settings=workspace).execute(AnalyticsRequest(benchmark_id="sp500"))
    assert "insufficient_observations" in result.warnings
    assert_json_tree(asdict(result))
    json.dumps(result.to_dict(), allow_nan=False)


def test_benchmark_receives_the_same_opening_date_as_other_analytics(workspace):
    from src.market_data.benchmarks import SyntheticBenchmarkProvider

    class RecordingProvider(SyntheticBenchmarkProvider):
        def fetch_daily_returns(self, definition, *, start_date, end_date, base_currency):
            # Providers select inclusive closing dates, retaining the opening baseline.
            assert start_date == START + timedelta(days=1)
            assert end_date == END
            return super().fetch_daily_returns(definition, start_date=start_date, end_date=end_date, base_currency=base_currency)

    result = GetBenchmarkComparisonUseCase(settings=workspace, benchmark_provider=RecordingProvider()).execute(
        AnalyticsRequest(benchmark_id="msci_world"),
    )
    assert result.data["benchmarks"]["comparison"]["comparisons"][0]["period_start"] == START.isoformat()


def test_catalog_covers_return_and_risk_definitions():
    result = GetMetricDefinitionsUseCase().execute()
    assert_json_tree(asdict(result))
    assert {"twr", "mwr_xirr", "volatility_annualized", "beta"} <= {row["metric_id"] for row in result.definitions}


def test_position_price_returns_ignore_quantity_and_missing_valuations():
    frame = sample_metrics().position_metrics
    baseline = analyze_positions(frame, start_date=START, end_date=END, bucket_mapping={"ISIN_A": "core"})
    frame.loc[frame.index > 30, "quantity"] *= 100
    changed = analyze_positions(frame, start_date=START, end_date=END, bucket_mapping={"ISIN_A": "core"})
    assert changed.asset_risk == baseline.asset_risk
    assert changed.diversification == baseline.diversification
    assert next(row for row in baseline.concentration.groups if row.dimension == "bucket").group == "core"
    frame.loc[frame["valuation_date"] == END, "market_value_base"] = float("nan")
    assert analyze_positions(frame, start_date=START, end_date=END, bucket_mapping={}).diversification is None


def test_risk_excludes_multiday_intervals(workspace, monkeypatch):
    metrics = sample_metrics()
    monkeypatch.setattr(application, "calculate_portfolio_metrics_from_normalized_degiro", lambda **kw: replace(
        metrics, portfolio_daily_metrics=metrics.portfolio_daily_metrics.drop(index=30),
    ))
    result = GetPortfolioRiskUseCase(settings=workspace).execute()
    assert "multiday_returns_excluded_from_risk" in result.warnings
    risk = {row["metric_id"]: row for row in result.data["risk"]["portfolio"]["metrics"]}
    assert risk["max_drawdown"]["value"] is None
    assert risk["volatility_annualized"]["coverage_ratio"] < 1


def test_read_only_repository_cannot_modify_or_create_database(tmp_path):
    settings = settings_for(tmp_path)
    repository = DuckDBMarketDataRepository(settings=settings, read_only=True)
    with pytest.raises(FileNotFoundError):
        with repository.connection():
            pass
    assert not settings.data_dir.exists()
    settings.portfolio_db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(settings.portfolio_db_path)) as connection:
        connection.execute("CREATE TABLE marker (id INTEGER)")
    with repository.connection() as connection:
        assert connection.execute("SELECT count(*) FROM marker").fetchone() == (0,)
        with pytest.raises(duckdb.InvalidInputException):
            connection.execute("INSERT INTO marker VALUES (1)")


def test_real_synthetic_pipeline_stays_in_isolated_workspace(tmp_path):
    from scripts.bootstrap_demo import _upsert_synthetic_prices, DEMO_AS_OF_DATE
    from src.degiro_exports.importer import import_degiro_exports
    from src.degiro_exports.warehouse import load_normalized_degiro_to_duckdb

    settings = settings_for(tmp_path)
    source = default_repo_root() / "demo" / "synthetic_degiro_exports"
    import_degiro_exports(settings=settings, incoming_dir=source / "incoming",
                         output_dir=settings.normalized_data_dir / "degiro", base_currency="EUR", source_root=source)
    load_normalized_degiro_to_duckdb(settings=settings)
    _upsert_synthetic_prices(DuckDBMarketDataRepository(settings=settings))
    before = {str(p.relative_to(tmp_path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
    result = GetAnalyticsSummaryUseCase(settings=settings).execute(AnalyticsRequest(
        as_of_date=DEMO_AS_OF_DATE.isoformat(), benchmark_id="portfolio_60_40",
    ))
    assert result.data["performance"]["period"]["twr"]["value"] is not None
    assert result.data["benchmarks"]["comparison"]["comparisons"][0]["provider_name"] == "synthetic"
    assert_json_tree(asdict(result))
    after = {str(p.relative_to(tmp_path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
    assert before == after
