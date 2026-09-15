"""No-network contracts for real-source adapters, cache integrity and API jobs."""

from datetime import date
import hashlib
import io
import json

from fastapi.testclient import TestClient
import pandas as pd
import pytest

import src.application.analytics as analytics
import src.market_data.benchmark_cache as cache
from src.api.app import create_app
from src.application.analytics import AnalyticsRequest, GetBenchmarkComparisonUseCase
from src.application.benchmarks import RefreshBenchmarksRequest, RefreshBenchmarksUseCase
from src.application.local_jobs import LocalJobManager, SubmitJobRequest, RetryJobRequest
from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.config import load_settings
from src.portfolio.benchmarks import build_benchmark_return_series
from tests.test_analytics_application import sample_metrics


START, END = date(2026, 1, 1), date(2026, 3, 7)


class OfflineDownloader:
    def levels(self, source, start, end):
        return [{"date": day.date().isoformat(), "value": 100 + i} for i, day in enumerate(pd.bdate_range(start, end))]


def settings_for(root, **values):
    return load_settings(repo_root=root, env_file=root / "absent.env", env={"PRICE_PROVIDER": "yfinance", **values})


@pytest.fixture(autouse=True)
def no_external(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Tests must not download external data")
    monkeypatch.setattr(cache, "urlopen", forbidden)
    monkeypatch.setattr(cache.PublicBenchmarkDownloader, "levels", forbidden)


def populated(root, currency="EUR", downloader=None):
    cache.refresh_benchmark_cache(root, START, END, currency, downloader=downloader or OfflineDownloader())
    return cache.CachedBenchmarkProvider(root)


def read(provider, key="msci_world", start=START, end=END, currency="EUR"):
    return build_benchmark_return_series(key, provider=provider, start_date=start, end_date=end, base_currency=currency)


def test_refresh_hashes_sources_and_four_offline_options(tmp_path):
    provider = populated(tmp_path)
    before = (tmp_path / "benchmark_cache.json").read_bytes()
    for key in ("msci_world", "sp500", "portfolio_60_40", "estr_cash"):
        result = read(provider, key)
        assert result.observations
        assert result.source_reference and result.sources
        assert result.sources[0]["content_sha256"].startswith("sha256:")
        assert ("benchmark_etf_proxy" in result.warning_codes) == (key != "estr_cash")
        assert result.source_currency == "EUR"
    assert (tmp_path / "benchmark_cache.json").read_bytes() == before


def test_missing_corrupt_and_incompatible_cache_fail_closed(tmp_path):
    provider = cache.CachedBenchmarkProvider(tmp_path)
    assert read(provider).reason_code == "benchmark_cache_missing"
    populated(tmp_path)
    assert read(provider, currency="USD").reason_code == "benchmark_cache_currency_mismatch"
    path = tmp_path / "benchmark_cache.json"
    data = json.loads(path.read_text())
    data["data"]["entries"]["msci_world"]["levels"][0]["value"] = 1000000
    path.write_text(json.dumps(data))
    assert read(provider).reason_code == "benchmark_cache_invalid"


def test_valid_hash_does_not_replace_schema_and_window_validation(tmp_path):
    provider = populated(tmp_path)
    path = tmp_path / "benchmark_cache.json"
    document = json.loads(path.read_text())
    document["data"]["requested_start"] = "2026-02-01"
    document["content_sha256"] = cache.content_hash(document["data"])
    path.write_text(json.dumps(document))
    assert read(provider).reason_code == "benchmark_cache_invalid"


def test_failed_refresh_preserves_last_complete_snapshot(tmp_path):
    populated(tmp_path)
    before = (tmp_path / "benchmark_cache.json").read_bytes()
    class Failing(OfflineDownloader):
        def levels(self, source, start, end):
            if source.provider == "ecb":
                raise RuntimeError("provider unavailable")
            return super().levels(source, start, end)
    with pytest.raises(RuntimeError):
        populated(tmp_path, downloader=Failing())
    assert (tmp_path / "benchmark_cache.json").read_bytes() == before


def test_failed_atomic_replace_preserves_cache_and_cleans_temp(tmp_path, monkeypatch):
    populated(tmp_path)
    before = (tmp_path / "benchmark_cache.json").read_bytes()
    def fail(*args):
        raise OSError("locked")
    monkeypatch.setattr(cache.os, "replace", fail)
    with pytest.raises(OSError):
        populated(tmp_path)
    assert (tmp_path / "benchmark_cache.json").read_bytes() == before
    assert not list(tmp_path.glob(".benchmarks-*.tmp"))


@pytest.mark.parametrize("values", [[1, -1], [1, float("inf")], [1, "broken"], [None, None]])
def test_invalid_levels_rejected(values):
    with pytest.raises(cache.BenchmarkCacheError):
        cache.normalize_levels(pd.DataFrame({"date": ["2026-01-01", "2026-01-02"], "value": values}))


def test_duplicate_dates_rejected():
    with pytest.raises(cache.BenchmarkCacheError):
        cache.normalize_levels(pd.DataFrame({"date": ["2026-01-01"] * 2, "value": [1, 2]}))


def test_explicit_hole_is_not_bridged_or_filled(tmp_path):
    class Hole(OfflineDownloader):
        def levels(self, source, start, end):
            rows = super().levels(source, start, end)
            rows[20]["value"] = None
            return rows
    provider = populated(tmp_path, downloader=Hole())
    series = read(provider)
    hole = date.fromisoformat(cache.load_benchmark_cache(tmp_path)["entries"]["msci_world"]["levels"][20]["date"])
    assert not any(row.previous_observation_date < hole <= row.observation_date for row in series.observations)
    assert "benchmark_missing_observations" in series.warning_codes
    composite = read(provider, "portfolio_60_40")
    assert "benchmark_common_window_truncated" in composite.warning_codes
    assert composite.observations[0].previous_observation_date > hole


def test_fx_orientation_uses_target_units_per_eur(tmp_path):
    class FX(OfflineDownloader):
        def levels(self, source, start, end):
            return [{"date": "2026-01-01", "value": 1 if source.source_id.startswith("EXR") else 100},
                    {"date": "2026-01-02", "value": 1.2 if source.source_id.startswith("EXR") else 110}]
    provider = populated(tmp_path, "USD", FX())
    result = read(provider, start=date(2026, 1, 2), end=date(2026, 1, 2), currency="USD")
    assert result.observations[0].return_decimal == pytest.approx(.32)
    assert result.sources[-1]["source_id"] == "EXR.D.USD.EUR.SP00.A"


def test_missing_fx_endpoints_are_not_filled(tmp_path):
    class FX(OfflineDownloader):
        def levels(self, source, start, end):
            rows = super().levels(source, start, end)
            if source.source_id.startswith("EXR"):
                rows[5]["value"] = None
            return rows
    result = read(populated(tmp_path, "USD", FX()), currency="USD")
    assert result.reason_code == "missing_fx_rates"
    assert result.coverage_ratio < 1


def test_stale_cache_remains_explicit(tmp_path):
    result = read(populated(tmp_path), end=date(2026, 4, 30))
    assert "benchmark_cache_stale" in result.warning_codes


@pytest.mark.parametrize("start,end,currency", [
    (END, START, "EUR"), (START, date.today(), "EUR"), (date(1999, 1, 1), END, "EUR"), (START, END, "XYZ"),
])
def test_invalid_window_or_currency_rejected_before_download(tmp_path, start, end, currency):
    with pytest.raises(cache.BenchmarkCacheError):
        cache.refresh_benchmark_cache(tmp_path, start, end, currency)
    assert not (tmp_path / "benchmark_cache.json").exists()


def test_ecb_parser_checks_series_identity(monkeypatch, tmp_path):
    data = b"KEY,TIME_PERIOD,OBS_VALUE,OBS_STATUS\nEST.B.EU000A2QQF08.CI,2026-01-01,100,A\nEST.B.EU000A2QQF08.CI,2026-01-02,101,A\n"
    urls = []
    def response(url, **kwargs):
        urls.append(url)
        return io.BytesIO(data)
    monkeypatch.setattr(cache, "urlopen", response)
    rows = cache.PublicBenchmarkDownloader(tmp_path).ecb_levels("EST.B.EU000A2QQF08.CI", START, END)
    assert rows[1]["value"] == 101
    assert urls[0].startswith("https://data-api.ecb.europa.eu/service/data/EST/")
    with pytest.raises(cache.BenchmarkCacheError):
        cache.PublicBenchmarkDownloader(tmp_path).ecb_levels("EXR.D.USD.EUR.SP00.A", START, END)


def test_yfinance_uses_adjusted_close_and_checks_quote_currency(tmp_path, monkeypatch):
    import yfinance as yf
    kwargs_seen = {}
    class Ticker:
        def __init__(self, symbol):
            assert symbol == "SXR8.DE"
        def history(self, **kwargs):
            kwargs_seen.update(kwargs)
            return pd.DataFrame({"Adj Close": [50, 55], "Close": [100, 55]}, index=pd.to_datetime(["2026-01-01", "2026-01-02"]))
        def get_history_metadata(self):
            return {"currency": "EUR"}
    monkeypatch.setattr(yf, "Ticker", Ticker)
    monkeypatch.setattr(yf, "set_tz_cache_location", lambda *args: None)
    # Restore this one adapter method; its underlying client is fully stubbed.
    levels_method = ORIGINAL_LEVELS
    downloader = cache.PublicBenchmarkDownloader(tmp_path)
    rows = levels_method(downloader, cache.SOURCES["sp500"], START, END)
    assert rows[0]["value"] == 50
    assert kwargs_seen["auto_adjust"] is False and kwargs_seen["keepna"] is True
    monkeypatch.setattr(Ticker, "get_history_metadata", lambda self: {"currency": "USD"})
    with pytest.raises(cache.BenchmarkCacheError):
        levels_method(downloader, cache.SOURCES["sp500"], START, END)


ORIGINAL_LEVELS = cache.PublicBenchmarkDownloader.levels


def test_application_real_reads_use_cache_and_do_not_write_or_download(tmp_path, monkeypatch):
    settings = settings_for(tmp_path)
    populated(settings.data_dir)
    settings.portfolio_db_path.touch()
    monkeypatch.setattr(analytics, "calculate_portfolio_metrics_from_normalized_degiro", lambda **kw: sample_metrics())
    monkeypatch.setattr(analytics, "load_normalized_degiro_cash_movements", lambda **kw: pd.DataFrame([{"movement_date": START, "movement_type": "DEPOSIT", "amount_base": 1000}]))
    before = hashlib.sha256((settings.data_dir / "benchmark_cache.json").read_bytes()).hexdigest()
    result = GetBenchmarkComparisonUseCase(settings=settings).execute(AnalyticsRequest(benchmark_id="sp500"))
    comparison = result.data["benchmarks"]["comparison"]["comparisons"][0]
    assert comparison["provider_name"] == "cached_yfinance_ecb"
    assert "proxy" in comparison["source_reference"]
    assert "benchmark_etf_proxy" in comparison["reason_codes"]
    assert comparison["sources"][0]["source_id"] == "SXR8.DE"
    assert hashlib.sha256((settings.data_dir / "benchmark_cache.json").read_bytes()).hexdigest() == before
    with TestClient(create_app(settings=settings), base_url="http://localhost") as client:
        assert client.get("/api/v1/analytics/benchmarks?benchmark_id=sp500").status_code == 200


def test_demo_refuses_refresh_even_with_provider_selected(tmp_path):
    settings = settings_for(tmp_path, PRICE_PROVIDER="synthetic", DATA_DIR="demo/local_data")
    with pytest.raises(ValueError, match="external_provider_forbidden"):
        RefreshBenchmarksUseCase(settings=settings).execute(RefreshBenchmarksRequest(START, END, "yfinance_ecb"))
    with TestClient(create_app(settings=settings, workspace_mode="demo"), base_url="http://localhost") as client:
        response = client.post("/api/v1/benchmarks/refresh", json={"workspace_mode": "demo", "confirm": True,
                               "provider": "yfinance_ecb", "start_date": str(START), "end_date": str(END)},
                               headers={"X-ML-Finance-Confirm": "local-write", "Idempotency-Key": "benchmark-test-001"})
        assert response.status_code == 422


def test_job_refresh_is_idempotent_and_retry_is_forbidden(tmp_path, monkeypatch):
    settings = settings_for(tmp_path)
    monkeypatch.setattr(cache.PublicBenchmarkDownloader, "levels", OfflineDownloader.levels)
    jobs = LocalJobManager(OperationalWorkspace(settings, "real"))
    jobs.open(start_worker=False)
    try:
        params = {"workspace_mode": "real", "confirm": True, "provider": "yfinance_ecb", "start_date": str(START), "end_date": str(END)}
        job = jobs.submit(SubmitJobRequest("benchmarks", params, "benchmark-test-001"))
        assert jobs.submit(SubmitJobRequest("benchmarks", params, "benchmark-test-001"))["job_id"] == job["job_id"]
        assert jobs.run_one()
        result = jobs.get(job["job_id"])
        assert result["state"] == "succeeded"
        assert result["result"]["artifacts"]["content_sha256"].startswith("sha256:")
        def fail(*args):
            raise RuntimeError("offline")
        monkeypatch.setattr(cache.PublicBenchmarkDownloader, "levels", fail)
        failed_job = jobs.submit(SubmitJobRequest("benchmarks", params, "benchmark-failure-001"))
        jobs.run_one()
        with pytest.raises(OperationError, match="retry_not_safe"):
            jobs.retry(RetryJobRequest(failed_job["job_id"], "benchmark-retry-001", "real", True))
    finally:
        jobs.close()


def test_http_refresh_requires_confirmation_and_completed_dates(tmp_path):
    settings = settings_for(tmp_path)
    with TestClient(create_app(settings=settings, workspace_mode="real"), base_url="http://localhost") as client:
        payload = {"workspace_mode": "real", "confirm": True, "provider": "yfinance_ecb", "start_date": str(START), "end_date": str(END)}
        assert client.post("/api/v1/benchmarks/refresh", json=payload).status_code == 403
        headers = {"X-ML-Finance-Confirm": "local-write", "Idempotency-Key": "benchmark-test-001"}
        for extra in ({"end_date": str(date.today())}, {"start_date": "invalid"}, {"provider": "synthetic"}, {"path": "private"}):
            assert client.post("/api/v1/benchmarks/refresh", json={**payload, **extra}, headers=headers).status_code == 422
