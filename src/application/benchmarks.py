"""Application boundary for an explicit real benchmark cache refresh."""

from dataclasses import dataclass
from datetime import date

from src.application.types import ApplicationResult
from src.config import Settings
from src.market_data.benchmark_cache import refresh_benchmark_cache, validate_window


@dataclass(frozen=True)
class RefreshBenchmarksRequest:
    start_date: date
    end_date: date
    provider: str

    def validate(self):
        if self.provider != "yfinance_ecb":
            raise ValueError("benchmark_provider_required")


class RefreshBenchmarksUseCase:
    def __init__(self, *, settings: Settings):
        self.settings = settings

    def execute(self, request: RefreshBenchmarksRequest) -> ApplicationResult:
        request.validate()
        if self.settings.price_provider == "synthetic" or self.settings.data_dir.resolve().is_relative_to(self.settings.repo_root / "demo"):
            raise ValueError("external_provider_forbidden_in_demo")
        validate_window(request.start_date, request.end_date, self.settings.default_currency)
        result = refresh_benchmark_cache(self.settings.data_dir, request.start_date, request.end_date, self.settings.default_currency)
        return ApplicationResult(name="refresh_benchmarks", status="succeeded",
                                 message="Real benchmark cache refreshed; ETF references are proxies.",
                                 warnings=("benchmark_etf_proxy",), artifacts={
                                     "content_sha256": result["content_sha256"], "fetched_at": result["fetched_at"],
                                     **{f"observations_{key}": count for key, count in result["observations"].items()},
                                 })
