from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
from uuid import uuid4

import pandas as pd
import pytest

from src.config import default_repo_root, load_settings
from src.market_data import (
    DuckDBMarketDataRepository,
    FxDataNotFoundError,
    FxFetchResult,
    FxProvider,
    FxRateRecord,
    FxRefreshService,
    YFinanceFxProvider,
    infer_fx_requirements_from_normalized_degiro,
)


class StubFxProvider(FxProvider):
    def __init__(self, responses: dict[tuple[str, str], FxFetchResult | Exception]) -> None:
        self._responses = responses
        self.requests: list[tuple[str, str, date, date]] = []

    @property
    def name(self) -> str:
        return "stub"

    def fetch_fx_rates(
        self,
        *,
        base_currency: str,
        quote_currency: str,
        start_date: date,
        end_date: date,
    ) -> FxFetchResult:
        self.requests.append((base_currency, quote_currency, start_date, end_date))
        response = self._responses[(base_currency, quote_currency)]
        if isinstance(response, Exception):
            raise response
        return response


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


def test_infer_fx_requirements_from_normalized_degiro_detects_non_base_currencies(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    transactions_dir = settings.normalized_data_dir / "degiro" / "transactions"
    cash_dir = settings.normalized_data_dir / "degiro" / "cash_movements"
    snapshots_dir = settings.normalized_data_dir / "degiro" / "portfolio_snapshots"
    transactions_dir.mkdir(parents=True, exist_ok=True)
    cash_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "trade_date": "2026-01-02",
                "transaction_currency": "USD",
                "base_currency": "EUR",
                "gross_amount_base": 100.0,
                "net_cash_amount_base": -101.0,
            },
            {
                "trade_date": "2026-01-03",
                "transaction_currency": "EUR",
                "base_currency": "EUR",
                "gross_amount_base": 50.0,
                "net_cash_amount_base": -51.0,
            },
        ]
    ).to_parquet(transactions_dir / "transactions.parquet", index=False)
    pd.DataFrame(
        [
            {
                "movement_date": "2026-01-04",
                "movement_currency": "CAD",
                "base_currency": "EUR",
                "amount_base": None,
            }
        ]
    ).to_parquet(cash_dir / "cash.parquet", index=False)
    pd.DataFrame(
        [
            {
                "snapshot_date": "2026-01-05",
                "position_currency": "USD",
                "base_currency": "EUR",
                "market_value_base": 120.0,
            }
        ]
    ).to_parquet(snapshots_dir / "snapshot.parquet", index=False)

    requirements = infer_fx_requirements_from_normalized_degiro(settings=settings)

    assert [(requirement.pair, requirement.start_date, requirement.end_date) for requirement in requirements] == [
        ("EUR/CAD", date(2026, 1, 4), date(2026, 1, 4)),
        ("EUR/USD", date(2026, 1, 2), date(2026, 1, 5)),
    ]
    assert requirements[0].missing_base_rows == 1
    assert requirements[1].source_rows == 2


def test_infer_fx_requirements_can_focus_on_missing_base_amounts(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    cash_dir = settings.normalized_data_dir / "degiro" / "cash_movements"
    cash_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "movement_date": "2026-01-04",
                "movement_currency": "USD",
                "base_currency": "EUR",
                "amount_base": 20.0,
            },
            {
                "movement_date": "2026-01-05",
                "movement_currency": "CAD",
                "base_currency": "EUR",
                "amount_base": None,
            },
        ]
    ).to_parquet(cash_dir / "cash.parquet", index=False)

    requirements = infer_fx_requirements_from_normalized_degiro(
        settings=settings,
        only_missing_base=True,
    )

    assert [requirement.pair for requirement in requirements] == ["EUR/CAD"]


def test_fx_refresh_service_persists_inferred_rates(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    cash_dir = settings.normalized_data_dir / "degiro" / "cash_movements"
    cash_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "movement_date": "2026-01-04",
                "movement_currency": "USD",
                "base_currency": "EUR",
                "amount_base": None,
            }
        ]
    ).to_parquet(cash_dir / "cash.parquet", index=False)

    provider = StubFxProvider(
        {
            ("EUR", "USD"): FxFetchResult(
                provider_name="stub",
                base_currency="EUR",
                quote_currency="USD",
                rate_records=(
                    FxRateRecord(rate_date=date(2026, 1, 4), rate=1.10),
                    FxRateRecord(rate_date=date(2026, 1, 5), rate=1.11),
                ),
            )
        }
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    service = FxRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_rates(end_date=date(2026, 1, 5))
    rows = repository.list_fx_rates(base_currency="EUR", quote_currency="USD", provider_name="stub")

    assert summary.updated_pairs == 1
    assert summary.total_records == 2
    assert provider.requests == [("EUR", "USD", date(2026, 1, 4), date(2026, 1, 5))]
    assert rows == [
        ("EUR", "USD", date(2026, 1, 4), "stub", 1.1),
        ("EUR", "USD", date(2026, 1, 5), "stub", 1.11),
    ]


def test_fx_refresh_service_reports_skipped_pairs(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    provider = StubFxProvider({("EUR", "USD"): FxDataNotFoundError("missing")})
    service = FxRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_rates(
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 5),
        pairs=[("EUR", "USD")],
        infer_from_normalized=False,
    )

    assert summary.skipped_pairs == 1
    assert summary.outcomes[0].pair == "EUR/USD"
    assert "missing" in str(summary.outcomes[0].message)
    assert repository.list_fx_rates() == []


def test_yfinance_fx_provider_parses_close_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = YFinanceFxProvider()
    requested_symbols: list[str] = []

    def fake_download_history(symbol: str, *, start_date: date, end_date: date) -> pd.DataFrame:
        requested_symbols.append(symbol)
        return pd.DataFrame(
            {"Close": [1.10, 1.11, None]},
            index=pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04"]),
        )

    monkeypatch.setattr(provider, "_download_history", fake_download_history)

    result = provider.fetch_fx_rates(
        base_currency="eur",
        quote_currency="usd",
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
    )

    assert requested_symbols == ["EURUSD=X"]
    assert result.base_currency == "EUR"
    assert result.quote_currency == "USD"
    assert result.rate_records == (
        FxRateRecord(rate_date=date(2026, 1, 2), rate=1.10),
        FxRateRecord(rate_date=date(2026, 1, 3), rate=1.11),
    )
