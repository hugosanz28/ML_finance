from datetime import date
from pathlib import Path
from types import SimpleNamespace

from src.application import extract_report_as_of_date_from_path
from src.market_data import FxRefreshOutcome, FxRefreshSummary, PriceRefreshOutcome, PriceRefreshSummary
from src.portfolio.dashboard_overview import refresh_market_data_to_date
from src.portfolio.dashboard_uploads import (
    _canonical_degiro_upload_name,
    _detect_degiro_upload_kind,
    _extract_dates_from_filename,
)


def test_canonical_degiro_upload_name_detects_portfolio_and_uses_filename_date() -> None:
    assert (
        _canonical_degiro_upload_name("Cartera DEGIRO 29-04-2026.csv", fallback_date=date(2026, 4, 30))
        == "portfolio_2026-04-29.csv"
    )


def test_canonical_degiro_upload_name_detects_transactions_and_date_range() -> None:
    assert (
        _canonical_degiro_upload_name(
            "Transactions_2025-11-01_2026-04-12.csv",
            fallback_date=date(2026, 4, 30),
        )
        == "transactions_2025-11-01_2026-04-12.csv"
    )


def test_canonical_degiro_upload_name_detects_account_and_compact_dates() -> None:
    assert (
        _canonical_degiro_upload_name(
            "Movimientos cuenta 01112025 12042026.csv",
            fallback_date=date(2026, 4, 30),
        )
        == "account_2025-11-01_2026-04-12.csv"
    )


def test_canonical_degiro_upload_name_uses_fallback_date_when_missing_dates() -> None:
    assert (
        _canonical_degiro_upload_name("portfolio.csv", fallback_date=date(2026, 4, 30))
        == "portfolio_2026-04-30.csv"
    )
    assert (
        _canonical_degiro_upload_name("transacciones.csv", fallback_date=date(2026, 4, 30))
        == "transactions_2026-04-30_2026-04-30.csv"
    )


def test_canonical_degiro_upload_name_rejects_unknown_names() -> None:
    assert _canonical_degiro_upload_name("export.csv", fallback_date=date(2026, 4, 30)) is None


def test_detect_degiro_upload_kind_handles_accents() -> None:
    assert _detect_degiro_upload_kind("transacción 2026.csv") == "transactions"


def test_extract_dates_from_filename_deduplicates_and_sorts() -> None:
    assert _extract_dates_from_filename("orders 2026-04-12 01_11_2025 20260412.csv") == [
        date(2025, 11, 1),
        date(2026, 4, 12),
    ]


def test_extract_report_as_of_date_from_path() -> None:
    assert extract_report_as_of_date_from_path(Path("2026-05-06-monthly-abc.md")) == date(2026, 5, 6)
    assert extract_report_as_of_date_from_path(Path("monthly-latest.md")) is None


def test_refresh_market_data_to_date_updates_fx_and_prices_to_target(monkeypatch) -> None:
    calls = {}
    settings = SimpleNamespace()
    target_date = date(2026, 5, 14)

    class FakeRefreshFxUseCase:
        def __init__(self, *, settings):
            calls["fx_settings"] = settings

        def execute(self, request):
            calls["fx_request"] = request
            return SimpleNamespace(
                summary=FxRefreshSummary(
                    provider_name="fake",
                    outcomes=(
                        FxRefreshOutcome(
                            base_currency="EUR",
                            quote_currency="USD",
                            provider_name="fake",
                            status="updated",
                            records_written=2,
                        ),
                    ),
                )
            )

    class FakeRefreshMarketDataUseCase:
        def __init__(self, *, settings):
            calls["price_settings"] = settings

        def execute(self, request):
            calls["price_request"] = request
            return SimpleNamespace(
                summary=PriceRefreshSummary(
                    provider_name="fake",
                    outcomes=(
                        PriceRefreshOutcome(
                            asset_id="db_asset",
                            provider_name="fake",
                            status="updated",
                            records_written=3,
                        ),
                    ),
                )
            )

    monkeypatch.setattr("src.portfolio.dashboard_overview.RefreshFxUseCase", FakeRefreshFxUseCase)
    monkeypatch.setattr("src.portfolio.dashboard_overview.RefreshMarketDataUseCase", FakeRefreshMarketDataUseCase)

    result = refresh_market_data_to_date(settings=settings, target_date=target_date)

    assert result["target_date"] == target_date
    assert result["fx_summary"].total_records == 2
    assert result["price_summary"].total_records == 3
    assert calls["fx_settings"] is settings
    assert calls["price_settings"] is settings
    assert calls["fx_request"].end_date == target_date
    assert calls["fx_request"].only_missing_base is False
    assert calls["price_request"].end_date == target_date
