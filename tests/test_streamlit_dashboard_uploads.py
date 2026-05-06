from datetime import date

from src.portfolio.dashboard import (
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
