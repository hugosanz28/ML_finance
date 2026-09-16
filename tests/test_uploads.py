from datetime import date
from pathlib import Path

from src.application.uploads import DegiroUpload, SaveDegiroUploadsRequest, SaveDegiroUploadsUseCase
from src.application import extract_report_as_of_date_from_path
from src.config import load_settings
from src.application.uploads import (
    canonical_degiro_upload_name as _canonical_degiro_upload_name,
    detect_degiro_upload_kind as _detect_degiro_upload_kind,
    extract_dates_from_filename as _extract_dates_from_filename,
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


def test_save_degiro_uploads_use_case_writes_only_recognized_files(tmp_path) -> None:
    settings = load_settings(
        repo_root=tmp_path,
        env={"DEGIRO_EXPORTS_DIR": "synthetic_exports"},
        env_file=tmp_path / ".env.missing",
    )
    result = SaveDegiroUploadsUseCase(settings=settings).execute(
        SaveDegiroUploadsRequest(
            uploads=(
                DegiroUpload(filename="Cartera 29-04-2026.csv", content=b"portfolio"),
                DegiroUpload(filename="export.csv", content=b"unknown"),
            ),
            uploaded_at=date(2026, 4, 30),
        )
    )

    assert result.result.status == "partial"
    assert [outcome.status for outcome in result.outcomes] == ["guardado", "omitido"]
    saved_path = settings.degiro_exports_dir / "incoming" / "portfolio_2026-04-29.csv"
    assert saved_path.read_bytes() == b"portfolio"
    assert not (settings.degiro_exports_dir / "incoming" / "export.csv").exists()


def test_extract_report_as_of_date_from_path() -> None:
    assert extract_report_as_of_date_from_path(Path("2026-05-06-monthly-abc.md")) == date(2026, 5, 6)
    assert extract_report_as_of_date_from_path(Path("monthly-latest.md")) is None
