from __future__ import annotations

import csv
from pathlib import Path
import shutil
import uuid

import pandas as pd

from src.config import load_settings
from src.degiro_exports.cash_movements import EXPECTED_ACCOUNT_HEADERS
from src.degiro_exports.importer import classify_degiro_export, import_degiro_exports
from src.degiro_exports.portfolio_snapshots import EXPECTED_PORTFOLIO_HEADERS
from src.degiro_exports.transactions import EXPECTED_TRANSACTION_HEADERS


def make_test_workspace() -> Path:
    base_dir = Path("src/data/local/test_runs")
    base_dir.mkdir(parents=True, exist_ok=True)
    workspace = base_dir / f"degiro-importer-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def test_classify_degiro_export_uses_canonical_filenames() -> None:
    assert classify_degiro_export("transactions_2025-11-01_2026-04-12.csv") == "transactions"
    assert classify_degiro_export("account_2025-11-01_2026-04-12.csv") == "cash_movements"
    assert classify_degiro_export("portfolio_2026-04-12.csv") == "portfolio_snapshot"
    assert classify_degiro_export("other.csv") == "unknown"


def test_import_degiro_exports_imports_all_canonical_csvs() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        incoming_dir = repo_root / "src" / "degiro_exports" / "local" / "incoming"
        incoming_dir.mkdir(parents=True)
        _write_transactions_fixture(incoming_dir / "transactions_2025-11-01_2026-04-12.csv")
        _write_account_fixture(incoming_dir / "account_2025-11-01_2026-04-12.csv")
        _write_portfolio_fixture(incoming_dir / "portfolio_2026-04-12.csv")

        settings = load_settings(repo_root=repo_root, env={})
        summary = import_degiro_exports(settings=settings)

        assert summary.imported_count == 3
        assert summary.failed_count == 0
        assert {outcome.kind for outcome in summary.outcomes} == {
            "transactions",
            "cash_movements",
            "portfolio_snapshot",
        }

        transactions_files = sorted((settings.normalized_data_dir / "degiro" / "transactions").glob("*.parquet"))
        assets_files = sorted((settings.normalized_data_dir / "degiro" / "assets").glob("*.parquet"))
        cash_files = sorted((settings.normalized_data_dir / "degiro" / "cash_movements").glob("*.parquet"))
        snapshot_files = sorted((settings.normalized_data_dir / "degiro" / "portfolio_snapshots").glob("*.parquet"))

        assert len(transactions_files) == 1
        assert len(assets_files) == 1
        assert len(cash_files) == 1
        assert len(snapshot_files) == 1
        assert len(pd.read_parquet(transactions_files[0])) == 1
        assert len(pd.read_parquet(cash_files[0])) == 1
        assert len(pd.read_parquet(snapshot_files[0])) == 1
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_import_degiro_exports_reports_unknown_csvs() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        incoming_dir = repo_root / "src" / "degiro_exports" / "local" / "incoming"
        incoming_dir.mkdir(parents=True)
        (incoming_dir / "unknown.csv").write_text("x,y\n1,2\n", encoding="utf-8")

        settings = load_settings(repo_root=repo_root, env={})
        summary = import_degiro_exports(settings=settings)
        ignored_summary = import_degiro_exports(settings=settings, ignore_unknown=True)

        assert summary.failed_count == 1
        assert summary.outcomes[0].status == "failed"
        assert ignored_summary.skipped_count == 1
        assert ignored_summary.outcomes[0].status == "skipped"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_import_degiro_exports_dry_run_does_not_write_outputs() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        incoming_dir = repo_root / "src" / "degiro_exports" / "local" / "incoming"
        incoming_dir.mkdir(parents=True)
        _write_portfolio_fixture(incoming_dir / "portfolio_2026-04-12.csv")

        settings = load_settings(repo_root=repo_root, env={})
        summary = import_degiro_exports(settings=settings, dry_run=True)

        assert summary.would_import_count == 1
        assert not (settings.normalized_data_dir / "degiro" / "portfolio_snapshots").exists()
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def _write_transactions_fixture(csv_path: Path) -> None:
    rows = [
        [
            "02-04-2026",
            "09:10",
            "STST SPDR MSCI ALL CNTRY WORLD EURH",
            "IE00BF1B7389",
            "XET",
            "XETA",
            "64",
            "23,1800",
            "EUR",
            "-1483,52",
            "EUR",
            "-1483,52",
            "",
            "0,00",
            "-3,00",
            "-1486,52",
            "",
            "1abb5ced-f580-4dfd-8a35-c838518e8ef9",
        ],
    ]
    _write_csv(
        csv_path,
        EXPECTED_TRANSACTION_HEADERS,
        rows,
    )


def _write_account_fixture(csv_path: Path) -> None:
    rows = [
        [
            "15-01-2026",
            "10:00",
            "15-01-2026",
            "",
            "",
            "Ingreso",
            "",
            "EUR",
            "1000,00",
            "EUR",
            "1000,00",
            "",
        ],
    ]
    _write_csv(
        csv_path,
        EXPECTED_ACCOUNT_HEADERS,
        rows,
    )


def _write_portfolio_fixture(csv_path: Path) -> None:
    _write_csv(
        csv_path,
        EXPECTED_PORTFOLIO_HEADERS,
        [["CASH & CASH FUND & FTX CASH (EUR)", "", "", "", "EUR", "1000,00", "1000,00"]],
    )


def _write_csv(csv_path: Path, header: list[str], rows: list[list[str]]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
