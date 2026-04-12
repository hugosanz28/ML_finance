from __future__ import annotations

import csv
from pathlib import Path
import shutil
import uuid

import pandas as pd
import pytest

from src.config import load_settings
from src.degiro_exports.portfolio_snapshots import (
    parse_and_persist_degiro_portfolio_snapshots,
    parse_degiro_portfolio_snapshot_csv,
)


PORTFOLIO_HEADER = [
    "Producto",
    "Symbol/ISIN",
    "Cantidad",
    "Precio de",
    "Valor local",
    "",
    "Valor en EUR",
]


def write_portfolio_fixture(csv_path: Path) -> None:
    rows = [
        ["CASH & CASH FUND & FTX CASH (EUR)", "", "", "", "EUR", "31,09", "31,09"],
        ["ALPHABET INC CLASS A", "US02079K3059", "4", "317,24", "USD", "1268,96", "1082,42"],
        ["BITCOIN", "XFC000A2YY6Q", "0,0059", "62655,46", "EUR", "367,47", "367,47"],
        ["AMUNDI PRIME EURO GOVERNMENT BOND", "LU2233156582", "18", "21,56", "EUR", "388,04", "388,04"],
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(PORTFOLIO_HEADER)
        writer.writerows(rows)


def make_test_workspace() -> Path:
    base_dir = Path("src/data/local/test_runs")
    base_dir.mkdir(parents=True, exist_ok=True)
    workspace = base_dir / f"degiro-portfolio-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def test_parse_degiro_portfolio_snapshot_csv_normalizes_snapshot_rows() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "portfolio_2026-04-12.csv"
        write_portfolio_fixture(csv_path)

        parsed = parse_degiro_portfolio_snapshot_csv(csv_path, base_currency="EUR")

        assert parsed.snapshot_date.isoformat() == "2026-04-12"
        assert len(parsed.snapshots) == 4

        cash_row = parsed.snapshots.loc[parsed.snapshots["asset_type"] == "cash"].iloc[0]
        assert cash_row["asset_id"] == "degiro:cash:eur"
        assert cash_row["quantity"] == pytest.approx(31.09)
        assert cash_row["market_price"] == pytest.approx(1.0)
        assert "cash_snapshot_derived_quantity_and_price" in str(cash_row["notes"])

        usd_row = parsed.snapshots.loc[parsed.snapshots["asset_name"] == "ALPHABET INC CLASS A"].iloc[0]
        assert usd_row["asset_id"] == "degiro:isin:US02079K3059"
        assert usd_row["position_currency"] == "USD"
        assert usd_row["market_value"] == pytest.approx(1268.96)
        assert usd_row["market_value_base"] == pytest.approx(1082.42)
        assert usd_row["fx_rate_to_base"] == pytest.approx(1.1723360618, abs=1e-10)
        assert "fx_rate_derived_from_market_values" in str(usd_row["notes"])

        crypto_row = parsed.snapshots.loc[parsed.snapshots["asset_name"] == "BITCOIN"].iloc[0]
        assert crypto_row["asset_type"] == "crypto"
        assert crypto_row["quantity"] == pytest.approx(0.0059)

        bond_row = parsed.snapshots.loc[parsed.snapshots["asset_name"] == "AMUNDI PRIME EURO GOVERNMENT BOND"].iloc[0]
        assert bond_row["asset_type"] == "bond"
        assert bond_row["fx_rate_to_base"] == pytest.approx(1.0)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_and_persist_degiro_portfolio_snapshots_writes_parquet_output() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        source_dir = repo_root / "incoming"
        source_dir.mkdir(parents=True)
        csv_path = source_dir / "portfolio_2026-04-12.csv"
        write_portfolio_fixture(csv_path)

        settings = load_settings(repo_root=repo_root, env={})
        parsed = parse_and_persist_degiro_portfolio_snapshots(csv_path, settings=settings)

        assert parsed.output_path is not None
        assert parsed.output_path.exists()

        frame = pd.read_parquet(parsed.output_path)
        assert len(frame) == 4
        assert frame["snapshot_id"].is_unique
        assert set(["snapshot_date", "asset_id", "market_value_base"]) <= set(frame.columns)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_degiro_portfolio_snapshot_csv_rejects_invalid_filename() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "portfolio_snapshot.csv"
        write_portfolio_fixture(csv_path)

        with pytest.raises(ValueError, match="portfolio_YYYY-MM-DD.csv"):
            parse_degiro_portfolio_snapshot_csv(csv_path)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
