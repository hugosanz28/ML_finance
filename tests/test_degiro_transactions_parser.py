from __future__ import annotations

import csv
from pathlib import Path
import shutil
import uuid

import pandas as pd
import pytest

from src.config import load_settings
from src.degiro_exports.transactions import (
    parse_and_persist_degiro_transactions,
    parse_degiro_transactions_csv,
)


TRANSACTION_HEADER = [
    "Fecha",
    "Hora",
    "Producto",
    "ISIN",
    "Bolsa de referencia",
    "Centro de ejecución",
    "Número",
    "Precio",
    "",
    "Valor local",
    "",
    "Valor EUR",
    "Tipo de cambio",
    "Comisión AutoFX",
    "Costes de transacción y/o externos EUR",
    "Total EUR",
    "ID Orden",
    "",
]


def write_transactions_fixture(csv_path: Path) -> None:
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
        [
            "23-01-2026",
            "16:34",
            "URANIUM ENERGY CORP",
            "US9168961038",
            "NSY",
            "XNYS",
            "-13",
            "19,8000",
            "USD",
            "257,40",
            "USD",
            "219,07",
            "1,1750",
            "-0,55",
            "-2,00",
            "216,52",
            "b7e8da75-7638-44c9-a878-0df4a4d049ba",
        ],
        [
            "11-12-2025",
            "18:51",
            "BITCOIN",
            "XFC000A2YY6Q",
            "TRD",
            "TRDS",
            "",
            "200,0000",
            "EUR",
            "-50,00",
            "EUR",
            "-50,00",
            "",
            "0,00",
            "-0,20",
            "-50,20",
            "",
            "5cc92d92-96f1-4b47-aba1-eb3d1a514d1c",
        ],
        [
            "12-01-2026",
            "09:00",
            "IBERDROLA SA-RTS",
            "ES06445809V1",
            "MAD",
            "XMAD",
            "13",
            "0,0000",
            "EUR",
            "0,00",
            "EUR",
            "0,00",
            "",
            "0,00",
            "",
            "0,00",
            "",
            "",
        ],
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(TRANSACTION_HEADER)
        writer.writerows(rows)


def make_test_workspace() -> Path:
    base_dir = Path("src/data/local/test_runs")
    base_dir.mkdir(parents=True, exist_ok=True)
    workspace = base_dir / f"degiro-transactions-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def test_parse_degiro_transactions_csv_normalizes_transactions_and_asset_hints() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "transactions_2025-11-01_2026-04-12.csv"
        write_transactions_fixture(csv_path)

        parsed = parse_degiro_transactions_csv(csv_path, base_currency="EUR")

        assert parsed.date_from.isoformat() == "2025-11-01"
        assert parsed.date_to.isoformat() == "2026-04-12"
        assert len(parsed.transactions) == 4
        assert len(parsed.asset_hints) == 4

        buy_row = parsed.transactions.loc[
            parsed.transactions["asset_name"] == "STST SPDR MSCI ALL CNTRY WORLD EURH"
        ].iloc[0]
        assert buy_row["transaction_type"] == "BUY"
        assert buy_row["quantity"] == pytest.approx(64.0)
        assert buy_row["gross_amount"] == pytest.approx(1483.52)
        assert buy_row["fees_amount_base"] == pytest.approx(3.0)
        assert buy_row["external_reference"] == "1abb5ced-f580-4dfd-8a35-c838518e8ef9"
        assert "external_reference_from_trailing_column" in str(buy_row["notes"])

        sell_row = parsed.transactions.loc[parsed.transactions["asset_name"] == "URANIUM ENERGY CORP"].iloc[0]
        assert sell_row["transaction_type"] == "SELL"
        assert sell_row["quantity"] == pytest.approx(13.0)
        assert sell_row["transaction_currency"] == "USD"
        assert sell_row["fx_rate_to_base"] == pytest.approx(1.175)
        assert sell_row["fees_amount_base"] == pytest.approx(2.55)
        assert sell_row["external_reference"] == "b7e8da75-7638-44c9-a878-0df4a4d049ba"

        crypto_row = parsed.transactions.loc[parsed.transactions["asset_name"] == "BITCOIN"].iloc[0]
        assert crypto_row["transaction_type"] == "BUY"
        assert crypto_row["quantity"] == pytest.approx(0.25)
        assert crypto_row["quantity_source"] == "derived_from_value_local"
        assert "quantity_derived_from_value_local" in str(crypto_row["notes"])

        zero_cash_row = parsed.transactions.loc[parsed.transactions["asset_name"] == "IBERDROLA SA-RTS"].iloc[0]
        assert zero_cash_row["transaction_type"] == "BUY"
        assert zero_cash_row["gross_amount"] == pytest.approx(0.0)
        assert "zero_cash_event" in str(zero_cash_row["notes"])

        asset_hint = parsed.asset_hints.loc[parsed.asset_hints["asset_name"] == "BITCOIN"].iloc[0]
        assert asset_hint["asset_id"] == "degiro:isin:XFC000A2YY6Q"
        assert asset_hint["asset_type"] == "crypto"
        assert asset_hint["trading_currency"] == "EUR"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_and_persist_degiro_transactions_writes_parquet_outputs() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        source_dir = repo_root / "incoming"
        source_dir.mkdir(parents=True)
        csv_path = source_dir / "transactions_2025-11-01_2026-04-12.csv"
        write_transactions_fixture(csv_path)

        settings = load_settings(repo_root=repo_root, env={})
        parsed = parse_and_persist_degiro_transactions(csv_path, settings=settings)

        assert parsed.transactions_output_path is not None
        assert parsed.asset_hints_output_path is not None
        assert parsed.transactions_output_path.exists()
        assert parsed.asset_hints_output_path.exists()

        transactions_frame = pd.read_parquet(parsed.transactions_output_path)
        assets_frame = pd.read_parquet(parsed.asset_hints_output_path)

        assert len(transactions_frame) == 4
        assert len(assets_frame) == 4
        assert transactions_frame["transaction_id"].is_unique
        assert set(["asset_id", "net_cash_amount_base", "source_file"]) <= set(transactions_frame.columns)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_degiro_transactions_csv_rejects_invalid_filename() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "degiro_transactions.csv"
        write_transactions_fixture(csv_path)

        with pytest.raises(ValueError, match="transactions_YYYY-MM-DD_YYYY-MM-DD"):
            parse_degiro_transactions_csv(csv_path)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
