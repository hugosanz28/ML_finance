from __future__ import annotations

import csv
from pathlib import Path
import shutil
import uuid

import pandas as pd
import pytest

from src.config import load_settings
from src.degiro_exports.cash_movements import (
    parse_and_persist_degiro_cash_movements,
    parse_degiro_cash_movements_csv,
)


ACCOUNT_HEADER = [
    "Fecha",
    "Hora",
    "Fecha valor",
    "Producto",
    "ISIN",
    "Descripción",
    "Tipo",
    "Variación",
    "",
    "Saldo",
    "",
    "ID Orden",
]


def write_account_fixture(csv_path: Path) -> None:
    rows = [
        [
            "02-04-2026",
            "09:10",
            "02-04-2026",
            "STST SPDR MSCI ALL CNTRY WORLD EURH",
            "IE00BF1B7389",
            "Compra 64 StSt SPDR MSCI All Cntry World EURH UCITS ETF Acc@23,18 EUR (IE00BF1B7389)",
            "",
            "EUR",
            "-1483,52",
            "EUR",
            "34,09",
            "1abb5ced-f580-4dfd-8a35-c838518e8ef9",
        ],
        [
            "02-04-2026",
            "09:10",
            "02-04-2026",
            "STST SPDR MSCI ALL CNTRY WORLD EURH",
            "IE00BF1B7389",
            "Costes de transacción y/o externos de DEGIRO",
            "",
            "EUR",
            "-3,00",
            "EUR",
            "31,09",
            "1abb5ced-f580-4dfd-8a35-c838518e8ef9",
        ],
        [
            "23-01-2026",
            "16:34",
            "23-01-2026",
            "URANIUM ENERGY CORP",
            "US9168961038",
            "Venta 13 Uranium Energy Corp@19,8 USD (US9168961038)",
            "",
            "USD",
            "257,40",
            "USD",
            "257,40",
            "b7e8da75-7638-44c9-a878-0df4a4d049ba",
        ],
        [
            "17-03-2026",
            "07:44",
            "16-03-2026",
            "ALPHABET INC CLASS A",
            "US02079K3059",
            "Dividendo",
            "",
            "USD",
            "0,84",
            "USD",
            "2,30",
            "",
        ],
        [
            "17-03-2026",
            "07:44",
            "16-03-2026",
            "ALPHABET INC CLASS A",
            "US02079K3059",
            "Retención del dividendo",
            "",
            "USD",
            "-0,13",
            "USD",
            "1,46",
            "",
        ],
        [
            "18-03-2026",
            "07:00",
            "17-03-2026",
            "",
            "",
            "Retirada Cambio de Divisa",
            "1,1570",
            "USD",
            "-2,30",
            "USD",
            "0,00",
            "",
        ],
        [
            "18-03-2026",
            "07:00",
            "17-03-2026",
            "",
            "",
            "Ingreso Cambio de Divisa",
            "",
            "EUR",
            "1,99",
            "EUR",
            "17,61",
            "",
        ],
        [
            "02-04-2026",
            "10:31",
            "02-04-2026",
            "",
            "",
            "Degiro Cash Sweep Transfer",
            "",
            "EUR",
            "1486,52",
            "EUR",
            "31,09",
            "",
        ],
        [
            "02-04-2026",
            "10:31",
            "02-04-2026",
            "",
            "",
            "Transferir desde su Cuenta de Efectivo en flatexDEGIRO Bank: 1.486,52 EUR",
            "",
            "",
            "",
            "EUR",
            "-1455,43",
            "",
        ],
        [
            "01-04-2026",
            "01:00",
            "31-03-2026",
            "",
            "",
            "Transferir a su Cuenta de Efectivo en flatexDEGIRO Bank: 1.500 EUR",
            "",
            "",
            "",
            "EUR",
            "3017,61",
            "",
        ],
        [
            "31-03-2026",
            "23:48",
            "31-03-2026",
            "",
            "",
            "Ingreso",
            "",
            "EUR",
            "1500,00",
            "EUR",
            "1517,61",
            "",
        ],
        [
            "06-04-2026",
            "06:01",
            "31-03-2026",
            "",
            "",
            "Flatex Interest Income",
            "",
            "EUR",
            "0,00",
            "EUR",
            "31,09",
            "",
        ],
        [
            "05-03-2026",
            "13:44",
            "05-03-2026",
            "",
            "",
            "Comisión de conectividad con el mercado 2026 (Euronext Amsterdam - EAM)",
            "",
            "EUR",
            "-2,50",
            "EUR",
            "14,95",
            "",
        ],
        [
            "20-01-2026",
            "15:39",
            "20-01-2026",
            "",
            "",
            "Promoción DEGIRO reembolso",
            "",
            "EUR",
            "22,29",
            "EUR",
            "68,37",
            "",
        ],
        [
            "31-12-2025",
            "12:13",
            "31-12-2025",
            "IBERDROLA SA",
            "ES0144580Y14",
            "Spanish Transaction Tax",
            "",
            "EUR",
            "-0,48",
            "EUR",
            "257,42",
            "",
        ],
        [
            "12-01-2026",
            "09:00",
            "12-01-2026",
            "IBERDROLA SA",
            "ES0144580Y14",
            "EMISIÓN DE DERECHOS: Compra 13 Iberdrola SA@0 EUR (ES06445809V1)",
            "",
            "EUR",
            "0,00",
            "EUR",
            "0,00",
            "",
        ],
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(ACCOUNT_HEADER)
        writer.writerows(rows)


def make_test_workspace() -> Path:
    base_dir = Path("src/data/local/test_runs")
    base_dir.mkdir(parents=True, exist_ok=True)
    workspace = base_dir / f"degiro-cash-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def test_parse_degiro_cash_movements_csv_normalizes_cash_movements() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "account_2025-11-01_2026-04-12.csv"
        write_account_fixture(csv_path)

        parsed = parse_degiro_cash_movements_csv(csv_path, base_currency="EUR")

        assert parsed.date_from.isoformat() == "2025-11-01"
        assert parsed.date_to.isoformat() == "2026-04-12"
        assert len(parsed.cash_movements) == 16

        buy_row = parsed.cash_movements.loc[
            parsed.cash_movements["movement_type"] == "TRADE_SETTLEMENT_BUY"
        ].iloc[0]
        assert buy_row["amount"] == pytest.approx(-1483.52)
        assert buy_row["movement_currency"] == "EUR"
        assert buy_row["asset_id"] == "degiro:isin:IE00BF1B7389"

        fx_out_row = parsed.cash_movements.loc[
            parsed.cash_movements["movement_type"] == "FX_CONVERSION_OUT"
        ].iloc[0]
        assert fx_out_row["fx_rate_to_base"] == pytest.approx(1.157)
        assert fx_out_row["amount_base"] == pytest.approx(-1.98789974, abs=1e-8)
        assert "fx_rate_from_type_column" in str(fx_out_row["notes"])

        transfer_in_row = parsed.cash_movements.loc[
            parsed.cash_movements["movement_type"] == "CASH_ACCOUNT_TRANSFER_IN"
        ].iloc[0]
        assert transfer_in_row["amount"] == pytest.approx(1486.52)
        assert transfer_in_row["movement_currency"] == "EUR"
        assert "amount_derived_from_description" in str(transfer_in_row["notes"])

        transfer_out_row = parsed.cash_movements.loc[
            parsed.cash_movements["movement_type"] == "CASH_ACCOUNT_TRANSFER_OUT"
        ].iloc[0]
        assert transfer_out_row["amount"] == pytest.approx(-1500.0)

        dividend_row = parsed.cash_movements.loc[parsed.cash_movements["movement_type"] == "DIVIDEND"].iloc[0]
        assert dividend_row["amount"] == pytest.approx(0.84)
        assert pd.isna(dividend_row["amount_base"])
        assert "amount_base_unavailable" in str(dividend_row["notes"])

        interest_row = parsed.cash_movements.loc[parsed.cash_movements["movement_type"] == "INTEREST"].iloc[0]
        assert interest_row["amount"] == pytest.approx(0.0)
        assert interest_row["amount_base"] == pytest.approx(0.0)

        tax_row = parsed.cash_movements.loc[parsed.cash_movements["movement_type"] == "TRANSACTION_TAX"].iloc[0]
        assert tax_row["amount"] == pytest.approx(-0.48)

        rights_row = parsed.cash_movements.loc[
            parsed.cash_movements["movement_type"] == "CORPORATE_ACTION_RIGHTS_ISSUE"
        ].iloc[0]
        assert rights_row["amount"] == pytest.approx(0.0)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_and_persist_degiro_cash_movements_writes_parquet_output() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        source_dir = repo_root / "incoming"
        source_dir.mkdir(parents=True)
        csv_path = source_dir / "account_2025-11-01_2026-04-12.csv"
        write_account_fixture(csv_path)

        settings = load_settings(repo_root=repo_root, env={})
        parsed = parse_and_persist_degiro_cash_movements(csv_path, settings=settings)

        assert parsed.output_path is not None
        assert parsed.output_path.exists()

        frame = pd.read_parquet(parsed.output_path)
        assert len(frame) == 16
        assert frame["cash_movement_id"].is_unique
        assert set(["movement_type", "amount", "source_file"]) <= set(frame.columns)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_parse_degiro_cash_movements_csv_rejects_invalid_filename() -> None:
    workspace = make_test_workspace()
    try:
        csv_path = workspace / "degiro_account.csv"
        write_account_fixture(csv_path)

        with pytest.raises(ValueError, match="account_YYYY-MM-DD_YYYY-MM-DD"):
            parse_degiro_cash_movements_csv(csv_path)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
