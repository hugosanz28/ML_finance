"""Parser for DEGIRO account cash movement exports."""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
from pathlib import Path
import re
import unicodedata

import pandas as pd

from src.config import Settings, ensure_local_directories, get_settings


ACCOUNT_FILENAME_RE = re.compile(
    r"^account_(?P<date_from>\d{4}-\d{2}-\d{2})_(?P<date_to>\d{4}-\d{2}-\d{2})\.csv$"
)

EXPECTED_ACCOUNT_HEADERS = [
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

LOGICAL_ROW_FIELDS = [
    "movement_date_raw",
    "movement_time_raw",
    "value_date_raw",
    "product_name",
    "isin",
    "description",
    "type_hint_raw",
    "variation_currency",
    "variation_amount_raw",
    "balance_currency",
    "balance_amount_raw",
    "order_id",
]

CASH_MOVEMENT_DATASET_COLUMNS = [
    "cash_movement_id",
    "broker",
    "account_id",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "external_reference",
    "movement_date",
    "movement_time",
    "value_date",
    "movement_type",
    "description",
    "amount",
    "movement_currency",
    "base_currency",
    "fx_rate_to_base",
    "amount_base",
    "running_balance",
    "running_balance_currency",
    "source_file",
    "source_row",
    "source_path",
    "notes",
]

TRANSFER_FROM_CASH_ACCOUNT_RE = re.compile(
    r"^Transferir desde su Cuenta de Efectivo .*: (?P<amount>[0-9.,]+) (?P<currency>[A-Z]{3})$"
)
TRANSFER_TO_CASH_ACCOUNT_RE = re.compile(
    r"^Transferir a su Cuenta de Efectivo .*: (?P<amount>[0-9.,]+) (?P<currency>[A-Z]{3})$"
)

AMOUNT_QUANTIZER = Decimal("0.00000001")
FX_QUANTIZER = Decimal("0.0000000001")


@dataclass(frozen=True)
class ParsedDegiroCashMovements:
    """Normalized DEGIRO cash movements plus persisted output path."""

    source_path: Path
    date_from: date
    date_to: date
    cash_movements: pd.DataFrame
    output_path: Path | None = None


def parse_degiro_cash_movements_csv(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
) -> ParsedDegiroCashMovements:
    """Parse a DEGIRO account CSV into normalized cash movements."""
    resolved_path = Path(source_path).expanduser().resolve()
    date_from, date_to = _parse_account_filename(resolved_path.name)
    normalized_base_currency = (base_currency or get_settings().default_currency).upper()

    rows = _read_csv_rows(resolved_path)
    if rows[0] != EXPECTED_ACCOUNT_HEADERS:
        raise ValueError(
            "Unexpected DEGIRO account header. "
            f"Expected {EXPECTED_ACCOUNT_HEADERS!r} but received {rows[0]!r}."
        )

    records: list[dict[str, object]] = []
    for row_number, raw_row in enumerate(rows[1:], start=2):
        row = _normalize_row(raw_row, len(EXPECTED_ACCOUNT_HEADERS))
        fields = dict(zip(LOGICAL_ROW_FIELDS, row))
        records.append(
            _parse_cash_movement_row(
                fields,
                source_path=resolved_path,
                source_row=row_number,
                base_currency=normalized_base_currency,
                account_id=account_id,
                source_root=source_root,
            )
        )

    cash_movements = pd.DataFrame(records, columns=CASH_MOVEMENT_DATASET_COLUMNS)
    return ParsedDegiroCashMovements(
        source_path=resolved_path,
        date_from=date_from,
        date_to=date_to,
        cash_movements=cash_movements,
    )


def persist_degiro_cash_movements_dataset(
    parsed: ParsedDegiroCashMovements,
    *,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroCashMovements:
    """Persist normalized DEGIRO cash movements as parquet."""
    resolved_settings = get_settings() if settings is None else settings
    ensure_local_directories(resolved_settings)

    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.normalized_data_dir / "degiro" / "cash_movements"
    )
    base_output_dir.mkdir(parents=True, exist_ok=True)

    output_path = (base_output_dir / f"{parsed.source_path.stem}.parquet").resolve()
    _parquet_ready_frame(parsed.cash_movements).to_parquet(output_path, index=False)
    return replace(parsed, output_path=output_path)


def parse_and_persist_degiro_cash_movements(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroCashMovements:
    """Parse an account CSV and persist the normalized cash movement dataset."""
    parsed = parse_degiro_cash_movements_csv(
        source_path,
        base_currency=base_currency,
        account_id=account_id,
        source_root=source_root,
    )
    return persist_degiro_cash_movements_dataset(parsed, output_dir=output_dir, settings=settings)


def _parse_account_filename(filename: str) -> tuple[date, date]:
    match = ACCOUNT_FILENAME_RE.match(filename)
    if match is None:
        raise ValueError("DEGIRO account filename must follow 'account_YYYY-MM-DD_YYYY-MM-DD.csv'.")

    date_from = date.fromisoformat(match.group("date_from"))
    date_to = date.fromisoformat(match.group("date_to"))
    if date_to < date_from:
        raise ValueError("Account export filename date range is invalid.")
    return date_from, date_to


def _read_csv_rows(source_path: Path) -> list[list[str]]:
    with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]

    if not rows:
        raise ValueError(f"Account CSV is empty: {source_path}")
    return rows


def _normalize_row(row: list[str], target_length: int) -> list[str]:
    if len(row) != target_length:
        raise ValueError(f"Unexpected account row width: {len(row)} columns.")
    return row


def _parse_cash_movement_row(
    fields: dict[str, str],
    *,
    source_path: Path,
    source_row: int,
    base_currency: str,
    account_id: str | None,
    source_root: str | Path | None,
) -> dict[str, object]:
    movement_date = _parse_eu_date(fields["movement_date_raw"], field_name="Fecha")
    movement_time = _optional_text(fields["movement_time_raw"])
    value_date = _parse_optional_eu_date(fields["value_date_raw"])
    product_name = _optional_text(fields["product_name"])
    isin = _optional_text(fields["isin"])
    description = _require_text(fields["description"], field_name="Descripción")
    movement_type = _classify_movement_type(description)

    notes: list[str] = []
    amount, movement_currency = _resolve_amount_and_currency(fields, description=description, movement_type=movement_type, notes=notes)
    running_balance = _parse_decimal(fields["balance_amount_raw"])
    running_balance_currency = _optional_text(fields["balance_currency"])
    if running_balance_currency is not None:
        running_balance_currency = _require_currency(running_balance_currency, field_name="Saldo")

    fx_rate_to_base = _derive_fx_rate_to_base(
        raw_type_hint=fields["type_hint_raw"],
        movement_currency=movement_currency,
        base_currency=base_currency,
        notes=notes,
    )
    amount_base = _derive_amount_base(
        amount=amount,
        movement_currency=movement_currency,
        base_currency=base_currency,
        fx_rate_to_base=fx_rate_to_base,
        notes=notes,
    )

    asset_id = _build_asset_id(product_name=product_name, isin=isin)
    asset_type = _infer_asset_type(product_name) if product_name else None
    external_reference = _optional_text(fields["order_id"])
    source_path_display = _display_source_path(source_path, source_root=source_root)

    fingerprint = "|".join(
        (
            source_path.name,
            str(source_row),
            movement_date.isoformat(),
            movement_time or "",
            value_date.isoformat() if value_date is not None else "",
            movement_type,
            description,
            _decimal_to_string(amount),
            movement_currency,
            external_reference or "",
        )
    )
    cash_movement_id = f"degiro_cash_{hashlib.sha1(fingerprint.encode('utf-8')).hexdigest()[:16]}"

    return {
        "cash_movement_id": cash_movement_id,
        "broker": "DEGIRO",
        "account_id": account_id,
        "asset_id": asset_id,
        "asset_name": product_name,
        "asset_type": asset_type,
        "isin": isin,
        "external_reference": external_reference,
        "movement_date": movement_date,
        "movement_time": movement_time,
        "value_date": value_date,
        "movement_type": movement_type,
        "description": description,
        "amount": float(amount),
        "movement_currency": movement_currency,
        "base_currency": base_currency,
        "fx_rate_to_base": float(fx_rate_to_base) if fx_rate_to_base is not None else None,
        "amount_base": float(amount_base) if amount_base is not None else None,
        "running_balance": float(running_balance) if running_balance is not None else None,
        "running_balance_currency": running_balance_currency,
        "source_file": source_path.name,
        "source_row": source_row,
        "source_path": source_path_display,
        "notes": "; ".join(notes) or None,
    }


def _resolve_amount_and_currency(
    fields: dict[str, str],
    *,
    description: str,
    movement_type: str,
    notes: list[str],
) -> tuple[Decimal, str]:
    variation_currency = _optional_text(fields["variation_currency"])
    variation_amount = _parse_decimal(fields["variation_amount_raw"])

    if variation_currency and variation_amount is not None:
        return (
            variation_amount.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP),
            _require_currency(variation_currency, field_name="Variación"),
        )

    derived = _extract_amount_from_description(description, movement_type=movement_type)
    if derived is None:
        raise ValueError(
            "Unable to resolve movement amount/currency from account row with blank variation fields."
        )

    amount, currency = derived
    notes.append("amount_derived_from_description")
    return amount.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP), currency


def _extract_amount_from_description(description: str, *, movement_type: str) -> tuple[Decimal, str] | None:
    if movement_type == "CASH_ACCOUNT_TRANSFER_IN":
        match = TRANSFER_FROM_CASH_ACCOUNT_RE.match(description)
        if match:
            return _parse_decimal(match.group("amount")), match.group("currency")
    if movement_type == "CASH_ACCOUNT_TRANSFER_OUT":
        match = TRANSFER_TO_CASH_ACCOUNT_RE.match(description)
        if match:
            return -(_parse_decimal(match.group("amount")) or Decimal("0")), match.group("currency")
    return None


def _classify_movement_type(description: str) -> str:
    if description.startswith("DESLISTAMIENTO"):
        return "CORPORATE_ACTION_DELISTING"
    if description.startswith("DIVIDENDO FLEXIBLE"):
        return "CORPORATE_ACTION_SCRIP_DIVIDEND"
    if description.startswith("EMISIÓN DE DERECHOS"):
        return "CORPORATE_ACTION_RIGHTS_ISSUE"
    if description.startswith("Compra "):
        return "TRADE_SETTLEMENT_BUY"
    if description.startswith("Venta "):
        return "TRADE_SETTLEMENT_SELL"
    if description == "Dividendo":
        return "DIVIDEND"
    if description == "Retención del dividendo":
        return "DIVIDEND_WITHHOLDING_TAX"
    if description == "Degiro Cash Sweep Transfer":
        return "CASH_SWEEP_TRANSFER"
    if description.startswith("Ingreso Cambio de Divisa"):
        return "FX_CONVERSION_IN"
    if description.startswith("Retirada Cambio de Divisa"):
        return "FX_CONVERSION_OUT"
    if description.startswith("Transferir desde su Cuenta de Efectivo"):
        return "CASH_ACCOUNT_TRANSFER_IN"
    if description.startswith("Transferir a su Cuenta de Efectivo"):
        return "CASH_ACCOUNT_TRANSFER_OUT"
    if "Costes de transacción y/o externos de DEGIRO" in description:
        return "TRANSACTION_FEE"
    if description.startswith("Comisión de conectividad"):
        return "CONNECTIVITY_FEE"
    if description == "Spanish Transaction Tax":
        return "TRANSACTION_TAX"
    if description in {"flatex Deposit", "Ingreso"}:
        return "DEPOSIT"
    if description == "Flatex Interest Income":
        return "INTEREST"
    if description.startswith("Promoción DEGIRO"):
        return "REBATE"
    return "OTHER"


def _derive_fx_rate_to_base(
    *,
    raw_type_hint: str,
    movement_currency: str,
    base_currency: str,
    notes: list[str],
) -> Decimal | None:
    if movement_currency == base_currency:
        return Decimal("1")

    parsed_hint = _parse_decimal(raw_type_hint)
    if parsed_hint is None:
        notes.append("amount_base_unavailable")
        return None

    notes.append("fx_rate_from_type_column")
    return parsed_hint.quantize(FX_QUANTIZER, rounding=ROUND_HALF_UP)


def _derive_amount_base(
    *,
    amount: Decimal,
    movement_currency: str,
    base_currency: str,
    fx_rate_to_base: Decimal | None,
    notes: list[str],
) -> Decimal | None:
    if movement_currency == base_currency:
        return amount.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)
    if fx_rate_to_base is None or fx_rate_to_base == 0:
        return None
    derived = (amount / fx_rate_to_base).quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)
    notes.append("amount_base_derived_via_fx")
    return derived


def _parquet_ready_frame(frame: pd.DataFrame) -> pd.DataFrame:
    ready = frame.copy()
    for date_column in ("movement_date", "value_date"):
        if date_column in ready.columns:
            ready[date_column] = pd.to_datetime(ready[date_column])
    return ready


def _parse_decimal(raw_value: str) -> Decimal | None:
    text = raw_value.strip()
    if not text:
        return None
    normalized = _normalize_decimal_text(text)
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {raw_value!r}") from exc


def _parse_eu_date(raw_value: str, *, field_name: str) -> date:
    text = raw_value.strip()
    if not text:
        raise ValueError(f"Missing required date field: {field_name}")
    return datetime.strptime(text, "%d-%m-%Y").date()


def _parse_optional_eu_date(raw_value: str) -> date | None:
    text = raw_value.strip()
    if not text:
        return None
    return datetime.strptime(text, "%d-%m-%Y").date()


def _require_currency(raw_value: str, *, field_name: str) -> str:
    text = raw_value.strip().upper()
    if len(text) != 3:
        raise ValueError(f"Invalid currency in field {field_name}: {raw_value!r}")
    return text


def _require_text(raw_value: str, *, field_name: str) -> str:
    text = raw_value.strip()
    if not text:
        raise ValueError(f"Missing required text field: {field_name}")
    return text


def _optional_text(raw_value: str) -> str | None:
    text = raw_value.strip()
    return text or None


def _build_asset_id(*, product_name: str | None, isin: str | None) -> str | None:
    if isin:
        return f"degiro:isin:{isin.upper()}"
    if product_name:
        return f"degiro:product:{_slugify(product_name)}"
    return None


def _infer_asset_type(product_name: str | None) -> str | None:
    if not product_name:
        return None
    normalized_name = product_name.upper()
    if "BITCOIN" in normalized_name or "ETHEREUM" in normalized_name:
        return "crypto"
    if "RTS" in normalized_name or "RIGHT" in normalized_name:
        return "right"
    if "ETC" in normalized_name:
        return "etc"
    if "ETF" in normalized_name or "UCITS" in normalized_name:
        return "etf"
    if "BOND" in normalized_name:
        return "bond"
    return "stock"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return cleaned or "unknown-asset"


def _decimal_to_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _display_source_path(source_path: Path, *, source_root: str | Path | None) -> str:
    resolved_path = source_path.resolve()
    if source_root is None:
        return str(resolved_path)

    resolved_root = Path(source_root).expanduser().resolve()
    if resolved_path.is_relative_to(resolved_root):
        return str(resolved_path.relative_to(resolved_root))
    return str(resolved_path)


def _normalize_decimal_text(text: str) -> str:
    if "," in text:
        return text.replace(".", "").replace(",", ".")

    if text.count(".") == 1:
        whole, fractional = text.split(".")
        if whole.lstrip("-").isdigit() and fractional.isdigit() and len(fractional) == 3:
            return whole + fractional

    return text
