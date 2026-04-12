"""Parser for DEGIRO transaction exports."""

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


TRANSACTION_FILENAME_RE = re.compile(
    r"^transactions_(?P<date_from>\d{4}-\d{2}-\d{2})_(?P<date_to>\d{4}-\d{2}-\d{2})\.csv$"
)

EXPECTED_TRANSACTION_HEADERS = [
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

LOGICAL_ROW_FIELDS = [
    "trade_date_raw",
    "trade_time_raw",
    "product_name",
    "isin",
    "reference_exchange",
    "execution_venue",
    "signed_quantity_raw",
    "unit_price_raw",
    "unit_price_currency",
    "value_local_raw",
    "value_local_currency",
    "value_eur_raw",
    "fx_rate_raw",
    "autofx_fee_base_raw",
    "transaction_costs_base_raw",
    "total_eur_raw",
    "order_id",
    "trailing_reference",
]

TRANSACTION_DATASET_COLUMNS = [
    "transaction_id",
    "broker",
    "account_id",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "reference_exchange",
    "execution_venue",
    "external_reference",
    "trade_date",
    "trade_time",
    "settlement_date",
    "transaction_type",
    "quantity",
    "quantity_source",
    "unit_price",
    "gross_amount",
    "transaction_currency",
    "gross_amount_base",
    "base_currency",
    "fx_rate_to_base",
    "transaction_costs_base",
    "autofx_fee_base",
    "fees_amount_base",
    "taxes_amount_base",
    "net_cash_amount_local",
    "net_cash_amount_base",
    "source_file",
    "source_row",
    "source_path",
    "notes",
]

ASSET_HINT_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "reference_exchange",
    "execution_venue",
    "trading_currency",
    "first_seen_date",
    "last_seen_date",
    "source_file",
]

QUANTITY_QUANTIZER = Decimal("0.00000001")
AMOUNT_QUANTIZER = Decimal("0.00000001")
FX_QUANTIZER = Decimal("0.0000000001")


@dataclass(frozen=True)
class ParsedDegiroTransactions:
    """Normalized DEGIRO transactions plus asset hints and output paths."""

    source_path: Path
    date_from: date
    date_to: date
    transactions: pd.DataFrame
    asset_hints: pd.DataFrame
    transactions_output_path: Path | None = None
    asset_hints_output_path: Path | None = None


def parse_degiro_transactions_csv(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
) -> ParsedDegiroTransactions:
    """Parse a DEGIRO transactions CSV into normalized transaction records."""
    resolved_path = Path(source_path).expanduser().resolve()
    date_from, date_to = _parse_transactions_filename(resolved_path.name)
    normalized_base_currency = (base_currency or get_settings().default_currency).upper()

    rows = _read_csv_rows(resolved_path)
    header = _normalize_header(rows[0])
    _validate_header(header)

    records: list[dict[str, object]] = []
    for row_number, raw_row in enumerate(rows[1:], start=2):
        row = _normalize_row(raw_row, len(header))
        fields = dict(zip(LOGICAL_ROW_FIELDS, row))
        record = _parse_transaction_row(
            fields,
            source_path=resolved_path,
            source_row=row_number,
            base_currency=normalized_base_currency,
            account_id=account_id,
            source_root=source_root,
        )
        records.append(record)

    transactions = pd.DataFrame(records, columns=TRANSACTION_DATASET_COLUMNS)
    asset_hints = _build_asset_hints_frame(transactions)
    return ParsedDegiroTransactions(
        source_path=resolved_path,
        date_from=date_from,
        date_to=date_to,
        transactions=transactions,
        asset_hints=asset_hints,
    )


def persist_degiro_transactions_dataset(
    parsed: ParsedDegiroTransactions,
    *,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroTransactions:
    """Persist normalized transactions and asset hints as parquet datasets."""
    resolved_settings = get_settings() if settings is None else settings
    ensure_local_directories(resolved_settings)

    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.normalized_data_dir / "degiro"
    )
    transactions_dir = base_output_dir / "transactions"
    assets_dir = base_output_dir / "assets"
    transactions_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)

    source_stem = parsed.source_path.stem
    transactions_output_path = (transactions_dir / f"{source_stem}.parquet").resolve()
    asset_hints_output_path = (assets_dir / f"{source_stem}_assets.parquet").resolve()

    _parquet_ready_frame(parsed.transactions).to_parquet(transactions_output_path, index=False)
    _parquet_ready_frame(parsed.asset_hints).to_parquet(asset_hints_output_path, index=False)

    return replace(
        parsed,
        transactions_output_path=transactions_output_path,
        asset_hints_output_path=asset_hints_output_path,
    )


def parse_and_persist_degiro_transactions(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroTransactions:
    """Parse a transactions CSV and persist the normalized datasets."""
    parsed = parse_degiro_transactions_csv(
        source_path,
        base_currency=base_currency,
        account_id=account_id,
        source_root=source_root,
    )
    return persist_degiro_transactions_dataset(parsed, output_dir=output_dir, settings=settings)


def _parse_transactions_filename(filename: str) -> tuple[date, date]:
    match = TRANSACTION_FILENAME_RE.match(filename)
    if match is None:
        raise ValueError(
            "DEGIRO transactions filename must follow "
            "'transactions_YYYY-MM-DD_YYYY-MM-DD.csv'."
        )

    date_from = date.fromisoformat(match.group("date_from"))
    date_to = date.fromisoformat(match.group("date_to"))
    if date_to < date_from:
        raise ValueError("Transaction export filename date range is invalid.")
    return date_from, date_to


def _read_csv_rows(source_path: Path) -> list[list[str]]:
    with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]

    if not rows:
        raise ValueError(f"Transactions CSV is empty: {source_path}")
    return rows


def _normalize_header(header: list[str]) -> list[str]:
    if len(header) == len(EXPECTED_TRANSACTION_HEADERS) - 1 and header == EXPECTED_TRANSACTION_HEADERS[:-1]:
        return header + [""]
    return header


def _validate_header(header: list[str]) -> None:
    if header != EXPECTED_TRANSACTION_HEADERS:
        raise ValueError(
            "Unexpected DEGIRO transactions header. "
            f"Expected {EXPECTED_TRANSACTION_HEADERS!r} but received {header!r}."
        )


def _normalize_row(row: list[str], target_length: int) -> list[str]:
    if len(row) > target_length:
        raise ValueError(f"Unexpected transaction row width: {len(row)} columns.")
    if len(row) < target_length:
        row = row + [""] * (target_length - len(row))
    return row


def _parse_transaction_row(
    fields: dict[str, str],
    *,
    source_path: Path,
    source_row: int,
    base_currency: str,
    account_id: str | None,
    source_root: str | Path | None,
) -> dict[str, object]:
    trade_date = _parse_eu_date(fields["trade_date_raw"], field_name="Fecha")
    trade_time = fields["trade_time_raw"].strip() or None
    product_name = _require_text(fields["product_name"], field_name="Producto")
    isin = _optional_text(fields["isin"])
    reference_exchange = _optional_text(fields["reference_exchange"])
    execution_venue = _optional_text(fields["execution_venue"])
    transaction_currency = _require_currency(
        fields["value_local_currency"] or fields["unit_price_currency"],
        field_name="Valor local/Precio moneda",
    )

    signed_quantity = _parse_decimal(fields["signed_quantity_raw"])
    unit_price = _require_non_negative_decimal(fields["unit_price_raw"], field_name="Precio")
    net_cash_amount_local = _require_decimal(fields["value_local_raw"], field_name="Valor local")
    gross_amount = abs(net_cash_amount_local).quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)

    quantity_source = "csv"
    notes: list[str] = []
    if signed_quantity is None:
        if unit_price == 0 or gross_amount == 0:
            raise ValueError(
                f"Cannot derive quantity for row {source_row} in {source_path.name}: "
                "missing 'Número' and insufficient price/value data."
            )
        signed_quantity = (gross_amount / unit_price).quantize(QUANTITY_QUANTIZER, rounding=ROUND_HALF_UP)
        quantity_source = "derived_from_value_local"
        notes.append("quantity_derived_from_value_local")

    quantity = abs(signed_quantity).quantize(QUANTITY_QUANTIZER, rounding=ROUND_HALF_UP)
    if quantity == 0:
        raise ValueError(f"Transaction quantity cannot be zero at row {source_row} in {source_path.name}.")

    gross_amount_base = abs(_require_decimal(fields["value_eur_raw"], field_name="Valor EUR")).quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    net_cash_amount_base = _require_decimal(fields["total_eur_raw"], field_name="Total EUR").quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    transaction_costs_base = abs(_parse_decimal(fields["transaction_costs_base_raw"]) or Decimal("0")).quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    autofx_fee_base = abs(_parse_decimal(fields["autofx_fee_base_raw"]) or Decimal("0")).quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    fees_amount_base = (transaction_costs_base + autofx_fee_base).quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    fx_rate = _parse_decimal(fields["fx_rate_raw"])
    fx_rate_to_base = _normalize_fx_rate(fx_rate, transaction_currency=transaction_currency, base_currency=base_currency)
    if net_cash_amount_base == 0:
        notes.append("zero_cash_event")

    transaction_type = _infer_transaction_type(
        net_cash_amount_base=net_cash_amount_base,
        signed_quantity=signed_quantity,
        net_cash_amount_local=net_cash_amount_local,
    )
    external_reference = _pick_external_reference(fields, notes)
    asset_id = _build_asset_id(
        isin=isin,
        product_name=product_name,
        reference_exchange=reference_exchange,
    )
    asset_type = _infer_asset_type(product_name)

    fingerprint = "|".join(
        (
            source_path.name,
            str(source_row),
            external_reference or "",
            trade_date.isoformat(),
            trade_time or "",
            asset_id,
            transaction_type,
            _decimal_to_string(quantity),
            _decimal_to_string(unit_price),
            _decimal_to_string(net_cash_amount_base),
        )
    )
    transaction_id = f"degiro_txn_{hashlib.sha1(fingerprint.encode('utf-8')).hexdigest()[:16]}"

    resolved_source_path = _display_source_path(source_path, source_root=source_root)

    return {
        "transaction_id": transaction_id,
        "broker": "DEGIRO",
        "account_id": account_id,
        "asset_id": asset_id,
        "asset_name": product_name,
        "asset_type": asset_type,
        "isin": isin,
        "reference_exchange": reference_exchange,
        "execution_venue": execution_venue,
        "external_reference": external_reference,
        "trade_date": trade_date,
        "trade_time": trade_time,
        "settlement_date": None,
        "transaction_type": transaction_type,
        "quantity": float(quantity),
        "quantity_source": quantity_source,
        "unit_price": float(unit_price.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)),
        "gross_amount": float(gross_amount),
        "transaction_currency": transaction_currency,
        "gross_amount_base": float(gross_amount_base),
        "base_currency": base_currency,
        "fx_rate_to_base": float(fx_rate_to_base) if fx_rate_to_base is not None else None,
        "transaction_costs_base": float(transaction_costs_base),
        "autofx_fee_base": float(autofx_fee_base),
        "fees_amount_base": float(fees_amount_base),
        "taxes_amount_base": 0.0,
        "net_cash_amount_local": float(net_cash_amount_local.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)),
        "net_cash_amount_base": float(net_cash_amount_base),
        "source_file": source_path.name,
        "source_row": source_row,
        "source_path": str(resolved_source_path),
        "notes": "; ".join(notes) or None,
    }


def _pick_external_reference(fields: dict[str, str], notes: list[str]) -> str | None:
    order_id = _optional_text(fields["order_id"])
    trailing_reference = _optional_text(fields["trailing_reference"])

    if trailing_reference:
        if not order_id:
            notes.append("external_reference_from_trailing_column")
        return trailing_reference
    return order_id


def _infer_transaction_type(
    *,
    net_cash_amount_base: Decimal,
    signed_quantity: Decimal,
    net_cash_amount_local: Decimal,
) -> str:
    if net_cash_amount_base < 0:
        return "BUY"
    if net_cash_amount_base > 0:
        return "SELL"
    if net_cash_amount_local < 0:
        return "BUY"
    if net_cash_amount_local > 0:
        return "SELL"
    if signed_quantity > 0:
        return "BUY"
    if signed_quantity < 0:
        return "SELL"
    raise ValueError("Unable to infer transaction type from zero-value transaction.")


def _build_asset_hints_frame(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame(columns=ASSET_HINT_COLUMNS)

    ordered = transactions.sort_values(["asset_id", "trade_date", "source_row"]).copy()
    grouped = (
        ordered.groupby("asset_id", as_index=False)
        .agg(
            asset_name=("asset_name", "last"),
            asset_type=("asset_type", "last"),
            isin=("isin", "last"),
            reference_exchange=("reference_exchange", "last"),
            execution_venue=("execution_venue", "last"),
            trading_currency=("transaction_currency", "last"),
            first_seen_date=("trade_date", "min"),
            last_seen_date=("trade_date", "max"),
            source_file=("source_file", "last"),
        )
        .loc[:, ASSET_HINT_COLUMNS]
    )
    return grouped.reset_index(drop=True)


def _parquet_ready_frame(frame: pd.DataFrame) -> pd.DataFrame:
    ready = frame.copy()
    for date_column in ("trade_date", "settlement_date", "first_seen_date", "last_seen_date"):
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


def _require_decimal(raw_value: str, *, field_name: str) -> Decimal:
    parsed = _parse_decimal(raw_value)
    if parsed is None:
        raise ValueError(f"Missing required decimal field: {field_name}")
    return parsed


def _require_non_negative_decimal(raw_value: str, *, field_name: str) -> Decimal:
    parsed = _require_decimal(raw_value, field_name=field_name)
    if parsed < 0:
        raise ValueError(f"Field {field_name} must be non-negative.")
    return parsed


def _parse_eu_date(raw_value: str, *, field_name: str) -> date:
    text = raw_value.strip()
    if not text:
        raise ValueError(f"Missing required date field: {field_name}")
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


def _normalize_fx_rate(
    fx_rate: Decimal | None,
    *,
    transaction_currency: str,
    base_currency: str,
) -> Decimal | None:
    if transaction_currency == base_currency:
        return Decimal("1")
    if fx_rate is None:
        return None
    return fx_rate.quantize(FX_QUANTIZER, rounding=ROUND_HALF_UP)


def _build_asset_id(*, isin: str | None, product_name: str, reference_exchange: str | None) -> str:
    if isin:
        return f"degiro:isin:{isin.upper()}"

    slug = _slugify(product_name)
    if reference_exchange:
        return f"degiro:product:{reference_exchange.lower()}:{slug}"
    return f"degiro:product:{slug}"


def _infer_asset_type(product_name: str) -> str:
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
