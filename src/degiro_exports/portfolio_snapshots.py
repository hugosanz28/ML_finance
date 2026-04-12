"""Parser for DEGIRO portfolio snapshot exports."""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
from pathlib import Path
import re
import unicodedata

import pandas as pd

from src.config import Settings, ensure_local_directories, get_settings


PORTFOLIO_FILENAME_RE = re.compile(r"^portfolio_(?P<snapshot_date>\d{4}-\d{2}-\d{2})\.csv$")
EXPECTED_PORTFOLIO_HEADERS = [
    "Producto",
    "Symbol/ISIN",
    "Cantidad",
    "Precio de",
    "Valor local",
    "",
    "Valor en EUR",
]

LOGICAL_ROW_FIELDS = [
    "product_name",
    "symbol_or_isin",
    "quantity_raw",
    "market_price_raw",
    "position_currency_raw",
    "market_value_raw",
    "market_value_base_raw",
]

PORTFOLIO_SNAPSHOT_COLUMNS = [
    "snapshot_id",
    "broker",
    "account_id",
    "snapshot_date",
    "snapshot_source",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "broker_symbol",
    "quantity",
    "average_cost",
    "market_price",
    "market_value",
    "position_currency",
    "base_currency",
    "fx_rate_to_base",
    "market_value_base",
    "unrealized_pnl_base",
    "source_file",
    "source_row",
    "source_path",
    "notes",
]

AMOUNT_QUANTIZER = Decimal("0.00000001")
FX_QUANTIZER = Decimal("0.0000000001")


@dataclass(frozen=True)
class ParsedDegiroPortfolioSnapshots:
    """Normalized DEGIRO portfolio snapshots plus persisted output path."""

    source_path: Path
    snapshot_date: date
    snapshots: pd.DataFrame
    output_path: Path | None = None


def parse_degiro_portfolio_snapshot_csv(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
) -> ParsedDegiroPortfolioSnapshots:
    """Parse a DEGIRO portfolio CSV into normalized snapshot rows."""
    resolved_path = Path(source_path).expanduser().resolve()
    snapshot_date = _parse_portfolio_filename(resolved_path.name)
    normalized_base_currency = (base_currency or get_settings().default_currency).upper()

    rows = _read_csv_rows(resolved_path)
    if rows[0] != EXPECTED_PORTFOLIO_HEADERS:
        raise ValueError(
            "Unexpected DEGIRO portfolio header. "
            f"Expected {EXPECTED_PORTFOLIO_HEADERS!r} but received {rows[0]!r}."
        )

    records: list[dict[str, object]] = []
    for row_number, raw_row in enumerate(rows[1:], start=2):
        row = _normalize_row(raw_row, len(EXPECTED_PORTFOLIO_HEADERS))
        fields = dict(zip(LOGICAL_ROW_FIELDS, row))
        records.append(
            _parse_snapshot_row(
                fields,
                source_path=resolved_path,
                source_row=row_number,
                snapshot_date=snapshot_date,
                base_currency=normalized_base_currency,
                account_id=account_id,
                source_root=source_root,
            )
        )

    snapshots = pd.DataFrame(records, columns=PORTFOLIO_SNAPSHOT_COLUMNS)
    return ParsedDegiroPortfolioSnapshots(
        source_path=resolved_path,
        snapshot_date=snapshot_date,
        snapshots=snapshots,
    )


def persist_degiro_portfolio_snapshots_dataset(
    parsed: ParsedDegiroPortfolioSnapshots,
    *,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroPortfolioSnapshots:
    """Persist normalized DEGIRO portfolio snapshots as parquet."""
    resolved_settings = get_settings() if settings is None else settings
    ensure_local_directories(resolved_settings)

    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.normalized_data_dir / "degiro" / "portfolio_snapshots"
    )
    base_output_dir.mkdir(parents=True, exist_ok=True)

    output_path = (base_output_dir / f"{parsed.source_path.stem}.parquet").resolve()
    ready = parsed.snapshots.copy()
    ready["snapshot_date"] = pd.to_datetime(ready["snapshot_date"])
    ready.to_parquet(output_path, index=False)
    return replace(parsed, output_path=output_path)


def parse_and_persist_degiro_portfolio_snapshots(
    source_path: str | Path,
    *,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    settings: Settings | None = None,
) -> ParsedDegiroPortfolioSnapshots:
    """Parse a portfolio CSV and persist the normalized snapshot dataset."""
    parsed = parse_degiro_portfolio_snapshot_csv(
        source_path,
        base_currency=base_currency,
        account_id=account_id,
        source_root=source_root,
    )
    return persist_degiro_portfolio_snapshots_dataset(parsed, output_dir=output_dir, settings=settings)


def _parse_portfolio_filename(filename: str) -> date:
    match = PORTFOLIO_FILENAME_RE.match(filename)
    if match is None:
        raise ValueError("DEGIRO portfolio filename must follow 'portfolio_YYYY-MM-DD.csv'.")
    return date.fromisoformat(match.group("snapshot_date"))


def _read_csv_rows(source_path: Path) -> list[list[str]]:
    with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]

    if not rows:
        raise ValueError(f"Portfolio CSV is empty: {source_path}")
    return rows


def _normalize_row(row: list[str], target_length: int) -> list[str]:
    if len(row) != target_length:
        raise ValueError(f"Unexpected portfolio row width: {len(row)} columns.")
    return row


def _parse_snapshot_row(
    fields: dict[str, str],
    *,
    source_path: Path,
    source_row: int,
    snapshot_date: date,
    base_currency: str,
    account_id: str | None,
    source_root: str | Path | None,
) -> dict[str, object]:
    asset_name = _require_text(fields["product_name"], field_name="Producto")
    raw_symbol_or_isin = _optional_text(fields["symbol_or_isin"])
    position_currency = _require_currency(fields["position_currency_raw"], field_name="Valor local")
    market_value = _require_decimal(fields["market_value_raw"], field_name="Valor local importe").quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )
    market_value_base = _require_decimal(fields["market_value_base_raw"], field_name="Valor en EUR").quantize(
        AMOUNT_QUANTIZER,
        rounding=ROUND_HALF_UP,
    )

    notes: list[str] = []
    is_cash_row = raw_symbol_or_isin is None and not fields["quantity_raw"].strip() and not fields["market_price_raw"].strip()
    if is_cash_row:
        quantity = market_value
        market_price = Decimal("1").quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)
        asset_type = "cash"
        asset_id = f"degiro:cash:{position_currency.lower()}"
        isin = None
        broker_symbol = None
        notes.append("cash_snapshot_derived_quantity_and_price")
    else:
        quantity = _require_decimal(fields["quantity_raw"], field_name="Cantidad").quantize(
            AMOUNT_QUANTIZER,
            rounding=ROUND_HALF_UP,
        )
        market_price = _require_decimal(fields["market_price_raw"], field_name="Precio de").quantize(
            AMOUNT_QUANTIZER,
            rounding=ROUND_HALF_UP,
        )
        isin = raw_symbol_or_isin if _is_isin(raw_symbol_or_isin) else None
        broker_symbol = raw_symbol_or_isin if raw_symbol_or_isin and not isin else None
        asset_id = _build_asset_id(
            asset_name=asset_name,
            position_currency=position_currency,
            isin=isin,
            broker_symbol=broker_symbol,
            is_cash=False,
        )
        asset_type = _infer_asset_type(asset_name)

    fx_rate_to_base = _derive_fx_rate_to_base(
        market_value=market_value,
        market_value_base=market_value_base,
        position_currency=position_currency,
        base_currency=base_currency,
        notes=notes,
    )
    source_path_display = _display_source_path(source_path, source_root=source_root)

    fingerprint = "|".join(
        (
            source_path.name,
            str(source_row),
            snapshot_date.isoformat(),
            asset_id,
            _decimal_to_string(quantity),
            _decimal_to_string(market_value),
            _decimal_to_string(market_value_base),
        )
    )
    snapshot_id = f"degiro_snap_{hashlib.sha1(fingerprint.encode('utf-8')).hexdigest()[:16]}"

    return {
        "snapshot_id": snapshot_id,
        "broker": "DEGIRO",
        "account_id": account_id,
        "snapshot_date": snapshot_date,
        "snapshot_source": "broker_export",
        "asset_id": asset_id,
        "asset_name": asset_name,
        "asset_type": asset_type,
        "isin": isin,
        "broker_symbol": broker_symbol,
        "quantity": float(quantity),
        "average_cost": None,
        "market_price": float(market_price),
        "market_value": float(market_value),
        "position_currency": position_currency,
        "base_currency": base_currency,
        "fx_rate_to_base": float(fx_rate_to_base) if fx_rate_to_base is not None else None,
        "market_value_base": float(market_value_base),
        "unrealized_pnl_base": None,
        "source_file": source_path.name,
        "source_row": source_row,
        "source_path": source_path_display,
        "notes": "; ".join(notes) or None,
    }


def _derive_fx_rate_to_base(
    *,
    market_value: Decimal,
    market_value_base: Decimal,
    position_currency: str,
    base_currency: str,
    notes: list[str],
) -> Decimal | None:
    if position_currency == base_currency:
        return Decimal("1")
    if market_value_base == 0:
        notes.append("fx_rate_unavailable_zero_base_value")
        return None
    notes.append("fx_rate_derived_from_market_values")
    return (market_value / market_value_base).quantize(FX_QUANTIZER, rounding=ROUND_HALF_UP)


def _require_decimal(raw_value: str, *, field_name: str) -> Decimal:
    parsed = _parse_decimal(raw_value)
    if parsed is None:
        raise ValueError(f"Missing required decimal field: {field_name}")
    return parsed


def _parse_decimal(raw_value: str) -> Decimal | None:
    text = raw_value.strip()
    if not text:
        return None
    normalized = _normalize_decimal_text(text)
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {raw_value!r}") from exc


def _normalize_decimal_text(text: str) -> str:
    if "," in text:
        return text.replace(".", "").replace(",", ".")

    if text.count(".") == 1:
        whole, fractional = text.split(".")
        if whole.lstrip("-").isdigit() and fractional.isdigit() and len(fractional) == 3:
            return whole + fractional
    return text


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


def _is_isin(value: str | None) -> bool:
    if value is None:
        return False
    return re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", value) is not None


def _build_asset_id(
    *,
    asset_name: str,
    position_currency: str,
    isin: str | None,
    broker_symbol: str | None,
    is_cash: bool,
) -> str:
    if is_cash:
        return f"degiro:cash:{position_currency.lower()}"
    if isin:
        return f"degiro:isin:{isin.upper()}"
    if broker_symbol:
        return f"degiro:symbol:{_slugify(broker_symbol)}"
    return f"degiro:product:{_slugify(asset_name)}"


def _infer_asset_type(asset_name: str) -> str:
    normalized_name = asset_name.upper()
    if "CASH" in normalized_name and "FUND" in normalized_name:
        return "cash"
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
