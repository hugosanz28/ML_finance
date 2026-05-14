"""Upload helpers for the Streamlit dashboard DEGIRO import flow."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import unicodedata
from typing import Any

from src.config import Settings


def _save_uploaded_degiro_files(uploaded_files: list[Any], *, settings: Settings, uploaded_at: date) -> list[dict[str, str]]:
    incoming_dir = settings.degiro_exports_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    outcomes: list[dict[str, str]] = []
    for uploaded_file in uploaded_files:
        canonical_name = _canonical_degiro_upload_name(uploaded_file.name, fallback_date=uploaded_at)
        if canonical_name is None:
            outcomes.append(
                {
                    "archivo_original": uploaded_file.name,
                    "tipo_detectado": "desconocido",
                    "guardado_como": "",
                    "status": "omitido",
                    "detalle": "Renombra el archivo incluyendo cartera/portfolio, transacciones/transactions o cuenta/account.",
                }
            )
            continue

        target_path = incoming_dir / canonical_name
        existed = target_path.exists()
        target_path.write_bytes(uploaded_file.getbuffer())
        outcomes.append(
            {
                "archivo_original": uploaded_file.name,
                "tipo_detectado": _friendly_degiro_kind(canonical_name),
                "guardado_como": canonical_name,
                "status": "guardado",
                "detalle": "Sobrescrito" if existed else "Nuevo",
            }
        )

    return outcomes


def _canonical_degiro_upload_name(filename: str, *, fallback_date: date) -> str | None:
    kind = _detect_degiro_upload_kind(filename)
    if kind is None:
        return None

    dates = _extract_dates_from_filename(filename)
    if kind == "portfolio":
        snapshot_date = max(dates) if dates else fallback_date
        return f"portfolio_{snapshot_date.isoformat()}.csv"

    date_from, date_to = _date_range_from_filename_dates(dates, fallback_date=fallback_date)
    if kind == "transactions":
        return f"transactions_{date_from.isoformat()}_{date_to.isoformat()}.csv"
    return f"account_{date_from.isoformat()}_{date_to.isoformat()}.csv"


def _detect_degiro_upload_kind(filename: str) -> str | None:
    normalized = _normalize_filename_text(filename)
    portfolio_tokens = ("portfolio", "cartera", "posiciones", "positions", "snapshot")
    transaction_tokens = ("transactions", "transaction", "transacciones", "transaccion", "operaciones", "ordenes", "orders")
    account_tokens = ("account", "cuenta", "cash", "efectivo", "movimientos", "actividad", "activity")

    if any(token in normalized for token in portfolio_tokens):
        return "portfolio"
    if any(token in normalized for token in transaction_tokens):
        return "transactions"
    if any(token in normalized for token in account_tokens):
        return "account"
    return None


def _extract_dates_from_filename(filename: str) -> list[date]:
    dates: list[date] = []
    seen: set[date] = set()
    normalized = _normalize_filename_text(filename)

    patterns = (
        (re.compile(r"(?<!\d)(\d{4})[-_.](\d{1,2})[-_.](\d{1,2})(?!\d)"), "ymd"),
        (re.compile(r"(?<!\d)(\d{1,2})[-_.](\d{1,2})[-_.](\d{4})(?!\d)"), "dmy"),
        (re.compile(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)"), "compact_ymd"),
        (re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)"), "compact_dmy"),
    )
    for pattern, order in patterns:
        for match in pattern.finditer(normalized):
            parsed = _parse_filename_date(match.groups(), order)
            if parsed is not None and parsed not in seen:
                dates.append(parsed)
                seen.add(parsed)
    return sorted(dates)


def _parse_filename_date(parts: tuple[str, ...], order: str) -> date | None:
    try:
        if order in {"ymd", "compact_ymd"}:
            year, month, day = (int(part) for part in parts)
        else:
            day, month, year = (int(part) for part in parts)
        return date(year, month, day)
    except ValueError:
        return None


def _date_range_from_filename_dates(dates: list[date], *, fallback_date: date) -> tuple[date, date]:
    if not dates:
        return fallback_date, fallback_date
    return min(dates), max(dates)


def _normalize_filename_text(filename: str) -> str:
    without_accents = unicodedata.normalize("NFKD", filename).encode("ascii", "ignore").decode("ascii")
    return without_accents.lower()


def _friendly_degiro_kind(canonical_name: str) -> str:
    if canonical_name.startswith("transactions_"):
        return "transacciones"
    if canonical_name.startswith("account_"):
        return "cuenta / efectivo"
    if canonical_name.startswith("portfolio_"):
        return "cartera"
    return "desconocido"
