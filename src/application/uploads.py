"""Application use case for saving DEGIRO exports uploaded by a UI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
import unicodedata

from src.application.types import ApplicationResult
from src.config import Settings, get_settings


@dataclass(frozen=True)
class DegiroUpload:
    filename: str
    content: bytes


@dataclass(frozen=True)
class SaveDegiroUploadsRequest:
    uploads: tuple[DegiroUpload, ...]
    uploaded_at: date


@dataclass(frozen=True)
class DegiroUploadOutcome:
    original_filename: str
    detected_kind: str
    saved_as: str | None
    status: str
    detail: str


@dataclass(frozen=True)
class SaveDegiroUploadsResult:
    result: ApplicationResult
    outcomes: tuple[DegiroUploadOutcome, ...]


class SaveDegiroUploadsUseCase:
    """Normalize uploaded filenames and persist their bytes in ``incoming``."""

    name = "save_degiro_uploads"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: SaveDegiroUploadsRequest) -> SaveDegiroUploadsResult:
        incoming_dir = self.settings.degiro_exports_dir / "incoming"
        incoming_dir.mkdir(parents=True, exist_ok=True)

        outcomes: list[DegiroUploadOutcome] = []
        for upload in request.uploads:
            canonical_name = canonical_degiro_upload_name(
                upload.filename,
                fallback_date=request.uploaded_at,
            )
            if canonical_name is None:
                outcomes.append(
                    DegiroUploadOutcome(
                        original_filename=upload.filename,
                        detected_kind="desconocido",
                        saved_as=None,
                        status="omitido",
                        detail=(
                            "Renombra el archivo incluyendo cartera/portfolio, "
                            "transacciones/transactions o cuenta/account."
                        ),
                    )
                )
                continue

            target_path = incoming_dir / canonical_name
            existed = target_path.exists()
            target_path.write_bytes(upload.content)
            outcomes.append(
                DegiroUploadOutcome(
                    original_filename=upload.filename,
                    detected_kind=friendly_degiro_kind(canonical_name),
                    saved_as=canonical_name,
                    status="guardado",
                    detail="Sobrescrito" if existed else "Nuevo",
                )
            )

        saved_count = sum(outcome.status == "guardado" for outcome in outcomes)
        skipped_count = len(outcomes) - saved_count
        status = "partial" if skipped_count else "succeeded"
        if not outcomes or not saved_count:
            status = "skipped"
        return SaveDegiroUploadsResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=f"Saved {saved_count} DEGIRO upload(s); skipped {skipped_count}.",
                artifacts={
                    "incoming_dir": incoming_dir,
                    "saved_count": saved_count,
                    "skipped_count": skipped_count,
                },
            ),
            outcomes=tuple(outcomes),
        )


def canonical_degiro_upload_name(filename: str, *, fallback_date: date) -> str | None:
    kind = detect_degiro_upload_kind(filename)
    if kind is None:
        return None

    dates = extract_dates_from_filename(filename)
    if kind == "portfolio":
        snapshot_date = max(dates) if dates else fallback_date
        return f"portfolio_{snapshot_date.isoformat()}.csv"

    date_from, date_to = _date_range_from_filename_dates(dates, fallback_date=fallback_date)
    if kind == "transactions":
        return f"transactions_{date_from.isoformat()}_{date_to.isoformat()}.csv"
    return f"account_{date_from.isoformat()}_{date_to.isoformat()}.csv"


def detect_degiro_upload_kind(filename: str) -> str | None:
    normalized = _normalize_filename_text(filename)
    portfolio_tokens = ("portfolio", "cartera", "posiciones", "positions", "snapshot")
    transaction_tokens = (
        "transactions",
        "transaction",
        "transacciones",
        "transaccion",
        "operaciones",
        "ordenes",
        "orders",
    )
    account_tokens = ("account", "cuenta", "cash", "efectivo", "movimientos", "actividad", "activity")

    if any(token in normalized for token in portfolio_tokens):
        return "portfolio"
    if any(token in normalized for token in transaction_tokens):
        return "transactions"
    if any(token in normalized for token in account_tokens):
        return "account"
    return None


def extract_dates_from_filename(filename: str) -> list[date]:
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


def friendly_degiro_kind(canonical_name: str) -> str:
    if canonical_name.startswith("transactions_"):
        return "transacciones"
    if canonical_name.startswith("account_"):
        return "cuenta / efectivo"
    if canonical_name.startswith("portfolio_"):
        return "cartera"
    return "desconocido"


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


__all__ = [
    "DegiroUpload",
    "DegiroUploadOutcome",
    "SaveDegiroUploadsRequest",
    "SaveDegiroUploadsResult",
    "SaveDegiroUploadsUseCase",
    "canonical_degiro_upload_name",
    "detect_degiro_upload_kind",
    "extract_dates_from_filename",
    "friendly_degiro_kind",
]
