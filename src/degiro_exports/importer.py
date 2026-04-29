"""Batch import helpers for DEGIRO export files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

from src.config import Settings, get_settings
from src.degiro_exports.cash_movements import parse_and_persist_degiro_cash_movements
from src.degiro_exports.portfolio_snapshots import parse_and_persist_degiro_portfolio_snapshots
from src.degiro_exports.transactions import parse_and_persist_degiro_transactions


DegiroImportKind = Literal["transactions", "cash_movements", "portfolio_snapshot", "unknown"]
DegiroImportStatus = Literal["imported", "skipped", "failed", "would_import"]


@dataclass(frozen=True)
class DegiroImportOutcome:
    """Result of importing, skipping, or rejecting one DEGIRO export file."""

    source_path: Path
    kind: DegiroImportKind
    status: DegiroImportStatus
    output_paths: tuple[Path, ...] = ()
    message: str = ""


@dataclass(frozen=True)
class DegiroImportSummary:
    """Summary returned by a batch DEGIRO import run."""

    incoming_dir: Path
    output_dir: Path
    outcomes: tuple[DegiroImportOutcome, ...]

    @property
    def imported_count(self) -> int:
        return sum(outcome.status == "imported" for outcome in self.outcomes)

    @property
    def failed_count(self) -> int:
        return sum(outcome.status == "failed" for outcome in self.outcomes)

    @property
    def skipped_count(self) -> int:
        return sum(outcome.status == "skipped" for outcome in self.outcomes)

    @property
    def would_import_count(self) -> int:
        return sum(outcome.status == "would_import" for outcome in self.outcomes)


def import_degiro_exports(
    *,
    settings: Settings | None = None,
    incoming_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    base_currency: str | None = None,
    account_id: str | None = None,
    source_root: str | Path | None = None,
    ignore_unknown: bool = False,
    dry_run: bool = False,
) -> DegiroImportSummary:
    """Import all canonical DEGIRO CSV exports from one incoming directory.

    The function is intentionally thin: it detects the file kind from the
    canonical filename and delegates parsing/persistence to the existing
    type-specific parsers.
    """
    resolved_settings = get_settings() if settings is None else settings
    resolved_incoming_dir = (
        resolved_settings.degiro_exports_dir / "incoming"
        if incoming_dir is None
        else Path(incoming_dir).expanduser().resolve()
    )
    resolved_output_dir = (
        resolved_settings.normalized_data_dir / "degiro"
        if output_dir is None
        else Path(output_dir).expanduser().resolve()
    )
    resolved_source_root = (
        source_root
        if source_root is not None
        else resolved_settings.degiro_exports_dir
    )

    outcomes: list[DegiroImportOutcome] = []
    for source_path in _iter_csv_files(resolved_incoming_dir):
        kind = classify_degiro_export(source_path)
        if kind == "unknown":
            status: DegiroImportStatus = "skipped" if ignore_unknown else "failed"
            outcomes.append(
                DegiroImportOutcome(
                    source_path=source_path,
                    kind=kind,
                    status=status,
                    message="Filename does not match a supported DEGIRO export pattern.",
                )
            )
            continue

        if dry_run:
            outcomes.append(
                DegiroImportOutcome(
                    source_path=source_path,
                    kind=kind,
                    status="would_import",
                    message="Dry run; file was not parsed or persisted.",
                )
            )
            continue

        try:
            outcomes.append(
                _import_one_file(
                    source_path,
                    kind=kind,
                    settings=resolved_settings,
                    output_dir=resolved_output_dir,
                    base_currency=base_currency,
                    account_id=account_id,
                    source_root=resolved_source_root,
                )
            )
        except Exception as exc:
            outcomes.append(
                DegiroImportOutcome(
                    source_path=source_path,
                    kind=kind,
                    status="failed",
                    message=str(exc),
                )
            )

    return DegiroImportSummary(
        incoming_dir=resolved_incoming_dir,
        output_dir=resolved_output_dir,
        outcomes=tuple(outcomes),
    )


def classify_degiro_export(source_path: str | Path) -> DegiroImportKind:
    """Classify a DEGIRO export using the canonical filename contract."""
    name = Path(source_path).name
    if name.startswith("transactions_") and name.endswith(".csv"):
        return "transactions"
    if name.startswith("account_") and name.endswith(".csv"):
        return "cash_movements"
    if name.startswith("portfolio_") and name.endswith(".csv"):
        return "portfolio_snapshot"
    return "unknown"


def _iter_csv_files(incoming_dir: Path) -> Sequence[Path]:
    if not incoming_dir.exists():
        return ()
    return tuple(path.resolve() for path in sorted(incoming_dir.glob("*.csv")) if path.is_file())


def _import_one_file(
    source_path: Path,
    *,
    kind: DegiroImportKind,
    settings: Settings,
    output_dir: Path,
    base_currency: str | None,
    account_id: str | None,
    source_root: str | Path | None,
) -> DegiroImportOutcome:
    if kind == "transactions":
        parsed = parse_and_persist_degiro_transactions(
            source_path,
            base_currency=base_currency,
            account_id=account_id,
            source_root=source_root,
            output_dir=output_dir,
            settings=settings,
        )
        output_paths = tuple(
            path
            for path in (parsed.transactions_output_path, parsed.asset_hints_output_path)
            if path is not None
        )
        return DegiroImportOutcome(
            source_path=source_path,
            kind=kind,
            status="imported",
            output_paths=output_paths,
            message=f"Imported {len(parsed.transactions)} transactions and {len(parsed.asset_hints)} asset hints.",
        )

    if kind == "cash_movements":
        parsed = parse_and_persist_degiro_cash_movements(
            source_path,
            base_currency=base_currency,
            account_id=account_id,
            source_root=source_root,
            output_dir=output_dir / "cash_movements",
            settings=settings,
        )
        output_paths = () if parsed.output_path is None else (parsed.output_path,)
        return DegiroImportOutcome(
            source_path=source_path,
            kind=kind,
            status="imported",
            output_paths=output_paths,
            message=f"Imported {len(parsed.cash_movements)} cash movements.",
        )

    if kind == "portfolio_snapshot":
        parsed = parse_and_persist_degiro_portfolio_snapshots(
            source_path,
            base_currency=base_currency,
            account_id=account_id,
            source_root=source_root,
            output_dir=output_dir / "portfolio_snapshots",
            settings=settings,
        )
        output_paths = () if parsed.output_path is None else (parsed.output_path,)
        return DegiroImportOutcome(
            source_path=source_path,
            kind=kind,
            status="imported",
            output_paths=output_paths,
            message=f"Imported {len(parsed.snapshots)} portfolio snapshot rows.",
        )

    raise ValueError(f"Unsupported DEGIRO import kind: {kind}")
