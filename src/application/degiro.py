"""Application use cases for DEGIRO import workflows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.degiro_exports.importer import DegiroImportSummary, import_degiro_exports
from src.degiro_exports.warehouse import DegiroWarehouseLoadSummary, load_normalized_degiro_to_duckdb


@dataclass(frozen=True)
class ImportDegiroRequest:
    incoming_dir: Path | None = None
    output_dir: Path | None = None
    base_currency: str | None = None
    account_id: str | None = None
    source_root: Path | None = None
    ignore_unknown: bool = False
    dry_run: bool = False
    load_duckdb: bool = True


@dataclass(frozen=True)
class ImportDegiroResult:
    result: ApplicationResult
    import_summary: DegiroImportSummary
    warehouse_summary: DegiroWarehouseLoadSummary | None = None


class ImportDegiroUseCase:
    """Import canonical DEGIRO CSV files and optionally load DuckDB."""

    name = "import_degiro"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: ImportDegiroRequest | None = None) -> ImportDegiroResult:
        resolved_request = request or ImportDegiroRequest()
        import_summary = import_degiro_exports(
            settings=self.settings,
            incoming_dir=resolved_request.incoming_dir,
            output_dir=resolved_request.output_dir,
            base_currency=resolved_request.base_currency,
            account_id=resolved_request.account_id,
            source_root=resolved_request.source_root,
            ignore_unknown=resolved_request.ignore_unknown,
            dry_run=resolved_request.dry_run,
        )

        warehouse_summary = None
        if (
            resolved_request.load_duckdb
            and not resolved_request.dry_run
            and import_summary.outcomes
            and import_summary.failed_count == 0
        ):
            warehouse_summary = load_normalized_degiro_to_duckdb(
                settings=self.settings,
                normalized_degiro_dir=import_summary.output_dir,
            )

        result = self._build_result(
            import_summary=import_summary,
            warehouse_summary=warehouse_summary,
            dry_run=resolved_request.dry_run,
        )
        return ImportDegiroResult(
            result=result,
            import_summary=import_summary,
            warehouse_summary=warehouse_summary,
        )

    def _build_result(
        self,
        *,
        import_summary: DegiroImportSummary,
        warehouse_summary: DegiroWarehouseLoadSummary | None,
        dry_run: bool,
    ) -> ApplicationResult:
        artifacts = {
            "incoming_dir": import_summary.incoming_dir,
            "output_dir": import_summary.output_dir,
            "imported_count": import_summary.imported_count,
            "failed_count": import_summary.failed_count,
            "skipped_count": import_summary.skipped_count,
            "would_import_count": import_summary.would_import_count,
            "duckdb_rows": warehouse_summary.total_rows if warehouse_summary else None,
        }
        if not import_summary.outcomes:
            return ApplicationResult(
                name=self.name,
                status="skipped",
                message="No CSV files found.",
                artifacts=artifacts,
            )
        if import_summary.failed_count:
            return ApplicationResult(
                name=self.name,
                status="failed",
                message=f"DEGIRO import finished with {import_summary.failed_count} failed file(s).",
                artifacts=artifacts,
            )
        if dry_run:
            return ApplicationResult(
                name=self.name,
                status="succeeded",
                message=f"Dry run found {import_summary.would_import_count} importable file(s).",
                artifacts=artifacts,
            )
        return ApplicationResult(
            name=self.name,
            status="succeeded",
            message=f"Imported {import_summary.imported_count} DEGIRO file(s).",
            artifacts=artifacts,
        )


__all__ = [
    "ImportDegiroRequest",
    "ImportDegiroResult",
    "ImportDegiroUseCase",
]
