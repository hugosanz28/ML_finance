"""Persistence helpers for generated report history."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Iterator

import duckdb

from src.config import Settings, default_repo_root, ensure_local_directories, get_settings


@dataclass(frozen=True)
class ReportHistoryEntry:
    """Metadata persisted for a generated report artifact."""

    report_id: str
    report_type: str
    report_period_start: date | None
    report_period_end: date | None
    as_of_date: date | None
    generated_at: datetime
    report_format: str
    report_path: str
    status: str
    base_currency: str | None
    source_snapshot_date: date | None
    parameters_json: str | None
    report_hash: str | None
    notes: str | None


class DuckDBReportHistoryRepository:
    """Read and write report metadata in the local DuckDB database."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.settings = get_settings() if settings is None else settings
        self.db_path = (
            self.settings.portfolio_db_path
            if db_path is None
            else Path(db_path).expanduser().resolve()
        )
        schema_path = self.settings.initial_schema_path
        if not schema_path.exists():
            schema_path = default_repo_root() / "src" / "data" / "sql" / "001_initial_schema.sql"
        self._schema_sql = schema_path.read_text(encoding="utf-8")

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Open a connection with the project schema applied."""
        ensure_local_directories(self.settings)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.db_path))
        try:
            connection.execute(self._schema_sql)
            yield connection
        finally:
            connection.close()

    def insert_report_entry(
        self,
        *,
        report_id: str,
        report_type: str,
        report_period_start: date | None,
        report_period_end: date | None,
        as_of_date: date | None,
        generated_at: datetime,
        report_path: str,
        report_format: str = "md",
        status: str = "generated",
        base_currency: str | None = None,
        source_snapshot_date: date | None = None,
        parameters: dict[str, Any] | None = None,
        report_hash: str | None = None,
        notes: str | None = None,
    ) -> ReportHistoryEntry:
        """Persist a new report metadata row without overwriting existing history."""
        parameters_json = json.dumps(parameters, sort_keys=True, ensure_ascii=True) if parameters is not None else None
        with self.connection() as connection:
            connection.execute(
                """
                INSERT INTO reports_history (
                    report_id,
                    report_type,
                    report_period_start,
                    report_period_end,
                    as_of_date,
                    generated_at,
                    report_format,
                    report_path,
                    status,
                    base_currency,
                    source_snapshot_date,
                    parameters_json,
                    report_hash,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    report_id,
                    report_type,
                    report_period_start,
                    report_period_end,
                    as_of_date,
                    generated_at,
                    report_format,
                    report_path,
                    status,
                    base_currency,
                    source_snapshot_date,
                    parameters_json,
                    report_hash,
                    notes,
                ],
            )
        return ReportHistoryEntry(
            report_id=report_id,
            report_type=report_type,
            report_period_start=report_period_start,
            report_period_end=report_period_end,
            as_of_date=as_of_date,
            generated_at=generated_at,
            report_format=report_format,
            report_path=report_path,
            status=status,
            base_currency=base_currency,
            source_snapshot_date=source_snapshot_date,
            parameters_json=parameters_json,
            report_hash=report_hash,
            notes=notes,
        )

    def get_latest_report(self, *, report_type: str) -> ReportHistoryEntry | None:
        """Return the latest generated report row for the requested type."""
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    report_id,
                    report_type,
                    report_period_start,
                    report_period_end,
                    as_of_date,
                    generated_at,
                    report_format,
                    report_path,
                    status,
                    base_currency,
                    source_snapshot_date,
                    parameters_json,
                    report_hash,
                    notes
                FROM reports_history
                WHERE report_type = ?
                ORDER BY generated_at DESC, report_id DESC
                LIMIT 1
                """,
                [report_type],
            ).fetchone()
        if row is None:
            return None
        return ReportHistoryEntry(
            report_id=row[0],
            report_type=row[1],
            report_period_start=row[2],
            report_period_end=row[3],
            as_of_date=row[4],
            generated_at=row[5],
            report_format=row[6],
            report_path=row[7],
            status=row[8],
            base_currency=row[9],
            source_snapshot_date=row[10],
            parameters_json=row[11],
            report_hash=row[12],
            notes=row[13],
        )
