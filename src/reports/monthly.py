"""Monthly portfolio report generation in Markdown."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from pathlib import Path
import re
import unicodedata
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

from src.config import Settings, ensure_local_directories, get_settings
from src.portfolio import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_transactions,
)
from src.reports.history import DuckDBReportHistoryRepository, ReportHistoryEntry

DEFAULT_MONTHLY_PERIODS: tuple[tuple[str, str, int], ...] = (
    ("1m", "Ultimo mes", 1),
    ("3m", "Ultimos 3 meses", 3),
    ("12m", "Ultimos 12 meses", 12),
)

DIVIDEND_MOVEMENT_TYPES = {
    "DIVIDEND",
    "CORPORATE_ACTION_SCRIP_DIVIDEND",
}
CONTRIBUTION_IN_MOVEMENT_TYPES = {
    "DEPOSIT",
    "CASH_ACCOUNT_TRANSFER_IN",
}
CONTRIBUTION_OUT_MOVEMENT_TYPES = {
    "CASH_ACCOUNT_TRANSFER_OUT",
}


@dataclass(frozen=True)
class MonthlyPeriodSummary:
    """Period comparison included in the monthly report."""

    code: str
    label: str
    months: int
    requested_start_date: date
    effective_start_date: date
    end_date: date
    available_coverage_days: int
    total_market_value_start_base: float
    total_market_value_end_base: float
    total_market_value_change_base: float
    total_market_value_change_pct: float | None
    portfolio_return_pct_end: float | None
    drawdown_pct_end: float | None
    valuation_coverage_ratio_end: float
    buy_count: int
    buy_amount_base: float
    sell_count: int
    sell_amount_base: float
    dividend_count: int
    dividend_amount_base: float | None
    dividend_missing_base_count: int
    contribution_in_count: int
    contribution_in_base: float | None
    contribution_out_count: int
    contribution_out_base: float | None
    notable_changes: pd.DataFrame
    notes: tuple[str, ...]


@dataclass(frozen=True)
class MonthlyReportResult:
    """Generated monthly report content plus metadata."""

    report_id: str | None
    as_of_date: date
    generated_at: datetime
    base_currency: str
    output_path: Path | None
    content: str
    current_allocation: pd.DataFrame
    period_summaries: tuple[MonthlyPeriodSummary, ...]
    notes: tuple[str, ...]
    history_entry: ReportHistoryEntry | None


def generate_monthly_report(
    *,
    settings: Settings | None = None,
    metrics: PortfolioMetricsResult | None = None,
    transactions: pd.DataFrame | None = None,
    cash_movements: pd.DataFrame | None = None,
    normalized_degiro_dir: str | Path | None = None,
    as_of_date: date | None = None,
    output_dir: str | Path | None = None,
    persist: bool = True,
) -> MonthlyReportResult:
    """Build a monthly Markdown report from the current portfolio state."""
    resolved_settings = get_settings() if settings is None else settings

    resolved_metrics = metrics or calculate_portfolio_metrics_from_normalized_degiro(
        settings=resolved_settings,
        normalized_degiro_dir=normalized_degiro_dir,
        persist=False,
    )
    position_metrics = _prepare_position_metrics_frame(resolved_metrics.position_metrics)
    portfolio_daily_metrics = _prepare_portfolio_daily_metrics_frame(resolved_metrics.portfolio_daily_metrics)
    resolved_as_of_date = _resolve_as_of_date(portfolio_daily_metrics, requested_as_of_date=as_of_date)

    resolved_transactions = _prepare_transactions_frame(
        transactions
        if transactions is not None
        else load_normalized_degiro_transactions(
            settings=resolved_settings,
            normalized_degiro_dir=normalized_degiro_dir,
        )
    )
    resolved_cash_movements = _prepare_cash_movements_frame(
        cash_movements
        if cash_movements is not None
        else load_normalized_degiro_cash_movements(
            settings=resolved_settings,
            normalized_degiro_dir=normalized_degiro_dir,
        )
    )

    daily_as_of = _select_daily_row(portfolio_daily_metrics, resolved_as_of_date)
    positions_as_of = _select_positions_for_date(position_metrics, resolved_as_of_date)
    period_summaries = tuple(
        _build_period_summary(
            code=code,
            label=label,
            months=months,
            as_of_date=resolved_as_of_date,
            portfolio_daily_metrics=portfolio_daily_metrics,
            position_metrics=position_metrics,
            transactions=resolved_transactions,
            cash_movements=resolved_cash_movements,
        )
        for code, label, months in DEFAULT_MONTHLY_PERIODS
    )
    current_allocation = _build_current_allocation_table(
        positions_as_of=positions_as_of,
        one_month_period=next(summary for summary in period_summaries if summary.code == "1m"),
        position_metrics=position_metrics,
    )
    report_notes = _build_report_notes(
        as_of_date=resolved_as_of_date,
        positions_as_of=positions_as_of,
        daily_as_of=daily_as_of,
        period_summaries=period_summaries,
        cash_movements=resolved_cash_movements,
    )

    generated_at = _resolve_generated_at(resolved_settings.default_timezone)
    draft = MonthlyReportResult(
        report_id=None,
        as_of_date=resolved_as_of_date,
        generated_at=generated_at,
        base_currency=resolved_metrics.base_currency,
        output_path=None,
        content="",
        current_allocation=current_allocation,
        period_summaries=period_summaries,
        notes=report_notes,
        history_entry=None,
    )
    content = render_monthly_report(
        report=draft,
        daily_as_of=daily_as_of,
        positions_as_of=positions_as_of,
    )
    final_result = MonthlyReportResult(
        report_id=None,
        as_of_date=resolved_as_of_date,
        generated_at=generated_at,
        base_currency=resolved_metrics.base_currency,
        output_path=None,
        content=content,
        current_allocation=current_allocation,
        period_summaries=period_summaries,
        notes=report_notes,
        history_entry=None,
    )

    if not persist:
        return final_result

    ensure_local_directories(resolved_settings)
    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.reports_dir
    )
    base_output_dir.mkdir(parents=True, exist_ok=True)
    report_id = _build_report_id(
        report_type="monthly",
        as_of_date=resolved_as_of_date,
        generated_at=generated_at,
    )
    output_path = (base_output_dir / f"{report_id}.md").resolve()
    output_path.write_text(content, encoding="utf-8")
    history_entry = _persist_monthly_report_history(
        report_id=report_id,
        output_path=output_path,
        report=final_result,
        settings=resolved_settings,
        content=content,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    return MonthlyReportResult(
        report_id=report_id,
        as_of_date=final_result.as_of_date,
        generated_at=final_result.generated_at,
        base_currency=final_result.base_currency,
        output_path=output_path,
        content=final_result.content,
        current_allocation=final_result.current_allocation,
        period_summaries=final_result.period_summaries,
        notes=final_result.notes,
        history_entry=history_entry,
    )


def render_monthly_report(
    *,
    report: MonthlyReportResult,
    daily_as_of: pd.Series,
    positions_as_of: pd.DataFrame,
) -> str:
    """Render a generated monthly report into Markdown."""
    lines: list[str] = [
        "---",
        "report_type: monthly",
        f"as_of_date: {report.as_of_date.isoformat()}",
        f"generated_at: {report.generated_at.isoformat()}",
        f"base_currency: {report.base_currency}",
        "periods:",
    ]
    for summary in report.period_summaries:
        lines.append(f"  - {summary.code}")
    lines.extend(
        [
            "---",
            "",
            f"# Informe mensual de cartera ({report.as_of_date.isoformat()})",
            "",
            "## Resumen de cartera",
            "",
            f"- Valor total: {_format_currency(daily_as_of['total_market_value_base'], report.base_currency)}",
            f"- Coste base conocido: {_format_currency(daily_as_of['total_cost_basis_base'], report.base_currency)}",
            f"- PnL no realizado: {_format_currency(daily_as_of['total_unrealized_pnl_base'], report.base_currency)}",
            f"- Rentabilidad no realizada: {_format_pct(daily_as_of['portfolio_return_pct'])}",
            f"- Drawdown actual: {_format_pct(daily_as_of['drawdown_pct'])}",
            f"- Cobertura de valoracion: {_format_pct(daily_as_of['valuation_coverage_ratio'])}",
            f"- Posiciones abiertas: {int(daily_as_of['total_positions_count'])}",
            f"- Posiciones valoradas: {int(daily_as_of['valued_positions_count'])}",
            "",
            "## Asignacion actual",
            "",
        ]
    )

    allocation_rows = [
        [
            row["asset_name"],
            _format_number(row["quantity"], decimals=8),
            _format_pct(row["weight"]),
            _format_signed_number(row["delta_1m_pp"], suffix=" pp", decimals=2),
            _format_currency(row["market_value_base"], report.base_currency),
            _format_currency(row["cost_basis_base"], report.base_currency),
            _format_currency(row["unrealized_pnl_base"], report.base_currency),
            _format_pct(row["unrealized_return_pct"]),
            row["valuation_status"],
        ]
        for _, row in report.current_allocation.iterrows()
    ]
    lines.extend(
        [
            _render_table(
                headers=[
                    "Activo",
                    "Cantidad",
                    "Peso",
                    "Delta 1m (pp)",
                    "Valor",
                    "Coste base",
                    "PnL",
                    "Rentab",
                    "Estado",
                ],
                rows=allocation_rows,
            ),
            "",
            "## Cambios por periodo",
            "",
        ]
    )
    for summary in report.period_summaries:
        period_rows = [
            [
                row["asset_name"],
                _format_pct(row["start_weight"]),
                _format_pct(row["end_weight"]),
                _format_signed_number(row["delta_weight_pp"], suffix=" pp", decimals=2),
                _format_currency(row["start_market_value_base"], report.base_currency),
                _format_currency(row["end_market_value_base"], report.base_currency),
                _format_signed_currency(row["delta_market_value_base"], report.base_currency),
                _format_signed_number(row["delta_quantity"], decimals=8),
            ]
            for _, row in summary.notable_changes.iterrows()
        ]
        lines.extend(
            [
                f"### {summary.label}",
                "",
                f"- Ventana solicitada: {summary.requested_start_date.isoformat()} -> {summary.end_date.isoformat()}",
                f"- Cobertura disponible: {summary.effective_start_date.isoformat()} -> {summary.end_date.isoformat()} ({summary.available_coverage_days} dias)",
                (
                    f"- Valor cartera: {_format_currency(summary.total_market_value_start_base, report.base_currency)} -> "
                    f"{_format_currency(summary.total_market_value_end_base, report.base_currency)} "
                    f"({_format_signed_currency(summary.total_market_value_change_base, report.base_currency)}, "
                    f"{_format_pct(summary.total_market_value_change_pct)})"
                ),
                f"- Rentabilidad no realizada al cierre: {_format_pct(summary.portfolio_return_pct_end)}",
                f"- Drawdown al cierre: {_format_pct(summary.drawdown_pct_end)}",
                f"- Cobertura de valoracion al cierre: {_format_pct(summary.valuation_coverage_ratio_end)}",
                (
                    f"- Actividad del periodo: compras={summary.buy_count} "
                    f"({_format_currency(summary.buy_amount_base, report.base_currency)}), "
                    f"ventas={summary.sell_count} "
                    f"({_format_currency(summary.sell_amount_base, report.base_currency)}), "
                    f"dividendos={summary.dividend_count} "
                    f"({_format_currency(summary.dividend_amount_base, report.base_currency)}), "
                    f"aportaciones={summary.contribution_in_count} "
                    f"({_format_currency(summary.contribution_in_base, report.base_currency)}), "
                    f"salidas={summary.contribution_out_count} "
                    f"({_format_currency(summary.contribution_out_base, report.base_currency)})"
                ),
            ]
        )
        for note in summary.notes:
            lines.append(f"- Nota: {note}")
        lines.extend(
            [
                "",
                "Variaciones relevantes:",
                "",
                _render_table(
                    headers=[
                        "Activo",
                        "Peso inicio",
                        "Peso cierre",
                        "Delta (pp)",
                        "Valor inicio",
                        "Valor cierre",
                        "Delta valor",
                        "Delta cantidad",
                    ],
                    rows=period_rows,
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Notas de cobertura",
            "",
        ]
    )
    for note in report.notes:
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Metadatos de generacion",
            "",
            f"- Reporte generado: {report.generated_at.isoformat()}",
            f"- Fecha de referencia: {report.as_of_date.isoformat()}",
            f"- Moneda base: {report.base_currency}",
            f"- Total de filas de asignacion actual: {len(positions_as_of)}",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def load_normalized_degiro_cash_movements(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Load normalized DEGIRO cash movement datasets from parquet."""
    resolved_settings = get_settings() if settings is None else settings
    base_dir = (
        resolved_settings.normalized_data_dir / "degiro"
        if normalized_degiro_dir is None
        else Path(normalized_degiro_dir).expanduser().resolve()
    )
    frames: list[pd.DataFrame] = []
    cash_dir = base_dir / "cash_movements"
    for parquet_path in sorted(cash_dir.glob("*.parquet")) if cash_dir.exists() else []:
        frame = pd.read_parquet(parquet_path)
        if frame.empty:
            continue
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _prepare_position_metrics_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("position_metrics cannot be empty when generating a monthly report.")
    ready = frame.copy()
    ready["valuation_date"] = pd.to_datetime(ready["valuation_date"], errors="raise").dt.date
    numeric_columns = [
        "quantity",
        "market_value_base",
        "cost_basis_base",
        "unrealized_pnl_base",
        "unrealized_return_pct",
        "weight",
    ]
    for column in numeric_columns:
        ready[column] = pd.to_numeric(ready[column], errors="coerce")
    ready["asset_name"] = ready["asset_name"].fillna(ready["asset_id"]).astype("string")
    ready["valuation_status"] = ready["valuation_status"].fillna("unknown").astype("string")
    return ready.sort_values(["valuation_date", "asset_id"]).reset_index(drop=True)


def _prepare_portfolio_daily_metrics_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("portfolio_daily_metrics cannot be empty when generating a monthly report.")
    ready = frame.copy()
    ready["valuation_date"] = pd.to_datetime(ready["valuation_date"], errors="raise").dt.date
    numeric_columns = [
        "total_positions_count",
        "valued_positions_count",
        "missing_price_positions_count",
        "missing_fx_positions_count",
        "valuation_coverage_ratio",
        "total_market_value_base",
        "total_cost_basis_base",
        "total_unrealized_pnl_base",
        "portfolio_return_pct",
        "drawdown_pct",
    ]
    for column in numeric_columns:
        ready[column] = pd.to_numeric(ready[column], errors="coerce")
    return ready.sort_values("valuation_date").reset_index(drop=True)


def _prepare_transactions_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "transaction_type",
                "asset_id",
                "asset_name",
                "quantity",
                "gross_amount_base",
                "fees_amount_base",
                "taxes_amount_base",
            ]
        )
    ready = frame.copy()
    ready["trade_date"] = pd.to_datetime(ready["trade_date"], errors="raise").dt.date
    ready["transaction_type"] = ready["transaction_type"].astype("string").str.upper()
    for column in ["quantity", "gross_amount_base", "fees_amount_base", "taxes_amount_base"]:
        ready[column] = pd.to_numeric(ready[column], errors="coerce").fillna(0.0)
    ready["asset_name"] = ready["asset_name"].fillna(ready["asset_id"]).astype("string")
    return ready


def _prepare_cash_movements_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "movement_date",
                "value_date",
                "movement_type",
                "asset_name",
                "amount_base",
                "amount",
            ]
        )
    ready = frame.copy()
    ready["movement_date"] = pd.to_datetime(ready["movement_date"], errors="raise").dt.date
    if "value_date" in ready.columns:
        ready["value_date"] = pd.to_datetime(ready["value_date"], errors="coerce").dt.date
    else:
        ready["value_date"] = pd.NaT
    ready["effective_date"] = ready["value_date"].where(ready["value_date"].notna(), ready["movement_date"])
    ready["movement_type"] = ready["movement_type"].fillna("UNKNOWN").astype("string")
    ready["asset_name"] = ready["asset_name"].fillna("").astype("string")
    for column in ["amount_base", "amount"]:
        ready[column] = pd.to_numeric(ready[column], errors="coerce")
    return ready


def _resolve_as_of_date(portfolio_daily_metrics: pd.DataFrame, *, requested_as_of_date: date | None) -> date:
    available_dates = sorted(portfolio_daily_metrics["valuation_date"].dropna().tolist())
    if not available_dates:
        raise ValueError("No valuation dates available for report generation.")
    if requested_as_of_date is None:
        return available_dates[-1]
    if requested_as_of_date in set(available_dates):
        return requested_as_of_date
    fallback_dates = [current for current in available_dates if current <= requested_as_of_date]
    if not fallback_dates:
        raise ValueError(
            "Requested as_of_date is earlier than the first available valuation date: "
            f"{requested_as_of_date.isoformat()}."
        )
    return fallback_dates[-1]


def _select_daily_row(portfolio_daily_metrics: pd.DataFrame, target_date: date) -> pd.Series:
    matches = portfolio_daily_metrics.loc[portfolio_daily_metrics["valuation_date"] == target_date]
    if matches.empty:
        raise ValueError(f"No daily portfolio metrics available for {target_date.isoformat()}.")
    return matches.iloc[-1]


def _select_first_daily_row_on_or_after(portfolio_daily_metrics: pd.DataFrame, target_date: date) -> pd.Series:
    matches = portfolio_daily_metrics.loc[portfolio_daily_metrics["valuation_date"] >= target_date]
    if matches.empty:
        raise ValueError(f"No daily portfolio metrics available on or after {target_date.isoformat()}.")
    return matches.iloc[0]


def _select_positions_for_date(position_metrics: pd.DataFrame, target_date: date) -> pd.DataFrame:
    matches = position_metrics.loc[position_metrics["valuation_date"] == target_date].copy()
    if matches.empty:
        raise ValueError(f"No position metrics available for {target_date.isoformat()}.")
    return matches.sort_values(["market_value_base", "asset_name"], ascending=[False, True]).reset_index(drop=True)


def _build_period_summary(
    *,
    code: str,
    label: str,
    months: int,
    as_of_date: date,
    portfolio_daily_metrics: pd.DataFrame,
    position_metrics: pd.DataFrame,
    transactions: pd.DataFrame,
    cash_movements: pd.DataFrame,
) -> MonthlyPeriodSummary:
    requested_start_date = (pd.Timestamp(as_of_date) - pd.DateOffset(months=months)).date()
    available_start_date = min(portfolio_daily_metrics["valuation_date"])
    effective_start_date = max(requested_start_date, available_start_date)
    start_row = _select_first_daily_row_on_or_after(portfolio_daily_metrics, effective_start_date)
    end_row = _select_daily_row(portfolio_daily_metrics, as_of_date)

    period_transactions = transactions.loc[
        (transactions["trade_date"] >= effective_start_date) & (transactions["trade_date"] <= as_of_date)
    ].copy()
    buy_transactions = period_transactions.loc[period_transactions["transaction_type"] == "BUY"]
    sell_transactions = period_transactions.loc[period_transactions["transaction_type"] == "SELL"]

    period_cash_movements = cash_movements.loc[
        (cash_movements["effective_date"] >= effective_start_date) & (cash_movements["effective_date"] <= as_of_date)
    ].copy()
    dividends = period_cash_movements.loc[period_cash_movements["movement_type"].isin(DIVIDEND_MOVEMENT_TYPES)]
    contribution_in = period_cash_movements.loc[
        period_cash_movements["movement_type"].isin(CONTRIBUTION_IN_MOVEMENT_TYPES)
    ]
    contribution_out = period_cash_movements.loc[
        period_cash_movements["movement_type"].isin(CONTRIBUTION_OUT_MOVEMENT_TYPES)
    ]

    notes: list[str] = []
    if effective_start_date > requested_start_date:
        notes.append(
            "Cobertura historica incompleta para la ventana solicitada. "
            f"Se usa el primer dato disponible ({effective_start_date.isoformat()})."
        )
    dividend_missing_base_count = int(dividends["amount_base"].isna().sum())
    if dividend_missing_base_count > 0:
        notes.append(
            "Hay dividendos sin amount_base disponible. La suma mostrada solo cubre los importes convertidos."
        )

    notable_changes = _build_notable_changes_table(
        start_positions=_select_positions_for_date(position_metrics, effective_start_date),
        end_positions=_select_positions_for_date(position_metrics, as_of_date),
    )
    value_start = _safe_float(start_row["total_market_value_base"])
    value_end = _safe_float(end_row["total_market_value_base"])
    value_change = round(value_end - value_start, 8)
    value_change_pct = None
    if abs(value_start) > 1e-12:
        value_change_pct = round(value_end / value_start - 1, 8)

    return MonthlyPeriodSummary(
        code=code,
        label=label,
        months=months,
        requested_start_date=requested_start_date,
        effective_start_date=effective_start_date,
        end_date=as_of_date,
        available_coverage_days=(as_of_date - effective_start_date).days + 1,
        total_market_value_start_base=value_start,
        total_market_value_end_base=value_end,
        total_market_value_change_base=value_change,
        total_market_value_change_pct=value_change_pct,
        portfolio_return_pct_end=_safe_optional_float(end_row.get("portfolio_return_pct")),
        drawdown_pct_end=_safe_optional_float(end_row.get("drawdown_pct")),
        valuation_coverage_ratio_end=_safe_float(end_row["valuation_coverage_ratio"]),
        buy_count=len(buy_transactions),
        buy_amount_base=round(
            float(
                (
                    buy_transactions["gross_amount_base"]
                    + buy_transactions["fees_amount_base"]
                    + buy_transactions["taxes_amount_base"]
                ).sum()
            ),
            8,
        ),
        sell_count=len(sell_transactions),
        sell_amount_base=round(float(sell_transactions["gross_amount_base"].sum()), 8),
        dividend_count=len(dividends),
        dividend_amount_base=_sum_or_none(dividends["amount_base"]),
        dividend_missing_base_count=dividend_missing_base_count,
        contribution_in_count=len(contribution_in),
        contribution_in_base=_sum_or_none(contribution_in["amount_base"]),
        contribution_out_count=len(contribution_out),
        contribution_out_base=_sum_or_none(contribution_out["amount_base"]),
        notable_changes=notable_changes,
        notes=tuple(notes),
    )


def _build_notable_changes_table(
    *,
    start_positions: pd.DataFrame,
    end_positions: pd.DataFrame,
    limit: int = 5,
) -> pd.DataFrame:
    start_ready = start_positions.loc[
        :,
        ["asset_id", "asset_name", "quantity", "weight", "market_value_base"],
    ].rename(
        columns={
            "asset_name": "start_asset_name",
            "quantity": "start_quantity",
            "weight": "start_weight",
            "market_value_base": "start_market_value_base",
        }
    )
    end_ready = end_positions.loc[
        :,
        ["asset_id", "asset_name", "quantity", "weight", "market_value_base"],
    ].rename(
        columns={
            "asset_name": "end_asset_name",
            "quantity": "end_quantity",
            "weight": "end_weight",
            "market_value_base": "end_market_value_base",
        }
    )
    merged = start_ready.merge(end_ready, on="asset_id", how="outer")
    merged["asset_name"] = (
        merged["end_asset_name"].fillna(merged["start_asset_name"]).fillna(merged["asset_id"]).astype("string")
    )
    for column in [
        "start_quantity",
        "end_quantity",
        "start_weight",
        "end_weight",
        "start_market_value_base",
        "end_market_value_base",
    ]:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0.0)

    merged["delta_quantity"] = (merged["end_quantity"] - merged["start_quantity"]).round(8)
    merged["delta_weight_pp"] = ((merged["end_weight"] - merged["start_weight"]) * 100).round(4)
    merged["delta_market_value_base"] = (
        merged["end_market_value_base"] - merged["start_market_value_base"]
    ).round(8)
    merged["_abs_weight_change"] = merged["delta_weight_pp"].abs()
    merged["_abs_value_change"] = merged["delta_market_value_base"].abs()

    ranked = merged.sort_values(
        ["_abs_weight_change", "_abs_value_change", "asset_name"],
        ascending=[False, False, True],
    )
    selected = ranked.head(limit).copy()
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "asset_name",
                "start_weight",
                "end_weight",
                "delta_weight_pp",
                "start_market_value_base",
                "end_market_value_base",
                "delta_market_value_base",
                "delta_quantity",
            ]
        )
    return selected.loc[
        :,
        [
            "asset_name",
            "start_weight",
            "end_weight",
            "delta_weight_pp",
            "start_market_value_base",
            "end_market_value_base",
            "delta_market_value_base",
            "delta_quantity",
        ],
    ].reset_index(drop=True)


def _build_current_allocation_table(
    *,
    positions_as_of: pd.DataFrame,
    one_month_period: MonthlyPeriodSummary,
    position_metrics: pd.DataFrame,
) -> pd.DataFrame:
    one_month_positions = _select_positions_for_date(position_metrics, one_month_period.effective_start_date)
    reference = one_month_positions.loc[:, ["asset_id", "weight"]].rename(columns={"weight": "weight_1m"})
    current = positions_as_of.merge(reference, on="asset_id", how="left")
    current["weight_1m"] = pd.to_numeric(current["weight_1m"], errors="coerce").fillna(0.0)
    current["delta_1m_pp"] = ((current["weight"] - current["weight_1m"]) * 100).round(4)
    ready = current.loc[
        :,
        [
            "asset_name",
            "quantity",
            "weight",
            "delta_1m_pp",
            "market_value_base",
            "cost_basis_base",
            "unrealized_pnl_base",
            "unrealized_return_pct",
            "valuation_status",
        ],
    ].copy()
    return ready.sort_values(["market_value_base", "asset_name"], ascending=[False, True]).reset_index(drop=True)


def _build_report_notes(
    *,
    as_of_date: date,
    positions_as_of: pd.DataFrame,
    daily_as_of: pd.Series,
    period_summaries: tuple[MonthlyPeriodSummary, ...],
    cash_movements: pd.DataFrame,
) -> tuple[str, ...]:
    notes: list[str] = [
        "Las valoraciones usan el ultimo precio disponible en o antes de cada fecha, segun la logica actual de metrics.",
        f"El informe toma como fecha de referencia el snapshot/estado disponible para {as_of_date.isoformat()}.",
    ]
    missing_positions = positions_as_of.loc[
        ~positions_as_of["valuation_status"].isin(["valued", "valued_cash"]),
        "asset_name",
    ].astype("string")
    missing_names = [name for name in missing_positions.tolist() if name]
    if missing_names:
        notes.append(
            "Hay posiciones con cobertura incompleta de precio o FX: " + ", ".join(sorted(set(missing_names))) + "."
        )
    if int(_safe_float(daily_as_of["missing_price_positions_count"])) > 0:
        notes.append(
            f"Faltan precios para {int(_safe_float(daily_as_of['missing_price_positions_count']))} posiciones en la fecha de cierre."
        )
    if int(_safe_float(daily_as_of["missing_fx_positions_count"])) > 0:
        notes.append(
            f"Faltan FX para {int(_safe_float(daily_as_of['missing_fx_positions_count']))} posiciones en la fecha de cierre."
        )
    truncated_periods = [summary.label for summary in period_summaries if summary.effective_start_date > summary.requested_start_date]
    if truncated_periods:
        notes.append("No hay historico completo para: " + ", ".join(truncated_periods) + ".")
    if not cash_movements.empty and "amount_base" in cash_movements.columns:
        missing_amount_base = int(cash_movements["amount_base"].isna().sum())
        if missing_amount_base > 0:
            notes.append(
                f"Hay {missing_amount_base} movimientos de efectivo sin amount_base; los totales del informe usan solo importes convertidos."
            )
    return tuple(notes)


def get_latest_monthly_report(
    *,
    settings: Settings | None = None,
) -> ReportHistoryEntry | None:
    """Return the latest persisted monthly report metadata."""
    repository = DuckDBReportHistoryRepository(settings=settings)
    return repository.get_latest_report(report_type="monthly")


def _persist_monthly_report_history(
    *,
    report_id: str,
    output_path: Path,
    report: MonthlyReportResult,
    settings: Settings,
    content: str,
    normalized_degiro_dir: str | Path | None,
) -> ReportHistoryEntry:
    repository = DuckDBReportHistoryRepository(settings=settings)
    report_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    earliest_period_start = min(summary.effective_start_date for summary in report.period_summaries)
    parameters = _build_history_parameters(
        report=report,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    notes = "; ".join(report.notes) if report.notes else None
    return repository.insert_report_entry(
        report_id=report_id,
        report_type="monthly",
        report_period_start=earliest_period_start,
        report_period_end=report.as_of_date,
        as_of_date=report.as_of_date,
        generated_at=report.generated_at,
        report_path=str(output_path),
        report_format="md",
        status="generated",
        base_currency=report.base_currency,
        source_snapshot_date=report.as_of_date,
        parameters=parameters,
        report_hash=report_hash,
        notes=notes,
    )


def _build_history_parameters(
    *,
    report: MonthlyReportResult,
    normalized_degiro_dir: str | Path | None,
) -> dict[str, object]:
    return {
        "sources": {
            "normalized_degiro_dir": (
                str(Path(normalized_degiro_dir).expanduser().resolve())
                if normalized_degiro_dir is not None
                else None
            ),
            "datasets": [
                "portfolio_daily_metrics",
                "position_metrics",
                "normalized_degiro.transactions",
                "normalized_degiro.cash_movements",
            ],
        },
        "periods": [
            {
                "code": summary.code,
                "label": summary.label,
                "requested_start_date": summary.requested_start_date.isoformat(),
                "effective_start_date": summary.effective_start_date.isoformat(),
                "end_date": summary.end_date.isoformat(),
            }
            for summary in report.period_summaries
        ],
    }


def _build_report_id(
    *,
    report_type: str,
    as_of_date: date,
    generated_at: datetime,
) -> str:
    normalized_type = _slugify(report_type)
    timestamp = generated_at.strftime("%Y%m%dT%H%M%S%f")
    return f"{as_of_date.isoformat()}-{normalized_type}-{timestamp}"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-") or "report"


def _resolve_generated_at(default_timezone: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(default_timezone))
    except ZoneInfoNotFoundError:
        return datetime.now()


def _render_table(*, headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        rows = [["-"] * len(headers)]
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"
    body_rows = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_row, separator_row, *body_rows])


def _format_currency(value: float | None, currency: str) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f} {currency}"


def _format_signed_currency(value: float | None, currency: str) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):+,.2f} {currency}"


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def _format_number(value: float | None, *, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.{decimals}f}"


def _format_signed_number(value: float | None, *, suffix: str = "", decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):+,.{decimals}f}{suffix}"


def _safe_float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _safe_optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _sum_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return 0.0
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return round(float(values.sum()), 8)
