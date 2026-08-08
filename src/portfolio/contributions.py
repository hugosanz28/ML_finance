"""Portfolio contribution queries and external-flow classification."""

from __future__ import annotations

from datetime import date
from math import isfinite
from typing import Any

import pandas as pd

from src.config import Settings
from src.market_data import DuckDBMarketDataRepository
from src.portfolio.performance_models import (
    CashFlowClassificationIssue,
    CashFlowClassificationResult,
    ExternalCashFlow,
)


EXTERNAL_MOVEMENT_TYPES = frozenset({"DEPOSIT", "WITHDRAWAL"})
INTERNAL_MOVEMENT_TYPES = frozenset(
    {
        "CASH_ACCOUNT_TRANSFER_IN",
        "CASH_ACCOUNT_TRANSFER_OUT",
        "CASH_SWEEP_TRANSFER",
        "CONNECTIVITY_FEE",
        "CORPORATE_ACTION_DELISTING",
        "CORPORATE_ACTION_RIGHTS_ISSUE",
        "CORPORATE_ACTION_SCRIP_DIVIDEND",
        "DIVIDEND",
        "DIVIDEND_WITHHOLDING_TAX",
        "FX_CONVERSION_IN",
        "FX_CONVERSION_OUT",
        "INTEREST",
        "REBATE",
        "TRADE_SETTLEMENT_BUY",
        "TRADE_SETTLEMENT_SELL",
        "TRANSACTION_FEE",
        "TRANSACTION_TAX",
    }
)


def net_external_contributions_until(settings: Settings, *, as_of_date: date) -> float | None:
    """Return deposits minus withdrawals up to and including a valuation date."""
    repository = DuckDBMarketDataRepository(settings=settings)
    query = """
        SELECT SUM(
            CASE
                WHEN UPPER(movement_type) = 'DEPOSIT' THEN ABS(amount_base)
                WHEN UPPER(movement_type) = 'WITHDRAWAL' THEN -ABS(amount_base)
                ELSE 0
            END
        ) AS net_external
        FROM cash_movements
        WHERE amount_base IS NOT NULL
          AND UPPER(movement_type) IN ('DEPOSIT', 'WITHDRAWAL')
          AND COALESCE(value_date, movement_date) <= ?
    """
    with repository.connection() as connection:
        row = connection.execute(query, [as_of_date]).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def classify_external_cash_flows(
    cash_movements: pd.DataFrame,
    *,
    base_currency: str,
) -> CashFlowClassificationResult:
    """Extract deposits and withdrawals without treating internal cash activity as capital."""
    required_columns = {"movement_date", "movement_type", "amount_base"}
    missing_columns = sorted(required_columns - set(cash_movements.columns))
    if missing_columns:
        raise ValueError(f"cash_movements missing required columns: {', '.join(missing_columns)}")

    normalized_currency = base_currency.strip().upper()
    if len(normalized_currency) != 3 or not normalized_currency.isascii() or not normalized_currency.isalpha():
        raise ValueError("base_currency must be a three-letter ISO currency code")
    cash_flows: list[ExternalCashFlow] = []
    issues: list[CashFlowClassificationIssue] = []
    ignored_internal_count = 0
    seen_ids: set[str] = set()

    for raw_row in cash_movements.to_dict(orient="records"):
        movement_type = str(raw_row.get("movement_type") or "").strip().upper()
        source_file = _optional_text(raw_row.get("source_file"))
        source_row = _optional_int(raw_row.get("source_row"))
        movement_id = _optional_text(raw_row.get("cash_movement_id"))
        flow_date = _effective_movement_date(raw_row)

        if movement_id and movement_id in seen_ids:
            issues.append(
                CashFlowClassificationIssue(
                    code="duplicate_cash_movement",
                    message=f"Duplicate cash movement excluded: {movement_id}.",
                    flow_date=flow_date,
                    source_file=source_file,
                    source_row=source_row,
                )
            )
            continue
        if movement_id:
            seen_ids.add(movement_id)

        if movement_type in INTERNAL_MOVEMENT_TYPES:
            ignored_internal_count += 1
            continue
        if movement_type not in EXTERNAL_MOVEMENT_TYPES:
            issues.append(
                CashFlowClassificationIssue(
                    code="ambiguous_cash_movement",
                    message=f"Cash movement type is not classified as external or internal: {movement_type or '<blank>'}.",
                    flow_date=flow_date,
                    source_file=source_file,
                    source_row=source_row,
                )
            )
            continue

        row_currency = _optional_text(raw_row.get("base_currency"))
        if row_currency is None or row_currency.upper() != normalized_currency:
            issues.append(
                CashFlowClassificationIssue(
                    code="external_flow_currency_mismatch",
                    message=(
                        "External cash flow does not declare the requested base currency: "
                        f"flow={row_currency or '<missing>'} requested={normalized_currency}."
                    ),
                    flow_date=flow_date,
                    source_file=source_file,
                    source_row=source_row,
                )
            )
            continue

        if flow_date is None:
            issues.append(
                CashFlowClassificationIssue(
                    code="external_flow_date_missing",
                    message="External cash flow does not contain a valid value_date or movement_date.",
                    source_file=source_file,
                    source_row=source_row,
                )
            )
            continue

        amount = _optional_float(raw_row.get("amount_base"))
        if amount is None:
            issues.append(
                CashFlowClassificationIssue(
                    code="external_flow_amount_base_missing",
                    message="External cash flow does not contain a finite amount_base.",
                    source_file=source_file,
                    source_row=source_row,
                )
            )
            continue

        signed_amount = abs(amount) if movement_type == "DEPOSIT" else -abs(amount)
        cash_flows.append(
            ExternalCashFlow(
                flow_date=flow_date,
                amount_base=signed_amount,
                movement_type=movement_type,
                source_file=source_file,
                source_row=source_row,
            )
        )

    return CashFlowClassificationResult(
        cash_flows=tuple(sorted(cash_flows, key=lambda flow: (flow.flow_date, flow.source_row or 0))),
        ignored_internal_count=ignored_internal_count,
        issues=tuple(issues),
    )


def _effective_movement_date(row: dict[str, Any]) -> date | None:
    for key in ("value_date", "movement_date"):
        value = row.get(key)
        if value is None or pd.isna(value):
            continue
        parsed = pd.to_datetime(value, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()
    return None


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "EXTERNAL_MOVEMENT_TYPES",
    "INTERNAL_MOVEMENT_TYPES",
    "classify_external_cash_flows",
    "net_external_contributions_until",
]
