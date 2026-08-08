from datetime import date

import pandas as pd
import pytest

from src.portfolio import (
    ExternalCashFlow,
    PortfolioValuation,
    calculate_daily_returns,
    calculate_money_weighted_return,
    calculate_portfolio_performance,
    calculate_time_weighted_return,
    classify_external_cash_flows,
)


def test_classify_external_cash_flows_excludes_internal_activity_and_normalizes_signs() -> None:
    cash_movements = pd.DataFrame(
        [
            _cash_movement("deposit", "DEPOSIT", 1_000, movement_date="2026-01-01", value_date="2026-01-02"),
            _cash_movement("withdrawal", "WITHDRAWAL", 200, movement_date="2026-01-03"),
            _cash_movement("dividend", "DIVIDEND", 25, movement_date="2026-01-04"),
            _cash_movement("fee", "TRANSACTION_FEE", -3, movement_date="2026-01-04"),
        ]
    )

    result = classify_external_cash_flows(cash_movements, base_currency="EUR")

    assert [(flow.flow_date, flow.amount_base) for flow in result.cash_flows] == [
        (date(2026, 1, 2), 1_000.0),
        (date(2026, 1, 3), -200.0),
    ]
    assert result.ignored_internal_count == 2
    assert result.issues == ()


def test_classify_external_cash_flows_reports_ambiguous_or_unusable_movements() -> None:
    cash_movements = pd.DataFrame(
        [
            _cash_movement("other", "OTHER", 10, movement_date="2026-01-01"),
            _cash_movement("missing", "DEPOSIT", None, movement_date="2026-01-02"),
            _cash_movement("usd", "DEPOSIT", 20, movement_date="2026-01-03", base_currency="USD"),
        ]
    )

    result = classify_external_cash_flows(cash_movements, base_currency="EUR")

    assert result.cash_flows == ()
    assert {issue.code for issue in result.issues} == {
        "ambiguous_cash_movement",
        "external_flow_amount_base_missing",
        "external_flow_currency_mismatch",
    }


def test_twr_removes_external_flows_before_chaining_subperiod_returns() -> None:
    valuations = (
        PortfolioValuation(date(2026, 1, 1), 1_000),
        PortfolioValuation(date(2026, 1, 2), 1_150),
        PortfolioValuation(date(2026, 1, 3), 1_165),
    )
    cash_flows = (
        ExternalCashFlow(date(2026, 1, 2), 100, "DEPOSIT"),
        ExternalCashFlow(date(2026, 1, 3), -100, "WITHDRAWAL"),
    )

    result = calculate_time_weighted_return(valuations, cash_flows)

    assert result.status == "available"
    assert result.reason_code == "ok"
    assert result.value == pytest.approx((1.05 * 1.10) - 1)
    assert result.observations == 2


def test_daily_returns_expose_flow_adjustment_and_coverage() -> None:
    valuations = (
        PortfolioValuation(date(2026, 1, 1), 1_000),
        PortfolioValuation(date(2026, 1, 2), 1_150, coverage_ratio=0.8),
    )

    result = calculate_daily_returns(
        valuations,
        (ExternalCashFlow(date(2026, 1, 2), 100, "DEPOSIT"),),
    )

    assert len(result) == 1
    assert result[0].net_external_flow_base == 100
    assert result[0].return_decimal == pytest.approx(0.05)
    assert result[0].coverage_ratio == 0.8
    assert result[0].status == "partial"
    assert result[0].reason_code == "partial_valuation_coverage"


def test_large_contribution_does_not_create_a_fictitious_twr_gain() -> None:
    valuations = (
        PortfolioValuation(date(2026, 1, 1), 1_000),
        PortfolioValuation(date(2026, 1, 2), 11_000),
        PortfolioValuation(date(2026, 1, 3), 11_000),
    )
    cash_flows = (ExternalCashFlow(date(2026, 1, 2), 10_000, "DEPOSIT"),)

    result = calculate_time_weighted_return(valuations, cash_flows)

    assert result.value == pytest.approx(0.0)


def test_mwr_xirr_matches_a_known_one_year_return() -> None:
    valuations = (
        PortfolioValuation(date(2025, 1, 1), 1_000),
        PortfolioValuation(date(2026, 1, 1), 1_100),
    )

    result = calculate_money_weighted_return(valuations)

    assert result.status == "available"
    assert result.reason_code == "ok"
    assert result.value == pytest.approx(0.10, abs=1e-9)
    assert result.observations == 2


def test_mwr_xirr_reports_multiple_solutions_instead_of_choosing_one() -> None:
    valuations = (
        PortfolioValuation(date(2024, 1, 1), 100),
        PortfolioValuation(date(2026, 1, 1), 1),
    )
    cash_flows = (
        ExternalCashFlow(date(2025, 1, 1), -230, "WITHDRAWAL"),
        ExternalCashFlow(date(2026, 1, 1), 132, "DEPOSIT"),
    )

    result = calculate_money_weighted_return(valuations, cash_flows)

    assert result.value is None
    assert result.status == "unavailable"
    assert result.reason_code == "xirr_multiple_solutions"


def test_mwr_xirr_reports_no_solution_instead_of_returning_zero() -> None:
    valuations = (
        PortfolioValuation(date(2024, 1, 1), 100),
        PortfolioValuation(date(2026, 1, 1), 1),
    )
    cash_flows = (
        ExternalCashFlow(date(2025, 1, 1), -50, "WITHDRAWAL"),
        ExternalCashFlow(date(2026, 1, 1), 11, "DEPOSIT"),
    )

    result = calculate_money_weighted_return(valuations, cash_flows)

    assert result.value is None
    assert result.status == "unavailable"
    assert result.reason_code == "xirr_no_solution"


def test_unavailable_performance_uses_none_and_reason_code_instead_of_zero() -> None:
    valuations = (PortfolioValuation(date(2026, 1, 1), 1_000),)

    twr = calculate_time_weighted_return(valuations)
    mwr = calculate_money_weighted_return(valuations)

    assert twr.value is None
    assert twr.status == "unavailable"
    assert twr.reason_code == "insufficient_valuations"
    assert mwr.value is None
    assert mwr.reason_code == "insufficient_valuations"


def test_portfolio_performance_builds_standard_horizons_and_preserves_actual_dates() -> None:
    daily_metrics = pd.DataFrame(
        [
            _valuation("2025-01-01", 1_000),
            _valuation("2025-08-01", 1_100),
            _valuation("2026-05-01", 1_200),
            _valuation("2026-07-01", 1_300),
            _valuation("2026-08-01", 1_400),
        ]
    )

    result = calculate_portfolio_performance(
        daily_metrics,
        _empty_cash_movements(),
        base_currency="eur",
        as_of_date=date(2026, 8, 1),
    )

    assert result.base_currency == "EUR"
    assert len(result.daily_returns) == 4
    assert [period.period_id for period in result.periods] == [
        "last_month",
        "last_quarter",
        "last_year",
        "since_inception",
    ]
    assert [period.actual_start for period in result.periods] == [
        date(2026, 7, 1),
        date(2026, 5, 1),
        date(2025, 8, 1),
        date(2025, 1, 1),
    ]
    assert all(period.twr.status == "available" for period in result.periods)
    assert result.periods[0].twr.value == pytest.approx((1_400 / 1_300) - 1)


def test_partial_coverage_and_ambiguous_cash_flow_are_explicit() -> None:
    daily_metrics = pd.DataFrame(
        [
            _valuation("2026-01-01", 1_000, coverage=1.0),
            _valuation("2026-02-01", 1_050, coverage=0.75),
        ]
    )
    cash_movements = pd.DataFrame(
        [_cash_movement("ambiguous", "OTHER", 50, movement_date="2026-01-15")]
    )

    result = calculate_portfolio_performance(
        daily_metrics,
        cash_movements,
        base_currency="EUR",
        as_of_date=date(2026, 2, 1),
    )
    period = result.periods[0]

    assert period.twr.value == pytest.approx(0.05)
    assert period.twr.status == "partial"
    assert period.twr.reason_code == "partial_valuation_coverage"
    assert period.reason_codes == (
        "partial_valuation_coverage",
        "cash_flow_classification_incomplete",
    )
    assert [issue.code for issue in result.cash_flow_issues] == ["ambiguous_cash_movement"]


def test_portfolio_performance_requires_an_exact_as_of_valuation() -> None:
    daily_metrics = pd.DataFrame([_valuation("2026-01-01", 1_000), _valuation("2026-01-02", 1_010)])

    with pytest.raises(ValueError, match="No portfolio valuation is available"):
        calculate_portfolio_performance(
            daily_metrics,
            _empty_cash_movements(),
            base_currency="EUR",
            as_of_date=date(2026, 1, 3),
        )


def test_future_cash_flow_issue_does_not_reduce_current_period_status() -> None:
    daily_metrics = pd.DataFrame(
        [_valuation("2026-01-01", 1_000), _valuation("2026-02-01", 1_050)]
    )
    cash_movements = pd.DataFrame(
        [_cash_movement("future", "OTHER", 50, movement_date="2026-03-01")]
    )

    result = calculate_portfolio_performance(
        daily_metrics,
        cash_movements,
        base_currency="EUR",
        as_of_date=date(2026, 2, 1),
    )

    assert result.cash_flow_issues == ()
    assert result.periods[0].twr.status == "available"


def test_portfolio_performance_rejects_invalid_base_currency() -> None:
    daily_metrics = pd.DataFrame(
        [_valuation("2026-01-01", 1_000), _valuation("2026-02-01", 1_050)]
    )

    with pytest.raises(ValueError, match="three-letter ISO currency code"):
        calculate_portfolio_performance(
            daily_metrics,
            _empty_cash_movements(),
            base_currency="",
        )


def _valuation(valuation_date: str, value: float, *, coverage: float = 1.0) -> dict[str, object]:
    return {
        "valuation_date": valuation_date,
        "total_market_value_base": value,
        "valuation_coverage_ratio": coverage,
    }


def _cash_movement(
    movement_id: str,
    movement_type: str,
    amount_base: float | None,
    *,
    movement_date: str,
    value_date: str | None = None,
    base_currency: str = "EUR",
) -> dict[str, object]:
    return {
        "cash_movement_id": movement_id,
        "movement_date": movement_date,
        "value_date": value_date,
        "movement_type": movement_type,
        "amount_base": amount_base,
        "base_currency": base_currency,
        "source_file": "account.csv",
        "source_row": 1,
    }


def _empty_cash_movements() -> pd.DataFrame:
    return pd.DataFrame(columns=["movement_date", "movement_type", "amount_base"])
