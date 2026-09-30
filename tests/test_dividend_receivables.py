from datetime import date

import pandas as pd
import pytest

from src.portfolio.cash_history import reconcile_cash_history
from src.portfolio.dividend_receivables import reconcile_dividend_receivables
from src.portfolio.metrics import calculate_portfolio_metrics
from src.portfolio.performance import calculate_portfolio_performance


def history():
    positions = pd.DataFrame([
        {"position_date": day, "asset_id": "rights", "asset_type": "rights", "quantity": 10}
        for day in pd.date_range("2026-01-01", "2026-01-04")
    ] + [
        {"position_date": day, "asset_id": "stock", "asset_type": "stock", "quantity": 1}
        for day in pd.date_range("2026-01-01", "2026-01-07")
    ])
    # Conversion value dates point to issuance; the actual election was booked later.
    cash = pd.DataFrame([
        dict(movement_date="2026-01-01", movement_type="DEPOSIT", amount=120, running_balance=120),
        dict(movement_date="2026-01-01", movement_type="TRADE_SETTLEMENT_BUY", amount=-100, running_balance=20),
        dict(movement_date="2026-01-03", value_date="2026-01-01", movement_type="CORPORATE_ACTION_SCRIP_DIVIDEND",
             description="Venta 10 Fixture rights@0 EUR", asset_id="rights", amount=0, running_balance=20),
        dict(movement_date="2026-01-03", value_date="2026-01-01", movement_type="CORPORATE_ACTION_SCRIP_DIVIDEND",
             description="Compra 10 Fixture - Non tradeable@0 EUR", asset_id="rights", amount=0, running_balance=20),
        dict(movement_date="2026-01-06", value_date="2026-01-05", movement_type="CORPORATE_ACTION_DELISTING",
             description="Venta 10 Fixture - Non tradeable@0 EUR", asset_id="rights", amount=0, running_balance=20),
        dict(movement_date="2026-01-06", value_date="2026-01-05", movement_type="DIVIDEND",
             asset_id="rights", amount=5, running_balance=25),
        dict(movement_date="2026-01-06", value_date="2026-01-05", movement_type="DIVIDEND_WITHHOLDING_TAX",
             asset_id="rights", amount=-1, running_balance=24),
    ]).assign(movement_currency="EUR", amount_base=lambda frame: frame.amount, base_currency="EUR")
    return positions, cash


def test_receivable_begins_on_conversion_and_survives_until_cash_booking():
    positions, cash = history()
    cash_positions = reconcile_cash_history(positions, cash, pd.DataFrame())
    rebuilt = reconcile_dividend_receivables(cash_positions, cash)
    rights = rebuilt.loc[rebuilt.asset_id == "rights"].sort_values("position_date")
    assert rights.asset_type.tolist() == ["rights", "rights", *["dividend_receivable"] * 3]
    assert rights.position_date.max() == pd.Timestamp("2026-01-05")
    assert rights.receivable_amount.dropna().tolist() == [5, 5, 5]
    prices = pd.DataFrame([dict(asset_id="stock", price_date="2026-01-01", price_currency="EUR", close_price=100)])
    metrics = calculate_portfolio_metrics(rebuilt, prices)
    rights_values = metrics.position_metrics.loc[metrics.position_metrics.asset_id == "rights"].sort_values("valuation_date")
    assert rights_values.market_value_base.iloc[:2].isna().all()  # No invented trading prices.
    assert rights_values.market_value_base.iloc[2:].tolist() == [5, 5, 5]
    assert set(rights_values.valuation_status.iloc[2:]) == {"valued_dividend_receivable"}
    assert rights_values.close_price.isna().all()  # A receivable is not a quote.
    daily = metrics.portfolio_daily_metrics.set_index("valuation_date")
    assert daily.loc["2026-01-05", "total_market_value_base"] == 125
    assert daily.loc["2026-01-06", "total_market_value_base"] == 124
    performance = calculate_portfolio_performance(metrics.portfolio_daily_metrics, cash, base_currency="EUR", as_of_date=date(2026, 1, 7))
    payment_return = next(row for row in performance.daily_returns if row.valuation_date == date(2026, 1, 6))
    assert payment_return.return_decimal == pytest.approx(-1 / 125)  # Only withholding, no duplicate dividend.


@pytest.mark.parametrize("case", ["unpaid", "other_asset", "partial_conversion", "no_counterpart", "conflicting_payment", "future_payment", "changed_quantity"])
def test_unconfirmed_or_ambiguous_claims_are_not_invented(case):
    positions, cash = history()
    if case == "unpaid":
        cash = cash.loc[cash.movement_type != "DIVIDEND"]
    elif case == "other_asset":
        cash.loc[cash.movement_type == "DIVIDEND", "asset_id"] = "unrelated"
    elif case == "partial_conversion":
        cash.loc[3, "description"] = "Compra 5 Fixture - Non tradeable@0 EUR"
    elif case == "no_counterpart":
        cash = cash.drop(index=2)
    elif case == "conflicting_payment":
        cash = pd.concat([cash, cash.loc[[5]].assign(amount=6)], ignore_index=True)
    elif case == "future_payment":
        positions = positions.loc[positions.position_date <= pd.Timestamp("2026-01-04")]
    elif case == "changed_quantity":
        positions.loc[(positions.asset_id == "rights") & (positions.position_date == pd.Timestamp("2026-01-04")), "quantity"] = 5
    rebuilt = reconcile_dividend_receivables(positions, cash)
    assert not rebuilt.asset_type.eq("dividend_receivable").any()


def test_foreign_receivable_needs_valuation_day_fx():
    positions, cash = history()
    cash["movement_currency"] = "USD"
    rebuilt = reconcile_dividend_receivables(positions, cash)
    fx = pd.DataFrame([dict(base_currency="EUR", quote_currency="USD", rate_date="2026-01-03", rate=2)])
    metrics = calculate_portfolio_metrics(rebuilt, pd.DataFrame(), fx_rates=fx)
    claims = metrics.position_metrics.loc[metrics.position_metrics.asset_type == "dividend_receivable"]
    assert claims.market_value_base.tolist() == [2.5, 2.5, 2.5]
    without_fx = calculate_portfolio_metrics(rebuilt, pd.DataFrame())
    assert without_fx.position_metrics.market_value_base.isna().all()
