import json

import pandas as pd
import pytest

from src.portfolio.metrics import calculate_portfolio_metrics
from src.portfolio.metrics_models import PortfolioDataUnavailableError
from src.portfolio.reviewed_rights_prices import apply_reviewed_rights_prices


ISIN = "ES0000000001"


def evidence(tmp_path, **changes):
    item = dict(isin=ISIN, date="2026-01-16", close=0.25, currency="EUR", exchange_mic="XMAD",
                instrument_type="subscription_right", source_url="https://example.org/bulletin.pdf",
                source_note="Synthetic test closing quote")
    item.update(changes)
    path = tmp_path / "reviewed_rights_prices.json"
    path.write_text(json.dumps(dict(schema_version=1, prices=[item])), encoding="utf-8")
    return path


def positions():
    return pd.DataFrame([dict(position_date=day, asset_id=f"degiro:isin:{ISIN}", quantity=10, asset_type="rights")
                         for day in pd.date_range("2026-01-15", "2026-01-20")])


def test_exact_quote_and_weekend_without_filling_missing_sessions(tmp_path):
    rows = apply_reviewed_rights_prices(positions(), evidence(tmp_path))
    result = calculate_portfolio_metrics(rows, pd.DataFrame(), pricing_policy="broker_snapshot_anchored")
    values = result.position_metrics.set_index("valuation_date")
    assert values.loc["2026-01-16":"2026-01-18", "market_value_base"].tolist() == [2.5, 2.5, 2.5]
    assert values.loc[["2026-01-15", "2026-01-19", "2026-01-20"], "market_value_base"].isna().all()
    assert set(values.loc["2026-01-16":"2026-01-18", "valuation_status"]) == {"valued_reviewed_rights"}


def test_does_not_match_other_isin_or_replace_receivable(tmp_path):
    path = evidence(tmp_path)
    other = positions().assign(asset_id="degiro:isin:ES0000000002")
    assert apply_reviewed_rights_prices(other, path).reviewed_rights_close.isna().all()
    claims = positions().assign(asset_type="dividend_receivable")
    assert apply_reviewed_rights_prices(claims, path).reviewed_rights_close.isna().all()


def test_exact_broker_snapshot_takes_precedence(tmp_path):
    rows = apply_reviewed_rights_prices(positions(), evidence(tmp_path))
    snapshots = pd.DataFrame([dict(asset_id=f"degiro:isin:{ISIN}", snapshot_date="2026-01-16",
                                  quantity=10, market_price=.26, market_value=2.6, position_currency="EUR")])
    result = calculate_portfolio_metrics(rows, pd.DataFrame(), snapshots=snapshots, pricing_policy="broker_snapshot_anchored")
    assert result.position_metrics.set_index("valuation_date").loc["2026-01-16", "market_value_base"] == 2.6


def test_requires_fx_for_another_base_currency(tmp_path):
    rows = apply_reviewed_rights_prices(positions(), evidence(tmp_path))
    result = calculate_portfolio_metrics(rows, pd.DataFrame(), base_currency="USD")
    assert result.position_metrics.market_value_base.isna().all()


@pytest.mark.parametrize("changes", [{"close": 0}, {"close": float("nan")}, {"close": True},
    {"date": "2026-01-17"}, {"currency": "USD"}, {"exchange_mic": "XETR"},
    {"source_url": "file:///private"}, {"source_note": ""}, {"instrument_type": "stock"}])
def test_invalid_evidence_is_rejected(tmp_path, changes):
    with pytest.raises(PortfolioDataUnavailableError):
        apply_reviewed_rights_prices(positions(), evidence(tmp_path, **changes))


def test_duplicate_dates_are_rejected(tmp_path):
    path = evidence(tmp_path)
    data = json.loads(path.read_text())
    data["prices"] *= 2
    path.write_text(json.dumps(data))
    with pytest.raises(PortfolioDataUnavailableError):
        apply_reviewed_rights_prices(positions(), path)


def nearest_evidence(tmp_path, **changes):
    path = evidence(tmp_path)
    data = json.loads(path.read_text())
    rule = dict(isin=ISIN, start_date="2026-01-15", end_date="2026-01-19", max_distance_days=3)
    rule.update(changes)
    data["nearest_price_ranges"] = [rule]
    path.write_text(json.dumps(data))
    return path


def test_nearest_quotes_are_scoped_and_keep_source_date(tmp_path):
    rows = apply_reviewed_rights_prices(positions(), nearest_evidence(tmp_path))
    values = calculate_portfolio_metrics(rows, pd.DataFrame()).position_metrics.set_index("valuation_date")
    for day in ("2026-01-15", "2026-01-19"):
        assert values.loc[day, "market_value_base"] == 2.5
        assert values.loc[day, "valuation_status"] == "valued_estimated_rights"
        assert values.loc[day, "pricing_policy"] == "nearest_reviewed_close"
        assert str(values.loc[day, "price_date"])[:10] == "2026-01-16"
    assert pd.isna(values.loc["2026-01-20", "market_value_base"])
    assert values.loc["2026-01-16", "valuation_status"] == "valued_reviewed_rights"


def test_nearest_limit_and_receivables_are_respected(tmp_path):
    path = nearest_evidence(tmp_path, max_distance_days=1)
    rows = apply_reviewed_rights_prices(positions(), path)
    assert pd.isna(rows.iloc[4].reviewed_rights_close)
    assert apply_reviewed_rights_prices(positions().assign(asset_type="dividend_receivable"), path).reviewed_rights_close.isna().all()
    assert apply_reviewed_rights_prices(positions().assign(asset_id="degiro:isin:ES0000000002"), path).reviewed_rights_close.isna().all()


def test_nearest_chooses_earlier_quote_on_tie(tmp_path):
    path = nearest_evidence(tmp_path)
    data = json.loads(path.read_text())
    data["prices"].append({**data["prices"][0], "date": "2026-01-14", "close": .20})
    path.write_text(json.dumps(data))
    rows = apply_reviewed_rights_prices(positions(), path)
    assert rows.iloc[0].reviewed_rights_close == .20


@pytest.mark.parametrize("changes", [{"max_distance_days": True}, {"max_distance_days": 32},
    {"start_date": "2026-01-20"}, {"isin": "ES0000000002"}, {"end_date": "2026-03-01"}])
def test_invalid_nearest_rules_are_rejected(tmp_path, changes):
    with pytest.raises(PortfolioDataUnavailableError):
        apply_reviewed_rights_prices(positions(), nearest_evidence(tmp_path, **changes))
