"""Portfolio contribution queries shared by application read models."""

from __future__ import annotations

from datetime import date

from src.config import Settings
from src.market_data import DuckDBMarketDataRepository


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


__all__ = ["net_external_contributions_until"]
