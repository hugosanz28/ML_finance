from datetime import date

from src.config import load_settings
from src.market_data import DuckDBMarketDataRepository
from src.portfolio.contributions import net_external_contributions_until


def test_net_external_contributions_respects_type_sign_and_cutoff(tmp_path) -> None:
    settings = load_settings(repo_root=tmp_path, env={}, env_file=tmp_path / ".env.missing")
    repository = DuckDBMarketDataRepository(settings=settings)
    with repository.connection() as connection:
        connection.execute(
            """
            INSERT INTO cash_movements (
                cash_movement_id,
                movement_date,
                value_date,
                movement_type,
                amount,
                movement_currency,
                base_currency,
                fx_rate_to_base,
                amount_base
            )
            VALUES
                ('deposit', DATE '2026-01-01', DATE '2026-01-02', 'DEPOSIT', 100, 'EUR', 'EUR', 1, 100),
                ('withdrawal', DATE '2026-01-03', NULL, 'WITHDRAWAL', -30, 'EUR', 'EUR', 1, -30),
                ('future', DATE '2026-02-01', NULL, 'DEPOSIT', 50, 'EUR', 'EUR', 1, 50),
                ('dividend', DATE '2026-01-03', NULL, 'DIVIDEND', 5, 'EUR', 'EUR', 1, 5)
            """
        )

    assert net_external_contributions_until(settings, as_of_date=date(2026, 1, 31)) == 70.0
