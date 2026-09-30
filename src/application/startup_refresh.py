"""Schedule authorized startup downloads through the existing single writer."""

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from src.application.local_jobs import LocalJobManager, SubmitJobRequest
from src.application.dashboard import LoadPortfolioTransactionsUseCase, LoadPortfolioSnapshotsUseCase


@dataclass(frozen=True)
class ScheduleStartupRefreshRequest:
    today: date | None = None


class ScheduleStartupRefreshUseCase:
    def __init__(self, manager: LocalJobManager):
        self.manager = manager

    def execute(self, request: ScheduleStartupRefreshRequest | None = None) -> list[dict]:
        if self.manager.workspace.mode != "real":
            raise ValueError("Startup network refresh requires real operations")
        today = (request or ScheduleStartupRefreshRequest()).today or date.today()
        settings = self.manager.workspace.settings
        dates = []
        with self.manager.reading():
            transactions = LoadPortfolioTransactionsUseCase(settings=settings).execute().transactions
            snapshots = LoadPortfolioSnapshotsUseCase(settings=settings).execute().snapshots
            for frame, column in ((transactions, "trade_date"), (snapshots, "snapshot_date")):
                if not frame.empty:
                    dates.append(pd.to_datetime(frame[column]).min().date())
        if not dates or min(dates) >= today:
            return []
        start = min(dates) - timedelta(days=7)
        end = today - timedelta(days=1)
        common = dict(workspace_mode="real", confirm=True, start_date=start.isoformat(), end_date=end.isoformat())
        # One attempt per calendar day. Restarting never retries failed/interrupted downloads.
        return [self.manager.submit(SubmitJobRequest(operation, parameters, f"startup-{operation}-{today.isoformat()}-{start.isoformat()}"))
                for operation, parameters in (
                    ("refresh", dict(common, fx_provider="yfinance", price_provider="yfinance", scope="both", include_classifications=True)),
                    ("benchmarks", dict(common, provider="yfinance_ecb")),
                )]
