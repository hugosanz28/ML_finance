from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.application import startup_refresh as startup
from src.application.local_jobs import LocalJobManager
from src.application.operational_workspace import OperationalWorkspace
from src.config import load_settings


def test_startup_jobs_are_deduplicated_and_never_retry_interrupted(tmp_path, monkeypatch):
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / "absent.env", env={"PRICE_PROVIDER": "yfinance"})
    monkeypatch.setattr(startup.LoadPortfolioTransactionsUseCase, "execute",
                        lambda self: SimpleNamespace(transactions=pd.DataFrame({"trade_date": [date(2026, 1, 5)]})))
    monkeypatch.setattr(startup.LoadPortfolioSnapshotsUseCase, "execute",
                        lambda self: SimpleNamespace(snapshots=pd.DataFrame()))
    manager = LocalJobManager(OperationalWorkspace(settings, "real"))
    manager.open(start_worker=False)
    request = startup.ScheduleStartupRefreshRequest(today=date(2026, 2, 1))
    try:
        first = startup.ScheduleStartupRefreshUseCase(manager).execute(request)
        second = startup.ScheduleStartupRefreshUseCase(manager).execute(request)
        assert [job["job_id"] for job in first] == [job["job_id"] for job in second]
        assert [job["operation"] for job in first] == ["refresh", "benchmarks"]
        assert len(manager.list(10)["jobs"]) == 2
    finally:
        manager.close()
    manager = LocalJobManager(OperationalWorkspace(settings, "real"))
    manager.open(start_worker=False)
    try:
        again = startup.ScheduleStartupRefreshUseCase(manager).execute(request)
        assert all(job["state"] == "failed" for job in again)
        assert len(manager.list(10)["jobs"]) == 2
    finally:
        manager.close()


def test_demo_startup_rejects_network():
    manager = SimpleNamespace(workspace=SimpleNamespace(mode="demo"))
    with pytest.raises(ValueError, match="real operations"):
        startup.ScheduleStartupRefreshUseCase(manager).execute()
