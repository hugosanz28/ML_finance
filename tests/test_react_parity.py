from datetime import date
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.api.operation_schemas import AgentsBody, RefreshBody
from src.application.operations import ExecuteOperationRequest, ExecuteOperationUseCase
from src.application.operational_workspace import OperationalWorkspace
from src.application.types import ApplicationResult
from src.config import load_settings


def workspace(tmp_path):
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / "absent.env", env={
        "DATA_DIR": "demo/local_data", "PRICE_PROVIDER": "synthetic",
    })
    return OperationalWorkspace(settings, "demo")


@pytest.mark.parametrize("weights", [{}, {"a": .4}, {"a": -1, "b": 2}, {"a": float("nan")}, {"": 1}])
def test_agent_override_weights_reject_invalid_contract(weights):
    with pytest.raises(ValidationError):
        AgentsBody(workspace_mode="demo", confirm=True, target_weights=weights)


def test_selected_report_and_overrides_reach_existing_agent_use_case(tmp_path, monkeypatch):
    space = workspace(tmp_path)
    space.settings.reports_dir.mkdir(parents=True)
    path = space.settings.reports_dir / "monthly_selected.md"
    path.write_text("Synthetic report", encoding="utf-8")
    calls = []
    class Agents:
        def __init__(self, **kwargs):
            pass

        def execute(self, request):
            calls.append(request)
            return SimpleNamespace(result=ApplicationResult(name="test", status="succeeded", message="OK"))
    monkeypatch.setattr("src.application.operations.RunMonthlyAgentsUseCase", Agents)
    body = AgentsBody(workspace_mode="demo", confirm=True, report_id="monthly_selected",
                      target_weights={"core": .8, "cash": .2}, investment_brief_text="Run-only synthetic brief")
    ExecuteOperationUseCase(space).execute(ExecuteOperationRequest("agents", body.model_dump()))
    assert calls[0].monthly_report_path == path.resolve()
    assert calls[0].request_parameters == {"target_weights": {"core": .8, "cash": .2}}
    assert calls[0].investment_brief_text == "Run-only synthetic brief"
    assert calls[0].portfolio_metrics_snapshot is None
    assert calls[0].llm_provider == "static" and calls[0].search_provider == "null"
    assert path.read_text(encoding="utf-8") == "Synthetic report"
    with pytest.raises(ValueError):
        ExecuteOperationUseCase(space).execute(ExecuteOperationRequest("agents", {
            **body.model_dump(), "report_id": "../outside",
        }))
    assert len(calls) == 1


@pytest.mark.parametrize("scope, expected", [("both", ["fx", "prices"]), ("fx", ["fx"]), ("prices", ["prices"])])
def test_refresh_scope_and_dates_are_preserved(tmp_path, monkeypatch, scope, expected):
    calls = []
    def use_case(kind):
        class Refresh:
            def __init__(self, **kwargs):
                pass

            def execute(self, request):
                calls.append(kind)
                assert request.end_date == date(2026, 4, 30)
                if kind == "fx":
                    assert request.only_missing_base is True
                return SimpleNamespace(result=ApplicationResult(name="test", status="succeeded", message="OK"))
        return Refresh
    monkeypatch.setattr("src.application.operations.RefreshFxUseCase", use_case("fx"))
    monkeypatch.setattr("src.application.operations.RefreshMarketDataUseCase", use_case("prices"))
    body = RefreshBody(workspace_mode="demo", confirm=True, fx_provider="synthetic", price_provider="synthetic",
                       scope=scope, only_missing_base=True, end_date="2026-04-30")
    ExecuteOperationUseCase(workspace(tmp_path)).execute(ExecuteOperationRequest("refresh", body.model_dump()))
    assert calls == expected
