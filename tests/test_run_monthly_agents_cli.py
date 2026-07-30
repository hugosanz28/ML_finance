from argparse import Namespace
from datetime import date
from types import SimpleNamespace

from src.agents import AgentResult
from src.application import ApplicationResult

import scripts.run_monthly_agents as cli


def test_cli_returns_non_zero_when_quality_preflight_blocks(monkeypatch, capsys) -> None:
    preflight = {
        "status": "blocked",
        "as_of_date": "2026-05-26",
        "counts": {"error": 1, "warning": 0, "info": 0},
        "issues": [
            {
                "severity": "error",
                "code": "missing_prices",
                "message": "Missing prices.",
            }
        ],
    }
    use_case_result = SimpleNamespace(
        quality_result=SimpleNamespace(to_dict=lambda: preflight),
        pipeline_result=None,
        result=ApplicationResult(
            name="run_monthly_agents",
            status="failed",
            message="Monthly agents blocked by quality preflight: missing_prices.",
            artifacts={"output_dir": "audit/run-001"},
        ),
    )

    class FakeUseCase:
        def __init__(self, **_kwargs) -> None:
            pass

        def execute(self, _request):
            return use_case_result

    monkeypatch.setattr(cli, "RunMonthlyAgentsUseCase", FakeUseCase)
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(
            investment_brief_file=None,
            investment_brief_text=None,
            monthly_report=None,
            user_satellite_interest=None,
            llm_provider="static",
            search_provider="null",
            no_persist=False,
            output_dir=None,
        ),
    )

    exit_code = cli.main()
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Preflight: blocked" in output
    assert "missing_prices" in output
    assert "Audit: audit/run-001" in output


def test_cli_returns_zero_when_preflight_warning_allows_pipeline(monkeypatch, capsys) -> None:
    pipeline_result = _pipeline_result()
    use_case_result = SimpleNamespace(
        quality_result=SimpleNamespace(
            to_dict=lambda: {
                "status": "passed_with_warnings",
                "as_of_date": "2026-05-26",
                "counts": {"error": 0, "warning": 1, "info": 0},
                "issues": [
                    {
                        "severity": "warning",
                        "code": "return_coverage_below_threshold",
                        "message": "Return coverage is incomplete.",
                    }
                ],
            }
        ),
        pipeline_result=pipeline_result,
        result=ApplicationResult(name="run_monthly_agents", status="partial"),
    )
    _stub_cli(monkeypatch, use_case_result)

    exit_code = cli.main()
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Preflight: passed_with_warnings" in output
    assert "Run: run-001" in output


def test_cli_returns_non_zero_when_an_agent_fails(monkeypatch, capsys) -> None:
    pipeline_result = _pipeline_result(agent_status="failed")
    use_case_result = SimpleNamespace(
        quality_result=SimpleNamespace(
            to_dict=lambda: {
                "status": "passed",
                "as_of_date": "2026-05-26",
                "counts": {"error": 0, "warning": 0, "info": 1},
                "issues": [],
            }
        ),
        pipeline_result=pipeline_result,
        result=ApplicationResult(name="run_monthly_agents", status="failed"),
    )
    _stub_cli(monkeypatch, use_case_result)

    exit_code = cli.main()
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "monitor_tematico: failed" in output


def _stub_cli(monkeypatch, use_case_result) -> None:
    class FakeUseCase:
        def __init__(self, **_kwargs) -> None:
            pass

        def execute(self, _request):
            return use_case_result

    monkeypatch.setattr(cli, "RunMonthlyAgentsUseCase", FakeUseCase)
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(
            investment_brief_file=None,
            investment_brief_text=None,
            monthly_report=None,
            user_satellite_interest=None,
            llm_provider="static",
            search_provider="null",
            no_persist=False,
            output_dir=None,
        ),
    )


def _pipeline_result(*, agent_status: str = "success"):
    return SimpleNamespace(
        run_id="run-001",
        as_of_date=date(2026, 5, 26),
        output_dir=None,
        monitor_tematico=AgentResult(status=agent_status, summary="monitor"),
        analista_activos=AgentResult(status="success", summary="analyst"),
        asistente_aportacion_mensual=AgentResult(status="success", summary="assistant"),
    )
