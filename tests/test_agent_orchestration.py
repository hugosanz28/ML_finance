from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import sqlite3
from pathlib import Path

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite import SqliteSaver

from src.agents.base import AgentValidationError
from src.agents.models import AgentResult
from src.agents.monitor_tematico import LangChainSearchToolProvider, OpenAIWebSearchProvider, SearchResult
import src.agents.monitor_tematico.providers as search_providers
from src.agents.orchestration import StaticSupervisorProvider, SupervisorDecision, run_monthly_graph


def _state(run_id: str = "orchestration-test") -> dict:
    return {
        "run_id": run_id,
        "as_of_date": "2026-08-31",
        "base_currency": "EUR",
        "graph_version": 1,
        "inputs": {},
        "input_summary": {},
        "decisions": [],
        "attempts": {},
        "repair_counts": {},
        "attempt_log": [],
        "results": {},
        "result_history": {},
        "next_agent": "supervisor",
        "delegation_instruction": "",
        "total_delegations": 0,
        "warnings": [],
        "errors": [],
        "blocking_issue_codes": [],
        "terminal_status": "failed",
        "terminal_reason": "not_started",
    }


def _success(agent_name: str) -> AgentResult:
    return AgentResult(status="success", summary=f"{agent_name} completed")


@dataclass
class _ScriptedSupervisor:
    decisions: list[SupervisorDecision]

    @property
    def name(self) -> str:
        return "scripted"

    def decide(self, state):
        return self.decisions.pop(0)


def _decision(agent: str, reason: str) -> SupervisorDecision:
    return SupervisorDecision(
        next_agent=agent,
        instruction=f"Run {agent}",
        expected_output="Structured result",
        reason_code=reason,
    )


def test_static_graph_runs_the_offline_baseline_and_stops_on_assistant(tmp_path: Path) -> None:
    calls: list[str] = []

    def execute(agent_name, instruction, attempt, prior_results):
        calls.append(agent_name)
        return _success(agent_name)

    result = run_monthly_graph(
        _state(),
        supervisor=StaticSupervisorProvider(),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert calls == ["monitor_tematico", "analista_activos", "asistente_aportacion_mensual"]
    assert result["terminal_status"] == "success"
    assert result["terminal_reason"] == "assistant_completed"
    assert result["total_delegations"] == 3
    assert len(result["result_history"]["asistente_aportacion_mensual"]) == 1


def test_supervisor_can_choose_arbitrary_order_and_repeat_a_specialist(tmp_path: Path) -> None:
    supervisor = _ScriptedSupervisor(
        [
            _decision("analista_activos", "analysis_first"),
            _decision("monitor_tematico", "monitor_second"),
            _decision("analista_activos", "analysis_refresh"),
            _decision("asistente_aportacion_mensual", "synthesize"),
        ]
    )
    calls: list[tuple[str, int]] = []

    def execute(agent_name, instruction, attempt, prior_results):
        calls.append((agent_name, attempt))
        return _success(agent_name)

    result = run_monthly_graph(
        _state(),
        supervisor=supervisor,
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert calls == [
        ("analista_activos", 1),
        ("monitor_tematico", 1),
        ("analista_activos", 2),
        ("asistente_aportacion_mensual", 1),
    ]
    assert len(result["result_history"]["analista_activos"]) == 2
    assert result["terminal_reason"] == "assistant_completed"


def test_premature_finish_is_rejected_until_an_assistant_result_exists(tmp_path: Path) -> None:
    supervisor = _ScriptedSupervisor([_decision("finish", "enough_context") for _ in range(5)])

    result = run_monthly_graph(
        _state(),
        supervisor=supervisor,
        execute_specialist=lambda agent_name, instruction, attempt, prior_results: _success(agent_name),
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert result["decisions"][0]["reason_code"] == "premature_finish_rejected"
    assert "asistente_aportacion_mensual" in result["results"]
    assert result["total_delegations"] == 5
    assert all(attempts <= 2 for attempts in result["attempts"].values())


def test_monitor_and_analyst_failures_do_not_block_the_assistant(tmp_path: Path) -> None:
    def execute(agent_name, instruction, attempt, prior_results):
        if agent_name == "asistente_aportacion_mensual":
            return _success(agent_name)
        return AgentResult(
            status="failed",
            summary=f"{agent_name} unavailable",
            errors=("specialist_unavailable",),
        )

    result = run_monthly_graph(
        _state(),
        supervisor=StaticSupervisorProvider(),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert result["attempts"] == {
        "monitor_tematico": 1,
        "analista_activos": 1,
        "asistente_aportacion_mensual": 1,
    }
    assert result["terminal_status"] == "success"


def test_missing_required_inputs_end_the_graph_as_a_verified_blocker(tmp_path: Path) -> None:
    calls: list[str] = []

    def execute(agent_name, instruction, attempt, prior_results):
        calls.append(agent_name)
        raise AgentValidationError(
            "Missing required agent inputs in context: investment_brief",
            reason_code="required_agent_inputs_missing",
        )

    result = run_monthly_graph(
        _state(),
        supervisor=_ScriptedSupervisor([_decision("monitor_tematico", "check_inputs")]),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert calls == ["monitor_tematico"]
    assert result["blocking_issue_codes"] == ["required_agent_inputs_missing"]
    assert result["errors"] == ["required_agent_inputs_missing"]
    assert result["terminal_status"] == "failed"
    assert result["terminal_reason"] == "verified_runtime_blocker"
    assert result["decisions"][-1]["next_agent"] == "finish"
    assert "asistente_aportacion_mensual" not in result["results"]


def test_other_validation_error_does_not_claim_a_verified_blocker(tmp_path: Path) -> None:
    def execute(agent_name, instruction, attempt, prior_results):
        if agent_name == "monitor_tematico":
            raise AgentValidationError("Unsupported request")
        return _success(agent_name)

    result = run_monthly_graph(
        _state(),
        supervisor=StaticSupervisorProvider(),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert result["blocking_issue_codes"] == []
    assert result["terminal_status"] == "success"
    assert result["attempts"] == {
        "monitor_tematico": 1,
        "analista_activos": 1,
        "asistente_aportacion_mensual": 1,
    }


def test_assistant_provider_failure_is_terminal_failure(tmp_path: Path) -> None:
    def execute(agent_name, instruction, attempt, prior_results):
        if agent_name == "monitor_tematico":
            return _success(agent_name)
        return AgentResult(
            status="failed",
            summary="Provider unavailable",
            errors=("assistant_llm_provider_failed",),
            metadata={"structured_output_invalid": False},
        )

    result = run_monthly_graph(
        _state(),
        supervisor=_ScriptedSupervisor(
            [_decision("monitor_tematico", "research"), _decision("asistente_aportacion_mensual", "decide")]
        ),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert result["attempts"] == {"monitor_tematico": 1, "asistente_aportacion_mensual": 1}
    assert result["terminal_status"] == "failed"
    assert result["terminal_reason"] == "assistant_provider_failed"


def test_invalid_assistant_output_gets_one_repair_then_stops(tmp_path: Path) -> None:
    attempts: list[int] = []

    def execute(agent_name, instruction, attempt, prior_results):
        attempts.append(attempt)
        if attempt == 1:
            return AgentResult(
                status="partial",
                summary="Invalid decision",
                metadata={"decision_validation_issues": ("budget_mismatch",)},
            )
        return _success(agent_name)

    result = run_monthly_graph(
        _state(),
        supervisor=_ScriptedSupervisor([_decision("asistente_aportacion_mensual", "direct")]),
        execute_specialist=execute,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert attempts == [1, 2]
    assert result["repair_counts"] == {"asistente_aportacion_mensual": 1}
    assert result["terminal_status"] == "success"


def test_second_invalid_assistant_output_returns_partial(tmp_path: Path) -> None:
    invalid = AgentResult(
        status="partial",
        summary="Invalid decision",
        metadata={"decision_validation_issues": ("budget_mismatch",)},
    )
    result = run_monthly_graph(
        _state(),
        supervisor=_ScriptedSupervisor([_decision("asistente_aportacion_mensual", "direct")]),
        execute_specialist=lambda agent_name, instruction, attempt, prior_results: invalid,
        persist=False,
        checkpoint_path=tmp_path / "unused.sqlite",
    ).state

    assert result["attempts"]["asistente_aportacion_mensual"] == 2
    assert result["terminal_status"] == "partial"
    assert result["terminal_reason"] == "assistant_validation_failed"


def test_sqlite_checkpoint_roundtrips_json_safe_state(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "langgraph.sqlite"
    run_monthly_graph(
        _state("sqlite-roundtrip"),
        supervisor=StaticSupervisorProvider(),
        execute_specialist=lambda agent_name, instruction, attempt, prior_results: _success(agent_name),
        persist=True,
        checkpoint_path=checkpoint_path,
    )

    serializer = JsonPlusSerializer(
        pickle_fallback=False,
        allowed_json_modules=None,
        allowed_msgpack_modules=None,
    )
    with sqlite3.connect(checkpoint_path) as connection:
        checkpoint = SqliteSaver(connection, serde=serializer).get_tuple(
            {"configurable": {"thread_id": "sqlite-roundtrip"}}
        )

    assert checkpoint is not None
    values = checkpoint.checkpoint["channel_values"]
    assert values["terminal_reason"] == "assistant_completed"
    assert values["total_delegations"] == 3


def test_openai_web_search_uses_hosted_tool_and_extracts_sources(monkeypatch) -> None:
    captured: dict = {}

    class Response:
        def model_dump(self, *, mode):
            return {
                "output": [
                    {
                        "type": "web_search_call",
                        "action": {
                            "sources": [
                                {
                                    "url": "https://example.com/source",
                                    "title": "Primary source",
                                    "snippet": "Relevant fact",
                                }
                            ]
                        },
                    }
                ]
            }

    class BoundModel:
        def invoke(self, messages):
            captured["messages"] = messages
            return Response()

    class Model:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs

        def bind_tools(self, tools):
            captured["tools"] = tools
            return BoundModel()

    monkeypatch.setattr(search_providers, "ChatOpenAI", Model)
    provider = OpenAIWebSearchProvider(
        model="test-model",
        api_key="test-key",  # pragma: allowlist secret
    )

    results = provider.search(
        "market catalyst",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 31),
        max_results=2,
    )

    assert captured["tools"] == [{"type": "web_search"}]
    assert captured["kwargs"]["use_responses_api"] is True
    assert captured["kwargs"]["store"] is True
    assert captured["kwargs"]["include"] == ["web_search_call.action.sources"]
    assert captured["kwargs"]["model_kwargs"] == {"max_tool_calls": 7}
    assert [(item.title, item.url) for item in results] == [
        ("Primary source", "https://example.com/source")
    ]


def test_legacy_search_provider_is_exposed_as_a_langchain_tool() -> None:
    captured: dict = {}

    class LegacyProvider:
        name = "legacy"

        def search(self, query, *, start_date, end_date, max_results):
            captured.update(
                query=query,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results,
            )
            return (SearchResult(title="Result", url="https://example.com", query=query),)

    provider = LangChainSearchToolProvider(LegacyProvider())
    results = provider.search(
        "query",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 31),
        max_results=3,
    )

    assert provider.tool.name == "legacy_search"
    assert captured == {
        "query": "query",
        "start_date": date(2026, 8, 1),
        "end_date": date(2026, 8, 31),
        "max_results": 3,
    }
    assert results[0].title == "Result"
