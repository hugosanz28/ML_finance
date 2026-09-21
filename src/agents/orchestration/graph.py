"""Bounded LangGraph orchestration for the monthly specialist agents."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import sqlite3
from pathlib import Path
from typing import Any, Iterator

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from src.agents.models import AgentResult
from src.agents.orchestration.schemas import SpecialistName, SupervisorDecision
from src.agents.orchestration.state import MonthlyAgentState, agent_result_to_payload
from src.agents.orchestration.supervisor import StaticSupervisorProvider, SupervisorProvider


MAX_DELEGATIONS = 5
MAX_SPECIALIST_ATTEMPTS = 2

SpecialistExecutor = Callable[[SpecialistName, str, int, Mapping[str, dict[str, Any]]], AgentResult]


@dataclass(frozen=True)
class MonthlyGraphResult:
    state: MonthlyAgentState


def run_monthly_graph(
    initial_state: MonthlyAgentState,
    *,
    supervisor: SupervisorProvider,
    execute_specialist: SpecialistExecutor,
    persist: bool,
    checkpoint_path: Path,
) -> MonthlyGraphResult:
    """Compile and run one graph with a private per-run checkpoint thread."""
    with _checkpointer(persist=persist, checkpoint_path=checkpoint_path) as checkpointer:
        graph = _build_graph(supervisor=supervisor, execute_specialist=execute_specialist).compile(
            checkpointer=checkpointer,
            name="monthly-agent-supervisor",
        )
        state = graph.invoke(
            initial_state,
            config={
                "configurable": {"thread_id": initial_state["run_id"]},
                "recursion_limit": 30,
            },
        )
    return MonthlyGraphResult(state=state)


def _build_graph(*, supervisor: SupervisorProvider, execute_specialist: SpecialistExecutor) -> StateGraph:
    workflow = StateGraph(MonthlyAgentState)
    fallback = StaticSupervisorProvider()

    def supervise(state: MonthlyAgentState) -> dict[str, Any]:
        if state.get("total_delegations", 0) >= MAX_DELEGATIONS:
            return _terminal_update(state, reason="delegation_limit_reached")
        warnings = list(state.get("warnings", []))
        try:
            decision = supervisor.decide(state)
        except Exception:
            warnings.append("supervisor_provider_failed_static_fallback")
            decision = fallback.decide(state)
        decision = _guard_decision(state, decision)
        decisions = [*state.get("decisions", []), decision.model_dump(mode="json")]
        if decision.next_agent == "finish":
            update = _terminal_update(state, reason=decision.reason_code)
            update.update(decisions=decisions, warnings=warnings)
            return update
        return {
            "decisions": decisions,
            "warnings": warnings,
            "next_agent": decision.next_agent,
            "delegation_instruction": decision.instruction,
        }

    def specialist_node(agent_name: SpecialistName):
        def run(state: MonthlyAgentState) -> dict[str, Any]:
            attempts = dict(state.get("attempts", {}))
            attempt = attempts.get(agent_name, 0) + 1
            attempts[agent_name] = attempt
            instruction = state.get("delegation_instruction", "Ejecuta tu responsabilidad especializada.")
            try:
                result = execute_specialist(agent_name, instruction, attempt, state.get("results", {}))
            except Exception:
                result = AgentResult(
                    status="failed",
                    summary=f"{agent_name} no pudo completar la ejecucion.",
                    errors=("specialist_execution_failed",),
                    metadata={"execution_status": "failed"},
                )
            payload = agent_result_to_payload(result)
            results = {**state.get("results", {}), agent_name: payload}
            result_history = {
                name: [*items]
                for name, items in state.get("result_history", {}).items()
            }
            result_history.setdefault(agent_name, []).append(payload)
            total = state.get("total_delegations", 0) + 1
            attempt_log = [
                *state.get("attempt_log", []),
                {
                    "agent": agent_name,
                    "attempt": attempt,
                    "instruction": instruction,
                    "status": result.status,
                    "summary": result.summary,
                    "warnings": list(result.warnings),
                    "errors": list(result.errors),
                },
            ]
            repairs = dict(state.get("repair_counts", {}))
            update: dict[str, Any] = {
                "attempts": attempts,
                "attempt_log": attempt_log,
                "results": results,
                "result_history": result_history,
                "total_delegations": total,
            }
            if agent_name == "asistente_aportacion_mensual" and _accepted(result):
                update.update(
                    next_agent="finish",
                    terminal_status=result.status,
                    terminal_reason="assistant_completed",
                )
                return update
            if _needs_repair(result) and repairs.get(agent_name, 0) == 0 and attempt < MAX_SPECIALIST_ATTEMPTS and total < MAX_DELEGATIONS:
                repairs[agent_name] = 1
                update.update(
                    repair_counts=repairs,
                    next_agent=agent_name,
                    delegation_instruction=(
                        "Repara el output anterior. Corrige estos errores y conserva las restricciones: "
                        + ", ".join(result.errors or ("structured_output_invalid",))
                    ),
                )
                return update
            if agent_name == "asistente_aportacion_mensual":
                update.update(
                    next_agent="finish",
                    terminal_status="failed" if not _usable_results(results) else "partial",
                    terminal_reason="assistant_validation_failed",
                )
                return update
            if total >= MAX_DELEGATIONS:
                update.update(_terminal_update({**state, "results": results}, reason="delegation_limit_reached"))
                return update
            update["next_agent"] = "supervisor"
            return update

        return run

    workflow.add_node("supervisor", supervise)
    for name in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"):
        workflow.add_node(name, specialist_node(name))
    workflow.add_edge(START, "supervisor")
    workflow.add_conditional_edges(
        "supervisor",
        lambda state: state.get("next_agent", "finish"),
        {
            "monitor_tematico": "monitor_tematico",
            "analista_activos": "analista_activos",
            "asistente_aportacion_mensual": "asistente_aportacion_mensual",
            "finish": END,
        },
    )
    for name in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"):
        workflow.add_conditional_edges(
            name,
            lambda state: state.get("next_agent", "supervisor"),
            {
                "monitor_tematico": "monitor_tematico",
                "analista_activos": "analista_activos",
                "asistente_aportacion_mensual": "asistente_aportacion_mensual",
                "supervisor": "supervisor",
                "finish": END,
            },
        )
    return workflow


def _guard_decision(state: MonthlyAgentState, decision: SupervisorDecision) -> SupervisorDecision:
    attempts = state.get("attempts", {})
    if decision.next_agent == "finish":
        if _accepted_payload(state.get("results", {}).get("asistente_aportacion_mensual")):
            return decision
        if state.get("total_delegations", 0) >= MAX_DELEGATIONS or _all_specialists_exhausted(attempts):
            return decision
        return _fallback_decision(state, reason_code="premature_finish_rejected")
    if attempts.get(decision.next_agent, 0) >= MAX_SPECIALIST_ATTEMPTS:
        return _fallback_decision(state, reason_code="specialist_attempt_limit_reached")
    return decision


def _fallback_decision(state: MonthlyAgentState, *, reason_code: str) -> SupervisorDecision:
    attempts = state.get("attempts", {})
    for agent_name, instruction, expected in (
        ("monitor_tematico", "Revisa el contexto tematico pendiente.", "Hallazgos tematicos trazables."),
        ("analista_activos", "Evalua los activos con el contexto disponible.", "Analisis estructurado por activo."),
        ("asistente_aportacion_mensual", "Produce la decision mensual final.", "Decision mensual validada."),
    ):
        if attempts.get(agent_name, 0) < MAX_SPECIALIST_ATTEMPTS:
            return SupervisorDecision(
                next_agent=agent_name,
                instruction=instruction,
                expected_output=expected,
                reason_code=reason_code,
            )
    return SupervisorDecision(
        next_agent="finish",
        instruction="Finaliza porque no quedan especialistas disponibles.",
        expected_output="Resultado parcial o fallido trazable.",
        reason_code="all_specialists_exhausted",
    )


def _terminal_update(state: MonthlyAgentState, *, reason: str) -> dict[str, Any]:
    results = state.get("results", {})
    assistant = results.get("asistente_aportacion_mensual")
    if _accepted_payload(assistant):
        status = str(assistant["status"])
    else:
        status = "partial" if _usable_results(results) else "failed"
    return {"next_agent": "finish", "terminal_status": status, "terminal_reason": reason}


def _accepted(result: AgentResult) -> bool:
    return (
        result.status in {"success", "partial"}
        and not result.errors
        and not _has_validation_issues(result.metadata)
    )


def _accepted_payload(payload: dict[str, Any] | None) -> bool:
    return bool(
        payload
        and payload.get("status") in {"success", "partial"}
        and not payload.get("errors")
        and not _has_validation_issues(payload.get("metadata"))
    )


def _needs_repair(result: AgentResult) -> bool:
    return _has_validation_issues(result.metadata)


def _has_validation_issues(metadata: Any) -> bool:
    return bool(
        isinstance(metadata, Mapping)
        and (
            metadata.get("structured_output_invalid")
            or metadata.get("decision_validation_issues")
        )
    )


def _usable_results(results: Mapping[str, dict[str, Any]]) -> bool:
    return any(result.get("status") in {"success", "partial"} for result in results.values())


def _all_specialists_exhausted(attempts: Mapping[str, int]) -> bool:
    return all(
        attempts.get(name, 0) >= MAX_SPECIALIST_ATTEMPTS
        for name in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual")
    )


@contextmanager
def _checkpointer(*, persist: bool, checkpoint_path: Path) -> Iterator[Any]:
    serializer = JsonPlusSerializer(
        pickle_fallback=False,
        allowed_json_modules=None,
        allowed_msgpack_modules=None,
    )
    if not persist:
        yield InMemorySaver(serde=serializer)
        return
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(checkpoint_path), check_same_thread=False)
    try:
        saver = SqliteSaver(connection, serde=serializer)
        saver.setup()
        yield saver
    finally:
        connection.close()


__all__ = [
    "MAX_DELEGATIONS",
    "MAX_SPECIALIST_ATTEMPTS",
    "MonthlyGraphResult",
    "SpecialistExecutor",
    "run_monthly_graph",
]
