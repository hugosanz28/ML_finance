"""Application use cases for monthly agent workflows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.agents import MonthlyAgentPipelineResult, run_monthly_agent_pipeline
from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.portfolio import PortfolioMetricsResult


@dataclass(frozen=True)
class RunMonthlyAgentsRequest:
    investment_brief_text: str | None = None
    investment_brief_path: Path | None = None
    monthly_report_path: Path | None = None
    metrics: PortfolioMetricsResult | None = None
    user_satellite_interest: str | None = None
    llm_provider: str = "static"
    search_provider: str = "null"
    persist: bool = True
    output_dir: Path | None = None
    request_parameters: Mapping[str, Any] | None = None
    portfolio_metrics_snapshot: Mapping[str, Any] | None = None
    monthly_budget: float | None = None


@dataclass(frozen=True)
class RunMonthlyAgentsResult:
    result: ApplicationResult
    pipeline_result: MonthlyAgentPipelineResult


class RunMonthlyAgentsUseCase:
    """Run the monthly three-agent pipeline through the application layer."""

    name = "run_monthly_agents"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: RunMonthlyAgentsRequest | None = None) -> RunMonthlyAgentsResult:
        resolved_request = request or RunMonthlyAgentsRequest()
        pipeline_result = run_monthly_agent_pipeline(
            settings=self.settings,
            investment_brief_text=resolved_request.investment_brief_text,
            investment_brief_path=resolved_request.investment_brief_path,
            monthly_report_path=resolved_request.monthly_report_path,
            metrics=resolved_request.metrics,
            user_satellite_interest=resolved_request.user_satellite_interest,
            llm_provider=resolved_request.llm_provider,
            search_provider=resolved_request.search_provider,
            persist=resolved_request.persist,
            output_dir=resolved_request.output_dir,
            request_parameters=resolved_request.request_parameters,
            portfolio_metrics_snapshot=resolved_request.portfolio_metrics_snapshot,
            monthly_budget=resolved_request.monthly_budget,
        )
        agent_statuses = {
            "monitor_tematico": pipeline_result.monitor_tematico.status,
            "analista_activos": pipeline_result.analista_activos.status,
            "asistente_aportacion_mensual": pipeline_result.asistente_aportacion_mensual.status,
        }
        failed_agents = [name for name, status in agent_statuses.items() if status == "failed"]
        partial_agents = [name for name, status in agent_statuses.items() if status == "partial"]
        if failed_agents:
            status = "failed"
            message = f"Monthly agents finished with failed agent(s): {', '.join(failed_agents)}."
        elif partial_agents:
            status = "partial"
            message = f"Monthly agents finished with partial agent(s): {', '.join(partial_agents)}."
        else:
            status = "succeeded"
            message = "Monthly agents completed successfully."

        return RunMonthlyAgentsResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=message,
                warnings=_collect_warnings(pipeline_result),
                artifacts={
                    "run_id": pipeline_result.run_id,
                    "as_of_date": pipeline_result.as_of_date.isoformat(),
                    "output_dir": pipeline_result.output_dir,
                    "monitor_tematico_status": pipeline_result.monitor_tematico.status,
                    "analista_activos_status": pipeline_result.analista_activos.status,
                    "asistente_aportacion_mensual_status": pipeline_result.asistente_aportacion_mensual.status,
                },
            ),
            pipeline_result=pipeline_result,
        )


def _collect_warnings(result: MonthlyAgentPipelineResult) -> tuple[str, ...]:
    warnings: list[str] = []
    for agent_name, agent_result in (
        ("monitor_tematico", result.monitor_tematico),
        ("analista_activos", result.analista_activos),
        ("asistente_aportacion_mensual", result.asistente_aportacion_mensual),
    ):
        warnings.extend(f"{agent_name}: {warning}" for warning in agent_result.warnings)
    return tuple(warnings)


__all__ = [
    "RunMonthlyAgentsRequest",
    "RunMonthlyAgentsResult",
    "RunMonthlyAgentsUseCase",
]
