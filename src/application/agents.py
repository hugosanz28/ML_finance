"""Application use cases for monthly agent workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from src.agents import MonthlyAgentPipelineResult, run_monthly_agent_pipeline
from src.agents.pipeline import extract_monthly_report_as_of_date
from src.application.agent_audit import persist_agent_preflight_audit
from src.application.quality_checks import (
    RunAgentQualityChecksRequest,
    RunAgentQualityChecksResult,
    RunAgentQualityChecksUseCase,
)
from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
from src.reports import get_latest_monthly_report


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
    quality_result: RunAgentQualityChecksResult
    pipeline_result: MonthlyAgentPipelineResult | None


class RunMonthlyAgentsUseCase:
    """Run the monthly three-agent pipeline through the application layer."""

    name = "run_monthly_agents"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: RunMonthlyAgentsRequest | None = None) -> RunMonthlyAgentsResult:
        resolved_request = request or RunMonthlyAgentsRequest()
        resolved_metrics = resolved_request.metrics or calculate_portfolio_metrics_from_normalized_degiro(
            settings=self.settings,
            persist=False,
        )
        resolved_report_path, monthly_report_date = _resolve_monthly_report_for_preflight(
            settings=self.settings,
            requested_path=resolved_request.monthly_report_path,
        )
        quality_result = RunAgentQualityChecksUseCase(settings=self.settings).execute(
            RunAgentQualityChecksRequest(
                metrics=resolved_metrics,
                monthly_report_date=monthly_report_date,
                require_monthly_report_date=resolved_report_path is not None,
                portfolio_metrics_snapshot=resolved_request.portfolio_metrics_snapshot,
            )
        )
        preflight_payload = quality_result.to_dict()
        if not quality_result.can_run_agents:
            return self._blocked_result(
                request=resolved_request,
                quality_result=quality_result,
                preflight_payload=preflight_payload,
            )

        pipeline_result = run_monthly_agent_pipeline(
            settings=self.settings,
            investment_brief_text=resolved_request.investment_brief_text,
            investment_brief_path=resolved_request.investment_brief_path,
            monthly_report_path=resolved_report_path,
            metrics=resolved_metrics,
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
        elif partial_agents or quality_result.result.status == "partial":
            status = "partial"
            if partial_agents:
                message = f"Monthly agents finished with partial agent(s): {', '.join(partial_agents)}."
            else:
                message = "Monthly agents completed with quality preflight warning(s)."
        else:
            status = "succeeded"
            message = "Monthly agents completed successfully."

        preflight_path = None
        if resolved_request.persist and pipeline_result.output_dir is not None:
            _, preflight_path = persist_agent_preflight_audit(
                settings=self.settings,
                run_id=pipeline_result.run_id,
                as_of_date=pipeline_result.as_of_date.isoformat(),
                generated_at=datetime.now().astimezone().isoformat(),
                execution_status=status,
                preflight=preflight_payload,
                output_dir=pipeline_result.output_dir,
                attach_to_existing_run=True,
            )

        return RunMonthlyAgentsResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=message,
                warnings=(*_quality_warnings(quality_result), *_collect_warnings(pipeline_result)),
                artifacts={
                    "run_id": pipeline_result.run_id,
                    "as_of_date": pipeline_result.as_of_date.isoformat(),
                    "output_dir": pipeline_result.output_dir,
                    "execution_status": status,
                    "preflight_status": preflight_payload["status"],
                    "preflight_error_count": quality_result.report.error_count,
                    "preflight_warning_count": quality_result.report.warning_count,
                    "preflight_path": preflight_path,
                    "monitor_tematico_status": pipeline_result.monitor_tematico.status,
                    "analista_activos_status": pipeline_result.analista_activos.status,
                    "asistente_aportacion_mensual_status": pipeline_result.asistente_aportacion_mensual.status,
                },
            ),
            quality_result=quality_result,
            pipeline_result=pipeline_result,
        )

    def _blocked_result(
        self,
        *,
        request: RunMonthlyAgentsRequest,
        quality_result: RunAgentQualityChecksResult,
        preflight_payload: dict[str, Any],
    ) -> RunMonthlyAgentsResult:
        generated_at = datetime.now().astimezone()
        run_id = generated_at.strftime("%Y%m%dT%H%M%S%f")
        output_dir = None
        preflight_path = None
        if request.persist:
            output_dir, preflight_path = persist_agent_preflight_audit(
                settings=self.settings,
                run_id=run_id,
                as_of_date=(
                    quality_result.report.as_of_date.isoformat()
                    if quality_result.report.as_of_date
                    else None
                ),
                generated_at=generated_at.isoformat(),
                execution_status="blocked",
                preflight=preflight_payload,
                output_dir=request.output_dir,
            )
        blocking_codes = ", ".join(
            issue.code for issue in quality_result.report.issues if issue.blocks_agents
        )
        return RunMonthlyAgentsResult(
            result=ApplicationResult(
                name=self.name,
                status="failed",
                message=f"Monthly agents blocked by quality preflight: {blocking_codes}.",
                warnings=_quality_warnings(quality_result),
                artifacts={
                    "run_id": run_id,
                    "as_of_date": (
                        quality_result.report.as_of_date.isoformat()
                        if quality_result.report.as_of_date
                        else None
                    ),
                    "output_dir": output_dir,
                    "execution_status": "blocked",
                    "preflight_status": preflight_payload["status"],
                    "preflight_error_count": quality_result.report.error_count,
                    "preflight_warning_count": quality_result.report.warning_count,
                    "preflight_path": preflight_path,
                    "blocking_issue_codes": blocking_codes,
                },
            ),
            quality_result=quality_result,
            pipeline_result=None,
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


def _quality_warnings(result: RunAgentQualityChecksResult) -> tuple[str, ...]:
    return tuple(
        f"quality_preflight: {issue.message}"
        for issue in result.report.issues
        if issue.severity == "warning"
    )


def _resolve_monthly_report_for_preflight(
    *,
    settings: Settings,
    requested_path: Path | None,
) -> tuple[Path | None, date | None]:
    if requested_path is not None:
        path = requested_path.expanduser().resolve()
        content = path.read_text(encoding="utf-8")
        return path, extract_monthly_report_as_of_date(content, path=path)

    latest = get_latest_monthly_report(settings=settings)
    if latest is None:
        return None, None
    path = Path(latest.report_path).expanduser().resolve()
    content = path.read_text(encoding="utf-8")
    return path, extract_monthly_report_as_of_date(content, path=path)


__all__ = [
    "RunMonthlyAgentsRequest",
    "RunMonthlyAgentsResult",
    "RunMonthlyAgentsUseCase",
]
