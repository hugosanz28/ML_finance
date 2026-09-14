"""HTTP operation submissions return immediately; the local worker executes use cases."""

from typing import Annotated

from fastapi import APIRouter, Header, Path, Query

from src.api.operation_schemas import (
    AgentsBody, BriefBody, BriefResponse, JobResponse, JobsResponse, OperationBody,
    RefreshBody, ReportBody, SimulationBody, TargetsBody, TargetsResponse, UploadsBody,
)
from src.api.schemas import ErrorResponse, ListQuery
from src.application.local_jobs import (
    GetJobRequest, GetJobUseCase, ListJobsRequest, ListJobsUseCase, SubmitJobRequest, SubmitJobUseCase,
    RetryJobRequest, RetryJobUseCase,
)
from src.application.settings import ReadInvestmentBriefUseCase
from src.application.portfolio_targets import ReadPortfolioTargetsUseCase


Key = Annotated[str, Header(alias="Idempotency-Key", pattern=r"^[A-Za-z0-9_-]{8,128}$")]


def operation_router(manager):
    router = APIRouter(prefix="/api/v1", responses={
        status: {"model": ErrorResponse} for status in (400, 403, 404, 409, 413, 415, 422, 429, 500, 503)
    })

    def submit(operation, body, key):
        return SubmitJobUseCase(manager).execute(SubmitJobRequest(operation, body.model_dump(), key))

    @router.post("/degiro/uploads", response_model=JobResponse, status_code=202)
    def uploads(body: UploadsBody, key: Key):
        return submit("uploads", body, key)

    @router.post("/degiro/import", response_model=JobResponse, status_code=202)
    def import_degiro(body: OperationBody, key: Key):
        return submit("import", body, key)

    @router.post("/market-data/refresh", response_model=JobResponse, status_code=202)
    def refresh(body: RefreshBody, key: Key):
        return submit("refresh", body, key)

    @router.post("/reports/monthly", response_model=JobResponse, status_code=202)
    def report(body: ReportBody, key: Key):
        return submit("report", body, key)

    @router.post("/portfolio/contributions/simulate", response_model=JobResponse, status_code=202)
    def simulate(body: SimulationBody, key: Key):
        return submit("simulation", body, key)

    @router.post("/agents/monthly-runs", response_model=JobResponse, status_code=202)
    def agents(body: AgentsBody, key: Key):
        return submit("agents", body, key)

    @router.put("/settings/investment-brief", response_model=JobResponse, status_code=202)
    def brief(body: BriefBody, key: Key):
        return submit("brief", body, key)

    @router.put("/settings/portfolio-targets", response_model=JobResponse, status_code=202)
    def targets(body: TargetsBody, key: Key):
        return submit("targets", body, key)

    @router.get("/settings/investment-brief", response_model=BriefResponse)
    def read_brief():
        with manager.reading():
            result = ReadInvestmentBriefUseCase(settings=manager.workspace.settings).execute().to_dict()
            result.pop("path")
            return result

    @router.get("/settings/portfolio-targets", response_model=TargetsResponse)
    def read_targets():
        with manager.reading():
            result = ReadPortfolioTargetsUseCase(settings=manager.workspace.settings).execute().to_dict()
            result.pop("path")
            if result["validation_error"]:
                result["validation_error"] = "invalid_portfolio_targets"
            return result

    @router.get("/jobs", response_model=JobsResponse)
    def jobs(query: Annotated[ListQuery, Query()]):
        return ListJobsUseCase(manager).execute(ListJobsRequest(query.limit))

    @router.get("/jobs/{job_id}", response_model=JobResponse)
    def job(job_id: Annotated[str, Path(pattern=r"^[a-f0-9]{32}$")]):
        return GetJobUseCase(manager).execute(GetJobRequest(job_id))

    @router.post("/jobs/{job_id}/retry", response_model=JobResponse, status_code=202)
    def retry(job_id: Annotated[str, Path(pattern=r"^[a-f0-9]{32}$")], body: OperationBody, key: Key):
        return RetryJobUseCase(manager).execute(RetryJobRequest(job_id, key, body.workspace_mode, body.confirm))

    return router
