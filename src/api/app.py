"""FastAPI adapter over application use cases, with local-only browser access."""

from typing import Annotated
from contextlib import asynccontextmanager, nullcontext
import asyncio
import re

from fastapi import APIRouter, FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.cors import CORSMiddleware

from src.application.analytics import (
    AnalyticsRequest, GetAnalyticsSummaryUseCase, GetBenchmarkComparisonUseCase,
    GetMetricDefinitionsUseCase, GetPortfolioPerformanceUseCase, GetPortfolioRiskUseCase,
)
from src.application.artifact_reads import (
    ListArtifactsRequest, ListAuditRunsUseCase, ListReportsUseCase, ReadAgentAuditUseCase,
    ReadArtifactRequest, ReadReportUseCase,
)
from src.application.portfolio_state import (
    GetPortfolioStateRequest, GetPortfolioStateUseCase, PortfolioStateUnavailableError,
)
from src.config import Settings, get_settings
from src.application.local_jobs import LocalJobManager
from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.api.body_limit import BodyLimitMiddleware
from src.api.operations import operation_router
from src.api.schemas import (
    AnalyticsQuery, AnalyticsResponse, AuditResponse, ErrorResponse, HealthResponse,
    ListQuery, MetricDefinition, MetricDefinitionsResponse, PortfolioQuery, PortfolioResponse,
    ReportResponse, ReportsResponse, RunsResponse,
)


LOCAL_ORIGINS = ("http://127.0.0.1:5173", "http://localhost:5173")
ArtifactId = Annotated[str, Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,159}$")]


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def create_app(*, settings: Settings | None = None, workspace_mode: str | None = None) -> FastAPI:
    # Resolve one environment per process; HTTP input cannot switch workspaces/providers.
    resolved = get_settings() if settings is None else settings
    manager = LocalJobManager(OperationalWorkspace(resolved, workspace_mode)) if workspace_mode else None
    if manager:
        resolved = manager.workspace.settings

    @asynccontextmanager
    async def lifespan(app):
        if manager:
            await asyncio.to_thread(manager.open)
        try:
            yield
        finally:
            if manager:
                await asyncio.to_thread(manager.close)

    app = FastAPI(title="ML_finance local API", version="1.0.0", docs_url=None, redoc_url=None, lifespan=lifespan)
    app.state.jobs = manager
    app.add_middleware(CORSMiddleware, allow_origins=list(LOCAL_ORIGINS),
                       allow_methods=["GET", "POST", "PUT"] if manager else ["GET"],
                       allow_headers=["Content-Type", "Idempotency-Key", "X-ML-Finance-Confirm"], allow_credentials=False)
    app.add_middleware(BodyLimitMiddleware)

    def reading():
        return manager.reading() if manager else nullcontext()

    @app.middleware("http")
    async def local_boundary(request: Request, call_next):
        host = request.headers.get("host", "")
        if not re.fullmatch(r"(?:127\.0\.0\.1|localhost)(?::[0-9]{1,5})?", host):
            response = _error(400, "invalid_host", "Only localhost is supported.")
        elif request.headers.get("origin") not in {None, *LOCAL_ORIGINS, f"http://{host}"}:
            response = _error(403, "origin_not_allowed", "This browser origin is not allowed.")
        elif manager and request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.headers.get("x-ml-finance-confirm") != "local-write":
            response = _error(403, "confirmation_required", "Explicit local write confirmation is required.")
        elif manager and request.method in {"POST", "PUT", "PATCH"} and request.headers.get("content-type", "").split(";")[0] != "application/json":
            response = _error(415, "json_required", "Use application/json.")
        else:
            try:
                response = await call_next(request)
            except Exception:
                # Never return exception text, SQL, paths or provider/configuration details.
                response = _error(500, "internal_error", "Unable to read local data.")
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        # Vite must also be able to read early validation/body-limit errors.
        origin = request.headers.get("origin")
        if origin in LOCAL_ORIGINS and "Access-Control-Allow-Origin" not in response.headers:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers.add_vary_header("Origin")
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Pydantic error details can echo arbitrary client input, including secrets.
        return _error(422, "invalid_request", "Invalid request parameters.")

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        codes = {404: "not_found", 405: "method_not_allowed", 422: "invalid_request"}
        return _error(exc.status_code, codes.get(exc.status_code, "request_failed"), "Request could not be completed.")

    @app.exception_handler(PortfolioStateUnavailableError)
    async def no_portfolio(request, exc):
        return _error(404, "portfolio_data_unavailable", "No portfolio data is available.")

    @app.exception_handler(FileNotFoundError)
    async def no_artifact(request, exc):
        return _error(404, "not_found", "The requested local artifact is unavailable.")

    @app.exception_handler(OperationError)
    async def operation_error(request, exc):
        return _error(exc.status_code, exc.code, "Local operation could not be completed.")

    router = APIRouter(prefix="/api/v1", responses={
        status: {"model": ErrorResponse} for status in (400, 403, 404, 405, 409, 422, 500)
    })

    @router.get("/health", response_model=HealthResponse)
    def health():
        return HealthResponse(mode="operations" if manager else "read_only")

    @router.get("/portfolio/state", response_model=PortfolioResponse)
    def portfolio(query: Annotated[PortfolioQuery, Query()]):
        with reading():
            return GetPortfolioStateUseCase(settings=resolved).execute(
                GetPortfolioStateRequest(persist=False, **query.model_dump()),
            ).to_dict()

    def analytics_endpoint(use_case):
        def read(query: Annotated[AnalyticsQuery, Query()]):
            request = AnalyticsRequest(**query.model_dump())
            try:
                request.validate()
            except ValueError as exc:
                raise HTTPException(422) from exc
            with reading():
                return use_case(settings=resolved).execute(request).to_dict()
        return read

    for section, use_case in (
        ("summary", GetAnalyticsSummaryUseCase), ("performance", GetPortfolioPerformanceUseCase),
        ("risk", GetPortfolioRiskUseCase), ("benchmarks", GetBenchmarkComparisonUseCase),
    ):
        router.add_api_route(f"/analytics/{section}", analytics_endpoint(use_case), methods=["GET"],
                             response_model=AnalyticsResponse, name=f"analytics_{section}")

    @router.get("/analytics/metric-definitions", response_model=MetricDefinitionsResponse)
    def definitions():
        return GetMetricDefinitionsUseCase().execute().to_dict()

    @router.get("/analytics/metrics/{metric_id}", response_model=MetricDefinition)
    def metric(metric_id: ArtifactId):
        for definition in GetMetricDefinitionsUseCase().execute().definitions:
            if definition["metric_id"] == metric_id:
                return definition
        raise HTTPException(404)

    @router.get("/reports", response_model=ReportsResponse)
    def reports(query: Annotated[ListQuery, Query()]):
        with reading():
            return ListReportsUseCase(settings=resolved).execute(ListArtifactsRequest(**query.model_dump())).to_dict()

    @router.get("/reports/{report_id}", response_model=ReportResponse)
    def report(report_id: ArtifactId):
        with reading():
            return ReadReportUseCase(settings=resolved).execute(ReadArtifactRequest(report_id)).to_dict()

    @router.get("/agents/runs", response_model=RunsResponse)
    def runs(query: Annotated[ListQuery, Query()]):
        with reading():
            return ListAuditRunsUseCase(settings=resolved).execute(ListArtifactsRequest(**query.model_dump())).to_dict()

    @router.get("/agents/runs/{run_id}", response_model=AuditResponse)
    def audit(run_id: ArtifactId):
        with reading():
            return ReadAgentAuditUseCase(settings=resolved).execute(ReadArtifactRequest(run_id)).to_dict()

    app.include_router(router)
    if manager:
        app.include_router(operation_router(manager))
    return app
