"""Adapt validated operations to existing use cases; no financial rules here."""

import base64
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from src.application.agents import RunMonthlyAgentsRequest, RunMonthlyAgentsUseCase
from src.application.benchmarks import RefreshBenchmarksRequest, RefreshBenchmarksUseCase
from src.application.artifact_reads import _safe_payload, resolve_report_path
from src.application.contribution_lab import SimulateContributionRequest, SimulateContributionUseCase
from src.application.degiro import ImportDegiroRequest, ImportDegiroUseCase
from src.application.market_data import RefreshFxRequest, RefreshFxUseCase, RefreshMarketDataRequest, RefreshMarketDataUseCase
from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.application.portfolio_targets import ReadPortfolioTargetsUseCase, UpdatePortfolioTargetsRequest, UpdatePortfolioTargetsUseCase
from src.application.reports import GenerateMonthlyReportRequest, GenerateMonthlyReportUseCase
from src.application.settings import ReadInvestmentBriefUseCase, UpdateInvestmentBriefRequest, UpdateInvestmentBriefUseCase
from src.application.uploads import DegiroUpload, SaveDegiroUploadsRequest, SaveDegiroUploadsUseCase, canonical_degiro_upload_name


MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_TOTAL_UPLOAD_BYTES = 10 * 1024 * 1024
OPERATIONS = {"uploads", "import", "refresh", "benchmarks", "report", "simulation", "agents", "brief", "targets"}


@dataclass(frozen=True)
class ExecuteOperationRequest:
    operation: str
    parameters: dict[str, Any]


def validate_operation(request: ExecuteOperationRequest, workspace: OperationalWorkspace):
    if request.operation not in OPERATIONS:
        raise OperationError("unknown_operation", 422)
    values = request.parameters
    if values.get("workspace_mode") != workspace.mode:
        raise OperationError("workspace_mode_mismatch", 422)
    if values.get("confirm") is not True:
        raise OperationError("confirmation_required", 422)
    if request.operation == "benchmarks":
        if workspace.mode != "real":
            raise OperationError("external_provider_forbidden_in_demo", 422)
        if values.get("provider") != "yfinance_ecb":
            raise OperationError("provider_required", 422)
    if request.operation == "refresh":
        for field in ("fx_provider", "price_provider"):
            if values[field] not in {"synthetic", "yfinance"}:
                raise OperationError("provider_required", 422)
            if values[field] == "synthetic" and workspace.mode != "demo":
                raise OperationError("synthetic_requires_demo", 422)
            if values[field] != "synthetic" and workspace.mode == "demo":
                raise OperationError("external_provider_forbidden_in_demo", 422)
    if request.operation == "agents":
        external = values.get("llm_provider", "static") != "static" or values.get("search_provider", "null") not in {"null", "static"}
        if external and workspace.mode == "demo":
            raise OperationError("external_provider_forbidden_in_demo", 422)
    if request.operation == "uploads":
        decode_uploads(values)


def decode_uploads(values):
    uploads, names, total = [], set(), 0
    day = date.fromisoformat(values["uploaded_at"])
    for entry in values["uploads"]:
        filename = entry["filename"]
        if any(char in filename for char in ("/", "\\", ":", "\x00")) or not filename.lower().endswith(".csv"):
            raise OperationError("invalid_upload_name", 422)
        name = canonical_degiro_upload_name(filename, fallback_date=day)
        if name is None or name in names:
            raise OperationError("unknown_or_duplicate_upload", 422)
        try:
            content = base64.b64decode(entry["content_base64"], validate=True)
        except ValueError as exc:
            raise OperationError("invalid_upload_encoding", 422) from exc
        total += len(content)
        if not content or len(content) > MAX_UPLOAD_BYTES or total > MAX_TOTAL_UPLOAD_BYTES:
            raise OperationError("upload_size_limit", 413)
        names.add(name)
        uploads.append(DegiroUpload(filename, content))
    if not 1 <= len(uploads) <= 5:
        raise OperationError("upload_count_limit", 422)
    return tuple(uploads)


class ExecuteOperationUseCase:
    def __init__(self, workspace: OperationalWorkspace):
        self.workspace = workspace
        self.settings = workspace.settings

    def execute(self, request: ExecuteOperationRequest, progress=lambda value, phase: None):
        validate_operation(request, self.workspace)
        self.workspace.validate()
        values = {key: value for key, value in request.parameters.items() if key not in {"confirm", "workspace_mode"}}
        operation = request.operation
        extra = {}
        if operation == "uploads":
            uploads = decode_uploads(values)
            incoming = self.settings.degiro_exports_dir / "incoming"
            for upload in uploads:
                name = canonical_degiro_upload_name(upload.filename, fallback_date=date.fromisoformat(values["uploaded_at"]))
                path = incoming / name
                if path.exists() and path.read_bytes() != upload.content:
                    # Replacing an existing export requires a separate explicit workflow.
                    raise OperationError("upload_already_exists")
            output = SaveDegiroUploadsUseCase(settings=self.settings).execute(SaveDegiroUploadsRequest(
                uploads=uploads, uploaded_at=date.fromisoformat(values["uploaded_at"]),
            ))
            extra["uploads"] = [asdict(item) for item in output.outcomes]
        elif operation == "import":
            output = ImportDegiroUseCase(settings=self.settings).execute(ImportDegiroRequest())
        elif operation == "refresh":
            dates = {key: date.fromisoformat(values[key]) if values.get(key) else None for key in ("start_date", "end_date")}
            scope = values.get("scope", "both")
            if scope == "prices":
                output = RefreshMarketDataUseCase(settings=self.settings).execute(RefreshMarketDataRequest(
                    provider=values["price_provider"], write_overrides_template=False, **dates,
                ))
                return self._result(output.result)
            fx = RefreshFxUseCase(settings=self.settings).execute(RefreshFxRequest(
                provider=values["fx_provider"], only_missing_base=values.get("only_missing_base", False), **dates,
            ))
            if scope == "fx":
                return self._result(fx.result)
            progress(50, "refresh_prices")
            prices = RefreshMarketDataUseCase(settings=self.settings).execute(RefreshMarketDataRequest(
                provider=values["price_provider"], write_overrides_template=False, **dates,
            ))
            payload = self._result(prices.result)
            payload["steps"] = {"fx": self._result(fx.result), "prices": self._result(prices.result)}
            if any(item.result.status in {"partial", "skipped"} for item in (fx, prices)):
                payload["status"] = "partial"
            payload["warnings"] = list(dict.fromkeys([*payload["warnings"], *fx.result.warnings]))
            return payload
        elif operation == "benchmarks":
            output = RefreshBenchmarksUseCase(settings=self.settings).execute(RefreshBenchmarksRequest(
                start_date=date.fromisoformat(values["start_date"]), end_date=date.fromisoformat(values["end_date"]),
                provider=values["provider"],
            ))
            return self._result(output)
        elif operation == "report":
            output = GenerateMonthlyReportUseCase(settings=self.settings).execute(GenerateMonthlyReportRequest(
                as_of_date=date.fromisoformat(values["as_of_date"]) if values.get("as_of_date") else None,
            ))
            extra["report_id"] = output.report.report_id
        elif operation == "simulation":
            output = SimulateContributionUseCase(settings=self.settings).execute(SimulateContributionRequest(**values))
            return _safe_payload(output.to_dict(), self.settings)
        elif operation == "agents":
            report_id = values.pop("report_id", None)
            if report_id is not None:
                values["monthly_report_path"] = resolve_report_path(report_id, self.settings)
            weights = values.pop("target_weights", None)
            if weights is not None:
                values["request_parameters"] = {"target_weights": weights}
            output = RunMonthlyAgentsUseCase(settings=self.settings).execute(RunMonthlyAgentsRequest(**values))
        elif operation == "brief":
            current = ReadInvestmentBriefUseCase(settings=self.settings).execute()
            if current.content_hash != values["expected_previous_hash"]:
                raise OperationError("content_conflict")
            output = UpdateInvestmentBriefUseCase(settings=self.settings).execute(UpdateInvestmentBriefRequest(**values))
            extra["content_hash"] = output.content_hash
        else:
            current = ReadPortfolioTargetsUseCase(settings=self.settings).execute()
            if current.content_hash != values["expected_previous_hash"]:
                raise OperationError("content_conflict")
            output = UpdatePortfolioTargetsUseCase(settings=self.settings).execute(UpdatePortfolioTargetsRequest(**values))
            extra["content_hash"] = output.content_hash
            if output.result.failed:
                raise OperationError("invalid_portfolio_targets", 422)
        return {**self._result(output.result), **extra}

    def _result(self, result):
        return _safe_payload({"status": result.status, "message": result.message,
                              "warnings": list(result.warnings), "artifacts": dict(result.artifacts)}, self.settings)
