"""Read-only, path-contained projections for HTTP reports and agent audits."""

from dataclasses import asdict, dataclass
from pathlib import Path
import re
from typing import Any

from src.agents.provider_audit import redact_sensitive_audit_payload
from src.application.agent_audit import (
    AGENT_NAMES, GetAgentRunAuditRequest, GetAgentRunAuditUseCase, ListAgentRunsUseCase,
)
from src.config import Settings


@dataclass(frozen=True)
class ReadArtifactRequest:
    artifact_id: str


@dataclass(frozen=True)
class ListArtifactsRequest:
    limit: int = 20


@dataclass(frozen=True)
class ArtifactReadResult:
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return self.payload


def _contained(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise FileNotFoundError("Artifact is outside the configured directory")
    return resolved


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,159}", value):
        raise ValueError("Invalid artifact identifier")
    return value


def _safe_payload(value: Any, settings: Settings) -> Any:
    """Omit filesystem metadata; content remains private, not anonymized."""
    value = redact_sensitive_audit_payload(value)
    if isinstance(value, dict):
        return {
            key: _safe_payload(item, settings) for key, item in value.items()
            if key not in {"path", "paths", "output_dir", "repo_root", "env_file"}
            and not key.endswith(("_path", "_paths", "_dir"))
        }
    if isinstance(value, list):
        return [_safe_payload(item, settings) for item in value]
    if isinstance(value, str):
        roots = sorted({str(item) for item in vars(settings).values() if isinstance(item, Path)}, key=len, reverse=True)
        for root in roots:
            for variant in {root, root.replace("\\", "/")}:
                value = value.replace(variant, "[LOCAL_PATH]")
        # Legacy free text can contain common credential assignments, not only JSON keys.
        value = re.sub(r"(?i)\bBearer\s+[^\s,;]+", "Bearer [REDACTED]", value)
        value = re.sub(r"(?i)\b(authorization|api[_-]?key|access[_-]?token|password|secret)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", value)
    return value


class ListReportsUseCase:
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    def execute(self, request: ListArtifactsRequest | None = None) -> ArtifactReadResult:
        request = request or ListArtifactsRequest()
        reports = []
        for path in sorted(self.settings.reports_dir.glob("monthly_*.md"), reverse=True):
            try:
                _identifier(path.stem)
                contained = _contained(path, self.settings.reports_dir)
            except (ValueError, FileNotFoundError):
                continue
            if contained.is_file():
                reports.append({"report_id": path.stem})
            if len(reports) >= request.limit:
                break
        return ArtifactReadResult({"reports": reports})


class ReadReportUseCase:
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    def execute(self, request: ReadArtifactRequest) -> ArtifactReadResult:
        report_id = _identifier(request.artifact_id)
        if not report_id.startswith("monthly_"):
            raise FileNotFoundError("Report not found")
        path = _contained(self.settings.reports_dir / f"{report_id}.md", self.settings.reports_dir)
        return ArtifactReadResult({
            "report_id": report_id, "content_markdown": _safe_payload(path.read_text(encoding="utf-8"), self.settings),
        })


class ReadAgentAuditUseCase:
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    def execute(self, request: ReadArtifactRequest) -> ArtifactReadResult:
        run_id = _identifier(request.artifact_id)
        root = self.settings.data_dir / "agents" / "monthly_pipeline"
        run_dir = _contained(root / run_id, root)
        # Validate every file the legacy reader may open, including symlinked subfolders.
        paths = [run_dir / name for name in ("run_metadata.json", "input_payload.json", "preflight.json")]
        for agent in AGENT_NAMES:
            paths.extend(run_dir / "agents" / agent / name for name in (
                "context.json", "request.json", "prompt_refs.json", "prompt_rendered.md", "provider.json",
                "raw_response.json", "parsed_output.json", "audit_metadata.json",
            ))
        for path in paths:
            _contained(path, run_dir)
        result = GetAgentRunAuditUseCase(settings=self.settings).execute(GetAgentRunAuditRequest(run_id))
        return ArtifactReadResult(_safe_payload(asdict(result), self.settings))


class ListAuditRunsUseCase:
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    def execute(self, request: ListArtifactsRequest | None = None) -> ArtifactReadResult:
        request = request or ListArtifactsRequest()
        root = self.settings.data_dir / "agents" / "monthly_pipeline"
        # Do not let even a listing follow metadata links out of its configured root.
        for run_dir in root.iterdir() if root.exists() else ():
            if run_dir.is_dir():
                _contained(run_dir, root)
                for name in ("run_metadata.json", "pipeline_result.json"):
                    _contained(run_dir / name, run_dir)
        result = ListAgentRunsUseCase(settings=self.settings).execute(limit=request.limit)
        return ArtifactReadResult(_safe_payload(asdict(result), self.settings))
