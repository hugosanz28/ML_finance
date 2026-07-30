"""Application use cases for user-editable local settings."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from uuid import uuid4

from src.application.types import ApplicationResult
from src.config import Settings, get_settings


@dataclass(frozen=True)
class ReadInvestmentBriefResult:
    content: str
    path: str
    exists: bool
    content_hash: str

    def to_dict(self) -> dict[str, object]:
        """Return the brief as a JSON-ready read model."""
        return {
            "content": self.content,
            "path": self.path,
            "exists": self.exists,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True)
class UpdateInvestmentBriefRequest:
    content: str
    expected_previous_hash: str | None = None


@dataclass(frozen=True)
class UpdateInvestmentBriefResult:
    result: ApplicationResult
    content_hash: str


class ReadInvestmentBriefUseCase:
    name = "read_investment_brief"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ReadInvestmentBriefResult:
        path = self.settings.investment_brief_path
        exists = path.exists()
        content = path.read_text(encoding="utf-8") if exists else ""
        return ReadInvestmentBriefResult(
            content=content,
            path=str(path),
            exists=exists,
            content_hash=_content_hash(content),
        )


class UpdateInvestmentBriefUseCase:
    """Persist the configured brief without accepting arbitrary target paths."""

    name = "update_investment_brief"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: UpdateInvestmentBriefRequest) -> UpdateInvestmentBriefResult:
        path = self.settings.investment_brief_path
        current_content = path.read_text(encoding="utf-8") if path.exists() else ""
        current_hash = _content_hash(current_content)
        if (
            request.expected_previous_hash is not None
            and request.expected_previous_hash != current_hash
        ):
            return UpdateInvestmentBriefResult(
                result=ApplicationResult(
                    name=self.name,
                    status="failed",
                    message="Investment brief changed since it was loaded; reload before saving.",
                    artifacts={
                        "path": str(path),
                        "content_hash": current_hash,
                    },
                ),
                content_hash=current_hash,
            )

        _write_text_atomically(path, request.content)
        updated_hash = _content_hash(request.content)
        return UpdateInvestmentBriefResult(
            result=ApplicationResult(
                name=self.name,
                status="succeeded",
                message="Investment brief updated.",
                artifacts={
                    "path": str(path),
                    "content_hash": updated_hash,
                },
            ),
            content_hash=updated_hash,
        )


def _content_hash(content: str) -> str:
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def _write_text_atomically(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary_path.write_text(content, encoding="utf-8")
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


__all__ = [
    "ReadInvestmentBriefResult",
    "ReadInvestmentBriefUseCase",
    "UpdateInvestmentBriefRequest",
    "UpdateInvestmentBriefResult",
    "UpdateInvestmentBriefUseCase",
]
