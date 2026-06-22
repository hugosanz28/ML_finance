"""Shared result types for application use cases."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, Union


ApplicationStatus = Literal["succeeded", "partial", "failed", "skipped"]
ArtifactValue = Union[str, int, float, bool, Path, None]


@dataclass(frozen=True)
class ApplicationResult:
    """Small, UI-friendly summary returned by application use cases."""

    name: str
    status: ApplicationStatus
    message: str = ""
    warnings: tuple[str, ...] = ()
    artifacts: Mapping[str, ArtifactValue] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status in {"succeeded", "partial", "skipped"}

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    def require_success(self) -> None:
        """Raise when the use case failed."""
        if self.failed:
            raise RuntimeError(self.message or f"Application use case failed: {self.name}")


def succeeded(name: str, message: str = "", **artifacts: ArtifactValue) -> ApplicationResult:
    return ApplicationResult(name=name, status="succeeded", message=message, artifacts=artifacts)


def failed(name: str, message: str = "", **artifacts: ArtifactValue) -> ApplicationResult:
    return ApplicationResult(name=name, status="failed", message=message, artifacts=artifacts)


def partial(
    name: str,
    message: str = "",
    *,
    warnings: tuple[str, ...] = (),
    **artifacts: ArtifactValue,
) -> ApplicationResult:
    return ApplicationResult(name=name, status="partial", message=message, warnings=warnings, artifacts=artifacts)


def skipped(name: str, message: str = "", **artifacts: ArtifactValue) -> ApplicationResult:
    return ApplicationResult(name=name, status="skipped", message=message, artifacts=artifacts)


__all__ = [
    "ApplicationResult",
    "ApplicationStatus",
    "ArtifactValue",
    "failed",
    "partial",
    "skipped",
    "succeeded",
]
