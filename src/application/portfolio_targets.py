"""Application boundary for reading and updating portfolio targets."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping

from src.application.local_files import text_content_hash, write_text_atomically
from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.portfolio.targets import load_portfolio_targets, portfolio_targets_from_mapping


DEFAULT_TARGET_WEIGHTS: dict[str, float] = {"core": 0.80, "satellite": 0.20}


@dataclass(frozen=True)
class ReadPortfolioTargetsResult:
    """Complete, JSON-ready view of the configured portfolio targets."""

    portfolio_targets: dict[str, Any] | None
    target_weights: dict[str, float]
    path: str
    exists: bool
    content_hash: str
    validation_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "portfolio_targets": self.portfolio_targets,
            "target_weights": dict(self.target_weights),
            "path": self.path,
            "exists": self.exists,
            "content_hash": self.content_hash,
            "validation_error": self.validation_error,
        }


@dataclass(frozen=True)
class UpdatePortfolioTargetsRequest:
    """Structured target values plus an optional optimistic-lock hash."""

    portfolio_targets: Mapping[str, Any]
    expected_previous_hash: str | None = None


@dataclass(frozen=True)
class UpdatePortfolioTargetsResult:
    result: ApplicationResult
    portfolio_targets: dict[str, Any] | None
    target_weights: dict[str, float]
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        """Return the operation and normalized targets as strict JSON data."""
        return {
            "status": self.result.status,
            "message": self.result.message,
            "warnings": list(self.result.warnings),
            "artifacts": dict(self.result.artifacts),
            "portfolio_targets": self.portfolio_targets,
            "target_weights": dict(self.target_weights),
            "content_hash": self.content_hash,
        }


class ReadPortfolioTargetsUseCase:
    """Read only the configured target file and expose a validated model."""

    name = "read_portfolio_targets"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ReadPortfolioTargetsResult:
        path = self.settings.portfolio_targets_path
        exists = path.exists()
        content = path.read_text(encoding="utf-8") if exists else ""
        content_hash = text_content_hash(content)
        if not exists:
            return ReadPortfolioTargetsResult(
                portfolio_targets=None,
                target_weights={},
                path=str(path),
                exists=False,
                content_hash=content_hash,
            )

        try:
            targets = load_portfolio_targets(settings=self.settings, required=True)
        except (TypeError, ValueError) as exc:
            return ReadPortfolioTargetsResult(
                portfolio_targets=None,
                target_weights={},
                path=str(path),
                exists=True,
                content_hash=content_hash,
                validation_error=str(exc),
            )

        assert targets is not None
        return ReadPortfolioTargetsResult(
            portfolio_targets=targets.to_storage_mapping(),
            target_weights=targets.target_weights(),
            path=str(path),
            exists=True,
            content_hash=content_hash,
        )


class UpdatePortfolioTargetsUseCase:
    """Validate and atomically replace the configured portfolio target file."""

    name = "update_portfolio_targets"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: UpdatePortfolioTargetsRequest) -> UpdatePortfolioTargetsResult:
        path = self.settings.portfolio_targets_path
        current_content = path.read_text(encoding="utf-8") if path.exists() else ""
        current_hash = text_content_hash(current_content)
        if (
            request.expected_previous_hash is not None
            and request.expected_previous_hash != current_hash
        ):
            return self._failed_result(
                message="Portfolio targets changed since they were loaded; reload before saving.",
                content_hash=current_hash,
                path=str(path),
            )

        if not isinstance(request.portfolio_targets, Mapping):
            return self._failed_result(
                message="Portfolio targets must be a structured object.",
                content_hash=current_hash,
                path=str(path),
            )

        try:
            targets = portfolio_targets_from_mapping(
                request.portfolio_targets,
                default_base_currency=self.settings.default_currency,
            )
            normalized = targets.to_storage_mapping()
            serialized = (
                json.dumps(
                    normalized,
                    ensure_ascii=False,
                    indent=2,
                    allow_nan=False,
                    sort_keys=True,
                )
                + "\n"
            )
        except (TypeError, ValueError) as exc:
            return self._failed_result(
                message=f"Invalid portfolio targets: {exc}",
                content_hash=current_hash,
                path=str(path),
            )

        write_text_atomically(path, serialized)
        updated_hash = text_content_hash(serialized)
        return UpdatePortfolioTargetsResult(
            result=ApplicationResult(
                name=self.name,
                status="succeeded",
                message="Portfolio targets updated.",
                artifacts={
                    "path": str(path),
                    "content_hash": updated_hash,
                },
            ),
            portfolio_targets=normalized,
            target_weights=targets.target_weights(),
            content_hash=updated_hash,
        )

    def _failed_result(
        self,
        *,
        message: str,
        content_hash: str,
        path: str,
    ) -> UpdatePortfolioTargetsResult:
        return UpdatePortfolioTargetsResult(
            result=ApplicationResult(
                name=self.name,
                status="failed",
                message=message,
                artifacts={
                    "path": path,
                    "content_hash": content_hash,
                },
            ),
            portfolio_targets=None,
            target_weights={},
            content_hash=content_hash,
        )


__all__ = [
    "DEFAULT_TARGET_WEIGHTS",
    "ReadPortfolioTargetsResult",
    "ReadPortfolioTargetsUseCase",
    "UpdatePortfolioTargetsRequest",
    "UpdatePortfolioTargetsResult",
    "UpdatePortfolioTargetsUseCase",
]
