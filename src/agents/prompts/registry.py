"""Registry for versioned agent prompts."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROMPTS_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class PromptSpec:
    key: str
    version: str
    filename: str

    @property
    def path(self) -> Path:
        return PROMPTS_DIR / self.filename


PROMPT_REGISTRY: dict[str, PromptSpec] = {
    "monitor_tematico.query": PromptSpec(
        key="monitor_tematico.query",
        version="v1",
        filename="monitor_tematico_query_v1.md",
    ),
    "monitor_tematico.synthesis": PromptSpec(
        key="monitor_tematico.synthesis",
        version="v1",
        filename="monitor_tematico_synthesis_v1.md",
    ),
    "analista_activos.analysis": PromptSpec(
        key="analista_activos.analysis",
        version="v1",
        filename="analista_activos_analysis_v1.md",
    ),
    "asistente_aportacion_mensual.decision": PromptSpec(
        key="asistente_aportacion_mensual.decision",
        version="v1",
        filename="asistente_aportacion_mensual_decision_v1.md",
    ),
}


class PromptRegistryError(KeyError):
    """Raised when a prompt key is not registered or cannot be loaded."""


@lru_cache(maxsize=None)
def load_prompt(key: str) -> str:
    """Load a registered prompt by stable key."""
    try:
        spec = PROMPT_REGISTRY[key]
    except KeyError as exc:
        raise PromptRegistryError(f"Unknown prompt key: {key}") from exc
    if not spec.path.exists():
        raise PromptRegistryError(f"Prompt file not found for {key}: {spec.path}")
    return spec.path.read_text(encoding="utf-8").strip()


def prompt_version(key: str) -> str:
    """Return the current version identifier for a prompt key."""
    try:
        return PROMPT_REGISTRY[key].version
    except KeyError as exc:
        raise PromptRegistryError(f"Unknown prompt key: {key}") from exc


__all__ = [
    "PROMPT_REGISTRY",
    "PROMPTS_DIR",
    "PromptRegistryError",
    "PromptSpec",
    "load_prompt",
    "prompt_version",
]
