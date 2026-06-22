import pytest

from src.agents.analista_activos import llm as asset_llm
from src.agents.asistente_aportacion_mensual import llm as contribution_llm
from src.agents.monitor_tematico import llm as theme_llm
from src.agents.prompts import PROMPT_REGISTRY, PromptRegistryError, load_prompt, prompt_version


def test_registered_agent_prompts_exist_and_are_non_empty() -> None:
    assert {
        "monitor_tematico.query",
        "monitor_tematico.synthesis",
        "analista_activos.analysis",
        "asistente_aportacion_mensual.decision",
    } <= set(PROMPT_REGISTRY)

    for key, spec in PROMPT_REGISTRY.items():
        text = load_prompt(key)
        assert spec.path.exists()
        assert spec.version == "v1"
        assert prompt_version(key) == "v1"
        assert len(text) > 80


def test_openai_llm_modules_use_versioned_prompts() -> None:
    assert theme_llm._QUERY_SYSTEM_PROMPT == load_prompt("monitor_tematico.query")
    assert theme_llm._SYNTHESIS_SYSTEM_PROMPT == load_prompt("monitor_tematico.synthesis")
    assert asset_llm._ANALYSIS_SYSTEM_PROMPT == load_prompt("analista_activos.analysis")
    assert contribution_llm._DECISION_SYSTEM_PROMPT == load_prompt("asistente_aportacion_mensual.decision")


def test_prompt_registry_rejects_unknown_prompt_key() -> None:
    with pytest.raises(PromptRegistryError, match="Unknown prompt key"):
        load_prompt("missing.prompt")
