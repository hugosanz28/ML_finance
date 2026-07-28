import pytest

from src.agents.analista_activos import llm as asset_llm
from src.agents.asistente_aportacion_mensual import llm as contribution_llm
from src.agents.monitor_tematico import llm as theme_llm
from src.agents.prompts import PROMPT_REGISTRY, PromptRegistryError, load_prompt, prompt_version
from src.config import default_repo_root


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


@pytest.mark.parametrize(
    "prompt_key",
    (
        "monitor_tematico.query",
        "monitor_tematico.synthesis",
        "analista_activos.analysis",
        "asistente_aportacion_mensual.decision",
    ),
)
def test_agent_prompts_take_the_mandate_from_investment_brief(prompt_key: str) -> None:
    prompt = load_prompt(prompt_key).lower()

    assert "investment_brief" in prompt
    assert "entrada de vivienda" not in prompt
    assert "3-4 anos" not in prompt


def test_agent_runtime_does_not_hardcode_the_example_mandate() -> None:
    agents_dir = default_repo_root() / "src" / "agents"
    source = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in agents_dir.rglob("*")
        if path.suffix in {".py", ".md"} and "__pycache__" not in path.parts
    )

    assert "entrada de vivienda" not in source
    assert "3-4 anos" not in source
    assert "housing down-payment" not in source


def test_prompt_registry_rejects_unknown_prompt_key() -> None:
    with pytest.raises(PromptRegistryError, match="Unknown prompt key"):
        load_prompt("missing.prompt")
