"""Shared LangChain adapter for OpenAI Structured Outputs."""

from __future__ import annotations

import json
from typing import Any, Mapping

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langsmith import tracing_context

from src.agents.provider_audit import record_provider_raw_response


class StructuredOutputError(ValueError):
    """The provider responded, but the structured payload could not be validated."""


def call_openai_structured(
    provider: Any,
    *,
    system_prompt: str,
    user_payload: Mapping[str, Any],
    schema_name: str,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Invoke Responses through LangChain and retain the raw message for audit."""
    model = getattr(provider, "_langchain_model", None)
    if model is None:
        model = ChatOpenAI(
            model=provider.model,
            api_key=provider.api_key or None,
            use_responses_api=True,
            store=True,
            max_retries=0,
        )
        provider._langchain_model = model
    resolved_schema = dict(schema)
    resolved_schema.setdefault("title", schema_name)
    runnable = model.with_structured_output(
        resolved_schema,
        method="json_schema",
        include_raw=True,
        strict=True,
    )
    resolved_payload = dict(user_payload)
    delegation_instruction = getattr(provider, "_ml_finance_delegation_instruction", None)
    if delegation_instruction:
        resolved_payload["supervisor_instruction"] = str(delegation_instruction)
        resolved_payload["orchestration_attempt"] = int(
            getattr(provider, "_ml_finance_orchestration_attempt", 1)
        )
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(
            content=(
                "Return JSON that matches the provided schema. "
                f"Schema name: {schema_name}. Input payload:\n"
                f"{json.dumps(resolved_payload, ensure_ascii=False)}"
            )
        ),
    ]
    # The repository owns its local audit trail; do not emit LangSmith traces.
    try:
        with tracing_context(enabled=False):
            response = runnable.invoke(messages)
    except Exception as exc:
        if type(exc).__name__ in {"OutputParserException", "ValidationError"}:
            raise StructuredOutputError(
                f"Structured output parsing failed for {schema_name}."
            ) from exc
        raise
    raw = response.get("raw") if isinstance(response, Mapping) else None
    parsed = response.get("parsed") if isinstance(response, Mapping) else response
    parsing_error = response.get("parsing_error") if isinstance(response, Mapping) else None
    if raw is not None:
        record_provider_raw_response(provider, raw, operation=schema_name)
    if parsing_error is not None:
        raise StructuredOutputError(f"Structured output parsing failed for {schema_name}.")
    if hasattr(parsed, "model_dump"):
        parsed = parsed.model_dump(mode="json")
    if not isinstance(parsed, Mapping):
        raise StructuredOutputError(f"Structured output root for {schema_name} was not an object.")
    return dict(parsed)


__all__ = ["StructuredOutputError", "call_openai_structured"]
