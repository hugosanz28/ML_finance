"""Focused tests for secret-safe provider audit metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json

import pytest

from src.agents import langchain_provider
from src.agents import provider_audit
from src.agents.analista_activos.llm import OpenAIAssetLLMProvider
from src.agents.asistente_aportacion_mensual.llm import (
    OpenAIContributionLLMProvider,
)
from src.agents.monitor_tematico.llm import (
    OpenAIThemeLLMProvider,
    StaticThemeLLMProvider,
    ThemeLLMProviderError,
)
from src.agents.orchestration.supervisor import OpenAISupervisorProvider


class _ConfiguredProvider:
    name = "openai"
    model = "test-model"
    timeout_seconds = 15.0
    api_key = "sentinel-secret-api-key"  # pragma: allowlist secret
    arbitrary_internal_option = "must-not-be-persisted"
    endpoint = "https://user:password@example.test/v1?api_key=sentinel-query"  # pragma: allowlist secret


class _FakeResponse:
    def model_dump(self, *, mode: str) -> dict[str, object]:
        assert mode == "json"
        return {
            "id": "resp_test",
            "model": "test-model",
            "output": [{"type": "message", "text": "resultado"}],
            "usage": {"input_tokens": 10, "output_tokens": 4},
        }


@pytest.mark.parametrize(
    "provider_type",
    [
        OpenAISupervisorProvider,
        OpenAIThemeLLMProvider,
        OpenAIAssetLLMProvider,
        OpenAIContributionLLMProvider,
    ],
)
def test_openai_providers_read_reasoning_effort_from_environment(monkeypatch, provider_type) -> None:
    monkeypatch.setenv("OPENAI_REASONING_EFFORT", " HIGH ")
    provider = provider_type(model="gpt-6-sol", api_key="test-key")  # pragma: allowlist secret

    assert provider.reasoning_effort == "high"
    assert provider_audit.provider_audit_config(provider, role="llm")["options"]["reasoning_effort"] == "high"


def test_reasoning_effort_is_optional_and_invalid_values_fail_fast() -> None:
    assert langchain_provider.parse_openai_reasoning_effort(None) is None
    assert langchain_provider.parse_openai_reasoning_effort(" ") is None
    with pytest.raises(ValueError, match="Invalid OPENAI_REASONING_EFFORT"):
        langchain_provider.parse_openai_reasoning_effort("turbo")


def test_openai_provider_uses_repo_env_when_process_variable_is_absent(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_REASONING_EFFORT", raising=False)
    monkeypatch.setattr(
        "src.agents.monitor_tematico.llm._repo_env_values",
        lambda: {"OPENAI_REASONING_EFFORT": "high"},
    )

    provider = OpenAIThemeLLMProvider(model="gpt-6-sol", api_key="test-key")  # pragma: allowlist secret

    assert provider.reasoning_effort == "high"


def test_structured_responses_pass_configured_reasoning_effort(monkeypatch) -> None:
    captured: dict = {}

    class Runnable:
        def invoke(self, messages):
            return {"raw": None, "parsed": {"ok": True}, "parsing_error": None}

    class Model:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def with_structured_output(self, schema, **kwargs):
            return Runnable()

    monkeypatch.setenv("OPENAI_REASONING_EFFORT", "high")
    monkeypatch.setattr(langchain_provider, "ChatOpenAI", Model)
    provider = OpenAIThemeLLMProvider(model="gpt-6-sol", api_key="test-key")  # pragma: allowlist secret

    assert langchain_provider.call_openai_structured(
        provider,
        system_prompt="system",
        user_payload={"input": "value"},
        schema_name="test_schema",
        schema={"type": "object"},
    ) == {"ok": True}
    assert captured["use_responses_api"] is True
    assert captured["reasoning"] == {"effort": "high"}

    # Leaving the variable unset must preserve the model's native default.
    provider.reasoning_effort = None
    provider._langchain_model = None
    captured.clear()
    langchain_provider.call_openai_structured(
        provider,
        system_prompt="system",
        user_payload={"input": "value"},
        schema_name="test_schema",
        schema={"type": "object"},
    )
    assert "reasoning" not in captured


def test_provider_audit_config_only_includes_allowlisted_fields() -> None:
    payload = provider_audit.provider_audit_config(_ConfiguredProvider(), role="llm")

    assert payload == {
        "role": "llm",
        "provider": "openai",
        "model": "test-model",
        "options": {
            "timeout_seconds": 15.0,
            "endpoint": "https://example.test/v1",
            "response_format": "json_schema",
        },
    }
    serialized = json.dumps(payload)
    assert "api_key" not in serialized
    assert "sentinel-secret-api-key" not in serialized
    assert "sentinel-query" not in serialized
    assert "password" not in serialized
    assert "arbitrary_internal_option" not in serialized

    class NoSchemeEndpointProvider(_ConfiguredProvider):
        endpoint = "user:password@example.test/v1?token=sentinel-query"  # pragma: allowlist secret

    no_scheme = provider_audit.provider_audit_config(
        NoSchemeEndpointProvider(),
        role="llm",
    )
    assert no_scheme["options"]["endpoint"] == "example.test/v1"


def test_recorded_model_dump_response_is_exposed_by_audit_snapshot() -> None:
    provider = _ConfiguredProvider()

    provider_audit.record_provider_raw_response(
        provider,
        _FakeResponse(),
        operation="structured_analysis",
    )

    payload = provider_audit.provider_raw_response_audit(provider, role="llm")
    assert payload["status"] == "captured"
    assert payload["reason_code"] is None
    assert payload["responses"] == [
        {
            "operation": "structured_analysis",
            "response": {
                "id": "resp_test",
                "model": "test-model",
                "output": [{"type": "message", "text": "resultado"}],
                "usage": {"input_tokens": 10, "output_tokens": 4},
            },
        }
    ]


def test_recorded_response_redacts_nested_sensitive_keys() -> None:
    class SensitiveResponse:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {
                "id": "resp_sensitive",
                "Authorization": "Bearer sentinel-auth",  # pragma: allowlist secret
                "nested": {
                    "api_key": "sentinel-nested",  # pragma: allowlist secret
                    "apiKey": "sentinel-camel",  # pragma: allowlist secret
                    "token": "sentinel-token",  # pragma: allowlist secret
                    "headers": [
                        ["Authorization", "Bearer sentinel-header"],  # pragma: allowlist secret
                    ],
                    "output_text": "resultado",
                },
            }

    provider = _ConfiguredProvider()
    provider_audit.record_provider_raw_response(
        provider,
        SensitiveResponse(),
        operation="sensitive_test",
    )

    serialized = json.dumps(
        provider_audit.provider_raw_response_audit(provider, role="llm")
    )
    assert "sentinel-auth" not in serialized
    assert "sentinel-nested" not in serialized
    assert "sentinel-camel" not in serialized
    assert "sentinel-token" not in serialized
    assert "sentinel-header" not in serialized
    assert serialized.count("[REDACTED]") == 5


def test_static_provider_has_stable_no_raw_response_reason() -> None:
    payload = provider_audit.provider_raw_response_audit(
        StaticThemeLLMProvider(),
        role="llm",
    )

    assert payload["status"] == "not_captured"
    assert payload["reason_code"] == "deterministic_provider_no_raw_response"
    assert payload["responses"] == []
    assert payload["provider"]["provider"] == "static_llm"


def test_failed_provider_request_uses_stable_reason_without_exception_text() -> None:
    provider = _ConfiguredProvider()
    provider_audit.record_provider_failure(
        provider,
        operation="structured_analysis",
    )

    payload = provider_audit.provider_raw_response_audit(provider, role="llm")

    assert payload["status"] == "not_captured"
    assert payload["reason_code"] == "provider_request_failed_before_response"
    assert payload["failures"] == [
        {
            "operation": "structured_analysis",
            "reason_code": "provider_request_failed_before_response",
        }
    ]


def test_raw_capture_serialization_failure_does_not_escape() -> None:
    class BrokenResponse:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            raise RuntimeError("serialization failed")

    provider = _ConfiguredProvider()

    provider_audit.record_provider_raw_response(
        provider,
        BrokenResponse(),
        operation="structured_analysis",
    )
    payload = provider_audit.provider_raw_response_audit(provider, role="llm")

    assert payload["status"] == "not_captured"
    assert payload["reason_code"] == "raw_response_serialization_failed"


def test_provider_aggregate_reports_every_role_and_partial_capture() -> None:
    llm_provider = _ConfiguredProvider()
    provider_audit.record_provider_raw_response(
        llm_provider,
        _FakeResponse(),
        operation="structured_analysis",
    )

    payload = provider_audit.providers_raw_response_audit(
        {
            "llm": llm_provider,
            "search": StaticThemeLLMProvider(),
        }
    )

    assert payload["status"] == "partial"
    assert payload["reason_code"] == "one_or_more_provider_responses_not_captured"
    assert set(payload["providers"]) == {"llm", "search"}
    assert payload["responses"][0]["role"] == "llm"


@pytest.mark.parametrize(
    "provider_type",
    [
        OpenAIThemeLLMProvider,
        OpenAIAssetLLMProvider,
        OpenAIContributionLLMProvider,
    ],
)
def test_openai_provider_records_sdk_response_after_successful_call(
    provider_type,
) -> None:
    class OpenAIResponse(_FakeResponse):
        output_text = '{"ok": true}'

    class Runnable:
        def invoke(self, messages):
            assert len(messages) == 2
            return {
                "raw": OpenAIResponse(),
                "parsed": {"ok": True},
                "parsing_error": None,
            }

    class Model:
        def with_structured_output(self, schema, **kwargs):
            assert schema["title"] == "audit_schema"
            assert kwargs == {
                "method": "json_schema",
                "include_raw": True,
                "strict": True,
            }
            return Runnable()

    provider = provider_type(
        model="audit-model",
        api_key="sentinel-secret-api-key",  # pragma: allowlist secret
    )
    provider._langchain_model = Model()

    parsed = provider._call_structured(
        system_prompt="system",
        user_payload={"input": "value"},
        schema_name="audit_schema",
        schema={"type": "object"},
    )
    audit = provider_audit.provider_raw_response_audit(provider, role="llm")

    assert parsed == {"ok": True}
    assert audit["status"] == "captured"
    assert audit["responses"][0]["operation"] == "audit_schema"
    assert "sentinel-secret-api-key" not in json.dumps(audit)


def test_openai_provider_records_failure_before_response() -> None:
    class FailingRunnable:
        def invoke(self, messages):
            raise RuntimeError("provider unavailable")

    class Model:
        def with_structured_output(self, schema, **kwargs):
            return FailingRunnable()

    provider = OpenAIThemeLLMProvider(
        model="audit-model",
        api_key="sentinel-secret-api-key",  # pragma: allowlist secret
    )
    provider._langchain_model = Model()

    with pytest.raises(ThemeLLMProviderError, match="OpenAI request failed"):
        provider._call_structured(
            system_prompt="system",
            user_payload={"input": "value"},
            schema_name="audit_schema",
            schema={"type": "object"},
        )

    audit = provider_audit.provider_raw_response_audit(provider, role="llm")
    assert audit["status"] == "not_captured"
    assert audit["reason_code"] == "provider_request_failed_before_response"


def test_openai_provider_classifies_invalid_structured_output_for_graph_repair() -> None:
    class Runnable:
        def invoke(self, messages):
            return {"raw": _FakeResponse(), "parsed": None, "parsing_error": ValueError("invalid")}

    class Model:
        def with_structured_output(self, schema, **kwargs):
            return Runnable()

    provider = OpenAIThemeLLMProvider(
        model="audit-model",
        api_key="test-key",  # pragma: allowlist secret
    )
    provider._langchain_model = Model()

    with pytest.raises(ThemeLLMProviderError) as error:
        provider._call_structured(
            system_prompt="system",
            user_payload={"input": "value"},
            schema_name="audit_schema",
            schema={"type": "object"},
        )

    assert error.value.reason_code == "structured_output_invalid"


@dataclass(frozen=True)
class _NestedAuditValue:
    as_of_date: date
    labels: tuple[str, ...]


def test_json_safe_normalizes_basic_nested_values() -> None:
    payload = provider_audit._json_safe(
        {
            "generated_at": datetime(2026, 7, 30, 12, 30, tzinfo=timezone.utc),
            "nested": _NestedAuditValue(
                as_of_date=date(2026, 7, 30),
                labels=("uno", "dos"),
            ),
        }
    )

    assert payload == {
        "generated_at": "2026-07-30T12:30:00+00:00",
        "nested": {
            "as_of_date": "2026-07-30",
            "labels": ["uno", "dos"],
        },
    }
    json.dumps(payload, allow_nan=False)


def test_json_safe_removes_internal_reasoning_content() -> None:
    payload = provider_audit._json_safe(
        {
            "output": [
                {"type": "reasoning", "summary": "private chain", "encrypted_content": "ciphertext"},
                {"type": "message", "content": "visible answer"},
            ],
            "reasoning_content": "private chain",
        }
    )

    assert payload == {
        "output": [
            {"type": "reasoning", "redacted": True},
            {"type": "message", "content": "visible answer"},
        ]
    }
