"""Focused tests for secret-safe provider audit metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace

import pytest

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

    class Responses:
        def create(self, **kwargs):
            assert kwargs["model"] == "audit-model"
            return OpenAIResponse()

    provider = provider_type(
        model="audit-model",
        api_key="sentinel-secret-api-key",  # pragma: allowlist secret
    )
    provider._client = SimpleNamespace(responses=Responses())

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
    class FailingResponses:
        def create(self, **kwargs):
            raise RuntimeError("provider unavailable")

    provider = OpenAIThemeLLMProvider(
        model="audit-model",
        api_key="sentinel-secret-api-key",  # pragma: allowlist secret
    )
    provider._client = SimpleNamespace(responses=FailingResponses())

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
