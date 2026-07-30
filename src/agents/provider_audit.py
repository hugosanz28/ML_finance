"""Safe, provider-agnostic metadata used by agent audit trails."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
import json
from math import isfinite
from pathlib import Path
import re
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit


_RAW_RESPONSES_ATTRIBUTE = "_ml_finance_audit_raw_responses"
_FAILURES_ATTRIBUTE = "_ml_finance_audit_provider_failures"
_SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "proxy_authorization",
        "bearer_token",
        "access_token",
        "refresh_token",
        "token",
        "client_secret",
        "private_key",
        "secret",
        "password",
        "cookie",
        "set_cookie",
        "headers",
        "request_headers",
        "response_headers",
    }
)


def provider_audit_config(provider: Any, *, role: str) -> dict[str, Any]:
    """Return an allowlisted provider configuration without credentials."""
    name = str(getattr(provider, "name", type(provider).__name__))
    model = getattr(provider, "model", None)
    options: dict[str, Any] = {}
    for attribute in ("timeout_seconds", "search_depth", "endpoint"):
        value = getattr(provider, attribute, None)
        if value is not None:
            options[attribute] = (
                _sanitize_endpoint(value)
                if attribute == "endpoint"
                else _json_safe(value)
            )
    if name == "openai":
        options["response_format"] = "json_schema"
    elif name in {"static", "static_llm", "null"}:
        options["mode"] = "deterministic_offline"
    return {
        "role": role,
        "provider": name,
        "model": str(model) if model is not None else None,
        "options": options,
    }


def record_provider_raw_response(provider: Any, response: Any, *, operation: str) -> None:
    """Capture one response already returned by a provider SDK."""
    try:
        response_payload = _provider_response_payload(response)
        # Validate now so audit serialization can never break a successful agent call.
        json.dumps(response_payload, allow_nan=False)
    except Exception:
        record_provider_failure(
            provider,
            operation=operation,
            reason_code="raw_response_serialization_failed",
        )
        return
    try:
        records = getattr(provider, _RAW_RESPONSES_ATTRIBUTE, None)
        if records is None:
            records = []
            setattr(provider, _RAW_RESPONSES_ATTRIBUTE, records)
        records.append(
            {
                "operation": operation,
                "response": response_payload,
            }
        )
    except Exception:
        record_provider_failure(
            provider,
            operation=operation,
            reason_code="raw_response_storage_failed",
        )


def record_provider_failure(
    provider: Any,
    *,
    operation: str,
    reason_code: str = "provider_request_failed_before_response",
) -> None:
    """Record a failure reason without persisting exception text or credentials."""
    try:
        failures = getattr(provider, _FAILURES_ATTRIBUTE, None)
        if failures is None:
            failures = []
            setattr(provider, _FAILURES_ATTRIBUTE, failures)
        failures.append({"operation": operation, "reason_code": reason_code})
    except Exception:
        # Audit is best-effort and must never replace the provider's real outcome.
        return


def provider_raw_response_audit(provider: Any, *, role: str) -> dict[str, Any]:
    """Return captured responses or a stable reason for their absence."""
    config = provider_audit_config(provider, role=role)
    records = list(getattr(provider, _RAW_RESPONSES_ATTRIBUTE, ()) or ())
    failures = list(getattr(provider, _FAILURES_ATTRIBUTE, ()) or ())
    if records:
        return {
            "schema_version": 2,
            "status": "partial" if failures else "captured",
            "reason_code": (
                "one_or_more_provider_responses_not_captured"
                if failures
                else None
            ),
            "provider": config,
            "responses": _json_safe(records),
            "failures": _json_safe(failures),
        }
    if failures:
        reason_code = str(failures[-1]["reason_code"])
    elif config["provider"] in {"static", "static_llm", "null"}:
        reason_code = "deterministic_provider_no_raw_response"
    elif role == "search":
        reason_code = "provider_contract_returns_normalized_results"
    else:
        reason_code = "provider_contract_no_raw_response"
    return {
        "schema_version": 2,
        "status": "not_captured",
        "reason_code": reason_code,
        "provider": config,
        "responses": [],
        "failures": _json_safe(failures),
    }


def providers_raw_response_audit(providers: Mapping[str, Any]) -> dict[str, Any]:
    """Aggregate raw-response status for every provider role used by one agent."""
    audits = {
        role: provider_raw_response_audit(provider, role=role)
        for role, provider in providers.items()
    }
    statuses = [str(audit["status"]) for audit in audits.values()]
    captured_count = statuses.count("captured")
    if statuses and captured_count == len(statuses):
        status = "captured"
        reason_code = None
    elif any(audit.get("responses") for audit in audits.values()):
        status = "partial"
        reason_code = "one_or_more_provider_responses_not_captured"
    else:
        status = "not_captured"
        reasons = {
            str(audit.get("reason_code"))
            for audit in audits.values()
            if audit.get("reason_code")
        }
        reason_code = (
            next(iter(reasons))
            if len(reasons) == 1
            else "provider_responses_not_captured"
        )
    responses = [
        {"role": role, **dict(response)}
        for role, audit in audits.items()
        for response in audit.get("responses", [])
    ]
    return {
        "schema_version": 2,
        "status": status,
        "reason_code": reason_code,
        "providers": audits,
        "responses": _json_safe(responses),
    }


def _provider_response_payload(response: Any) -> Any:
    model_dump = getattr(response, "model_dump", None)
    if callable(model_dump):
        try:
            return _json_safe(model_dump(mode="json"))
        except TypeError:
            return _json_safe(model_dump())
    to_dict = getattr(response, "to_dict", None)
    if callable(to_dict):
        return _json_safe(to_dict())

    payload = {}
    for attribute in ("id", "model", "created_at", "status", "output_text", "usage", "error"):
        value = getattr(response, attribute, None)
        if value is not None:
            payload[attribute] = _json_safe(value)
    return payload


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): (
                "[REDACTED]"
                if _is_sensitive_key(str(key))
                else _json_safe(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not isfinite(value):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        return _json_safe(item())
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _json_safe(model_dump(mode="json"))
    return value


def redact_sensitive_audit_payload(value: Any) -> Any:
    """Return a JSON-safe copy with credential-shaped keys redacted."""
    return _json_safe(value)


def _is_sensitive_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_")
    compact = normalized.replace("_", "")
    return (
        normalized in _SENSITIVE_KEYS
        or compact
        in {
            "apikey",
            "authorization",
            "proxyauthorization",
            "bearertoken",
            "accesstoken",
            "refreshtoken",
            "clientsecret",
            "privatekey",
            "password",
            "setcookie",
            "headers",
            "requestheaders",
            "responseheaders",
        }
        or normalized.endswith("_api_key")
        or normalized.endswith("_access_token")
        or normalized.endswith("_refresh_token")
        or normalized.endswith("_token")
        or normalized.endswith("_authorization")
        or normalized.endswith("_password")
        or normalized.endswith("_private_key")
        or normalized.endswith("_secret")
    )


def _sanitize_endpoint(value: Any) -> str:
    endpoint = str(value)
    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.netloc:
        sanitized = endpoint.split("?", maxsplit=1)[0].split("#", maxsplit=1)[0]
        return sanitized.rsplit("@", maxsplit=1)[-1]
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    try:
        port = parsed.port
    except ValueError:
        port = None
    netloc = f"{hostname}:{port}" if port is not None else hostname
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


__all__ = [
    "provider_audit_config",
    "provider_raw_response_audit",
    "providers_raw_response_audit",
    "redact_sensitive_audit_payload",
    "record_provider_failure",
    "record_provider_raw_response",
]
