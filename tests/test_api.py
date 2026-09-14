from dataclasses import replace
import hashlib
import json
import socket

from fastapi.testclient import TestClient
import pytest

from src.api.app import create_app
from src.application.artifact_reads import ReadArtifactRequest, ReadReportUseCase, ReadAgentAuditUseCase
from src.config import default_repo_root, load_settings


def settings_for(path, provider="synthetic"):
    return load_settings(repo_root=path, env_file=path / "absent.env", env={"PRICE_PROVIDER": provider})


def block_external_network(monkeypatch):
    original_connect = socket.socket.connect

    def connect(sock, address):
        # Windows asyncio uses a loopback socketpair for its internal wake-up pipe.
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original_connect(sock, address)
        raise AssertionError("HTTP reads must not use external network")

    def forbidden(*args, **kwargs):
        raise AssertionError("HTTP reads must not resolve external hosts")
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


@pytest.fixture
def workspace(tmp_path):
    return settings_for(tmp_path)


@pytest.fixture
def client(workspace, monkeypatch):
    block_external_network(monkeypatch)
    with TestClient(create_app(settings=workspace), base_url="http://127.0.0.1") as result:
        yield result


def files_snapshot(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


def test_empty_workspace_health_catalog_and_queries_never_create_data(client, workspace):
    assert client.get("/api/v1/health").json() == {"status": "ok", "mode": "read_only", "api_version": "v1"}
    definitions = client.get("/api/v1/analytics/metric-definitions")
    assert definitions.status_code == 200
    metric = definitions.json()["definitions"][0]
    assert client.get(f'/api/v1/analytics/metrics/{metric["metric_id"]}').json() == metric
    assert client.get("/api/v1/reports").json() == {"reports": []}
    assert client.get("/api/v1/agents/runs").json() == {"runs": []}
    state = client.get("/api/v1/portfolio/state")
    assert state.status_code == 404
    assert state.json()["error"]["code"] == "portfolio_data_unavailable"
    for section in ("summary", "performance", "risk", "benchmarks"):
        response = client.get(f"/api/v1/analytics/{section}")
        assert response.status_code == 200
        assert response.json()["reason_code"] == "portfolio_data_unavailable"
    assert not workspace.data_dir.exists()


@pytest.mark.parametrize("query", [
    "period=unknown", "as_of_date=2099-01-01", "as_of_date=2026-02-30", "as_of_date=20260101",
    "risk_free_rate_annual=nan", "risk_free_rate_annual=inf", "risk_free_rate_annual=-1",
    "benchmark_id=unknown", "path=private", "provider=openai", "env_file=private.env", "persist=true",
])
def test_analytics_rejects_bad_or_extra_input(client, query, workspace):
    response = client.get(f"/api/v1/analytics/summary?{query}")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert not workspace.data_dir.exists()


@pytest.mark.parametrize("route", [
    "/portfolio/state?persist=true", "/portfolio/state?include_history=maybe",
    "/portfolio/state?as_of_date=2026-02-30", "/reports?limit=0", "/agents/runs?limit=101",
    "/reports/monthly_bad%5Csecret", "/agents/runs/bad..id",
])
def test_invalid_read_parameters(client, route):
    assert client.get(f"/api/v1{route}").status_code == 422


def test_missing_artifacts_and_no_write_routes(client):
    for path in ("reports/monthly_missing", "agents/runs/missing", "analytics/metrics/missing"):
        assert client.get(f"/api/v1/{path}").status_code == 404
    for method in ("post", "put", "patch", "delete"):
        response = getattr(client, method)("/api/v1/portfolio/state")
        assert response.status_code == 405
        assert response.json()["error"]["code"] == "method_not_allowed"


def test_local_host_origin_and_cache_protection(client):
    for host in ("attacker.example", "[malformed", "localhost@attacker.example", "localhost/other"):
        assert client.get("/api/v1/health", headers={"Host": host}).status_code == 400
    for origin in ("https://attacker.example", "null", "http://localhost:9999"):
        response = client.get("/api/v1/health", headers={"Origin": origin})
        assert response.status_code == 403
        assert "access-control-allow-origin" not in response.headers
    response = client.get("/api/v1/health", headers={"Origin": "http://localhost:5173"})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_openapi_documents_models_and_read_routes(client):
    spec = client.get("/openapi.json").json()
    assert len(spec["paths"]) == 12
    for path in spec["paths"].values():
        assert set(path) == {"get"}
        assert "$ref" in path["get"]["responses"]["200"]["content"]["application/json"]["schema"]
        assert "$ref" in path["get"]["responses"]["422"]["content"]["application/json"]["schema"]
    # No remote Swagger/ReDoc scripts are loaded implicitly.
    assert client.get("/docs").status_code == 404


def test_errors_do_not_echo_inputs_or_exception_text(client, monkeypatch):
    import src.api.app as api
    def malformed(*args, **kwargs):
        raise ValueError("Malformed data in private/location with credentials")
    monkeypatch.setattr(api.GetAnalyticsSummaryUseCase, "execute", malformed)
    response = client.get("/api/v1/analytics/summary")
    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal_error", "message": "Unable to read local data."}}
    assert response.headers["cache-control"] == "no-store"
    assert "private" not in client.get("/api/v1/analytics/summary?path=private").text


def test_reports_and_audits_are_private_read_only_projections(client, workspace):
    workspace.reports_dir.mkdir(parents=True)
    report_id = "monthly_2026-01-31_fixture"
    (workspace.reports_dir / f"{report_id}.md").write_text(
        f"# Fixture\nSource: {workspace.data_dir}\nAuthorization: Bearer credential-fixture-not-real", encoding="utf-8",
    )
    run_dir = workspace.data_dir / "agents" / "monthly_pipeline" / "fixture"
    run_dir.mkdir(parents=True)
    (run_dir / "run_metadata.json").write_text(json.dumps({
        "schema_version": 2, "run_id": "fixture", "as_of_date": "2026-01-31", "execution_status": "blocked",
        "output_dir": str(run_dir), "api_key": "credential-fixture-not-real",
    }), encoding="utf-8")
    (run_dir / "input_payload.json").write_text(json.dumps({"source_path": str(workspace.env_file)}), encoding="utf-8")
    before = files_snapshot(workspace.repo_root)
    assert client.get("/api/v1/reports").json()["reports"] == [{"report_id": report_id}]
    response = client.get(f"/api/v1/reports/{report_id}")
    assert response.status_code == 200
    assert "[LOCAL_PATH]" in response.json()["content_markdown"]
    assert "credential-fixture-not-real" not in response.text
    runs = client.get("/api/v1/agents/runs").json()["runs"]
    assert runs[0]["status"] == "blocked"
    assert "output_dir" not in runs[0]
    audit = client.get("/api/v1/agents/runs/fixture")
    assert audit.status_code == 200
    assert audit.json()["schema_version"] == 2
    assert "credential-fixture-not-real" not in audit.text
    assert "source_path" not in audit.text
    assert "output_dir" not in audit.text
    assert str(workspace.repo_root) not in audit.text
    assert before == files_snapshot(workspace.repo_root)


@pytest.mark.parametrize("use_case", [ReadReportUseCase, ReadAgentAuditUseCase])
def test_artifact_application_rejects_traversal(workspace, use_case):
    for identifier in ("../private", "C:\\private", "/private", ".env", "x:y", "a/b", "a\\b"):
        with pytest.raises(ValueError):
            use_case(settings=workspace).execute(ReadArtifactRequest(identifier))


def test_report_symlink_escape_is_rejected(workspace, tmp_path):
    workspace.reports_dir.mkdir(parents=True)
    outside = tmp_path / "outside.md"
    outside.write_text("not a report", encoding="utf-8")
    link = workspace.reports_dir / "monthly_escape.md"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Creating symlinks requires Windows developer mode or elevated privileges")
    with pytest.raises(FileNotFoundError):
        ReadReportUseCase(settings=workspace).execute(ReadArtifactRequest("monthly_escape"))


def test_synthetic_http_pipeline_no_writes_or_network(tmp_path, monkeypatch):
    from scripts.bootstrap_demo import _upsert_synthetic_prices, DEMO_AS_OF_DATE
    from src.degiro_exports.importer import import_degiro_exports
    from src.degiro_exports.warehouse import load_normalized_degiro_to_duckdb
    from src.market_data.repository import DuckDBMarketDataRepository

    settings = settings_for(tmp_path)
    source = default_repo_root() / "demo" / "synthetic_degiro_exports"
    import_degiro_exports(settings=settings, incoming_dir=source / "incoming",
                         output_dir=settings.normalized_data_dir / "degiro", base_currency="EUR", source_root=source)
    load_normalized_degiro_to_duckdb(settings=settings)
    _upsert_synthetic_prices(DuckDBMarketDataRepository(settings=settings))
    before = files_snapshot(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("Network or schema writes during GET")
    block_external_network(monkeypatch)
    monkeypatch.setattr(DuckDBMarketDataRepository, "ensure_schema", forbidden)
    original_connection = DuckDBMarketDataRepository.connection
    def read_only_connection(repository):
        assert repository.read_only, "All GET database connections must be strictly read-only"
        return original_connection(repository)
    monkeypatch.setattr(DuckDBMarketDataRepository, "connection", read_only_connection)
    with TestClient(create_app(settings=settings), base_url="http://localhost") as client:
        state = client.get("/api/v1/portfolio/state", params={"include_history": "true", "as_of_date": DEMO_AS_OF_DATE.isoformat()})
        assert state.status_code == 200, state.text
        assert state.json()["positions"]
        assert state.json()["history"]
        summary = client.get("/api/v1/analytics/summary", params={"benchmark_id": "portfolio_60_40"})
        assert summary.status_code == 200, summary.text
        data = summary.json()["data"]
        assert data["performance"]["period"]["twr"]["value"] is not None
        assert data["benchmarks"]["comparison"]["comparisons"][0]["provider_name"] == "synthetic"
        for section in ("performance", "risk", "benchmarks"):
            response = client.get(f"/api/v1/analytics/{section}", params={"benchmark_id": "portfolio_60_40"})
            assert response.status_code == 200
            assert response.json()["data"][section] == data[section]
        # Another app with distinct settings cannot see this portfolio.
        with TestClient(create_app(settings=settings_for(tmp_path / "empty")), base_url="http://localhost") as empty:
            assert empty.get("/api/v1/analytics/summary").json()["status"] == "unavailable"
        # Real mode never substitutes synthetic benchmarks or downloads them.
        with TestClient(create_app(settings=replace(settings, price_provider="yfinance")), base_url="http://localhost") as real:
            payload = real.get("/api/v1/analytics/benchmarks?benchmark_id=sp500").json()
            assert payload["data"]["benchmarks"]["reason_code"] == "benchmark_provider_unavailable"
    assert before == files_snapshot(tmp_path)


def test_api_launcher_uses_loopback(monkeypatch, workspace):
    import scripts.run_api as launcher
    import uvicorn
    calls = []
    monkeypatch.setattr("sys.argv", ["run_api.py"])
    monkeypatch.setattr(launcher, "load_settings", lambda **kw: workspace)
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: calls.append(kw))
    launcher.main()
    assert calls == [{"host": "127.0.0.1", "port": 8000, "access_log": False, "proxy_headers": False}]
