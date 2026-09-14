import base64
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import shutil
from threading import Event
import time

import duckdb
from fastapi.testclient import TestClient
import pytest

from src.api.app import create_app
from src.application.local_jobs import LocalJobManager, SubmitJobRequest, RetryJobRequest
from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.application.operations import ExecuteOperationRequest
from src.config import default_repo_root, load_settings


CONFIRM = {"workspace_mode": "demo", "confirm": True}
HEADERS = {"X-ML-Finance-Confirm": "local-write", "Idempotency-Key": "request-0001"}


def settings_for(root, mode="demo"):
    return load_settings(repo_root=root, env_file=root / "absent.env", env={
        "DATA_DIR": "demo/local_data" if mode == "demo" else "src/data/local",
        "PRICE_PROVIDER": "synthetic" if mode == "demo" else "yfinance",
    })


def snapshot(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file() and p.name != "api-worker.lock"}


@pytest.fixture
def manager(tmp_path):
    jobs = LocalJobManager(OperationalWorkspace(settings_for(tmp_path), "demo"))
    jobs.open(start_worker=False)
    try:
        yield jobs
    finally:
        jobs.close()


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(settings=settings_for(tmp_path), workspace_mode="demo"), base_url="http://localhost") as value:
        yield value


def finished(client, job_id):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200, response.text
        if response.json()["state"] not in {"pending", "running"}:
            return response.json()
        time.sleep(.02)
    pytest.fail("Local job did not complete")


def submit(client, route, body, key="request-0001", method="post"):
    response = getattr(client, method)(f"/api/v1/{route}", json={**CONFIRM, **body},
                                      headers={**HEADERS, "Idempotency-Key": key})
    assert response.status_code == 202, response.text
    return finished(client, response.json()["job_id"])


def test_job_states_progress_and_private_errors(manager):
    pending = manager.submit(SubmitJobRequest("report", CONFIRM, "job-00001"))
    assert pending["state"] == "pending"
    def execute(request, progress):
        assert manager.get(pending["job_id"])["state"] == "running"
        progress(50, "test_stage")
        assert manager.get(pending["job_id"])["progress"] == 50
        return {"status": "partial", "warnings": ["coverage_partial"], "artifacts": {"report_id": "fixture"}}
    manager.executor.execute = execute
    assert manager.run_one()
    result = manager.get(pending["job_id"])
    assert result["state"] == "partial"
    assert result["warnings"] == ["coverage_partial"]
    assert result["progress"] == 100
    assert result["started_at"] and result["finished_at"]
    pending = manager.submit(SubmitJobRequest("report", CONFIRM, "job-00002"))
    def broken(*args):
        raise RuntimeError("private path and credential fixture")
    manager.executor.execute = broken
    manager.run_one()
    result = manager.get(pending["job_id"])
    assert result["state"] == "failed"
    assert result["error_code"] == "operation_failed"
    assert "credential" not in json.dumps(result)


def test_concurrent_duplicate_submission_executes_once(manager):
    request = SubmitJobRequest("report", CONFIRM, "job-once-001")
    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(lambda _: manager.submit(request), range(8)))
    assert len({item["job_id"] for item in records}) == 1
    with pytest.raises(OperationError, match="idempotency_conflict"):
        manager.submit(SubmitJobRequest("report", {**CONFIRM, "as_of_date": "2026-01-01"}, request.idempotency_key))
    calls = []
    manager.executor.execute = lambda *args: calls.append(1) or {"status": "succeeded", "warnings": []}
    assert manager.run_one()
    assert not manager.run_one()
    assert len(calls) == 1
    assert manager.submit(request)["state"] == "succeeded"


def test_restart_marks_inflight_failed_without_replaying(tmp_path):
    workspace = OperationalWorkspace(settings_for(tmp_path), "demo")
    first = LocalJobManager(workspace)
    first.open(start_worker=False)
    record = first.submit(SubmitJobRequest("agents", CONFIRM, "agent-once-01"))
    first.close()
    # Simulate a process crash after persisting running but before persisting its result.
    record.update(state="running", started_at="2026-01-01T00:00:00Z")
    with duckdb.connect(str(first.path)) as db:
        LocalJobManager._save(db, record)
    second = LocalJobManager(OperationalWorkspace(settings_for(tmp_path), "demo"))
    second.open(start_worker=False)
    try:
        recovered = second.get(record["job_id"])
        assert recovered["state"] == "failed"
        assert recovered["error_code"] == "worker_interrupted"
        assert second.submit(SubmitJobRequest("agents", CONFIRM, "agent-once-01"))["job_id"] == record["job_id"]
        assert not second.run_one()
    finally:
        second.close()


def test_single_worker_lock_and_workspace_validation(manager, tmp_path):
    other = LocalJobManager(OperationalWorkspace(settings_for(tmp_path), "demo"))
    with pytest.raises(OperationError, match="workspace_already_in_use"):
        other.open(start_worker=False)
    with pytest.raises(OperationError, match="workspace_path_not_allowed"):
        OperationalWorkspace(settings_for(tmp_path), "real")


def test_http_returns_202_while_running_and_reads_are_busy(client):
    entered, release = Event(), Event()
    manager = client.app.state.jobs
    def execute(*args):
        entered.set()
        assert release.wait(5)
        return {"status": "succeeded", "warnings": []}
    manager.executor.execute = execute
    try:
        response = client.post("/api/v1/reports/monthly", json=CONFIRM, headers=HEADERS)
        assert response.status_code == 202
        assert entered.wait(2)
        job = client.get(f'/api/v1/jobs/{response.json()["job_id"]}').json()
        assert job["state"] == "running"
        assert client.get("/api/v1/health").status_code == 200
        assert client.get("/api/v1/portfolio/state").json()["error"]["code"] == "workspace_busy"
        assert client.get("/api/v1/reports").status_code == 409
    finally:
        release.set()
    assert finished(client, job["job_id"])["state"] == "succeeded"


def test_brief_and_targets_optimistic_concurrency(client):
    old = client.get("/api/v1/settings/investment-brief").json()
    job = submit(client, "settings/investment-brief", {"content": "My fixture mandate", "expected_previous_hash": old["content_hash"]}, method="put")
    assert job["state"] == "succeeded"
    stale = submit(client, "settings/investment-brief", {"content": "stale", "expected_previous_hash": old["content_hash"]}, key="request-0002", method="put")
    assert stale["error_code"] == "content_conflict"
    assert client.get("/api/v1/settings/investment-brief").json()["content"] == "My fixture mandate"
    old = client.get("/api/v1/settings/portfolio-targets").json()
    job = submit(client, "settings/portfolio-targets", {"portfolio_targets": {"target_allocation": {"core": 1}},
                 "expected_previous_hash": old["content_hash"]}, key="request-0003", method="put")
    assert job["state"] == "succeeded", job
    stale = submit(client, "settings/portfolio-targets", {"portfolio_targets": {"target_allocation": {"core": 1}},
                   "expected_previous_hash": old["content_hash"]}, key="request-0004", method="put")
    assert stale["error_code"] == "content_conflict"


@pytest.mark.parametrize("body,code", [
    ({"workspace_mode": "real", "confirm": True}, "workspace_mode_mismatch"),
    ({"workspace_mode": "demo", "confirm": False}, "invalid_request"),
    ({**CONFIRM, "incoming_dir": "private"}, "invalid_request"),
])
def test_invalid_writes_never_queue(client, body, code):
    response = client.post("/api/v1/degiro/import", json=body, headers=HEADERS)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == code
    assert client.get("/api/v1/jobs").json() == {"jobs": []}


def test_csrf_content_type_idempotency_and_cors(client):
    assert client.post("/api/v1/degiro/import", json=CONFIRM).status_code == 403
    assert client.post("/api/v1/degiro/import", json=CONFIRM, headers={**HEADERS, "Origin": "https://attacker.example"}).status_code == 403
    assert client.post("/api/v1/degiro/import", content="{}", headers=HEADERS).status_code == 415
    assert client.post("/api/v1/degiro/import", json=CONFIRM, headers={"X-ML-Finance-Confirm": "local-write"}).status_code == 422
    response = client.options("/api/v1/degiro/import", headers={"Origin": "http://localhost:5173",
        "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type,idempotency-key,x-ml-finance-confirm"})
    assert response.status_code == 200


def test_chunked_body_limit(client, monkeypatch):
    monkeypatch.setattr("src.api.body_limit.MAX_BODY_BYTES", 32)
    response = client.post("/api/v1/degiro/import", content=iter([b"x" * 20, b"x" * 20]),
                           headers={**HEADERS, "Content-Type": "application/json", "Origin": "http://localhost:5173"})
    assert response.status_code == 413
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"
    assert client.get("/api/v1/jobs").json() == {"jobs": []}


def test_provider_selection_and_demo_restrictions(client):
    assert client.post("/api/v1/market-data/refresh", json=CONFIRM, headers=HEADERS).status_code == 422
    response = client.post("/api/v1/agents/monthly-runs", json={**CONFIRM, "llm_provider": "openai"}, headers=HEADERS)
    assert response.json()["error"]["code"] == "external_provider_forbidden_in_demo"
    request = client.post("/api/v1/market-data/refresh", json={**CONFIRM, "fx_provider": "synthetic", "price_provider": "yfinance"}, headers=HEADERS)
    assert request.status_code == 422
    assert client.get("/api/v1/jobs").json() == {"jobs": []}


def test_uploads_canonical_names_limits_and_no_overwrite(client, monkeypatch):
    values = {"uploaded_at": "2026-01-31", "uploads": [{"filename": "cartera.csv", "content_base64": base64.b64encode(b"fixture").decode()}]}
    job = submit(client, "degiro/uploads", values)
    assert job["state"] == "succeeded"
    assert job["result"]["uploads"][0]["saved_as"] == "portfolio_2026-01-31.csv"
    values["uploads"][0]["content_base64"] = base64.b64encode(b"different").decode()
    conflict = submit(client, "degiro/uploads", values, key="request-0002")
    assert conflict["error_code"] == "upload_already_exists"
    values["uploads"][0]["filename"] = "../../cartera.csv"
    assert client.post("/api/v1/degiro/uploads", json={**CONFIRM, **values}, headers=HEADERS).status_code == 422
    values["uploads"][0]["filename"] = "cartera.csv"
    monkeypatch.setattr("src.application.operations.MAX_UPLOAD_BYTES", 1)
    assert client.post("/api/v1/degiro/uploads", json={**CONFIRM, **values}, headers=HEADERS).status_code == 413


def test_agent_operation_uses_offline_defaults_and_keeps_preflight(manager, monkeypatch):
    import src.application.operations as operations
    from src.application.types import ApplicationResult
    from types import SimpleNamespace
    calls = []
    def execute(self, request):
        calls.append(request)
        return SimpleNamespace(result=ApplicationResult(name="run_monthly_agents", status="failed", artifacts={"preflight_status": "failed"}))
    monkeypatch.setattr(operations.RunMonthlyAgentsUseCase, "execute", execute)
    result = manager.executor.execute(ExecuteOperationRequest("agents", CONFIRM))
    assert result["status"] == "failed"
    assert calls[0].llm_provider == "static"
    assert calls[0].search_provider == "null"
    assert calls[0].portfolio_metrics_snapshot is None
    assert calls[0].persist is True


def test_demo_pipeline_keeps_fixtures_unchanged(tmp_path, monkeypatch):
    from scripts.bootstrap_demo import _upsert_synthetic_prices
    from src.market_data.repository import DuckDBMarketDataRepository
    import socket

    source = default_repo_root() / "demo"
    for folder in ("synthetic_config", "synthetic_degiro_exports"):
        shutil.copytree(source / folder, tmp_path / "demo" / folder)
    before = {folder: snapshot(tmp_path / "demo" / folder) for folder in ("synthetic_config", "synthetic_degiro_exports")}
    settings = settings_for(tmp_path)
    app = create_app(settings=settings, workspace_mode="demo")
    with TestClient(app, base_url="http://localhost") as client:
        original = socket.socket.connect
        def offline(sock, address):
            if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
                return original(sock, address)
            raise AssertionError("Unexpected external connection")
        monkeypatch.setattr(socket.socket, "connect", offline)
        assert submit(client, "degiro/import", {}, key="pipeline-import")["state"] == "succeeded"
        _upsert_synthetic_prices(DuckDBMarketDataRepository(settings=app.state.jobs.workspace.settings))
        refresh = submit(client, "market-data/refresh", {"fx_provider": "synthetic", "price_provider": "synthetic"}, key="pipeline-refresh")
        assert refresh["state"] in {"succeeded", "partial"}, refresh
        report = submit(client, "reports/monthly", {}, key="pipeline-report")
        assert report["state"] == "succeeded", report
        simulation = submit(client, "portfolio/contributions/simulate", {"contribution_amount": 600}, key="pipeline-simulation")
        assert simulation["state"] in {"succeeded", "partial"}, simulation
        agents = submit(client, "agents/monthly-runs", {}, key="pipeline-agents")
        assert agents["state"] in {"succeeded", "partial"}, agents
        assert agents["result"]["artifacts"]["run_id"]
        # Job GETs do not mutate the persistent store or copy upload/brief inputs to HTTP.
        data_before = snapshot(app.state.jobs.workspace.root)
        jobs = client.get("/api/v1/jobs").json()["jobs"]
        assert len(jobs) == 5
        assert all("parameters" not in item for item in jobs)
        assert data_before == snapshot(app.state.jobs.workspace.root)
    for folder in before:
        assert before[folder] == snapshot(tmp_path / "demo" / folder)


def test_default_app_remains_read_only_and_unchanged(tmp_path):
    settings = settings_for(tmp_path)
    with TestClient(create_app(settings=settings), base_url="http://localhost") as client:
        assert client.post("/api/v1/degiro/import", json=CONFIRM, headers=HEADERS).status_code == 404
        assert client.get("/api/v1/health").json()["mode"] == "read_only"
    assert not settings.data_dir.exists()


def test_retries_only_reexecute_failed_simulations(manager):
    manager.executor.execute = lambda *args: {"status": "failed", "warnings": []}
    record = manager.submit(SubmitJobRequest("simulation", CONFIRM, "simulation-001"))
    manager.run_one()
    retry = manager.retry(RetryJobRequest(record["job_id"], "simulation-002", "demo", True))
    assert retry["job_id"] != record["job_id"]
    assert manager.retry(RetryJobRequest(record["job_id"], "simulation-002", "demo", True))["job_id"] == retry["job_id"]
    with pytest.raises(OperationError, match="retry_requires_new_key"):
        manager.retry(RetryJobRequest(record["job_id"], "simulation-001", "demo", True))
    agent = manager.submit(SubmitJobRequest("agents", CONFIRM, "agents-00001"))
    manager.run_one()
    manager.run_one()
    with pytest.raises(OperationError, match="retry_not_safe"):
        manager.retry(RetryJobRequest(agent["job_id"], "agents-00002", "demo", True))


def test_queue_limit_and_persistent_read_contract(manager):
    for i in range(20):
        manager.submit(SubmitJobRequest("report", CONFIRM, f"queue-key-{i:04}"))
    with pytest.raises(OperationError, match="job_queue_full"):
        manager.submit(SubmitJobRequest("report", CONFIRM, "queue-overflow"))
    before = hashlib.sha256(manager.path.read_bytes()).hexdigest()
    records = manager.list(10)["jobs"]
    assert len(records) == 10
    assert manager.get(records[0]["job_id"])
    assert before == hashlib.sha256(manager.path.read_bytes()).hexdigest()
    assert "idempotency_key" not in records[0]
    assert "parameters" not in records[0]


def test_wrong_output_paths_rejected_before_startup(tmp_path):
    settings = replace(settings_for(tmp_path), reports_dir=tmp_path / "outside")
    with pytest.raises(OperationError, match="workspace_path_not_allowed"):
        create_app(settings=settings, workspace_mode="demo")
    assert not settings.data_dir.exists()


def test_real_mode_requires_explicit_real_market_providers(tmp_path):
    settings = settings_for(tmp_path, "real")
    with TestClient(create_app(settings=settings, workspace_mode="real"), base_url="http://localhost") as client:
        response = client.post("/api/v1/market-data/refresh", json={"workspace_mode": "real", "confirm": True,
                               "fx_provider": "synthetic", "price_provider": "synthetic"}, headers=HEADERS)
        assert response.json()["error"]["code"] == "synthetic_requires_demo"
        assert client.get("/api/v1/jobs").json() == {"jobs": []}


def test_http_retries_openapi_and_invalid_targets(client):
    failed = submit(client, "portfolio/contributions/simulate", {})
    assert failed["state"] == "failed"
    response = client.post(f'/api/v1/jobs/{failed["job_id"]}/retry', json=CONFIRM,
                           headers={**HEADERS, "Idempotency-Key": "retry-000001"})
    assert response.status_code == 202
    finished(client, response.json()["job_id"])
    old = client.get("/api/v1/settings/portfolio-targets").json()
    job = submit(client, "settings/portfolio-targets", {"portfolio_targets": {"target_allocation": {"core": -1}},
                 "expected_previous_hash": old["content_hash"]}, key="bad-targets-01", method="put")
    assert job["error_code"] == "invalid_portfolio_targets"
    assert client.get("/api/v1/settings/portfolio-targets").json() == old
    spec = client.get("/openapi.json").json()
    operation = spec["paths"]["/api/v1/degiro/import"]["post"]
    assert "$ref" in operation["responses"]["202"]["content"]["application/json"]["schema"]
    assert operation["parameters"][0]["name"] == "Idempotency-Key"
