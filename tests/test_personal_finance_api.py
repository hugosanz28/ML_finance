"""Personal plans persist locally without requiring any DEGIRO data."""

import time
import json
from types import SimpleNamespace

import duckdb

from fastapi.testclient import TestClient

from src.api.app import create_app
from src.application.personal_plan import PreviewPersonalPlanRequest, PreviewPersonalPlanUseCase
from src.config import load_settings
from src.personal_finance.model import PersonalPlan


def settings_for(root):
    return load_settings(repo_root=root, env_file=root / "absent.env", env={
        "DATA_DIR": "demo/local_data", "PRICE_PROVIDER": "synthetic",
    })


def example_plan():
    return {
        "payday_day": 1,
        "net_salary_cents": 200_000,
        "bank_name": "BBVA",
        "bank_balance_cents": 500_000,
        "bank_balance_date": "2026-09-27",
        "emergency_reserved_cents": 0,
        "immediate_commitments_cents": 0,
        "goals": [{
            "id": "car", "name": "Coche", "source": "bank",
            "target_cents": 1_000_000, "due_date": "2027-09-27",
            "contribution_mode": "deadline", "manual_monthly_cents": 0,
            "opening_reserved_cents": 400_000, "allocations": [],
        }, {
            "id": "home", "name": "Vivienda", "source": "degiro",
            "target_cents": 3_000_000, "due_date": None,
            "contribution_mode": "manual", "manual_monthly_cents": 40_000,
            "opening_reserved_cents": 0, "allocations": [],
        }],
        "expenses": [{
            "id": "home_food", "name": "Casa y comida", "kind": "fixed",
            "monthly_cents": 60_000, "due_day": 5,
        }],
        "actual_expenses": [],
        "actual_income": [],
    }


def submit(client, plan, revision, key):
    response = client.put("/api/v1/planning/plan", json={
        "workspace_mode": "demo", "confirm": True,
        "plan": plan, "expected_previous_hash": revision,
    }, headers={"X-ML-Finance-Confirm": "local-write", "Idempotency-Key": key})
    assert response.status_code == 202, response.text
    job_id = response.json()["job_id"]
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job = client.get(f"/api/v1/jobs/{job_id}").json()
        if job["state"] not in {"pending", "running"}:
            return job
        time.sleep(.02)
    raise AssertionError("Planning job did not finish")


def test_plan_is_optional_private_and_persistent(tmp_path):
    settings = settings_for(tmp_path)
    path = settings.data_dir / "personal_finance.duckdb"
    with TestClient(create_app(settings=settings), base_url="http://localhost") as reader:
        empty = reader.get("/api/v1/planning/plan")
        assert empty.status_code == 200
        assert empty.json()["configured"] is False
        preview = reader.post("/api/v1/planning/preview", json=example_plan())
        assert preview.status_code == 200
        assert preview.json()["unallocated_bank_cents"] == 100_000
        assert reader.get("/api/v1/portfolio/state").status_code == 404
    assert not path.exists()

    with TestClient(create_app(settings=settings, workspace_mode="demo"), base_url="http://localhost") as client:
        assert client.post("/api/v1/planning/preview", json=example_plan()).status_code == 200
        first = client.get("/api/v1/planning/plan").json()
        job = submit(client, example_plan(), first["content_hash"], "plan-save-001")
        assert job["state"] == "succeeded"
        saved = client.get("/api/v1/planning/plan").json()
        assert saved["configured"] is True
        assert saved["plan"]["goals"][0]["opening_reserved_cents"] == 400_000
        assert saved["summary"]["unallocated_bank_cents"] == 100_000
        assert saved["summary"]["goals"][0]["current_cents"] == 400_000
        assert saved["summary"]["goals"][1]["current_cents"] is None
        assert client.get("/api/v1/portfolio/state").status_code == 404
        duplicate = submit(client, example_plan(), first["content_hash"], "plan-save-001")
        assert duplicate["job_id"] == job["job_id"]
        stale = submit(client, example_plan(), first["content_hash"], "plan-save-002")
        assert stale["state"] == "failed"
        assert stale["error_code"] == "content_conflict"

    with TestClient(create_app(settings=settings), base_url="http://localhost") as reader:
        saved_again = reader.get("/api/v1/planning/plan").json()
        assert saved_again["configured"] is True
        assert saved_again["content_hash"] == saved["content_hash"]


def test_plan_validation_does_not_echo_private_values(tmp_path):
    with TestClient(create_app(settings=settings_for(tmp_path), workspace_mode="demo"), base_url="http://localhost") as client:
        broken = example_plan()
        broken["actual_expenses"] = [{"id": "private value"}]
        response = client.put("/api/v1/planning/plan", json={
            "workspace_mode": "demo", "confirm": True,
            "plan": broken, "expected_previous_hash": "sha256:" + "0" * 64,
        }, headers={"X-ML-Finance-Confirm": "local-write", "Idempotency-Key": "plan-invalid-001"})
        assert response.status_code == 422
        assert "private value" not in response.text


def test_only_selected_goal_uses_degiro_valuation(tmp_path, monkeypatch):
    # A cached portfolio value belongs to the one linked goal, never to bank reserves.
    monkeypatch.setattr("src.application.personal_plan.GetPortfolioStateUseCase.execute", lambda *_: SimpleNamespace(
        summary={"total_market_value_base": 12_345.67},
        as_of_date="2026-09-26", data_quality={"warnings": []},
    ))
    plan = PersonalPlan.model_validate(example_plan())
    summary = PreviewPersonalPlanUseCase(settings=settings_for(tmp_path)).execute(
        PreviewPersonalPlanRequest(plan)
    )
    assert summary["portfolio_value_cents"] == 1_234_567
    assert summary["portfolio_as_of_date"] == "2026-09-26"
    assert summary["goals"][0]["current_cents"] == 400_000
    assert summary["goals"][1]["current_cents"] == 1_234_567
    assert summary["unallocated_bank_cents"] == 100_000


def test_legacy_fixed_goals_migrate_on_read_without_writing(tmp_path):
    settings = settings_for(tmp_path)
    path = settings.data_dir / "personal_finance.duckdb"
    path.parent.mkdir(parents=True)
    legacy = example_plan()
    legacy.pop("goals")
    legacy.update({
        "car_target_cents": 1_000_000, "car_due_date": "2027-09-27",
        "car_opening_reserved_cents": 400_000, "car_allocations": [],
        "house_target_cents": 3_000_000, "house_due_date": None,
        "house_monthly_cents": 40_000,
    })
    with duckdb.connect(str(path)) as db:
        db.execute("CREATE TABLE personal_plan (id INTEGER PRIMARY KEY, schema_version INTEGER, document VARCHAR, content_hash VARCHAR)")
        db.execute("INSERT INTO personal_plan VALUES (1, 1, ?, ?)", [json.dumps(legacy), "sha256:" + "a" * 64])
    with TestClient(create_app(settings=settings), base_url="http://localhost") as client:
        response = client.get("/api/v1/planning/plan")
        assert response.status_code == 200
        body = response.json()
        assert [goal["name"] for goal in body["plan"]["goals"]] == ["Coche", "Vivienda"]
        assert body["content_hash"] == "sha256:" + "a" * 64
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT schema_version FROM personal_plan").fetchone()[0] == 1
