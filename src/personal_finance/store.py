"""Private DuckDB persistence for the editable plan and its revisions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb

from src.personal_finance.model import PersonalPlan


EMPTY_HASH = "sha256:" + hashlib.sha256(b"").hexdigest()


class PlanConflictError(Exception):
    """The caller edited an obsolete version of the local plan."""


def _serialize(plan: PersonalPlan) -> str:
    return json.dumps(plan.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _upgrade_v1(document: str) -> PersonalPlan:
    """Read the former fixed car/house document without rewriting private data on GET."""
    legacy = json.loads(document)
    car = {
        "id": "car", "name": "Coche", "source": "bank",
        "target_cents": legacy.pop("car_target_cents"),
        "due_date": legacy.pop("car_due_date"),
        "contribution_mode": "deadline", "manual_monthly_cents": 0,
        "opening_reserved_cents": legacy.pop("car_opening_reserved_cents"),
        "allocations": legacy.pop("car_allocations"),
    }
    house = {
        "id": "house", "name": "Vivienda", "source": "degiro",
        "target_cents": legacy.pop("house_target_cents") or 0,
        "due_date": legacy.pop("house_due_date"),
        "contribution_mode": "manual",
        "manual_monthly_cents": legacy.pop("house_monthly_cents"),
        "opening_reserved_cents": 0, "allocations": [],
    }
    return PersonalPlan.model_validate({**legacy, "bank_name": "BBVA", "goals": [car, house]})


class PersonalPlanStore:
    def __init__(self, data_dir: Path):
        self.path = data_dir / "personal_finance.duckdb"

    def read(self) -> tuple[PersonalPlan | None, str]:
        # GET is side-effect free, including when no personal plan exists yet.
        if not self.path.is_file():
            return None, EMPTY_HASH
        with duckdb.connect(str(self.path), read_only=True) as db:
            row = db.execute("SELECT schema_version, document, content_hash FROM personal_plan WHERE id = 1").fetchone()
        if row is None:
            return None, EMPTY_HASH
        if row[0] == 1:
            return _upgrade_v1(row[1]), row[2]
        if row[0] != 2:
            raise ValueError("Unsupported personal plan schema")
        return PersonalPlan.model_validate_json(row[1]), row[2]

    def write(self, plan: PersonalPlan, expected_hash: str) -> str:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        document = _serialize(plan)
        updated_hash = _hash(document)
        with duckdb.connect(str(self.path)) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS personal_plan (
                id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL,
                document VARCHAR NOT NULL, content_hash VARCHAR NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS personal_plan_revisions (
                content_hash VARCHAR PRIMARY KEY, saved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                document VARCHAR NOT NULL
            )""")
            db.execute("BEGIN TRANSACTION")
            try:
                row = db.execute("SELECT content_hash FROM personal_plan WHERE id = 1").fetchone()
                current_hash = row[0] if row else EMPTY_HASH
                if current_hash != expected_hash:
                    raise PlanConflictError()
                db.execute("""INSERT INTO personal_plan (id, schema_version, document, content_hash)
                    VALUES (1, 2, ?, ?) ON CONFLICT (id) DO UPDATE SET
                    schema_version = excluded.schema_version, document = excluded.document,
                    content_hash = excluded.content_hash""", [document, updated_hash])
                db.execute("""INSERT INTO personal_plan_revisions (content_hash, document)
                    VALUES (?, ?) ON CONFLICT DO NOTHING""", [updated_hash, document])
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return updated_hash
