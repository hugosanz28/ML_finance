"""Persistent local jobs with one worker, explicit recovery and deduplicated submissions."""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from threading import Event, Lock, Thread
from uuid import uuid4

import duckdb

from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.application.operations import ExecuteOperationRequest, ExecuteOperationUseCase, validate_operation


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)


@dataclass(frozen=True)
class SubmitJobRequest:
    operation: str
    parameters: dict
    idempotency_key: str


@dataclass(frozen=True)
class GetJobRequest:
    job_id: str


@dataclass(frozen=True)
class ListJobsRequest:
    limit: int = 20


@dataclass(frozen=True)
class RetryJobRequest:
    job_id: str
    idempotency_key: str
    workspace_mode: str
    confirm: bool


class LocalJobManager:
    def __init__(self, workspace: OperationalWorkspace):
        self.workspace = workspace
        self.path = workspace.root / "jobs.duckdb"
        self.store_lock = Lock()
        self.data_lock = Lock()
        self.wake = Event()
        self.stop = Event()
        self.thread = None
        self.opened = False
        self.executor = ExecuteOperationUseCase(workspace)

    def open(self, *, start_worker=True):
        self.workspace.open()
        try:
            with self.store_lock, duckdb.connect(str(self.path)) as db:
                db.execute("""CREATE TABLE IF NOT EXISTS local_jobs (
                    job_id VARCHAR PRIMARY KEY, idempotency_key VARCHAR UNIQUE,
                    request_hash VARCHAR, operation VARCHAR, state VARCHAR,
                    created_at VARCHAR, parameters VARCHAR, record VARCHAR)""")
                rows = db.execute("SELECT record FROM local_jobs WHERE state IN ('pending', 'running')").fetchall()
                for (raw,) in rows:
                    record = json.loads(raw)
                    record.update(state="failed", phase="interrupted", error_code="worker_interrupted",
                                  finished_at=_now(), warnings=["review_side_effects_before_resubmitting"])
                    self._save(db, record)
            self.opened = True
            if start_worker:
                self.thread = Thread(target=self._loop, name="ml-finance-local-jobs", daemon=True)
                self.thread.start()
        except Exception:
            self.workspace.close()
            raise

    def close(self):
        self.stop.set()
        self.wake.set()
        if self.thread:
            self.thread.join()  # Finish the active operation; never tear down a DuckDB writer mid-write.
        if self.opened:
            with self.store_lock, duckdb.connect(str(self.path)) as db:
                for (raw,) in db.execute("SELECT record FROM local_jobs WHERE state='pending'").fetchall():
                    record = json.loads(raw)
                    record.update(state="failed", phase="stopped", error_code="worker_stopped", finished_at=_now())
                    self._save(db, record)
        self.opened = False
        self.workspace.close()

    @contextmanager
    def reading(self):
        # Reads fail quickly rather than mixing connections/snapshots with an active write.
        if not self.data_lock.acquire(blocking=False):
            raise OperationError("workspace_busy")
        try:
            yield
        finally:
            self.data_lock.release()

    def submit(self, request: SubmitJobRequest):
        if not self.opened or self.stop.is_set():
            raise OperationError("worker_unavailable", 503)
        if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", request.idempotency_key):
            raise OperationError("invalid_idempotency_key", 422)
        validate_operation(ExecuteOperationRequest(request.operation, request.parameters), self.workspace)
        try:
            parameters = _json(request.parameters)
        except (TypeError, ValueError) as exc:
            raise OperationError("invalid_parameters", 422) from exc
        if request.operation != "uploads" and len(parameters.encode()) > 131072:
            raise OperationError("parameters_size_limit", 413)
        digest = hashlib.sha256((request.operation + "\n" + parameters).encode()).hexdigest()
        with self.store_lock, duckdb.connect(str(self.path)) as db:
            existing = db.execute("SELECT request_hash, record FROM local_jobs WHERE idempotency_key=?", [request.idempotency_key]).fetchone()
            if existing:
                if existing[0] != digest:
                    raise OperationError("idempotency_conflict")
                return json.loads(existing[1])
            if db.execute("SELECT count(*) FROM local_jobs WHERE state IN ('pending', 'running')").fetchone()[0] >= 20:
                raise OperationError("job_queue_full", 429)
            record = dict(job_id=uuid4().hex, operation=request.operation, state="pending", progress=0,
                          phase="queued", created_at=_now(), started_at=None, finished_at=None,
                          warnings=[], error_code=None, result=None)
            db.execute("INSERT INTO local_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [record["job_id"],
                       request.idempotency_key, digest, request.operation, "pending", record["created_at"], parameters, _json(record)])
        self.wake.set()
        return record

    def get(self, job_id):
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise OperationError("invalid_job_id", 422)
        with self.store_lock, duckdb.connect(str(self.path), read_only=True) as db:
            row = db.execute("SELECT record FROM local_jobs WHERE job_id=?", [job_id]).fetchone()
        if row is None:
            raise OperationError("job_not_found", 404)
        return json.loads(row[0])

    def list(self, limit):
        if not 1 <= limit <= 100:
            raise OperationError("invalid_limit", 422)
        with self.store_lock, duckdb.connect(str(self.path), read_only=True) as db:
            rows = db.execute("SELECT record FROM local_jobs ORDER BY created_at DESC LIMIT ?", [limit]).fetchall()
        return {"jobs": [json.loads(row[0]) for row in rows]}

    def retry(self, request: RetryJobRequest):
        record = self.get(request.job_id)
        # Only the pure simulation can be re-executed: no writes, external calls or charges.
        if record["operation"] != "simulation" or record["state"] != "failed":
            raise OperationError("retry_not_safe")
        with self.store_lock, duckdb.connect(str(self.path), read_only=True) as db:
            key, raw = db.execute("SELECT idempotency_key, parameters FROM local_jobs WHERE job_id=?", [request.job_id]).fetchone()
        if key == request.idempotency_key:
            raise OperationError("retry_requires_new_key")
        parameters = json.loads(raw)
        parameters.update(workspace_mode=request.workspace_mode, confirm=request.confirm)
        return self.submit(SubmitJobRequest("simulation", parameters, request.idempotency_key))

    def run_one(self):
        with self.store_lock, duckdb.connect(str(self.path)) as db:
            row = db.execute("SELECT record, parameters FROM local_jobs WHERE state='pending' ORDER BY created_at LIMIT 1").fetchone()
            if row is None:
                return False
            record, parameters = json.loads(row[0]), json.loads(row[1])
            record.update(state="running", progress=5, phase="executing", started_at=_now())
            self._save(db, record)
        def progress(value, phase):
            record.update(progress=value, phase=phase)
            with self.store_lock, duckdb.connect(str(self.path)) as db:
                self._save(db, record)
        with self.data_lock:
            try:
                result = self.executor.execute(ExecuteOperationRequest(record["operation"], parameters), progress)
                state = result["status"]
                record.update(state="partial" if state == "skipped" else state, result=result,
                              warnings=result.get("warnings", []), error_code="operation_failed" if state == "failed" else None)
            except Exception as exc:
                record.update(state="failed", error_code=exc.code if isinstance(exc, OperationError) else "operation_failed")
        # Publishing a terminal status means portfolio reads can resume immediately.
        record.update(progress=100, phase="finished", finished_at=_now())
        with self.store_lock, duckdb.connect(str(self.path)) as db:
            self._save(db, record)
        return True

    def _loop(self):
        while not self.stop.is_set():
            self.wake.wait()
            self.wake.clear()
            try:
                while not self.stop.is_set() and self.run_one():
                    pass
            except Exception:
                # Storage failure stops dispatch; persisted in-flight work is recovered on restart.
                self.stop.set()

    @staticmethod
    def _save(db, record):
        db.execute("UPDATE local_jobs SET state=?, record=? WHERE job_id=?", [record["state"], _json(record), record["job_id"]])


class SubmitJobUseCase:
    def __init__(self, manager):
        self.manager = manager

    def execute(self, request: SubmitJobRequest):
        return self.manager.submit(request)


class GetJobUseCase:
    def __init__(self, manager):
        self.manager = manager

    def execute(self, request: GetJobRequest):
        return self.manager.get(request.job_id)


class ListJobsUseCase:
    def __init__(self, manager):
        self.manager = manager

    def execute(self, request: ListJobsRequest):
        return self.manager.list(request.limit)


class RetryJobUseCase:
    def __init__(self, manager):
        self.manager = manager

    def execute(self, request: RetryJobRequest):
        return self.manager.retry(request)
