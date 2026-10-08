"""SQLite persistence for jobs, uploads and the named-notebook cache."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

from .errors import GatewayError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, kind TEXT NOT NULL, params TEXT NOT NULL,
  status TEXT NOT NULL, error_code TEXT, error_message TEXT,
  notebook_id TEXT, notebook_title TEXT, notebook_url TEXT,
  urls TEXT NOT NULL DEFAULT '[]', upload_ids TEXT NOT NULL DEFAULT '[]', source_ids TEXT NOT NULL DEFAULT '[]',
  planned_sources INTEGER NOT NULL DEFAULT 0,
  task_id TEXT, artifact_id TEXT, accepted INTEGER NOT NULL DEFAULT 0,
  size_bytes INTEGER, mime TEXT, text TEXT,
  rollback_pending INTEGER NOT NULL DEFAULT 0, generation_requested INTEGER NOT NULL DEFAULT 0,
  rollback_attempts INTEGER NOT NULL DEFAULT 0, title TEXT,
  created_at REAL NOT NULL, updated_at REAL NOT NULL, finished_at REAL);
CREATE INDEX IF NOT EXISTS jobs_kind_created ON jobs(kind, created_at);
CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status, created_at);
CREATE TABLE IF NOT EXISTS uploads (
  id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, filename TEXT NOT NULL, path TEXT NOT NULL,
  size_bytes INTEGER NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL, job_id TEXT);
CREATE TABLE IF NOT EXISTS notebooks (
  title TEXT PRIMARY KEY, notebook_id TEXT NOT NULL, consumer_id TEXT NOT NULL,
  source_count INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS events (
  consumer_id TEXT NOT NULL, kind TEXT NOT NULL, created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS events_kind_created ON events(kind, created_at);
"""
_JSON = {"params", "urls", "upload_ids", "source_ids"}
_UPDATABLE = {"status", "error_code", "error_message", "notebook_id", "notebook_title", "notebook_url", "urls",
              "source_ids", "task_id", "artifact_id", "accepted", "size_bytes", "mime", "text", "finished_at", "rollback_pending", "generation_requested",
              "rollback_attempts", "title"}


@dataclass(frozen=True)
class Job:
    id: str
    consumer_id: str
    kind: str
    params: dict
    status: str
    error_code: str | None
    error_message: str | None
    notebook_id: str | None
    notebook_title: str | None
    notebook_url: str | None
    urls: list
    upload_ids: list
    source_ids: list
    planned_sources: int
    task_id: str | None
    artifact_id: str | None
    accepted: bool
    size_bytes: int | None
    mime: str | None
    text: str | None
    created_at: float
    updated_at: float
    finished_at: float | None
    rollback_pending: bool
    generation_requested: bool
    rollback_attempts: int
    title: str | None = None


@dataclass(frozen=True)
class Upload:
    id: str
    consumer_id: str
    filename: str
    path: str
    size_bytes: int
    created_at: float
    expires_at: float
    job_id: str | None


class Store:
    def __init__(self, path: Path):
        path = Path(path)
        new = not path.exists()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(_SCHEMA)
            cols = {r[1] for r in self._db.execute("PRAGMA table_info(jobs)")}
            if "title" not in cols:  # databases created before logical titles
                self._db.execute("ALTER TABLE jobs ADD COLUMN title TEXT")
        if new:
            os.chmod(path, 0o600)

    # -- jobs ------------------------------------------------------------
    def _job(self, row) -> Job | None:
        if row is None:
            return None
        d = dict(row)
        for k in _JSON:
            d[k] = json.loads(d[k])
        d["accepted"] = bool(d["accepted"])
        d["rollback_pending"] = bool(d["rollback_pending"])
        d["generation_requested"] = bool(d["generation_requested"])
        return Job(**d)

    def create_job(self, consumer_id, kind, params, urls, upload_ids, now) -> Job:
        job_id = uuid.uuid4().hex
        with self._lock:
            self._db.execute(
                "INSERT INTO jobs (id, consumer_id, kind, params, status, urls, upload_ids, planned_sources, created_at, updated_at)"
                " VALUES (?,?,?,?, 'new', ?,?,?,?,?)",
                (job_id, consumer_id, kind, json.dumps(params), json.dumps(list(urls)), json.dumps(list(upload_ids)),
                 len(urls) + len(upload_ids), now, now))
        return self.get_job(job_id)

    def get_job(self, job_id, consumer_id=None) -> Job | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        job = self._job(row)
        if job is not None and consumer_id is not None and job.consumer_id != consumer_id:
            return None
        return job

    def update_job(self, job_id, now, **fields) -> None:
        for k in fields:
            if k not in _UPDATABLE:
                raise KeyError(k)
        cols, vals = ["updated_at = ?"], [now]
        for k, v in fields.items():
            cols.append(f"{k} = ?")
            vals.append(json.dumps(v) if k in _JSON else int(v) if k in ("accepted", "rollback_pending", "generation_requested") else v)
        with self._lock:
            self._db.execute(f"UPDATE jobs SET {', '.join(cols)} WHERE id = ?", (*vals, job_id))

    def claim_next(self, now) -> Job | None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute("SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1").fetchone()
                if row is not None:
                    self._db.execute("UPDATE jobs SET status = 'running', updated_at = ? WHERE id = ?", (now, row["id"]))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return None if row is None else self.get_job(row["id"])

    def jobs_with_status(self, status) -> list[Job]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM jobs WHERE status = ? ORDER BY created_at", (status,)).fetchall()
        return [self._job(r) for r in rows]

    def jobs_pending_rollback(self) -> list[Job]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM jobs WHERE status = 'failed' AND rollback_pending = 1 ORDER BY created_at").fetchall()
        return [self._job(r) for r in rows]

    def count_status(self, status) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM jobs WHERE status = ?", (status,)).fetchone()[0]

    def count_billable(self, kind, since, consumer_id=None) -> int:
        sql = "SELECT COUNT(*) FROM jobs WHERE kind = ? AND created_at > ? AND (status != 'failed' OR accepted = 1)"
        args: list = [kind, since]
        if consumer_id is not None:
            sql += " AND consumer_id = ?"
            args.append(consumer_id)
        with self._lock:
            return self._db.execute(sql, args).fetchone()[0]

    # -- counted events (chat turns) -------------------------------------
    def add_event(self, consumer_id, kind, now) -> None:
        with self._lock:
            self._db.execute("INSERT INTO events VALUES (?,?,?)", (consumer_id, kind, now))

    def count_events(self, kind, since, consumer_id=None) -> int:
        sql, args = "SELECT COUNT(*) FROM events WHERE kind = ? AND created_at > ?", [kind, since]
        if consumer_id is not None:
            sql += " AND consumer_id = ?"
            args.append(consumer_id)
        with self._lock:
            return self._db.execute(sql, args).fetchone()[0]

    # -- uploads ---------------------------------------------------------
    def add_upload(self, consumer_id, filename, path, size_bytes, now, ttl_s, upload_id=None) -> Upload:
        upload_id = upload_id or "up_" + uuid.uuid4().hex
        with self._lock:
            self._db.execute("INSERT INTO uploads VALUES (?,?,?,?,?,?,?,NULL)",
                             (upload_id, consumer_id, filename, str(path), size_bytes, now, now + ttl_s))
        return self.get_upload(upload_id)

    def get_upload(self, upload_id) -> Upload | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()
        return None if row is None else Upload(**dict(row))

    def claim_uploads(self, upload_ids, consumer_id, job_id, now) -> list[Upload]:
        if len(set(upload_ids)) != len(upload_ids):
            raise GatewayError("bad_request", "upload_ids must not repeat")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                out = []
                for i, uid in enumerate(upload_ids):
                    row = self._db.execute("SELECT * FROM uploads WHERE id = ?", (uid,)).fetchone()
                    if row is None or row["consumer_id"] != consumer_id or row["job_id"] is not None or row["expires_at"] < now:
                        raise GatewayError("bad_request", f"upload_ids[{i}] is unknown, expired, or already used")
                    out.append(Upload(**dict(row)))
                for u in out:
                    self._db.execute("UPDATE uploads SET job_id = ? WHERE id = ?", (job_id, u.id))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return [Upload(**{**u.__dict__, "job_id": job_id}) for u in out]

    def release_uploads(self, job_id) -> None:
        with self._lock:
            self._db.execute("UPDATE uploads SET job_id = NULL WHERE job_id = ?", (job_id,))

    def uploads_for_job(self, job_id) -> list[Upload]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM uploads WHERE job_id = ?", (job_id,)).fetchall()
        return [Upload(**dict(r)) for r in rows]

    def delete_upload(self, upload_id) -> None:
        with self._lock:
            self._db.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))

    def count_pending_uploads(self, consumer_id, now) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM uploads WHERE consumer_id = ? AND job_id IS NULL AND expires_at >= ?",
                                    (consumer_id, now)).fetchone()[0]

    def expired_uploads(self, now) -> list[Upload]:
        """Unclaimed uploads past their TTL, plus any upload still attached to a finished job."""
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM uploads WHERE (job_id IS NULL AND expires_at < ?)"
                " OR job_id IN (SELECT id FROM jobs WHERE status IN ('failed', 'done'))", (now,)).fetchall()
        return [Upload(**dict(r)) for r in rows]

    # -- named notebook cache ---------------------------------------------
    def get_notebook(self, title) -> tuple[str, int] | None:
        with self._lock:
            row = self._db.execute("SELECT notebook_id, source_count FROM notebooks WHERE title = ?", (title,)).fetchone()
        return None if row is None else (row[0], row[1])

    def put_notebook(self, title, notebook_id, consumer_id, source_count, now) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO notebooks VALUES (?,?,?,?,?)", (title, notebook_id, consumer_id, source_count, now))

    def forget_notebook(self, title) -> None:
        with self._lock:
            self._db.execute("DELETE FROM notebooks WHERE title = ?", (title,))

    def adjust_sources(self, title, delta) -> None:
        with self._lock:
            self._db.execute("UPDATE notebooks SET source_count = MAX(0, source_count + ?) WHERE title = ?", (delta, title))
