"""Idempotent event store (SQLite).

The dedupe record and the state change are written in ONE transaction, so an
event's effect is applied at most once even though RabbitMQ delivers at least
once. With Postgres the same pattern is ``INSERT ... ON CONFLICT DO NOTHING``
on a ``processed_events`` table inside the business transaction.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from .events import JobFinished, JobStarted
from .retry import TransientError

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    job_id       INTEGER NOT NULL,
    processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_state (
    job_id        INTEGER PRIMARY KEY,
    project_id    INTEGER NOT NULL,
    workspace_id  INTEGER,
    status        TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    duration_ms   INTEGER,
    stems         INTEGER,
    error         TEXT,
    last_event_at TEXT NOT NULL
);
"""

# Retries can reorder events (a retried job.started may land after job.finished),
# so only overwrite state with an event that is at least as new as the stored one.
UPSERT = """
INSERT INTO job_state (job_id, project_id, workspace_id, status, started_at, finished_at,
                       duration_ms, stems, error, last_event_at)
VALUES (:job_id, :project_id, :workspace_id, :status, :started_at, :finished_at,
        :duration_ms, :stems, :error, :last_event_at)
ON CONFLICT(job_id) DO UPDATE SET
    status = excluded.status,
    started_at = COALESCE(excluded.started_at, job_state.started_at),
    finished_at = COALESCE(excluded.finished_at, job_state.finished_at),
    duration_ms = COALESCE(excluded.duration_ms, job_state.duration_ms),
    stems = COALESCE(excluded.stems, job_state.stems),
    error = excluded.error,
    last_event_at = excluded.last_event_at
WHERE excluded.last_event_at >= job_state.last_event_at
"""


def _iso(dt: datetime | None) -> str | None:
    return None if dt is None else dt.astimezone(UTC).isoformat(timespec="microseconds")


class EventStore:
    def __init__(self, path: str | Path, timeout_s: float = 5.0) -> None:
        self.path = str(path)
        self.timeout_s = timeout_s
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=self.timeout_s, isolation_level=None)

    def apply(
        self,
        event: JobStarted | JobFinished,
        side_effect: Callable[[JobStarted | JobFinished], None] | None = None,
    ) -> bool:
        """Apply an event once. Returns False if it was already processed (duplicate).

        ``side_effect`` runs inside the transaction: if it raises, nothing is recorded
        and the message can be retried.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            seen = conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (str(event.event_id),)
            ).fetchone()
            if seen:
                conn.execute("ROLLBACK")
                return False
            if side_effect is not None:
                side_effect(event)
            conn.execute(
                "INSERT INTO processed_events VALUES (?, ?, ?, ?)",
                (str(event.event_id), event.event_type, event.job_id, _iso(datetime.now(UTC))),
            )
            conn.execute(UPSERT, self._row(event))
            conn.execute("COMMIT")
            return True
        except sqlite3.OperationalError as exc:  # e.g. "database is locked"
            self._rollback(conn)
            raise TransientError(f"sqlite: {exc}") from exc
        except BaseException:
            self._rollback(conn)
            raise
        finally:
            conn.close()

    @staticmethod
    def _rollback(conn: sqlite3.Connection) -> None:
        if conn.in_transaction:
            conn.execute("ROLLBACK")

    @staticmethod
    def _row(event: JobStarted | JobFinished) -> dict:
        base = {
            "job_id": event.job_id,
            "project_id": event.project_id,
            "workspace_id": event.workspace_id,
            "started_at": _iso(event.started_at),
            "last_event_at": _iso(event.occurred_at),
            "finished_at": None,
            "duration_ms": None,
            "stems": None,
            "error": None,
        }
        if isinstance(event, JobStarted):
            return {**base, "status": "processing"}
        return {
            **base,
            "status": event.status,
            "finished_at": _iso(event.finished_at),
            "duration_ms": event.duration_ms,
            "stems": event.stems,
            "error": event.error,
        }

    # -- read helpers (used by tests and for inspection) --
    def processed_count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]

    def job(self, job_id: int) -> dict | None:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT * FROM job_state WHERE job_id = ?", (job_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
