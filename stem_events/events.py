"""Event schemas (pydantic v2) and helpers to build them.

The events mirror the lifecycle of a Stem SaaS ``Job`` row
(queued -> processing -> completed | failed). A real Celery worker would publish
``job.started`` when it flips the job to ``processing`` and ``job.finished``
when it writes ``completed`` / ``failed``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from .retry import PermanentError

MAX_BODY_BYTES = 64 * 1024


class InvalidEvent(PermanentError):
    """The payload can never be processed (bad JSON, schema violation, too large)."""


class _EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: uuid.UUID
    schema_version: Literal[1] = 1
    occurred_at: AwareDatetime
    job_id: PositiveInt
    project_id: PositiveInt
    workspace_id: PositiveInt | None = None


class JobStarted(_EventBase):
    event_type: Literal["job.started"]
    started_at: AwareDatetime


class JobFinished(_EventBase):
    event_type: Literal["job.finished"]
    status: Literal["completed", "failed"]
    started_at: AwareDatetime
    finished_at: AwareDatetime
    duration_ms: NonNegativeInt
    stems: NonNegativeInt = 0
    error: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _consistent(self) -> JobFinished:
        if self.finished_at < self.started_at:
            raise ValueError("finished_at is before started_at")
        expected = round((self.finished_at - self.started_at).total_seconds() * 1000)
        if abs(expected - self.duration_ms) > 1:
            raise ValueError(f"duration_ms={self.duration_ms} does not match timings ({expected})")
        if self.status == "completed" and (self.error is not None or self.stems == 0):
            raise ValueError("a completed job must have stems > 0 and no error")
        if self.status == "failed" and not self.error:
            raise ValueError("a failed job must carry an error message")
        return self


Event = Annotated[JobStarted | JobFinished, Field(discriminator="event_type")]
_adapter: TypeAdapter[Event] = TypeAdapter(Event)


def parse_event(body: bytes) -> JobStarted | JobFinished:
    """Validate a raw message body. Raises ``InvalidEvent`` (a permanent error)."""
    if len(body) > MAX_BODY_BYTES:
        raise InvalidEvent(f"payload too large: {len(body)} bytes")
    try:
        return _adapter.validate_json(body)
    except ValidationError as exc:
        # Keep the error compact: it travels in a message header.
        details = "; ".join(
            f"{'.'.join(str(p) for p in e['loc']) or '<root>'}: {e['msg']}" for e in exc.errors()
        )
        raise InvalidEvent(details) from exc


def _now() -> datetime:
    return datetime.now(UTC)


def job_started(
    job_id: int, project_id: int, started_at: datetime, workspace_id: int | None = None
) -> JobStarted:
    return JobStarted(
        event_id=uuid.uuid4(),
        event_type="job.started",
        occurred_at=_now(),
        job_id=job_id,
        project_id=project_id,
        workspace_id=workspace_id,
        started_at=started_at,
    )


def job_finished(
    job_id: int,
    project_id: int,
    status: Literal["completed", "failed"],
    started_at: datetime,
    finished_at: datetime,
    stems: int = 0,
    error: str | None = None,
    workspace_id: int | None = None,
) -> JobFinished:
    return JobFinished(
        event_id=uuid.uuid4(),
        event_type="job.finished",
        occurred_at=_now(),
        job_id=job_id,
        project_id=project_id,
        workspace_id=workspace_id,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=round((finished_at - started_at).total_seconds() * 1000),
        stems=stems,
        error=error,
    )
