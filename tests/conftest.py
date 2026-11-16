from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from stem_events.events import JobFinished, job_finished, job_started

T0 = datetime(2026, 11, 16, 12, 0, 0, tzinfo=UTC)


def finished(job_id: int = 1, **overrides) -> JobFinished:
    evt = job_finished(job_id, 7, "completed", T0, T0 + timedelta(milliseconds=2500), stems=4)
    return evt.model_copy(update=overrides) if overrides else evt


def finished_dict(**overrides) -> dict:
    data = json.loads(finished().model_dump_json())
    data.update(overrides)
    return data


@pytest.fixture
def started_event():
    return job_started(1, 7, T0)


@pytest.fixture
def finished_event():
    return finished()


class ScriptedNotifier:
    """Fails the first `failures[job_id]` calls for a job with a TransientError."""

    def __init__(self, failures: dict[int, int] | None = None, always_fail: set[int] | None = None):
        self.failures = dict(failures or {})
        self.always_fail = set(always_fail or ())
        self.calls: dict[int, int] = {}

    def notify(self, event) -> None:
        from stem_events.retry import TransientError

        self.calls[event.job_id] = self.calls.get(event.job_id, 0) + 1
        if event.job_id in self.always_fail:
            raise TransientError(f"downstream down for job {event.job_id}")
        if self.failures.get(event.job_id, 0) > 0:
            self.failures[event.job_id] -= 1
            raise TransientError(f"downstream timeout for job {event.job_id}")
