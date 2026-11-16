import json
from datetime import timedelta

import pytest

from stem_events.events import MAX_BODY_BYTES, InvalidEvent, JobFinished, JobStarted, parse_event
from stem_events.retry import PermanentError

from .conftest import T0, finished_dict


def body(d: dict) -> bytes:
    return json.dumps(d).encode()


def test_valid_finished_round_trip(finished_event):
    parsed = parse_event(finished_event.model_dump_json().encode())
    assert isinstance(parsed, JobFinished)
    assert parsed == finished_event
    assert parsed.duration_ms == 2500


def test_valid_started_round_trip(started_event):
    parsed = parse_event(started_event.model_dump_json().encode())
    assert isinstance(parsed, JobStarted)


def test_invalid_event_is_a_permanent_error():
    assert issubclass(InvalidEvent, PermanentError)


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"",
        b"[]",
        body(finished_dict(event_type="job.exploded")),
        body(finished_dict(job_id=-1)),
        body(finished_dict(status="done")),
        body(finished_dict(event_id="not-a-uuid")),
        body(finished_dict(unexpected_field=1)),
        body(finished_dict(occurred_at="2026-11-16T12:00:00")),  # naive datetime
        body(finished_dict(schema_version=2)),
    ],
)
def test_malformed_payloads_rejected(payload):
    with pytest.raises(InvalidEvent):
        parse_event(payload)


def test_missing_field_rejected():
    d = finished_dict()
    del d["job_id"]
    with pytest.raises(InvalidEvent, match="job_id"):
        parse_event(body(d))


def test_duration_must_match_timings():
    with pytest.raises(InvalidEvent, match="duration_ms"):
        parse_event(body(finished_dict(duration_ms=99999)))


def test_finished_before_started_rejected():
    d = finished_dict(finished_at=(T0 - timedelta(seconds=1)).isoformat(), duration_ms=0)
    with pytest.raises(InvalidEvent, match="before started_at"):
        parse_event(body(d))


def test_completed_job_cannot_carry_error():
    with pytest.raises(InvalidEvent, match="completed"):
        parse_event(body(finished_dict(error="boom")))


def test_failed_job_needs_error():
    with pytest.raises(InvalidEvent, match="failed"):
        parse_event(body(finished_dict(status="failed", stems=0, error=None)))
    ok = parse_event(body(finished_dict(status="failed", stems=0, error="demucs exited 1")))
    assert ok.status == "failed"


def test_oversized_payload_rejected():
    with pytest.raises(InvalidEvent, match="too large"):
        parse_event(b"x" * (MAX_BODY_BYTES + 1))
