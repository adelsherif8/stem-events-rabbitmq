import json

import pytest

from stem_events.dlq import reason_of
from stem_events.handler import Action, MessageHandler, make_processor, replay_headers
from stem_events.notifier import FlakyNotifier
from stem_events.retry import RetryPolicy, TransientError
from stem_events.store import EventStore
from stem_events.topology import Topology

from .conftest import ScriptedNotifier, finished

POLICY = RetryPolicy((1000, 5000, 25000))
TOPO = Topology()


@pytest.fixture
def make_handler(tmp_path):
    def _make(notifier):
        store = EventStore(tmp_path / "events.sqlite3")
        return MessageHandler(make_processor(store, notifier), POLICY, TOPO), store

    return _make


def test_processed_then_duplicate(make_handler):
    handler, store = make_handler(ScriptedNotifier())
    body = finished().model_dump_json().encode()
    first = handler.handle(body, None, "job.finished")
    second = handler.handle(body, None, "job.finished")
    assert (first.action, first.result) == (Action.ACK, "processed")
    assert (second.action, second.result) == (Action.ACK, "duplicate")
    assert store.processed_count() == 1


def test_bad_payload_goes_to_dlq_with_reason(make_handler):
    handler, store = make_handler(ScriptedNotifier())
    out = handler.handle(json.dumps({"job_id": "x"}).encode(), {}, "job.finished")
    assert out.action is Action.DEAD_LETTER
    assert out.target_queue == "stem.events.jobs.dlq"
    assert out.headers["x-dlq-reason"] == "invalid_payload"
    assert out.headers["x-original-routing-key"] == "job.finished"
    assert "InvalidEvent" in out.headers["x-last-error"]
    assert store.processed_count() == 0


def test_transient_failure_retries_through_tiers_then_dead_letters(make_handler):
    handler, store = make_handler(ScriptedNotifier(always_fail={1}))
    body = finished().model_dump_json().encode()
    headers, routing_key, path = None, "job.finished", []
    for _ in range(POLICY.max_attempts):
        out = handler.handle(body, headers, routing_key)
        path.append((out.action, out.target_queue))
        headers, routing_key = out.headers, out.target_queue  # what the broker hands back
    assert path == [
        (Action.RETRY, "stem.events.jobs.retry.1"),
        (Action.RETRY, "stem.events.jobs.retry.2"),
        (Action.RETRY, "stem.events.jobs.retry.3"),
        (Action.DEAD_LETTER, "stem.events.jobs.dlq"),
    ]
    assert headers["x-dlq-reason"] == "retries_exhausted"
    assert headers["x-attempt"] == 3
    assert headers["x-original-routing-key"] == "job.finished"  # survives the retry hops
    assert store.processed_count() == 0


def test_recovers_after_one_retry(make_handler):
    notifier = ScriptedNotifier(failures={1: 1})
    handler, store = make_handler(notifier)
    body = finished().model_dump_json().encode()
    first = handler.handle(body, None, "job.finished")
    assert first.action is Action.RETRY and first.delay_ms == 1000
    second = handler.handle(body, first.headers, first.target_queue)
    assert (second.action, second.result, second.attempt) == (Action.ACK, "processed", 1)
    assert notifier.calls[1] == 2 and store.processed_count() == 1


def test_replay_headers_reset_attempt_budget():
    dead = {
        "x-attempt": 3,
        "x-dlq-reason": "retries_exhausted",
        "x-last-error": "TransientError: x",
        "x-dead-lettered-at": "2026-11-16T00:00:00+00:00",
        "x-death": [{"reason": "delivery_limit"}],
        "x-original-routing-key": "job.finished",
    }
    out = replay_headers(dead)
    assert out == {"x-original-routing-key": "job.finished", "x-replay-count": 1}
    assert replay_headers(out)["x-replay-count"] == 2


def test_dlq_reason_label():
    assert reason_of({"x-dlq-reason": "invalid_payload"}) == "invalid_payload"
    assert reason_of({"x-death": [{"reason": "delivery_limit"}]}) == "broker:delivery_limit"
    assert reason_of({}) == "unknown"


def test_flaky_notifier_bounds():
    evt = finished()
    FlakyNotifier(0.0).notify(evt)
    with pytest.raises(TransientError):
        FlakyNotifier(1.0).notify(evt)
    with pytest.raises(ValueError):
        FlakyNotifier(1.5)
