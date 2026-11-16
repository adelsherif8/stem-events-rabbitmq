import sqlite3
from datetime import timedelta

import pytest

from stem_events.events import job_started
from stem_events.retry import TransientError
from stem_events.store import EventStore

from .conftest import T0, finished


@pytest.fixture
def store(tmp_path):
    return EventStore(tmp_path / "events.sqlite3", timeout_s=0.2)


def test_duplicate_event_applied_once(store, finished_event):
    calls = []
    assert store.apply(finished_event, side_effect=calls.append) is True
    assert store.apply(finished_event, side_effect=calls.append) is False
    assert store.processed_count() == 1
    assert len(calls) == 1  # side effect not repeated for the duplicate
    assert store.job(1)["status"] == "completed"


def test_failed_side_effect_rolls_back(store, finished_event):
    def boom(_):
        raise TransientError("downstream down")

    with pytest.raises(TransientError):
        store.apply(finished_event, side_effect=boom)
    assert store.processed_count() == 0
    assert store.job(1) is None
    assert store.apply(finished_event) is True  # a later retry succeeds


def test_out_of_order_events_do_not_regress_state(store):
    done = finished()
    late_start = job_started(1, 7, T0).model_copy(update={"occurred_at": T0 - timedelta(seconds=5)})
    store.apply(done)
    store.apply(late_start)  # retried job.started arrives after job.finished
    row = store.job(1)
    assert row["status"] == "completed"
    assert row["duration_ms"] == 2500
    assert store.processed_count() == 2


def test_started_then_finished(store, started_event):
    store.apply(started_event)
    assert store.job(1)["status"] == "processing"
    store.apply(finished())
    assert store.job(1)["status"] == "completed"


def test_locked_database_is_transient(store, finished_event):
    blocker = sqlite3.connect(store.path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(TransientError, match="locked"):
            store.apply(finished_event)
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
    assert store.apply(finished_event) is True
