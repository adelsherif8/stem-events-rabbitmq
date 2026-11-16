import pytest

from stem_events.config import Settings
from stem_events.events import InvalidEvent
from stem_events.retry import RetryPolicy, TransientError, attempt_from_headers, decide

POLICY = RetryPolicy((1000, 5000, 25000))


def test_max_attempts():
    assert POLICY.max_attempts == 4


@pytest.mark.parametrize("bad", [(), (0,), (100, -1)])
def test_policy_rejects_bad_delays(bad):
    with pytest.raises(ValueError):
        RetryPolicy(bad)


def test_transient_errors_walk_up_the_tiers_then_dead_letter():
    seen = []
    for attempt in range(4):
        d = decide(TransientError("timeout"), attempt, POLICY)
        seen.append((d.kind, d.tier, d.delay_ms, d.next_attempt))
    assert seen == [
        ("retry", 0, 1000, 1),
        ("retry", 1, 5000, 2),
        ("retry", 2, 25000, 3),
        ("dead_letter", None, None, None),
    ]
    assert decide(TransientError("x"), 3, POLICY).reason == "retries_exhausted"


def test_permanent_error_dead_letters_immediately():
    d = decide(InvalidEvent("bad"), 0, POLICY)
    assert d.kind == "dead_letter" and d.reason == "invalid_payload"


def test_unknown_exception_is_retried_but_labelled():
    d = decide(KeyError("bug"), 0, POLICY)
    assert d.kind == "retry" and d.reason == "unexpected_error"


def test_tampered_attempt_header_still_dead_letters():
    assert decide(TransientError("x"), 999, POLICY).kind == "dead_letter"


@pytest.mark.parametrize(
    "headers,expected",
    [
        (None, 0),
        ({}, 0),
        ({"x-attempt": 2}, 2),
        ({"x-attempt": "3"}, 3),
        ({"x-attempt": "abc"}, 0),
        ({"x-attempt": -5}, 0),
        ({"x-attempt": None}, 0),
    ],
)
def test_attempt_from_headers(headers, expected):
    assert attempt_from_headers(headers) == expected


def test_settings_from_env():
    s = Settings.from_env({"RETRY_DELAYS_MS": "200, 400", "DELIVERY_LIMIT": "3"})
    assert s.retry_delays_ms == (200, 400) and s.delivery_limit == 3
    with pytest.raises(ValueError):
        Settings.from_env({"RETRY_DELAYS_MS": "0"})
