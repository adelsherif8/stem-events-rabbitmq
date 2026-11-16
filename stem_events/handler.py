"""Broker-agnostic message handling: bytes + headers in, an ``Outcome`` out.

The aio-pika consumer only executes the outcome (publish somewhere, then ack).
Keeping the decision here makes validation, dedupe and retry logic unit-testable
without a broker.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from .events import JobFinished, JobStarted, parse_event
from .retry import RetryPolicy, attempt_from_headers, decide
from .topology import Topology

MAX_ERROR_HEADER = 500
_RESET_ON_REPLAY = ("x-attempt", "x-last-error", "x-dlq-reason", "x-dead-lettered-at", "x-death")


class Action(StrEnum):
    ACK = "ack"  # done (processed or duplicate)
    RETRY = "retry"  # republish to a delay queue, then ack
    DEAD_LETTER = "dead_letter"  # republish to the DLQ, then ack


@dataclass(frozen=True)
class Outcome:
    action: Action
    # processed | duplicate | transient_error | unexpected_error
    # | invalid_payload | retries_exhausted
    result: str
    attempt: int
    event_id: str | None = None
    target_queue: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    delay_ms: int | None = None


Processor = Callable[[JobStarted | JobFinished], bool]


class MessageHandler:
    def __init__(self, process: Processor, policy: RetryPolicy, topology: Topology) -> None:
        self.process = process
        self.policy = policy
        self.topology = topology

    def handle(
        self, body: bytes, headers: Mapping[str, Any] | None, routing_key: str | None = None
    ) -> Outcome:
        headers = dict(headers or {})
        attempt = attempt_from_headers(headers)
        event_id: str | None = None
        try:
            event = parse_event(body)
            event_id = str(event.event_id)
            applied = self.process(event)
            return Outcome(
                action=Action.ACK,
                result="processed" if applied else "duplicate",
                attempt=attempt,
                event_id=event_id,
            )
        except Exception as exc:  # noqa: BLE001 - classification happens in decide()
            error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_HEADER]
            decision = decide(exc, attempt, self.policy)
            out_headers = {
                **headers,
                "x-original-routing-key": headers.get("x-original-routing-key", routing_key),
                "x-last-error": error,
            }
            if decision.kind == "retry":
                assert decision.tier is not None and decision.next_attempt is not None
                out_headers["x-attempt"] = decision.next_attempt
                return Outcome(
                    action=Action.RETRY,
                    result=decision.reason,
                    attempt=attempt,
                    event_id=event_id,
                    target_queue=self.topology.retry_queue(decision.tier),
                    headers=out_headers,
                    error=error,
                    delay_ms=decision.delay_ms,
                )
            out_headers["x-attempt"] = attempt
            out_headers["x-dlq-reason"] = decision.reason
            out_headers["x-dead-lettered-at"] = datetime.now(UTC).isoformat()
            return Outcome(
                action=Action.DEAD_LETTER,
                result=decision.reason,
                attempt=attempt,
                event_id=event_id,
                target_queue=self.topology.dlq,
                headers=out_headers,
                error=error,
            )


def replay_headers(headers: Mapping[str, Any] | None) -> dict[str, Any]:
    """Headers for a message moved from the DLQ back to the main queue: fresh attempt budget."""
    out = {k: v for k, v in (headers or {}).items() if k not in _RESET_ON_REPLAY}
    try:
        out["x-replay-count"] = int(out.get("x-replay-count", 0)) + 1
    except (TypeError, ValueError):
        out["x-replay-count"] = 1
    return out


def make_processor(store, notifier) -> Processor:
    """Glue: dedupe + persist in the store, with the notifier as the in-transaction side effect."""

    def process(event: JobStarted | JobFinished) -> bool:
        return store.apply(event, side_effect=notifier.notify)

    return process
