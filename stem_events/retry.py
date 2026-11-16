"""Error classification and the retry decision (pure functions, no broker needed)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal


class PermanentError(Exception):
    """Retrying will not help (e.g. invalid payload). Goes straight to the DLQ."""


class TransientError(Exception):
    """Might succeed later (downstream timeout, locked database...)."""


@dataclass(frozen=True)
class RetryPolicy:
    """Delay tiers, one RabbitMQ retry queue per tier.

    ``delays_ms=(1000, 5000, 25000)`` means: first failure waits 1 s, second 5 s,
    third 25 s; a fourth failure dead-letters. So ``max_attempts = len(delays) + 1``.
    """

    delays_ms: tuple[int, ...] = (1000, 5000, 25000)

    def __post_init__(self) -> None:
        if not self.delays_ms or any(d <= 0 for d in self.delays_ms):
            raise ValueError("delays_ms must be a non-empty tuple of positive ints")

    @property
    def max_attempts(self) -> int:
        return len(self.delays_ms) + 1


@dataclass(frozen=True)
class Decision:
    kind: Literal["retry", "dead_letter"]
    reason: str
    tier: int | None = None  # index into RetryPolicy.delays_ms
    delay_ms: int | None = None
    next_attempt: int | None = None


def attempt_from_headers(headers: Mapping[str, Any] | None) -> int:
    """Number of failed attempts so far (0 on first delivery). Defensive about bad input."""
    raw = (headers or {}).get("x-attempt", 0)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(value, 0)


def decide(exc: BaseException, attempt: int, policy: RetryPolicy) -> Decision:
    """What to do with a message whose processing raised ``exc``.

    ``attempt`` is the number of failures *before* this one. Unknown exceptions are
    treated as transient: a bug gets a few retries and then lands in the DLQ with its
    error attached, instead of being silently dropped.
    """
    if isinstance(exc, PermanentError):
        return Decision(kind="dead_letter", reason="invalid_payload")
    if attempt >= len(policy.delays_ms):
        return Decision(kind="dead_letter", reason="retries_exhausted")
    return Decision(
        kind="retry",
        reason="transient_error" if isinstance(exc, TransientError) else "unexpected_error",
        tier=attempt,
        delay_ms=policy.delays_ms[attempt],
        next_attempt=attempt + 1,
    )
