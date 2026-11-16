"""The downstream side effect: tell the user their stems are ready (or the job failed).

In the demo this is a stand-in that can be told to fail randomly, so the retry
path is exercised. A real one would call an email/webhook API and raise
``TransientError`` on timeouts / 5xx.
"""

from __future__ import annotations

import random
from typing import Protocol

from .events import JobFinished, JobStarted
from .logs import get_logger
from .retry import TransientError

log = get_logger(__name__)


class Notifier(Protocol):
    def notify(self, event: JobStarted | JobFinished) -> None: ...


class FlakyNotifier:
    def __init__(self, fail_rate: float = 0.0, seed: int | None = None) -> None:
        if not 0.0 <= fail_rate <= 1.0:
            raise ValueError("fail_rate must be between 0 and 1")
        self.fail_rate = fail_rate
        self._rng = random.Random(seed)

    def notify(self, event: JobStarted | JobFinished) -> None:
        if not isinstance(event, JobFinished):
            return  # only finished jobs notify the user
        if self._rng.random() < self.fail_rate:
            raise TransientError("notification service unavailable (simulated)")
        log.info("user_notified", job_id=event.job_id, status=event.status)
