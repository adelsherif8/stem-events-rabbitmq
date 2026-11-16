"""Settings read from environment variables (12-factor style, no extra dependency)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _delays(raw: str) -> tuple[int, ...]:
    values = tuple(int(part) for part in raw.split(",") if part.strip())
    if not values or any(v <= 0 for v in values):
        raise ValueError(f"RETRY_DELAYS_MS must be positive integers, got {raw!r}")
    return values


@dataclass(frozen=True)
class Settings:
    amqp_url: str = "amqp://stem:stem@localhost:5672/"
    prefix: str = "stem.events"
    retry_delays_ms: tuple[int, ...] = (1000, 5000, 25000)
    delivery_limit: int = 5
    prefetch: int = 10
    store_path: str = "./data/events.sqlite3"
    notifier_fail_rate: float = 0.0
    log_level: str = "INFO"
    log_format: str = "json"
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        env = dict(os.environ if env is None else env)
        d = cls()
        return cls(
            amqp_url=env.get("AMQP_URL", d.amqp_url),
            prefix=env.get("EVENTS_PREFIX", d.prefix),
            retry_delays_ms=_delays(env["RETRY_DELAYS_MS"])
            if "RETRY_DELAYS_MS" in env
            else d.retry_delays_ms,
            delivery_limit=int(env.get("DELIVERY_LIMIT", d.delivery_limit)),
            prefetch=int(env.get("PREFETCH", d.prefetch)),
            store_path=env.get("STORE_PATH", d.store_path),
            notifier_fail_rate=float(env.get("NOTIFIER_FAIL_RATE", d.notifier_fail_rate)),
            log_level=env.get("LOG_LEVEL", d.log_level),
            log_format=env.get("LOG_FORMAT", d.log_format),
        )
