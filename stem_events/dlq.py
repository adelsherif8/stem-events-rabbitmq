"""Inspect and replay the dead-letter queue.

    python -m stem_events.dlq stats
    python -m stem_events.dlq list [--limit 20] [--json]
    python -m stem_events.dlq replay [--reason retries_exhausted] [--message-id ID]
                                     [--limit N] [--dry-run]
    python -m stem_events.dlq purge --yes

``list`` peeks: it fetches messages without acking and closes the channel, so the
broker puts them back. ``replay`` republishes matching messages to the main queue
with a fresh attempt budget (``x-replay-count`` is incremented) and only acks the
DLQ copy after the broker confirms the republish.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from .config import Settings
from .handler import replay_headers
from .topology import Topology, declare


def _plain(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def reason_of(headers: dict[str, Any]) -> str:
    """Our consumer sets x-dlq-reason; broker dead-lettering (delivery limit) sets x-death."""
    if headers.get("x-dlq-reason"):
        return str(_plain(headers["x-dlq-reason"]))
    deaths = headers.get("x-death") or []
    if deaths:
        return f"broker:{_plain(deaths[0].get('reason', 'unknown'))}"
    return "unknown"


def summarize(msg: AbstractIncomingMessage) -> dict[str, Any]:
    headers = dict(msg.headers or {})
    return {
        "message_id": msg.message_id,
        "reason": reason_of(headers),
        "attempts": _plain(headers.get("x-attempt")),
        "replay_count": _plain(headers.get("x-replay-count", 0)),
        "last_error": _plain(headers.get("x-last-error")),
        "original_routing_key": _plain(headers.get("x-original-routing-key")),
        "dead_lettered_at": _plain(headers.get("x-dead-lettered-at")),
        "body": msg.body.decode(errors="replace")[:300],
    }


async def _open(settings: Settings):
    connection = await aio_pika.connect(settings.amqp_url)
    channel = await connection.channel(publisher_confirms=True)
    topo = Topology(prefix=settings.prefix)
    declared = await declare(channel, topo, settings.retry_delays_ms, settings.delivery_limit)
    return connection, channel, topo, declared


async def _fetch(queue, limit: int) -> list[AbstractIncomingMessage]:
    out = []
    while len(out) < limit:
        msg = await queue.get(no_ack=False, fail=False)
        if msg is None:
            break
        out.append(msg)
    return out


async def cmd_stats(settings: Settings) -> dict[str, int]:
    connection, _channel, topo, d = await _open(settings)
    async with connection:
        names = [topo.queue, *[topo.retry_queue(i) for i in range(len(d.retry_queues))], topo.dlq]
        ch = await connection.channel()
        counts = {}
        for name in names:
            q = await ch.declare_queue(name, passive=True)
            counts[name] = q.declaration_result.message_count
        return counts


async def cmd_list(settings: Settings, limit: int) -> list[dict[str, Any]]:
    connection, _channel, _topo, d = await _open(settings)
    async with connection:  # closing without ack returns the messages to the DLQ
        return [summarize(m) for m in await _fetch(d.dlq, limit)]


async def cmd_replay(
    settings: Settings,
    limit: int,
    reason: str | None = None,
    message_id: str | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    connection, channel, topo, d = await _open(settings)
    replayed = []
    async with connection:
        for msg in await _fetch(d.dlq, limit):
            info = summarize(msg)
            if reason and info["reason"] != reason:
                continue
            if message_id and msg.message_id != message_id:
                continue
            replayed.append(info)
            if dry_run:
                continue
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=msg.body,
                    headers=replay_headers(msg.headers),
                    content_type=msg.content_type or "application/json",
                    message_id=msg.message_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=topo.queue,
            )
            await msg.ack()
        # anything not acked goes back to the DLQ when the connection closes
    return replayed


async def cmd_purge(settings: Settings) -> int:
    connection, _channel, _topo, d = await _open(settings)
    async with connection:
        result = await d.dlq.purge()
        return result.message_count


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    except aio_pika.exceptions.AMQPConnectionError as exc:
        print(f"cannot reach RabbitMQ ({Settings.from_env().amqp_url}): {exc}", file=sys.stderr)
        return 1


def _main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="stem_events.dlq", description="Inspect and replay the DLQ")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats", help="message counts for main, retry and dead-letter queues")
    lp = sub.add_parser("list", help="peek at dead-lettered messages (non-destructive)")
    lp.add_argument("--limit", type=int, default=20)
    lp.add_argument("--json", action="store_true")
    rp = sub.add_parser("replay", help="move messages back to the main queue")
    rp.add_argument("--limit", type=int, default=100)
    rp.add_argument(
        "--reason", help="e.g. retries_exhausted, invalid_payload, broker:delivery_limit"
    )
    rp.add_argument("--message-id")
    rp.add_argument("--dry-run", action="store_true")
    pp = sub.add_parser("purge", help="delete everything in the DLQ")
    pp.add_argument("--yes", action="store_true", help="required, to avoid accidents")
    args = p.parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "stats":
        for name, count in asyncio.run(cmd_stats(settings)).items():
            print(f"{count:>6}  {name}")
    elif args.cmd == "list":
        items = asyncio.run(cmd_list(settings, args.limit))
        if args.json:
            print(json.dumps(items, indent=2))
        elif not items:
            print("DLQ is empty")
        for i, it in enumerate(items if not args.json else [], 1):
            print(
                f"[{i}] {it['message_id']}  reason={it['reason']}  attempts={it['attempts']}"
                f"  replays={it['replay_count']}"
            )
            print(f"    error: {it['last_error']}")
            print(f"    body:  {it['body']}")
    elif args.cmd == "replay":
        items = asyncio.run(
            cmd_replay(settings, args.limit, args.reason, args.message_id, args.dry_run)
        )
        verb = "would replay" if args.dry_run else "replayed"
        print(f"{verb} {len(items)} message(s)")
        for it in items:
            print(f"  {it['message_id']}  reason={it['reason']}")
    elif args.cmd == "purge":
        if not args.yes:
            print("refusing to purge without --yes", file=sys.stderr)
            return 2
        print(f"purged {asyncio.run(cmd_purge(settings))} message(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
