"""Simulates the Stem SaaS Celery worker publishing job lifecycle events.

    python -m stem_events.publisher --jobs 20 --bad-every 7 --dup-every 5

``--bad-every K`` sends a malformed payload every K jobs (exercises the DLQ path);
``--dup-every K`` re-sends the same job.finished event every K jobs (exercises dedupe).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from datetime import UTC, datetime, timedelta

import aio_pika
from aio_pika.abc import AbstractExchange

from .config import Settings
from .events import JobFinished, JobStarted, job_finished, job_started
from .logs import configure_logging, get_logger
from .topology import Topology, declare

log = get_logger(__name__)


async def publish_event(exchange: AbstractExchange, event: JobStarted | JobFinished) -> None:
    await publish_raw(
        exchange, event.event_type, event.model_dump_json().encode(), str(event.event_id)
    )


async def publish_raw(
    exchange: AbstractExchange, routing_key: str, body: bytes, message_id: str | None = None
) -> None:
    await exchange.publish(
        aio_pika.Message(
            body=body,
            content_type="application/json",
            message_id=message_id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


def simulate_job(job_id: int, rng: random.Random) -> tuple[JobStarted, JobFinished]:
    project_id = rng.randint(1, 50)
    workspace_id = rng.randint(1, 5)
    started = datetime.now(UTC)
    finished = started + timedelta(milliseconds=rng.randint(800, 6000))
    start_evt = job_started(job_id, project_id, started, workspace_id)
    if rng.random() < 0.15:
        end_evt = job_finished(
            job_id,
            project_id,
            "failed",
            started,
            finished,
            error="demucs exited with code 1",
            workspace_id=workspace_id,
        )
    else:
        end_evt = job_finished(
            job_id, project_id, "completed", started, finished, stems=4, workspace_id=workspace_id
        )
    return start_evt, end_evt


async def run(args: argparse.Namespace, settings: Settings) -> None:
    rng = random.Random(args.seed)
    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        topo = Topology(prefix=settings.prefix)
        declared = await declare(channel, topo, settings.retry_delays_ms, settings.delivery_limit)
        for n in range(1, args.jobs + 1):
            job_id = args.start_id + n - 1
            start_evt, end_evt = simulate_job(job_id, rng)
            await publish_event(declared.exchange, start_evt)
            await publish_event(declared.exchange, end_evt)
            log.info(
                "published",
                job_id=job_id,
                status=end_evt.status,
                event_id=str(end_evt.event_id),
            )
            if args.dup_every and n % args.dup_every == 0:
                await publish_event(declared.exchange, end_evt)
                log.info("published_duplicate", job_id=job_id, event_id=str(end_evt.event_id))
            if args.bad_every and n % args.bad_every == 0:
                bad = json.dumps({"event_type": "job.finished", "job_id": -1, "status": "done"})
                await publish_raw(declared.exchange, "job.finished", bad.encode())
                log.info("published_bad_payload", job_id=job_id)
            await asyncio.sleep(args.interval)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--jobs", type=int, default=10)
    p.add_argument("--start-id", type=int, default=1000)
    p.add_argument("--interval", type=float, default=0.5, help="seconds between jobs")
    p.add_argument("--bad-every", type=int, default=0)
    p.add_argument("--dup-every", type=int, default=0)
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    asyncio.run(run(args, settings))


if __name__ == "__main__":
    main()
