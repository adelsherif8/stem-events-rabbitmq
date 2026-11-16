"""RabbitMQ topology: names plus one idempotent ``declare`` used by every component.

    stem.events (topic) --job.*--> stem.events.jobs (quorum, delivery-limit, DLX)
    consumer failure ---default exchange---> stem.events.jobs.retry.N (TTL=delay_N)
    retry.N TTL expiry ---dead-letter to default exchange---> stem.events.jobs
    poison / exhausted ---default exchange---> stem.events.jobs.dlq
    crash loop (delivery-limit hit) ---stem.events.dlx (fanout)---> stem.events.jobs.dlq

Everything is declared from this one module so the arguments always match;
RabbitMQ rejects a redeclaration with different arguments (PRECONDITION_FAILED).
"""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue


@dataclass(frozen=True)
class Topology:
    prefix: str = "stem.events"
    binding_key: str = "job.*"

    @property
    def exchange(self) -> str:
        return self.prefix

    @property
    def queue(self) -> str:
        return f"{self.prefix}.jobs"

    @property
    def dlx(self) -> str:
        return f"{self.prefix}.dlx"

    @property
    def dlq(self) -> str:
        return f"{self.prefix}.jobs.dlq"

    def retry_queue(self, tier: int) -> str:
        return f"{self.prefix}.jobs.retry.{tier + 1}"


@dataclass
class Declared:
    exchange: AbstractExchange
    queue: AbstractQueue
    retry_queues: list[AbstractQueue]
    dlq: AbstractQueue


async def declare(
    channel: AbstractChannel,
    topo: Topology,
    delays_ms: tuple[int, ...],
    delivery_limit: int = 5,
) -> Declared:
    exchange = await channel.declare_exchange(
        topo.exchange, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dlx = await channel.declare_exchange(topo.dlx, aio_pika.ExchangeType.FANOUT, durable=True)

    # Classic durable queue: no delivery limit, so peeking with get+requeue is safe.
    dlq = await channel.declare_queue(topo.dlq, durable=True)
    await dlq.bind(dlx)

    # Quorum queue: replicated, and it counts redeliveries. A message that keeps
    # crashing the consumer is dead-lettered to the DLX after `delivery_limit` tries.
    queue = await channel.declare_queue(
        topo.queue,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": delivery_limit,
            "x-dead-letter-exchange": topo.dlx,
        },
    )
    await queue.bind(exchange, routing_key=topo.binding_key)

    retry_queues = []
    for tier, delay in enumerate(delays_ms):
        # Per-queue TTL (not per-message) so every message in a tier expires in order.
        rq = await channel.declare_queue(
            topo.retry_queue(tier),
            durable=True,
            arguments={
                "x-message-ttl": delay,
                "x-dead-letter-exchange": "",  # default exchange
                "x-dead-letter-routing-key": topo.queue,  # -> back to the main queue
            },
        )
        retry_queues.append(rq)

    return Declared(exchange=exchange, queue=queue, retry_queues=retry_queues, dlq=dlq)
