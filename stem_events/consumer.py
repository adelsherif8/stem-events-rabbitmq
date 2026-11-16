"""aio-pika consumer: executes the ``Outcome`` the handler decides on.

Ordering rule: publish (retry/DLQ copy, with publisher confirms) BEFORE acking the
original. A crash in between can duplicate a message, never lose it; the store's
dedupe makes the duplicate harmless.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

import aio_pika
import structlog
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from .config import Settings
from .handler import Action, MessageHandler, make_processor
from .logs import configure_logging, get_logger
from .notifier import FlakyNotifier, Notifier
from .retry import RetryPolicy
from .store import EventStore
from .topology import Topology, declare

log = get_logger(__name__)


class Consumer:
    def __init__(self, settings: Settings, handler: MessageHandler) -> None:
        self.settings = settings
        self.handler = handler
        self.topology = handler.topology
        self._connection: AbstractRobustConnection | None = None
        self._channel = None
        self._queue = None
        self._tag: str | None = None

    async def start(self) -> None:
        self._connection = await aio_pika.connect_robust(self.settings.amqp_url)
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self.settings.prefetch)
        declared = await declare(
            self._channel,
            self.topology,
            self.handler.policy.delays_ms,
            self.settings.delivery_limit,
        )
        self._queue = declared.queue
        self._tag = await self._queue.consume(self._on_message)
        log.info("consumer_started", queue=self.topology.queue, prefetch=self.settings.prefetch)

    async def stop(self) -> None:
        if self._queue is not None and self._tag is not None:
            with contextlib.suppress(Exception):
                await self._queue.cancel(self._tag)
        if self._connection is not None:
            await self._connection.close()
        log.info("consumer_stopped")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        structlog.contextvars.bind_contextvars(
            message_id=message.message_id, routing_key=message.routing_key
        )
        try:
            outcome = await asyncio.to_thread(
                self.handler.handle, message.body, message.headers, message.routing_key
            )
            fields = {
                "event_id": outcome.event_id,
                "result": outcome.result,
                "attempt": outcome.attempt,
            }
            if outcome.action is not Action.ACK:
                await self._channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.body,
                        headers=outcome.headers,
                        content_type=message.content_type or "application/json",
                        message_id=message.message_id,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=outcome.target_queue,
                )
            await message.ack()
            if outcome.action is Action.ACK:
                log.info("event_" + outcome.result, **fields)
            elif outcome.action is Action.RETRY:
                log.warning(
                    "event_retry_scheduled",
                    **fields,
                    retry_queue=outcome.target_queue,
                    delay_ms=outcome.delay_ms,
                    error=outcome.error,
                )
            else:
                log.error("event_dead_lettered", **fields, error=outcome.error)
        except Exception:
            # Could not publish the retry/DLQ copy (broker trouble): give the message
            # back. If the channel is gone the broker requeues it anyway.
            log.exception("message_handling_failed")
            with contextlib.suppress(Exception):
                await message.nack(requeue=True)
        finally:
            structlog.contextvars.clear_contextvars()


def build_consumer(settings: Settings, notifier: Notifier | None = None) -> Consumer:
    store = EventStore(settings.store_path)
    notifier = notifier or FlakyNotifier(settings.notifier_fail_rate)
    handler = MessageHandler(
        make_processor(store, notifier),
        RetryPolicy(settings.retry_delays_ms),
        Topology(prefix=settings.prefix),
    )
    return Consumer(settings, handler)


async def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    consumer = build_consumer(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await consumer.start()
    await stop.wait()
    await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
