"""End-to-end tests against a real RabbitMQ.

Skipped when no broker is reachable at RABBITMQ_URL (default
amqp://stem:stem@localhost:5672/). Set REQUIRE_RABBITMQ=1 (CI does) to turn the
skip into a failure. Every test uses its own random exchange/queue prefix and
deletes it afterwards.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
import uuid

import aio_pika
import pytest

from stem_events import dlq
from stem_events.config import Settings
from stem_events.consumer import build_consumer
from stem_events.publisher import publish_event, publish_raw
from stem_events.store import EventStore
from stem_events.topology import Topology, declare

from .conftest import ScriptedNotifier, finished

pytestmark = pytest.mark.integration

URL = os.environ.get("RABBITMQ_URL", "amqp://stem:stem@localhost:5672/")


@pytest.fixture
async def broker():
    try:
        conn = await aio_pika.connect(URL, timeout=3)
    except Exception as exc:  # noqa: BLE001
        msg = f"RabbitMQ not reachable at {URL}: {type(exc).__name__}"
        if os.environ.get("REQUIRE_RABBITMQ") == "1":
            pytest.fail(msg)
        pytest.skip(msg)
    yield conn
    await conn.close()


@pytest.fixture
async def settings(broker, tmp_path):
    s = Settings(
        amqp_url=URL,
        prefix=f"test.{uuid.uuid4().hex[:8]}",
        retry_delays_ms=(200, 400),
        delivery_limit=2,
        prefetch=5,
        store_path=str(tmp_path / "events.sqlite3"),
    )
    yield s
    topo = Topology(prefix=s.prefix)
    ch = await broker.channel()
    for name in [topo.queue, topo.dlq, *[topo.retry_queue(i) for i in range(2)]]:
        await ch.queue_delete(name)
    for name in (topo.exchange, topo.dlx):
        await ch.exchange_delete(name)


async def wait_for(predicate, within: float = 10.0) -> None:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition not met within timeout")


async def exchange_for(broker, settings):
    ch = await broker.channel(publisher_confirms=True)
    d = await declare(
        ch, Topology(prefix=settings.prefix), settings.retry_delays_ms, settings.delivery_limit
    )
    return d.exchange


async def dlq_size(settings) -> int:
    return (await dlq.cmd_stats(settings))[Topology(prefix=settings.prefix).dlq]


async def test_processes_event_and_dedupes_duplicate_delivery(broker, settings):
    consumer = build_consumer(settings, ScriptedNotifier())
    await consumer.start()
    try:
        ex = await exchange_for(broker, settings)
        evt = finished(job_id=11)
        await publish_event(ex, evt)
        await publish_event(ex, evt)  # duplicate delivery
        store = EventStore(settings.store_path)
        await wait_for(lambda: store.job(11) is not None)
        await asyncio.sleep(0.5)  # give the duplicate time to arrive
        assert store.processed_count() == 1
        assert store.job(11)["status"] == "completed"
    finally:
        await consumer.stop()


async def test_bad_payload_lands_in_dlq(broker, settings):
    consumer = build_consumer(settings, ScriptedNotifier())
    await consumer.start()
    try:
        ex = await exchange_for(broker, settings)
        await publish_raw(ex, "job.finished", b'{"job_id": "nope"}', message_id="bad-1")

        async def one_in_dlq():
            return await dlq_size(settings) == 1

        await wait_for(one_in_dlq)
        [item] = await dlq.cmd_list(settings, limit=10)
        assert item["reason"] == "invalid_payload" and item["message_id"] == "bad-1"
        assert await dlq_size(settings) == 1  # listing did not consume it
    finally:
        await consumer.stop()


async def test_transient_failure_retried_via_ttl_queues(broker, settings):
    notifier = ScriptedNotifier(failures={21: 2})  # fail twice, then succeed
    consumer = build_consumer(settings, notifier)
    await consumer.start()
    try:
        ex = await exchange_for(broker, settings)
        await publish_event(ex, finished(job_id=21))
        store = EventStore(settings.store_path)
        await wait_for(lambda: store.job(21) is not None)
        assert notifier.calls[21] == 3
        assert await dlq_size(settings) == 0
    finally:
        await consumer.stop()


async def test_exhausted_message_dead_lettered_then_replayed(broker, settings):
    notifier = ScriptedNotifier(always_fail={31})
    consumer = build_consumer(settings, notifier)
    await consumer.start()
    try:
        ex = await exchange_for(broker, settings)
        evt = finished(job_id=31)
        await publish_event(ex, evt)

        async def one_in_dlq():
            return await dlq_size(settings) == 1

        await wait_for(one_in_dlq)
        assert notifier.calls[31] == 3  # 1 try + 2 retries
        [item] = await dlq.cmd_list(settings, limit=10)
        assert item["reason"] == "retries_exhausted" and item["attempts"] == 2

        notifier.always_fail.clear()  # downstream recovered
        replayed = await dlq.cmd_replay(settings, limit=10, reason="retries_exhausted")
        assert [r["message_id"] for r in replayed] == [str(evt.event_id)]
        store = EventStore(settings.store_path)
        await wait_for(lambda: store.job(31) is not None)
        assert await dlq_size(settings) == 0
    finally:
        await consumer.stop()


async def test_crash_loop_hits_delivery_limit_and_is_dead_lettered(broker, settings):
    """Simulate a consumer that dies mid-message: take it unacked, drop the channel."""
    ex = await exchange_for(broker, settings)
    await publish_event(ex, finished(job_id=41))
    topo = Topology(prefix=settings.prefix)
    for _ in range(settings.delivery_limit + 1):
        ch = await broker.channel()
        q = await ch.declare_queue(topo.queue, passive=True)
        msg = None
        for _ in range(50):
            msg = await q.get(no_ack=False, fail=False)
            if msg:
                break
            await asyncio.sleep(0.1)
        if msg is None:
            break  # already dead-lettered by the broker
        await ch.close()  # "crash": message returns to the queue, delivery count +1

    async def one_in_dlq():
        return await dlq_size(settings) == 1

    await wait_for(one_in_dlq)
    [item] = await dlq.cmd_list(settings, limit=10)
    assert item["reason"] == "broker:delivery_limit"
