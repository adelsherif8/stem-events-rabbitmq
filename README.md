# stem-events-rabbitmq

An event-driven companion to [Stem SaaS](https://github.com/adelsherif8/stem-saas) (FastAPI + Celery + Redis + Postgres).
When a stem-separation job finishes, the worker publishes a `job.finished` event to a
RabbitMQ **topic exchange**. A small async Python consumer validates each event with
**pydantic**, applies it **idempotently** (dedupe by `event_id`), retries transient
failures through **delayed retry queues** (per-queue TTL + dead-lettering back to the
main queue, capped attempts, growing delays), and sends poison or exhausted messages to a
**dead-letter queue** that you can inspect and replay from a CLI.

It is self-contained: `docker compose up` runs the broker, the consumer and a publisher
that simulates Celery job events. The original repo is not needed.

## Architecture

```mermaid
flowchart LR
    P["Celery worker / simulator<br/>(publisher)"] -- "job.started / job.finished" --> X{{"stem.events<br/>topic exchange"}}
    X -- "job.*" --> Q[["stem.events.jobs<br/>quorum queue<br/>x-delivery-limit"]]
    Q --> C["consumer<br/>validate - dedupe - apply"]
    C -- "ok / duplicate: ack" --> S[("SQLite<br/>processed_events<br/>job_state")]
    C -- "1st failure" --> R1[["retry.1 TTL 1s"]]
    C -- "2nd failure" --> R2[["retry.2 TTL 5s"]]
    C -- "3rd failure" --> R3[["retry.3 TTL 25s"]]
    R1 & R2 & R3 -- "TTL expires: dead-letter<br/>via default exchange" --> Q
    C -- "invalid payload /<br/>retries exhausted" --> D[["stem.events.jobs.dlq"]]
    Q -- "delivery limit hit<br/>(crash loop)" --> DLX{{"stem.events.dlx<br/>fanout"}} --> D
    D -. "dlq replay" .-> Q
```

How a message moves:

1. The publisher sends a persistent JSON message with routing key `job.finished`
   (publisher confirms on).
2. The consumer (`aio-pika`, prefetch 10) parses it into a pydantic model. Bad JSON, a
   schema violation, an inconsistent payload (for example `duration_ms` that does not match
   `finished_at - started_at`) or an oversized body is a **permanent** error: the message
   goes straight to the DLQ with `x-dlq-reason: invalid_payload`.
3. A valid event is applied in one SQLite transaction: check `processed_events`, run the
   side effect (a stand-in "notify the user" call), insert the `event_id`, upsert
   `job_state`. If the `event_id` is already there, it is a duplicate and is acked without
   doing anything.
4. If the side effect raises a **transient** error (or anything unexpected), the
   transaction rolls back and the consumer republishes the message to retry queue
   `retry.N` with `x-attempt = N`, then acks the original. When the TTL expires RabbitMQ
   dead-letters it back into the main queue.
5. After the last tier (`RETRY_DELAYS_MS`, default `1000,5000,25000`, so 4 attempts in total)
   the message goes to the DLQ with `x-dlq-reason: retries_exhausted` and the last error.

Design choices worth calling out:

| Choice | Why |
|---|---|
| Topic exchange, consumer binds `job.*` | Other services (billing, analytics) can bind their own queues without touching the publisher. |
| Main queue is a **quorum** queue with `x-delivery-limit` | Replicated, and it counts redeliveries, so a message that keeps crashing the consumer is dead-lettered by the broker instead of looping forever. |
| One retry queue per delay, **per-queue** TTL | Per-message TTL only expires from the head of a queue, so a long delay would block short ones behind it. |
| Retries re-enter only the main queue (default exchange + queue name) | Re-publishing to the topic exchange would also redeliver to every other bound service. |
| Publish the retry/DLQ copy **before** acking | A crash in between can duplicate a message but never lose it; dedupe absorbs the duplicate. |
| Dedupe record written in the same transaction as the state change | Gives an "effectively once" effect on top of at-least-once delivery. |
| `job_state` only updated by events at least as new as the stored one | Retries reorder events (a retried `job.started` can arrive after `job.finished`). |
| DLQ is a classic queue | Peeking (get without ack, then close) does not count toward any delivery limit. |
| Broker-agnostic `MessageHandler` returns an `Outcome` | Validation, dedupe and retry logic are unit-tested without a broker; the aio-pika layer only executes the outcome. |

## Failure modes

| Scenario | What happens | Covered by |
|---|---|---|
| **Bad payload** (invalid JSON, missing/extra field, naive datetime, inconsistent timings, wrong `event_type`, > 64 KB) | Classified as permanent, never retried. Republished to the DLQ with `x-dlq-reason=invalid_payload`, the validation error in `x-last-error`, and the original routing key, then acked. | unit: `test_events.py`, `test_handler.py`; integration: `test_bad_payload_lands_in_dlq` |
| **Transient downstream failure** (notifier timeout, SQLite `database is locked`) | Transaction rolls back (nothing recorded), message goes to `retry.1`, `retry.2`, `retry.3` with growing TTLs, then to the DLQ with `retries_exhausted`. Unknown exceptions are treated the same way but labelled `unexpected_error`. | unit: `test_retry.py`, `test_store.py`, `test_handler.py`; integration: `test_transient_failure_retried_via_ttl_queues`, `test_exhausted_message_dead_lettered_then_replayed` |
| **Consumer crash mid-message** | The message was never acked, so RabbitMQ redelivers it when the connection drops. If it was already processed, dedupe skips it. If the message itself causes the crash, the quorum queue's delivery limit (default 5) dead-letters it to the DLX, and it shows up in the DLQ as `broker:delivery_limit`. Note the side effect runs inside the transaction: if the process dies after the external call but before commit, that call can happen twice (at-least-once for external effects). | integration: `test_crash_loop_hits_delivery_limit_and_is_dead_lettered` (simulates the crash by dropping the channel) |
| **Broker restart** | Exchanges and queues are durable, messages are persistent, the main queue is a quorum queue, and the compose file pins the hostname and mounts a volume so the node keeps its data. Publishers use confirms, so an unconfirmed publish raises instead of being silently lost. The consumer uses `connect_robust`, which reconnects and re-declares its consumer. Messages in flight and unacked are redelivered and deduped. | design only, not tested automatically |
| **Duplicate delivery** (publisher retry, redelivery after crash, replay) | `event_id` is the primary key of `processed_events`; the second copy is acked as `duplicate` and its side effect is not run. | unit: `test_store.py::test_duplicate_event_applied_once`, `test_handler.py`; integration: `test_processes_event_and_dedupes_duplicate_delivery` |
| **Retry or DLQ publish fails** (broker down at that moment) | The consumer nacks with requeue (or the broker requeues on channel loss), so the message is not lost. | design only |
| **Out-of-order events** | `job_state` ignores an event older than the one it already applied. | unit: `test_store.py::test_out_of_order_events_do_not_regress_state` |

## Run it

### Full demo with Docker

```bash
docker compose up --build
# management UI: http://localhost:15672  (user stem / password stem)
```

The publisher sends 20 simulated jobs (about 15% end in `failed`), re-sends every 5th
`job.finished` as a duplicate and injects a malformed payload every 7th job. The consumer
runs with `NOTIFIER_FAIL_RATE=0.3`, so you will see `event_retry_scheduled` lines in its JSON
logs and, because the failure is random per attempt, some messages may exhaust their
retries and land in the DLQ.

```bash
docker compose logs -f consumer
docker compose run --rm consumer python -m stem_events.dlq stats
docker compose run --rm consumer python -m stem_events.dlq list
docker compose run --rm consumer python -m stem_events.dlq replay --reason retries_exhausted --dry-run
docker compose run --rm consumer python -m stem_events.dlq replay --reason retries_exhausted
docker compose run --rm publisher python -m stem_events.publisher --jobs 5   # send more
```

### Locally

```bash
python3.14 -m venv .venv            # 3.12+ works
.venv/bin/pip install -r requirements-dev.txt
docker compose up -d rabbitmq
.venv/bin/python -m stem_events.consumer                      # terminal 1
.venv/bin/python -m stem_events.publisher --jobs 10 --bad-every 4 --dup-every 3   # terminal 2
.venv/bin/python -m stem_events.dlq list
```

Configuration (environment variables):

| Variable | Default | Meaning |
|---|---|---|
| `AMQP_URL` | `amqp://stem:stem@localhost:5672/` | Broker URL |
| `EVENTS_PREFIX` | `stem.events` | Prefix for every exchange/queue name |
| `RETRY_DELAYS_MS` | `1000,5000,25000` | One retry queue per value; attempts = values + 1 |
| `DELIVERY_LIMIT` | `5` | Quorum-queue redelivery cap before broker dead-lettering |
| `PREFETCH` | `10` | Unacked messages per consumer |
| `STORE_PATH` | `./data/events.sqlite3` | Idempotency + state store |
| `NOTIFIER_FAIL_RATE` | `0.0` | Demo knob: probability the side effect raises a transient error |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `json` | `console` for human-readable logs |

Changing `RETRY_DELAYS_MS` or `DELIVERY_LIMIT` changes queue arguments, and RabbitMQ refuses
to redeclare an existing queue with different arguments. Delete the queues (or use a new
`EVENTS_PREFIX`) after changing them.

### Tests

```bash
.venv/bin/pytest -v -rs          # unit tests; integration tests skip if no broker is reachable
./scripts/integration.sh         # starts RabbitMQ in Docker and runs the integration tests
```

Integration tests connect to `RABBITMQ_URL` (default `amqp://stem:stem@localhost:5672/`),
use a random queue prefix per test and delete it afterwards. They skip when the broker is
unreachable; with `REQUIRE_RABBITMQ=1` (set in CI) they fail instead of skipping.
`.github/workflows/ci.yml` runs ruff, the unit tests and the integration tests against a
`rabbitmq:4.4-management` service container, and builds the Docker image.

## Hooking it into Stem SaaS

This repo does not modify Stem SaaS. To publish real events, the worker would call a
publisher where `app/tasks.py::separate_stems` marks the job `processing` and
`completed`/`failed`. A sketch (not wired up or tested here):

```python
# in stem-saas, after job.status = JobStatus.completed; db.commit()
from stem_events.events import job_finished

event = job_finished(
    job.id,
    job.project_id,
    "completed",
    started_at,
    finished_at,
    stems=len(stems),
    workspace_id=job.workspace_id,
)
publish(event)  # a sync pika/kombu publisher with confirms, routing key event.event_type
```

To avoid losing an event if the broker is down right after the DB commit, a production
version would write the event to an outbox table in the same transaction and publish
from there (transactional outbox).

## Layout

```
stem_events/
  events.py     pydantic event models, parse_event, builders
  retry.py      PermanentError / TransientError, RetryPolicy, decide()
  handler.py    MessageHandler: bytes + headers -> Outcome (no broker code)
  store.py      SQLite idempotent store (processed_events + job_state)
  notifier.py   stand-in side effect with a configurable failure rate
  topology.py   exchange/queue names and the single declare() used everywhere
  consumer.py   aio-pika consumer that executes Outcomes
  publisher.py  simulated Celery job events
  dlq.py        stats / list / replay / purge CLI
tests/          unit tests + integration tests (real RabbitMQ)
```

Stack: Python 3.14 (3.12+), aio-pika 10.2, pydantic 2.14, structlog 26.1, pytest 9.2,
RabbitMQ 4.4.
