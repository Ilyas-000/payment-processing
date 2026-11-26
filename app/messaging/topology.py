"""Single source of truth for the RabbitMQ topology.

Both the outbox worker and the consumer declare it on startup, so it does not
matter which process starts first: a message is never published into an
exchange that has no bound queue yet.

    payments_exchange (direct)
      ├─ payments.new             ← outbox publishes here; consumer reads
      │    dead-letter → payments_dlx / payments.dead
      └─ payments.retry.<N>ms     ← failed attempts wait here (queue TTL)
           dead-letter → payments_exchange / payments.new

    payments_dlx (direct)
      └─ payments.dead            ← messages that exhausted all attempts
                                     (no consumer: they stay for inspection)
"""
from dataclasses import dataclass

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel

PAYMENTS_EXCHANGE = "payments_exchange"
DLX_EXCHANGE = "payments_dlx"

PAYMENTS_QUEUE = "payments.new"
DEAD_QUEUE = "payments.dead"

ATTEMPT_HEADER = "x-attempt"


@dataclass(frozen=True)
class RetryQueue:
    name: str
    delay_ms: int


def retry_delay_ms(failed_attempt: int, base_delay_seconds: float) -> int:
    """Exponential backoff: 1st retry waits base, 2nd waits 2*base, ..."""
    return int(base_delay_seconds * 1000 * 2 ** (failed_attempt - 1))


def retry_queue_for(failed_attempt: int, base_delay_seconds: float) -> RetryQueue:
    delay_ms = retry_delay_ms(failed_attempt, base_delay_seconds)
    # Delay is part of the name: changing the config creates new queues instead of
    # clashing with an already-declared queue that has a different x-message-ttl.
    return RetryQueue(name=f"payments.retry.{delay_ms}ms", delay_ms=delay_ms)


def retry_queues(max_attempts: int, base_delay_seconds: float) -> list[RetryQueue]:
    # One fixed-TTL queue per retry step. Per-message TTL in a single queue would
    # suffer from head-of-line blocking (a 4s message would hold back a 2s one).
    return [
        retry_queue_for(attempt, base_delay_seconds)
        for attempt in range(1, max_attempts)
    ]


PAYMENTS_QUEUE_ARGUMENTS = {
    "x-dead-letter-exchange": DLX_EXCHANGE,
    "x-dead-letter-routing-key": DEAD_QUEUE,
}


def retry_queue_arguments(queue: RetryQueue) -> dict:
    return {
        "x-message-ttl": queue.delay_ms,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
        "x-dead-letter-routing-key": PAYMENTS_QUEUE,
    }


async def declare_topology(
    channel: AbstractChannel,
    max_attempts: int,
    base_delay_seconds: float,
) -> None:
    """Idempotently declare all exchanges, queues and bindings."""
    exchange = await channel.declare_exchange(
        PAYMENTS_EXCHANGE, ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(
        DLX_EXCHANGE, ExchangeType.DIRECT, durable=True
    )

    dead = await channel.declare_queue(DEAD_QUEUE, durable=True)
    await dead.bind(dlx, routing_key=DEAD_QUEUE)

    main = await channel.declare_queue(
        PAYMENTS_QUEUE, durable=True, arguments=PAYMENTS_QUEUE_ARGUMENTS
    )
    await main.bind(exchange, routing_key=PAYMENTS_QUEUE)

    for rq in retry_queues(max_attempts, base_delay_seconds):
        queue = await channel.declare_queue(
            rq.name, durable=True, arguments=retry_queue_arguments(rq)
        )
        await queue.bind(exchange, routing_key=rq.name)
