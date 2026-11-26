"""End-to-end retry/DLQ behaviour against a real RabbitMQ.

The business logic (handle_payment_event) is stubbed; what is tested here is the
message flow: payments.new -> payments.retry.<N>ms -> payments.new -> ... -> payments.dead.
"""
import asyncio
import json
import uuid

import pytest
import pytest_asyncio
from aio_pika import DeliveryMode, Message

import app.consumer.handlers as handlers_module
from app.consumer.broker import broker
from app.messaging.topology import (
    ATTEMPT_HEADER,
    DEAD_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    retry_queue_for,
)
from app.services.webhook_service import PermanentWebhookError
from tests.conftest import TEST_MAX_ATTEMPTS, TEST_RETRY_BASE_DELAY
from tests.helpers import fetch


@pytest_asyncio.fixture
async def running_consumer(rabbit_channel, rabbitmq_url):
    await broker.connect(rabbitmq_url)
    await broker.start()
    yield
    await broker.close()


async def _publish(channel, body) -> None:
    exchange = await channel.get_exchange(PAYMENTS_EXCHANGE)
    await exchange.publish(
        Message(
            body=json.dumps(body).encode(),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=PAYMENTS_QUEUE,
    )


def _event() -> dict:
    return {"payment_id": str(uuid.uuid4()), "idempotency_key": "k"}


def _stub_handler(monkeypatch, *outcomes):
    """Each call pops the next outcome: an exception to raise, or None for success."""
    calls = []
    queue = list(outcomes)

    async def handler(event, *_):
        calls.append(asyncio.get_running_loop().time())
        outcome = queue.pop(0) if queue else None
        if outcome is not None:
            raise outcome

    monkeypatch.setattr(handlers_module, "handle_payment_event", handler)
    return calls


@pytest.mark.asyncio
async def test_message_goes_to_dlq_after_max_attempts_with_backoff(
    monkeypatch, rabbit_channel, running_consumer
):
    # Fetching from payments.dead while the consumer runs is also the regression
    # check for the old DLQ subscriber that consumed and acked dead messages.
    calls = _stub_handler(monkeypatch, *[RuntimeError("db down")] * 10)
    body = _event()

    await _publish(rabbit_channel, body)

    dead = await fetch(await rabbit_channel.get_queue(DEAD_QUEUE), timeout=10)
    assert dead is not None, "message did not reach the DLQ"
    assert json.loads(dead.body) == body
    assert dead.headers[ATTEMPT_HEADER] == TEST_MAX_ATTEMPTS
    await dead.ack()

    assert len(calls) == TEST_MAX_ATTEMPTS
    # Exponential backoff between attempts: base, then 2 * base.
    gaps = [b - a for a, b in zip(calls, calls[1:])]
    assert gaps[0] >= TEST_RETRY_BASE_DELAY * 0.9
    assert gaps[1] >= TEST_RETRY_BASE_DELAY * 2 * 0.9


@pytest.mark.asyncio
async def test_transient_failure_recovers_without_dlq(
    monkeypatch, rabbit_channel, running_consumer
):
    calls = _stub_handler(monkeypatch, RuntimeError("blip"), None)

    await _publish(rabbit_channel, _event())
    await asyncio.sleep(TEST_RETRY_BASE_DELAY * 4)

    assert len(calls) == 2
    assert await fetch(await rabbit_channel.get_queue(DEAD_QUEUE), timeout=0.5) is None


@pytest.mark.asyncio
async def test_permanent_webhook_error_skips_retries(
    monkeypatch, rabbit_channel, running_consumer
):
    calls = _stub_handler(monkeypatch, PermanentWebhookError("HTTP 404"))

    await _publish(rabbit_channel, _event())

    dead = await fetch(await rabbit_channel.get_queue(DEAD_QUEUE))
    assert dead is not None
    await dead.ack()
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_malformed_message_goes_straight_to_dlq(
    monkeypatch, rabbit_channel, running_consumer
):
    calls = _stub_handler(monkeypatch)

    await _publish(rabbit_channel, {"unexpected": "payload"})

    dead = await fetch(await rabbit_channel.get_queue(DEAD_QUEUE))
    assert dead is not None
    await dead.ack()
    assert calls == []


@pytest.mark.asyncio
async def test_retry_queue_routes_back_after_ttl(rabbit_channel):
    retry_queue = retry_queue_for(1, TEST_RETRY_BASE_DELAY)
    exchange = await rabbit_channel.get_exchange(PAYMENTS_EXCHANGE)
    await exchange.publish(
        Message(body=b"{}", headers={ATTEMPT_HEADER: 2}),
        routing_key=retry_queue.name,
    )

    main = await rabbit_channel.get_queue(PAYMENTS_QUEUE)
    assert await fetch(main, timeout=TEST_RETRY_BASE_DELAY / 2) is None
    message = await fetch(main, timeout=TEST_RETRY_BASE_DELAY * 5)
    assert message is not None
    assert message.headers[ATTEMPT_HEADER] == 2
    await message.ack()
