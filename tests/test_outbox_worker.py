import json
from unittest.mock import AsyncMock

import pytest
from aio_pika import ExchangeType, connect_robust
from aio_pika.exceptions import ChannelInvalidStateError
from sqlalchemy import select

from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from app.models.outbox import OutboxMessage
from app.outbox.worker import publish_pending_messages
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService
from tests.helpers import fetch


async def _create_payments(db_session, count: int) -> list:
    service = PaymentService(db_session)
    payments = []
    for i in range(count):
        payments.append(
            await service.create_payment(
                PaymentCreateRequest(
                    amount="99.00",
                    currency="EUR",
                    description=f"Outbox test {i}",
                    webhook_url="https://example.com/hook",
                ),
                f"outbox-key-{i}",
            )
        )
    return payments


async def _outbox_rows(session_factory) -> list[OutboxMessage]:
    async with session_factory() as session:
        result = await session.execute(
            select(OutboxMessage).order_by(OutboxMessage.created_at)
        )
        return list(result.scalars())


@pytest.mark.asyncio
async def test_outbox_publishes_to_rabbitmq(db_session, session_factory, rabbit_channel):
    [payment] = await _create_payments(db_session, 1)
    exchange = await rabbit_channel.get_exchange(PAYMENTS_EXCHANGE)

    published = await publish_pending_messages(session_factory, exchange, batch_size=10)
    assert published == 1

    message = await fetch(await rabbit_channel.get_queue(PAYMENTS_QUEUE))
    assert message is not None
    payload = json.loads(message.body)
    assert payload["payment_id"] == str(payment.id)
    assert payload["idempotency_key"] == "outbox-key-0"
    await message.ack()

    [row] = await _outbox_rows(session_factory)
    assert row.processed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [ConnectionError("refused"), ChannelInvalidStateError("closed"), TimeoutError()],
)
async def test_broker_outage_does_not_burn_retries(db_session, session_factory, error):
    """Regression: every failed tick incremented retries, so a ~5s RabbitMQ outage
    left messages permanently unpublished and payments stuck in pending."""
    await _create_payments(db_session, 1)
    exchange = AsyncMock()
    exchange.publish.side_effect = error

    for _ in range(10):
        assert await publish_pending_messages(session_factory, exchange) == 0

    [row] = await _outbox_rows(session_factory)
    assert row.retries == 0
    assert row.processed_at is None


@pytest.mark.asyncio
async def test_outage_mid_batch_keeps_already_published(db_session, session_factory):
    await _create_payments(db_session, 3)
    exchange = AsyncMock()
    exchange.publish.side_effect = [None, ConnectionError("lost")]

    assert await publish_pending_messages(session_factory, exchange) == 1

    rows = await _outbox_rows(session_factory)
    assert [r.processed_at is not None for r in rows] == [True, False, False]
    assert all(r.retries == 0 for r in rows)


@pytest.mark.asyncio
async def test_unroutable_message_is_not_marked_published(
    db_session, session_factory, rabbitmq_url
):
    """Regression: with no queue bound, the broker confirmed and dropped the message
    while the outbox marked it as published."""
    await _create_payments(db_session, 1)

    connection = await connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        exchange = await channel.declare_exchange(
            "test_unroutable", ExchangeType.DIRECT, auto_delete=True
        )
        assert await publish_pending_messages(session_factory, exchange) == 0
    finally:
        await connection.close()

    [row] = await _outbox_rows(session_factory)
    assert row.processed_at is None
    assert row.retries == 1
    assert "NO_ROUTE" in row.error
