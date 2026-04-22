import json

import pytest
from aio_pika import connect_robust, ExchangeType
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.outbox.worker import publish_pending_messages
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService


@pytest.mark.asyncio
async def test_outbox_publishes_to_rabbitmq(
    db_session: AsyncSession,
    session_factory,
    rabbitmq_url,
):
    service = PaymentService(db_session)
    request = PaymentCreateRequest(
        amount="99.00",
        currency="EUR",
        description="Outbox test",
        webhook_url="https://example.com/hook",
    )
    payment = await service.create_payment(request, "outbox-test-key")

    result = await db_session.execute(
        select(OutboxMessage).where(
            OutboxMessage.aggregate_id == str(payment.id)
        )
    )
    outbox_msg = result.scalar_one()
    assert outbox_msg.processed_at is None

    connection = await connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "payments_exchange",
            type=ExchangeType.DIRECT,
            durable=True,
        )
        queue = await channel.declare_queue("payments.new", durable=True)
        await queue.bind(exchange, routing_key="payments.new")

        published = await publish_pending_messages(
            session_factory,
            exchange,
            batch_size=10,
        )
        assert published == 1

        message = await queue.get(timeout=5)
        assert message is not None
        payload = json.loads(message.body)
        assert payload["payment_id"] == str(payment.id)
        assert payload["idempotency_key"] == "outbox-test-key"
        await message.ack()
    finally:
        await connection.close()

    async with session_factory() as session:
        result = await session.execute(
            select(OutboxMessage).where(
                OutboxMessage.aggregate_id == str(payment.id)
            )
        )
        outbox_msg = result.scalar_one()

    assert outbox_msg.processed_at is not None
