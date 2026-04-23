import json

import pytest
from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust


@pytest.mark.asyncio
async def test_rejected_message_lands_in_dlq(rabbitmq_url):
    connection = await connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()

        payments_exchange = await channel.declare_exchange(
            "payments_exchange", ExchangeType.DIRECT, durable=True,
        )
        dlx = await channel.declare_exchange(
            "payments_dlx", ExchangeType.DIRECT, durable=True,
        )
        dead_queue = await channel.declare_queue("payments.dead", durable=True)
        await dead_queue.bind(dlx, routing_key="payments.dead")

        main_queue = await channel.declare_queue(
            "payments.new",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "payments_dlx",
                "x-dead-letter-routing-key": "payments.dead",
            },
        )
        await main_queue.bind(payments_exchange, routing_key="payments.new")

        body = {"payment_id": "00000000-0000-0000-0000-000000000001", "idempotency_key": "dlq-k"}
        await payments_exchange.publish(
            Message(
                body=json.dumps(body).encode(),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key="payments.new",
        )

        delivered = await main_queue.get(timeout=5)
        assert delivered is not None
        await delivered.reject(requeue=False)

        dlq_message = await dead_queue.get(timeout=5)
        assert dlq_message is not None
        assert json.loads(dlq_message.body) == body
        await dlq_message.ack()
    finally:
        await connection.close()
