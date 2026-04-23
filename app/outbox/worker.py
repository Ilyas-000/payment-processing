import asyncio
import json
import logging
from datetime import datetime, timezone

from aio_pika import DeliveryMode, ExchangeType, Message as AMQPMessage, connect_robust
from aio_pika.abc import AbstractExchange
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.database import async_session_factory
from app.models.outbox import OutboxMessage

logger = logging.getLogger(__name__)


async def publish_pending_messages(
    session_factory: async_sessionmaker,
    exchange: AbstractExchange,
    batch_size: int = 50,
) -> int:
    published = 0
    async with session_factory() as session:
        result = await session.execute(
            select(OutboxMessage)
            .where(OutboxMessage.processed_at.is_(None))
            .where(OutboxMessage.retries < settings.outbox_publish_max_retries)
            .order_by(OutboxMessage.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        messages = result.scalars().all()

        for msg in messages:
            try:
                amqp_message = AMQPMessage(
                    body=json.dumps(msg.payload).encode(),
                    content_type="application/json",
                    delivery_mode=DeliveryMode.PERSISTENT,
                )
                await exchange.publish(
                    amqp_message,
                    routing_key="payments.new",
                )
                msg.processed_at = datetime.now(timezone.utc)
                published += 1
            except Exception as exc:
                msg.retries += 1
                msg.error = str(exc)
                logger.error("Failed to publish outbox %s: %s", msg.id, exc)

        await session.commit()

    return published


async def run_outbox_worker() -> None:
    connection = await connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "payments_exchange",
        type=ExchangeType.DIRECT,
        durable=True,
    )

    logger.info("Outbox worker started")

    while True:
        try:
            await publish_pending_messages(
                async_session_factory, exchange, settings.outbox_batch_size
            )
        except Exception as exc:
            logger.exception("Outbox worker error: %s", exc)

        await asyncio.sleep(settings.outbox_poll_interval_seconds)


if __name__ == "__main__":
    from app.logging_config import setup_logging

    setup_logging()
    asyncio.run(run_outbox_worker())
