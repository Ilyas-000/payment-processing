import asyncio
import json
import logging
from datetime import datetime, timezone

from aio_pika import DeliveryMode, Message as AMQPMessage, connect_robust
from aio_pika.abc import AbstractExchange
from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.database import async_session_factory
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology
from app.models.outbox import OutboxMessage

logger = logging.getLogger(__name__)

# Broker is unreachable: nothing is wrong with the message itself, so these errors
# must not count towards outbox_publish_max_retries.
TRANSIENT_PUBLISH_ERRORS = (
    AMQPConnectionError,
    ChannelInvalidStateError,
    ConnectionError,
    asyncio.TimeoutError,
)


def describe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:1000]


async def publish_pending_messages(
    session_factory: async_sessionmaker,
    exchange: AbstractExchange,
    batch_size: int = 50,
    routing_key: str = PAYMENTS_QUEUE,
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
                await exchange.publish(
                    AMQPMessage(
                        body=json.dumps(msg.payload).encode(),
                        content_type="application/json",
                        delivery_mode=DeliveryMode.PERSISTENT,
                        message_id=str(msg.id),
                    ),
                    routing_key=routing_key,
                    mandatory=True,
                    timeout=settings.outbox_publish_timeout_seconds,
                )
            except TRANSIENT_PUBLISH_ERRORS as exc:
                # Keep what was already published in this batch, leave the rest
                # untouched and try again on the next tick.
                logger.warning(
                    "Broker unavailable, postponing outbox batch: %s",
                    describe_error(exc),
                )
                break
            except Exception as exc:
                # E.g. PublishError(NO_ROUTE): the broker returned the message as unroutable.
                msg.retries += 1
                msg.error = describe_error(exc)
                logger.error(
                    "Failed to publish outbox %s (attempt %d/%d): %s",
                    msg.id, msg.retries, settings.outbox_publish_max_retries, msg.error,
                )
                if msg.retries >= settings.outbox_publish_max_retries:
                    logger.critical(
                        "Outbox %s exceeded publish retries and needs manual action",
                        msg.id,
                    )
            else:
                msg.processed_at = datetime.now(timezone.utc)
                msg.error = None
                published += 1

        await session.commit()

    return published


async def run_outbox_worker() -> None:
    connection = await connect_robust(settings.rabbitmq_url)
    # Publisher confirms + on_return_raises: publish() only succeeds once the broker
    # has actually routed the message to a queue. Without it an unroutable message
    # is confirmed and silently dropped.
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    await declare_topology(
        channel,
        settings.max_processing_attempts,
        settings.retry_base_delay_seconds,
    )
    exchange = await channel.get_exchange(PAYMENTS_EXCHANGE)

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
