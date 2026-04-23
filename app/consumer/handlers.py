import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.consumer import runtime
from app.consumer.broker import broker, payments_exchange, payments_queue
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentEvent
from app.services.webhook_service import send_webhook

logger = logging.getLogger(__name__)


async def handle_payment_event(
    event: PaymentEvent,
    session_factory: async_sessionmaker,
    http_client: httpx.AsyncClient,
) -> None:
    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)

        if payment is None:
            logger.warning("Payment %s not found, skipping", event.payment_id)
            return

        if payment.status != PaymentStatus.PENDING.value:
            logger.info("Payment %s already processed, skipping", event.payment_id)
            return

    new_status = await _emulate_gateway(event.payment_id)

    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)
        payment.status = new_status.value
        payment.processed_at = datetime.now(timezone.utc)
        await session.commit()

    await send_webhook(http_client, payment)
    logger.info(
        "Payment %s processed with status %s",
        event.payment_id, new_status.value,
    )


async def _emulate_gateway(payment_id: uuid.UUID) -> PaymentStatus:
    delay = random.uniform(2.0, 5.0)
    await asyncio.sleep(delay)
    if random.random() < 0.9:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED


@broker.subscriber(payments_queue, payments_exchange, retry=False)
async def process_payment(body: dict) -> None:
    event = PaymentEvent(**body)
    logger.info("Processing payment %s", event.payment_id)

    assert runtime.session_factory is not None, "session_factory not initialized"
    assert runtime.http_client is not None, "http_client not initialized"

    await handle_payment_event(
        event,
        runtime.session_factory,
        runtime.http_client,
    )
