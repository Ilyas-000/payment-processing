import asyncio
import logging
import random
from datetime import datetime, timezone

from faststream.rabbit import RabbitMessage
from sqlalchemy import select, update

from app.consumer.broker import broker, payments_queue, payments_exchange
from app.database import async_session_factory
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentEvent
from app.services.webhook_service import send_webhook

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


@broker.subscriber(payments_queue, payments_exchange, no_ack=True)
async def process_payment(body: dict, msg: RabbitMessage) -> None:
    event = PaymentEvent(**body)
    logger.info("Processing payment %s", event.payment_id)

    async with async_session_factory() as session:
        result = await session.execute(
            select(Payment).where(Payment.id == event.payment_id)
        )
        payment = result.scalar_one_or_none()

        if payment is None:
            logger.error("Payment %s not found, acking", event.payment_id)
            await msg.ack()
            return

        if payment.status != PaymentStatus.PENDING.value:
            logger.info("Payment %s already processed, acking", event.payment_id)
            await msg.ack()
            return

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            new_status = await _emulate_gateway(event.payment_id)
            await _finalize_payment(event, new_status)
            await msg.ack()
            logger.info("Payment %s completed with status %s", event.payment_id, new_status.value)
            return
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Payment %s attempt %d/%d failed: %s",
                event.payment_id, attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                backoff = 2 ** attempt
                await asyncio.sleep(backoff)

    await _finalize_payment(event, PaymentStatus.FAILED)
    await msg.reject(requeue=False)
    logger.error(
        "Payment %s failed after %d attempts, sent to DLQ: %s",
        event.payment_id, MAX_RETRIES, last_error,
    )


async def _emulate_gateway(payment_id) -> PaymentStatus:
    delay = random.uniform(2.0, 5.0)
    await asyncio.sleep(delay)

    if random.random() < 0.9:
        return PaymentStatus.SUCCEEDED

    raise RuntimeError(f"Payment gateway declined payment {payment_id}")


async def _finalize_payment(event: PaymentEvent, status: PaymentStatus) -> None:
    now = datetime.now(timezone.utc)

    async with async_session_factory() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == event.payment_id)
            .values(status=status.value, processed_at=now)
        )
        await session.commit()

        result = await session.execute(
            select(Payment).where(Payment.id == event.payment_id)
        )
        payment = result.scalar_one()

    await send_webhook(payment)
