import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

import httpx
from faststream.exceptions import RejectMessage
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.consumer import runtime
from app.consumer.broker import broker, payments_exchange, payments_queue
from app.messaging.topology import ATTEMPT_HEADER, retry_queue_for
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentEvent
from app.services.webhook_service import PermanentWebhookError, send_webhook

logger = logging.getLogger(__name__)


async def handle_payment_event(
    event: PaymentEvent,
    session_factory: async_sessionmaker,
    http_client: httpx.AsyncClient,
) -> None:
    """One processing attempt. Safe to run any number of times for the same event.

    The two steps are tracked independently, so a retry (or a message replayed
    from the DLQ) resumes where the previous attempt stopped:
      1. gateway result -> status / processed_at   (applied exactly once)
      2. webhook        -> webhook_delivered_at    (at-least-once)
    """
    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)

    if payment is None:
        logger.warning("Payment %s not found, skipping", event.payment_id)
        return

    if payment.status == PaymentStatus.PENDING.value:
        new_status = await _emulate_gateway(event.payment_id)
        payment = await _apply_gateway_result(
            session_factory, event.payment_id, new_status
        )

    if payment.webhook_delivered_at is not None:
        logger.info("Webhook for payment %s already delivered, skipping", payment.id)
        return

    await send_webhook(http_client, payment)
    await _mark_webhook_delivered(session_factory, payment.id)
    logger.info("Payment %s processed with status %s", payment.id, payment.status)


async def _emulate_gateway(payment_id: uuid.UUID) -> PaymentStatus:
    delay = random.uniform(2.0, 5.0)
    await asyncio.sleep(delay)
    if random.random() < 0.9:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED


async def _apply_gateway_result(
    session_factory: async_sessionmaker,
    payment_id: uuid.UUID,
    new_status: PaymentStatus,
) -> Payment:
    async with session_factory() as session:
        # Conditional UPDATE instead of read-modify-write: if the same message is
        # being processed concurrently, only one worker wins the transition.
        result = await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .where(Payment.status == PaymentStatus.PENDING.value)
            .values(status=new_status.value, processed_at=datetime.now(timezone.utc))
            .returning(Payment)
        )
        payment = result.scalar_one_or_none()
        await session.commit()

        if payment is None:
            logger.info(
                "Payment %s was finalized by another worker, keeping its result",
                payment_id,
            )
            payment = await session.get(Payment, payment_id, populate_existing=True)
        return payment


async def _mark_webhook_delivered(
    session_factory: async_sessionmaker, payment_id: uuid.UUID
) -> None:
    async with session_factory() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=datetime.now(timezone.utc))
        )
        await session.commit()


def get_attempt(headers: dict | None) -> int:
    try:
        return max(1, int((headers or {}).get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


@broker.subscriber(payments_queue, payments_exchange, retry=False)
async def process_payment(body: dict, message: RabbitMessage) -> None:
    attempt = get_attempt(message.headers)
    max_attempts = settings.max_processing_attempts

    try:
        event = PaymentEvent(**body)
    except (TypeError, ValidationError) as exc:
        logger.error("Malformed message, sending to DLQ: %s", body)
        raise RejectMessage() from exc

    logger.info(
        "Processing payment %s (attempt %d/%d)", event.payment_id, attempt, max_attempts
    )

    try:
        await handle_payment_event(event, runtime.session_factory, runtime.http_client)
    except PermanentWebhookError as exc:
        logger.error("%s; not retryable, sending to DLQ", exc)
        raise RejectMessage() from exc
    except Exception as exc:
        if attempt >= max_attempts:
            logger.error(
                "Payment %s failed after %d attempts, sending to DLQ: %s",
                event.payment_id, attempt, exc,
            )
            # reject(requeue=False) -> payments_dlx -> payments.dead
            raise RejectMessage() from exc

        retry_queue = retry_queue_for(attempt, settings.retry_base_delay_seconds)
        # Publish the copy first, ack the original after (on normal return).
        # A crash in between only produces a duplicate, which the handler tolerates.
        await broker.publish(
            body,
            exchange=payments_exchange,
            routing_key=retry_queue.name,
            headers={ATTEMPT_HEADER: attempt + 1},
            message_id=message.message_id,
            persist=True,
        )
        logger.warning(
            "Payment %s attempt %d/%d failed: %s; retry in %d ms",
            event.payment_id, attempt, max_attempts, exc, retry_queue.delay_ms,
        )
