import asyncio
import logging

import httpx

from app.config import settings
from app.models.payment import Payment

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass


async def send_webhook(client: httpx.AsyncClient, payment: Payment) -> None:
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }

    last_error: Exception | None = None
    for attempt in range(1, settings.webhook_max_retries + 1):
        try:
            response = await client.post(
                payment.webhook_url,
                json=payload,
                timeout=settings.webhook_timeout_seconds,
            )
            response.raise_for_status()
            logger.info("Webhook sent for payment %s", payment.id)
            return
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_error = exc
            logger.warning(
                "Webhook attempt %d/%d failed for payment %s: %s",
                attempt, settings.webhook_max_retries, payment.id, exc,
            )
            if attempt < settings.webhook_max_retries:
                await asyncio.sleep(2 ** attempt)

    raise WebhookDeliveryError(
        f"Webhook delivery failed for payment {payment.id} "
        f"after {settings.webhook_max_retries} attempts"
    ) from last_error
