import logging

import httpx

from app.config import settings
from app.models.payment import Payment

logger = logging.getLogger(__name__)

# 4xx means the receiver rejected the request itself; retrying will not help.
# 408/429 are the exceptions: the receiver asks us to come back later.
RETRYABLE_CLIENT_STATUSES = {408, 429}


class WebhookDeliveryError(Exception):
    """Base class for webhook delivery failures."""


class RetryableWebhookError(WebhookDeliveryError):
    """Network error, timeout, 5xx, 408 or 429."""


class PermanentWebhookError(WebhookDeliveryError):
    """Any other 4xx: not worth retrying, goes straight to DLQ."""


def build_payload(payment: Payment) -> dict:
    return {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


async def send_webhook(client: httpx.AsyncClient, payment: Payment) -> None:
    """Single delivery attempt. Retries are driven by the broker (retry queues)."""
    try:
        response = await client.post(
            payment.webhook_url,
            json=build_payload(payment),
            timeout=settings.webhook_timeout_seconds,
        )
    except httpx.HTTPError as exc:
        raise RetryableWebhookError(
            f"Webhook for payment {payment.id} failed: {type(exc).__name__}: {exc}"
        ) from exc

    if response.is_success:
        logger.info("Webhook sent for payment %s", payment.id)
        return

    message = f"Webhook for payment {payment.id} returned HTTP {response.status_code}"
    if response.status_code >= 500 or response.status_code in RETRYABLE_CLIENT_STATUSES:
        raise RetryableWebhookError(message)
    raise PermanentWebhookError(message)
