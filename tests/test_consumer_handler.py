from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentEvent
from app.services.payment_service import PaymentService


@pytest.mark.asyncio
async def test_consumer_processes_payment_successfully(
    db_session: AsyncSession,
    session_factory,
):
    service = PaymentService(db_session)
    request = PaymentCreateRequest(
        amount="200.00",
        currency="RUB",
        description="Consumer test",
        webhook_url="https://example.com/hook",
    )
    payment = await service.create_payment(request, "consumer-test-key")

    event = PaymentEvent(
        payment_id=payment.id,
        idempotency_key="consumer-test-key",
    )

    msg = AsyncMock()
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()

    with patch("app.consumer.handlers.async_session_factory", session_factory), \
         patch("app.consumer.handlers._emulate_gateway", return_value=PaymentStatus.SUCCEEDED), \
         patch("app.consumer.handlers.send_webhook", new_callable=AsyncMock) as mock_webhook:

        from app.consumer.handlers import process_payment
        await process_payment(event.model_dump(mode="json"), msg)

    msg.ack.assert_called_once()
    msg.reject.assert_not_called()
    mock_webhook.assert_called_once()

    async with session_factory() as session:
        result = await session.execute(
            select(Payment).where(Payment.id == payment.id)
        )
        updated = result.scalar_one()

    assert updated.status == PaymentStatus.SUCCEEDED.value
    assert updated.processed_at is not None
