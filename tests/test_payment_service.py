import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.models.payment import Payment
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService


def make_request(**overrides):
    data = {
        "amount": "50.00",
        "currency": "RUB",
        "description": "Service test",
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return PaymentCreateRequest(**data)


@pytest.mark.asyncio
async def test_create_payment_creates_outbox_message(db_session: AsyncSession):
    service = PaymentService(db_session)
    request = make_request()
    payment = await service.create_payment(request, "svc-atomic-key")

    assert payment.id is not None
    assert payment.status == "pending"
    assert payment.amount == 50

    result = await db_session.execute(
        select(OutboxMessage).where(
            OutboxMessage.aggregate_id == str(payment.id)
        )
    )
    outbox_msg = result.scalar_one()
    assert outbox_msg.event_type == "payment.created"
    assert outbox_msg.payload["payment_id"] == str(payment.id)


@pytest.mark.asyncio
async def test_idempotency_returns_existing(db_session: AsyncSession):
    service = PaymentService(db_session)
    request = make_request()

    p1 = await service.create_payment(request, "svc-idem-key")
    p2 = await service.create_payment(request, "svc-idem-key")

    assert p1.id == p2.id


@pytest.mark.asyncio
async def test_get_payment(db_session: AsyncSession):
    service = PaymentService(db_session)
    request = make_request()
    created = await service.create_payment(request, "svc-get-key")

    found = await service.get_payment(created.id)
    assert found is not None
    assert found.id == created.id


@pytest.mark.asyncio
async def test_get_nonexistent_returns_none(db_session: AsyncSession):
    import uuid
    service = PaymentService(db_session)
    result = await service.get_payment(uuid.uuid4())
    assert result is None
