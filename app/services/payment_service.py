import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentEvent


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        existing = await self.session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        existing_payment = existing.scalar_one_or_none()
        if existing_payment is not None:
            return existing_payment

        payment_id = uuid.uuid4()
        payment = Payment(
            id=payment_id,
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            metadata_=request.metadata or {},
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            webhook_url=request.webhook_url,
        )
        self.session.add(payment)

        event = PaymentEvent(
            payment_id=payment_id,
            idempotency_key=idempotency_key,
        )
        outbox_msg = OutboxMessage(
            aggregate_type="payment",
            aggregate_id=str(payment_id),
            event_type="payment.created",
            payload=event.model_dump(mode="json"),
        )
        self.session.add(outbox_msg)

        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            result = await self.session.execute(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            return result.scalar_one()

        await self.session.refresh(payment)
        return payment

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id)
        )
        return result.scalar_one_or_none()
