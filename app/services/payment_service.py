import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentEvent


class IdempotencyKeyConflict(Exception):
    """The Idempotency-Key was already used with a different request body."""


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        request_hash = request.fingerprint()

        existing_payment = await self._get_by_idempotency_key(idempotency_key)
        if existing_payment is not None:
            return self._replay(existing_payment, request_hash)

        payment_id = uuid.uuid4()
        payment = Payment(
            id=payment_id,
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            metadata_=request.metadata or {},
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=str(request.webhook_url),
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
            # Concurrent request with the same key won the race.
            await self.session.rollback()
            existing_payment = await self._get_by_idempotency_key(idempotency_key)
            if existing_payment is None:
                raise
            return self._replay(existing_payment, request_hash)

        await self.session.refresh(payment)
        return payment

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id)
        )
        return result.scalar_one_or_none()

    async def _get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _replay(payment: Payment, request_hash: str) -> Payment:
        # Rows created before request_hash existed have NULL and are not checked.
        if payment.request_hash is not None and payment.request_hash != request_hash:
            raise IdempotencyKeyConflict(
                "Idempotency-Key was already used with a different request body"
            )
        return payment
