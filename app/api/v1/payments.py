import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, verify_api_key
from app.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreateResponse,
    response_model_by_alias=True,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_api_key)],
)
async def create_payment(
    request: PaymentCreateRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_db_session),
):
    service = PaymentService(session)
    payment = await service.create_payment(request, idempotency_key)
    return payment


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
):
    service = PaymentService(session)
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return payment
