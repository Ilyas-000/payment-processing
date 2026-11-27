import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.dependencies import get_payment_service, verify_api_key
from app.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import IdempotencyKeyConflict, PaymentService

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
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1, max_length=255
    ),
    service: PaymentService = Depends(get_payment_service),
):
    try:
        return await service.create_payment(request, idempotency_key)
    except IdempotencyKeyConflict as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_payment(
    payment_id: uuid.UUID,
    service: PaymentService = Depends(get_payment_service),
):
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return payment
