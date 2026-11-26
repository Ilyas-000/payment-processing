import hashlib
import json
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field


class CurrencyEnum(str, Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreateRequest(BaseModel):
    # max_digits/decimal_places mirror the Numeric(12, 2) column, so an amount the
    # DB cannot store is a 422 instead of a 500.
    amount: Decimal = Field(..., gt=0, max_digits=12, decimal_places=2)
    currency: CurrencyEnum
    description: str | None = Field(None, max_length=1000)
    metadata: dict | None = None
    webhook_url: AnyHttpUrl

    def fingerprint(self) -> str:
        """Stable hash of the request, used to detect Idempotency-Key reuse."""
        normalized = {
            "amount": str(self.amount.quantize(Decimal("0.01"))),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata or {},
            "webhook_url": str(self.webhook_url),
        }
        raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode()).hexdigest()


class PaymentCreateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(serialization_alias="payment_id")
    status: str
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: str
    description: str | None
    metadata: dict | None = Field(None, validation_alias="metadata_")
    status: str
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    payment_id: uuid.UUID
    idempotency_key: str
