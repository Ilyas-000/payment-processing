import secrets
from typing import AsyncGenerator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_factory
from app.services.payment_service import PaymentService


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


def get_payment_service(
    session: AsyncSession = Depends(get_db_session),
) -> PaymentService:
    return PaymentService(session)


async def verify_api_key(x_api_key: str | None = Header(None)) -> str:
    # Constant-time comparison to avoid leaking the key through response timing.
    if not x_api_key or not secrets.compare_digest(
        x_api_key.encode(), settings.api_key.encode()
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return x_api_key
