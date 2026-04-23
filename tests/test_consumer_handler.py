from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.consumer.handlers import handle_payment_event
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentEvent
from app.services.payment_service import PaymentService
from app.services.webhook_service import WebhookDeliveryError


def _make_http_client_ok() -> AsyncMock:
    client = AsyncMock(spec=httpx.AsyncClient)
    response = MagicMock()
    response.raise_for_status = MagicMock()
    client.post = AsyncMock(return_value=response)
    return client


def _make_http_client_failing() -> AsyncMock:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(
        side_effect=httpx.ConnectError("refused"),
    )
    return client


@pytest.mark.asyncio
async def test_handle_payment_event_success(
    db_session: AsyncSession,
    session_factory,
    monkeypatch,
):
    service = PaymentService(db_session)
    payment = await service.create_payment(
        PaymentCreateRequest(
            amount="200.00",
            currency="RUB",
            description="Consumer test",
            webhook_url="https://example.com/hook",
        ),
        "consumer-success-key",
    )

    import app.consumer.handlers as handlers_module
    monkeypatch.setattr(
        handlers_module,
        "_emulate_gateway",
        AsyncMock(return_value=PaymentStatus.SUCCEEDED),
    )

    http_client = _make_http_client_ok()
    event = PaymentEvent(payment_id=payment.id, idempotency_key="consumer-success-key")

    await handle_payment_event(event, session_factory, http_client)

    async with session_factory() as session:
        updated = await session.get(Payment, payment.id)

    assert updated.status == PaymentStatus.SUCCEEDED.value
    assert updated.processed_at is not None
    http_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_handle_payment_event_gateway_failure_sets_final_failed(
    db_session: AsyncSession,
    session_factory,
    monkeypatch,
):
    service = PaymentService(db_session)
    payment = await service.create_payment(
        PaymentCreateRequest(
            amount="10.00",
            currency="USD",
            webhook_url="https://example.com/hook",
        ),
        "consumer-failed-key",
    )

    import app.consumer.handlers as handlers_module
    monkeypatch.setattr(
        handlers_module,
        "_emulate_gateway",
        AsyncMock(return_value=PaymentStatus.FAILED),
    )

    http_client = _make_http_client_ok()
    event = PaymentEvent(payment_id=payment.id, idempotency_key="consumer-failed-key")

    await handle_payment_event(event, session_factory, http_client)

    async with session_factory() as session:
        updated = await session.get(Payment, payment.id)

    assert updated.status == PaymentStatus.FAILED.value
    assert updated.processed_at is not None
    http_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_handle_payment_event_raises_when_webhook_fails(
    db_session: AsyncSession,
    session_factory,
    monkeypatch,
):
    service = PaymentService(db_session)
    payment = await service.create_payment(
        PaymentCreateRequest(
            amount="10.00",
            currency="USD",
            webhook_url="https://example.com/hook",
        ),
        "consumer-webhook-fail-key",
    )

    import app.consumer.handlers as handlers_module
    monkeypatch.setattr(
        handlers_module,
        "_emulate_gateway",
        AsyncMock(return_value=PaymentStatus.SUCCEEDED),
    )
    monkeypatch.setattr("app.services.webhook_service.asyncio.sleep", AsyncMock())

    http_client = _make_http_client_failing()
    event = PaymentEvent(
        payment_id=payment.id, idempotency_key="consumer-webhook-fail-key"
    )

    with pytest.raises(WebhookDeliveryError):
        await handle_payment_event(event, session_factory, http_client)

    async with session_factory() as session:
        updated = await session.get(Payment, payment.id)

    assert updated.status == PaymentStatus.SUCCEEDED.value
    assert http_client.post.call_count == 3


@pytest.mark.asyncio
async def test_handle_payment_event_idempotent_on_already_processed(
    db_session: AsyncSession,
    session_factory,
):
    service = PaymentService(db_session)
    payment = await service.create_payment(
        PaymentCreateRequest(
            amount="10.00",
            currency="USD",
            webhook_url="https://example.com/hook",
        ),
        "consumer-idempotent-key",
    )

    async with session_factory() as session:
        p = await session.get(Payment, payment.id)
        p.status = PaymentStatus.SUCCEEDED.value
        await session.commit()

    http_client = _make_http_client_ok()
    event = PaymentEvent(
        payment_id=payment.id, idempotency_key="consumer-idempotent-key"
    )

    await handle_payment_event(event, session_factory, http_client)

    http_client.post.assert_not_called()
