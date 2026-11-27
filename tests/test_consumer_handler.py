from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

import app.consumer.handlers as handlers_module
from app.consumer.handlers import get_attempt, handle_payment_event
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentEvent
from app.services.payment_service import PaymentService
from app.services.webhook_service import PermanentWebhookError, RetryableWebhookError


def _http_client(*responses) -> AsyncMock:
    """Client whose consecutive POSTs return the given status codes / raise errors."""
    side_effect = []
    for item in responses:
        if isinstance(item, Exception):
            side_effect.append(item)
        else:
            response = MagicMock(spec=httpx.Response)
            response.status_code = item
            response.is_success = 200 <= item < 300
            side_effect.append(response)
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(side_effect=side_effect)
    return client


async def _create_payment(db_session: AsyncSession, key: str) -> Payment:
    return await PaymentService(db_session).create_payment(
        PaymentCreateRequest(
            amount="200.00",
            currency="RUB",
            description="Consumer test",
            webhook_url="https://example.com/hook",
        ),
        key,
    )


async def _reload(session_factory, payment_id) -> Payment:
    async with session_factory() as session:
        return await session.get(Payment, payment_id)


@pytest.fixture
def gateway(monkeypatch):
    mock = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr(handlers_module, "_emulate_gateway", mock)
    return mock


@pytest.mark.asyncio
async def test_success_sets_status_and_marks_webhook_delivered(
    db_session, session_factory, gateway
):
    payment = await _create_payment(db_session, "consumer-success")
    client = _http_client(200)

    await handle_payment_event(
        PaymentEvent(payment_id=payment.id, idempotency_key="consumer-success"),
        session_factory,
        client,
    )

    updated = await _reload(session_factory, payment.id)
    assert updated.status == PaymentStatus.SUCCEEDED.value
    assert updated.processed_at is not None
    assert updated.webhook_delivered_at is not None
    client.post.assert_called_once()


@pytest.mark.asyncio
async def test_gateway_failure_is_final_status_and_still_notifies(
    db_session, session_factory, gateway
):
    gateway.return_value = PaymentStatus.FAILED
    payment = await _create_payment(db_session, "consumer-failed")
    client = _http_client(200)

    await handle_payment_event(
        PaymentEvent(payment_id=payment.id, idempotency_key="consumer-failed"),
        session_factory,
        client,
    )

    updated = await _reload(session_factory, payment.id)
    assert updated.status == PaymentStatus.FAILED.value
    assert client.post.call_args.kwargs["json"]["status"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [503, 429, 408, httpx.ConnectError("refused")])
async def test_retryable_webhook_error_keeps_status_but_not_delivery(
    db_session, session_factory, gateway, outcome
):
    payment = await _create_payment(db_session, "consumer-retryable")

    with pytest.raises(RetryableWebhookError):
        await handle_payment_event(
            PaymentEvent(payment_id=payment.id, idempotency_key="consumer-retryable"),
            session_factory,
            _http_client(outcome),
        )

    updated = await _reload(session_factory, payment.id)
    assert updated.status == PaymentStatus.SUCCEEDED.value
    assert updated.webhook_delivered_at is None


@pytest.mark.asyncio
async def test_client_error_is_permanent(db_session, session_factory, gateway):
    payment = await _create_payment(db_session, "consumer-permanent")

    with pytest.raises(PermanentWebhookError):
        await handle_payment_event(
            PaymentEvent(payment_id=payment.id, idempotency_key="consumer-permanent"),
            session_factory,
            _http_client(404),
        )


@pytest.mark.asyncio
async def test_retry_after_webhook_failure_resends_without_rerunning_gateway(
    db_session, session_factory, gateway
):
    """Regression: a retried or DLQ-replayed message used to be skipped as
    'already processed', so the webhook was never delivered."""
    payment = await _create_payment(db_session, "consumer-resume")
    event = PaymentEvent(payment_id=payment.id, idempotency_key="consumer-resume")
    client = _http_client(httpx.ConnectError("refused"), 200)

    with pytest.raises(RetryableWebhookError):
        await handle_payment_event(event, session_factory, client)
    await handle_payment_event(event, session_factory, client)

    gateway.assert_awaited_once()
    assert client.post.call_count == 2
    updated = await _reload(session_factory, payment.id)
    assert updated.webhook_delivered_at is not None


@pytest.mark.asyncio
async def test_already_delivered_is_a_no_op(db_session, session_factory, gateway):
    payment = await _create_payment(db_session, "consumer-done")
    event = PaymentEvent(payment_id=payment.id, idempotency_key="consumer-done")
    await handle_payment_event(event, session_factory, _http_client(200))

    client = _http_client()
    await handle_payment_event(event, session_factory, client)

    client.post.assert_not_called()
    assert gateway.await_count == 1


@pytest.mark.asyncio
async def test_concurrent_finalization_keeps_first_result(
    db_session, session_factory, gateway
):
    """If another worker finalizes the payment while we wait for the gateway,
    our result must not overwrite theirs."""
    payment = await _create_payment(db_session, "consumer-race")

    async def other_worker_wins(_payment_id):
        async with session_factory() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment.id)
                .values(status=PaymentStatus.FAILED.value)
            )
            await session.commit()
        return PaymentStatus.SUCCEEDED

    gateway.side_effect = other_worker_wins
    client = _http_client(200)

    await handle_payment_event(
        PaymentEvent(payment_id=payment.id, idempotency_key="consumer-race"),
        session_factory,
        client,
    )

    updated = await _reload(session_factory, payment.id)
    assert updated.status == PaymentStatus.FAILED.value
    assert client.post.call_args.kwargs["json"]["status"] == "failed"


@pytest.mark.parametrize(
    "headers, expected",
    [(None, 1), ({}, 1), ({"x-attempt": 2}, 2), ({"x-attempt": "3"}, 3), ({"x-attempt": "x"}, 1)],
)
def test_get_attempt(headers, expected):
    assert get_attempt(headers) == expected
