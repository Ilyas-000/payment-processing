import pytest
from httpx import AsyncClient

API_KEY = "secret-api-key"
HEADERS = {"X-API-Key": API_KEY}


def payment_body(**overrides):
    data = {
        "amount": "100.50",
        "currency": "USD",
        "description": "Test payment",
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return data


@pytest.mark.asyncio
async def test_create_payment_returns_202(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers={**HEADERS, "Idempotency-Key": "key-create-202"},
    )
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert "payment_id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_idempotency_returns_same_payment(client: AsyncClient):
    headers = {**HEADERS, "Idempotency-Key": "key-idempotency"}
    body = payment_body()

    r1 = await client.post("/api/v1/payments", json=body, headers=headers)
    r2 = await client.post("/api/v1/payments", json=body, headers=headers)

    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]


@pytest.mark.asyncio
async def test_missing_api_key_returns_401(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers={"Idempotency-Key": "key-no-auth"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_invalid_api_key_returns_401(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": "key-bad-auth"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_payment(client: AsyncClient):
    create_resp = await client.post(
        "/api/v1/payments",
        json=payment_body(metadata={"order_id": 42}),
        headers={**HEADERS, "Idempotency-Key": "key-get-detail"},
    )
    payment_id = create_resp.json()["payment_id"]

    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers=HEADERS,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == payment_id
    assert data["amount"] == "100.50"
    assert data["currency"] == "USD"
    assert data["description"] == "Test payment"
    assert data["metadata"] == {"order_id": 42}
    assert data["status"] == "pending"
    assert data["idempotency_key"] == "key-get-detail"
    assert data["webhook_url"] == "https://example.com/hook"
    assert data["created_at"] is not None
    assert data["processed_at"] is None


@pytest.mark.asyncio
async def test_get_nonexistent_payment_returns_404(client: AsyncClient):
    response = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers=HEADERS,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_invalid_currency_returns_422(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(currency="GBP"),
        headers={**HEADERS, "Idempotency-Key": "key-bad-currency"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_negative_amount_returns_422(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(amount="-10.00"),
        headers={**HEADERS, "Idempotency-Key": "key-neg-amount"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_missing_idempotency_key_returns_422(client: AsyncClient):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers=HEADERS,
    )
    assert response.status_code == 422
