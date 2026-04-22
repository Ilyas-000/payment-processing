# Payment Processing Service

Микросервис для асинхронной обработки платежей. Принимает запросы на оплату через REST API, обрабатывает их через платёжный шлюз и отправляет результат на webhook.

Основные возможности:
- Создание и отслеживание платежей
- Асинхронная обработка через RabbitMQ
- Гарантированная доставка событий (Outbox pattern)
- Идемпотентность через Idempotency-Key
- Dead Letter Queue для необработанных сообщений
- Retry с экспоненциальной задержкой

## Стек

Python 3.12, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker.

## Запуск

```bash
docker-compose up --build
```

Сервисы:
- API: http://localhost:8000
- RabbitMQ Management: http://localhost:15672 (guest/guest)
- PostgreSQL: localhost:5432

## API

Все запросы требуют заголовок `X-API-Key: secret-api-key`.

### Создание платежа

```bash
API_URL=http://localhost:8000
API_KEY=secret-api-key
IDEMPOTENCY_KEY=unique-key-123

CREATE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/payments" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d '{
    "amount": "150.00",
    "currency": "RUB",
    "description": "Оплата заказа #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/webhook"
  }')

echo "$CREATE_RESPONSE"
PAYMENT_ID=$(echo "$CREATE_RESPONSE" | jq -r '.payment_id')
```

Ответ (202 Accepted):
```json
{
  "payment_id": "<generated-payment-id>",
  "status": "pending",
  "created_at": "<created-at>"
}
```

### Получение платежа

```bash
curl -s "$API_URL/api/v1/payments/$PAYMENT_ID" \
  -H "X-API-Key: $API_KEY"
```

Ответ (200 OK):
```json
{
  "id": "<generated-payment-id>",
  "amount": "150.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "unique-key-123",
  "webhook_url": "https://example.com/webhook",
  "created_at": "<created-at>",
  "processed_at": "<processed-at>"
}
```

### Валюты

`RUB`, `USD`, `EUR`.

### Статусы

- `pending` — создан, ожидает обработки
- `succeeded` — успешно обработан
- `failed` — обработка завершилась ошибкой

## Webhook

После обработки платежа на указанный `webhook_url` отправляется POST-запрос:

```json
{
  "payment_id": "<generated-payment-id>",
  "status": "succeeded",
  "amount": "150.00",
  "currency": "RUB",
  "processed_at": "<processed-at>"
}
```

## Тесты

Тесты используют testcontainers (нужен запущенный Docker):

```bash
poetry install
poetry run pytest -v
```
