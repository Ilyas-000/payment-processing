# Payment Processing Service

Микросервис для асинхронной обработки платежей. Принимает запросы на оплату через REST API, обрабатывает их через платёжный шлюз и отправляет результат на webhook.

Основные возможности:
- Создание и отслеживание платежей
- Асинхронная обработка через RabbitMQ
- Гарантированная доставка событий (Outbox pattern)
- Идемпотентность через Idempotency-Key
- Dead Letter Queue для необработанных сообщений
- Retry с экспоненциальной задержкой через retry-очереди RabbitMQ

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

### Ошибки

| Код | Когда |
|-----|-------|
| 401 | нет или неверный `X-API-Key` |
| 404 | платёж не найден |
| 409 | `Idempotency-Key` уже использован с другим телом запроса |
| 422 | невалидное тело или `Idempotency-Key` (пустой / длиннее 255 символов) |

Повторный запрос с тем же ключом и тем же телом возвращает уже созданный платёж (тот же `payment_id`). Тело сравнивается по SHA-256 нормализованного запроса, поэтому `"100.5"` и `"100.50"` считаются одинаковыми.

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

## Как это работает

```
POST /payments ──► payments + outbox_messages   (одна транзакция)
                          │
                    outbox-worker  (poll, FOR UPDATE SKIP LOCKED, publisher confirms)
                          │
                          ▼
payments_exchange ──► payments.new ──► consumer ──► шлюз (2–5 с, 90/10) ──► webhook
        ▲                                 │ ошибка, попытка N < 3
        │                                 ▼
        └──── TTL ──── payments.retry.<N>ms          (2 с, затем 4 с)
                                          │ ошибка на 3-й попытке / 4xx / битое сообщение
                                          ▼
                    payments_dlx ──► payments.dead   (без consumer — сообщения лежат для разбора)
```

**Outbox.** Платёж и событие пишутся в одной транзакции, публикацией занимается отдельный `outbox-worker`. Публикация считается успешной только после publisher confirm, а неразроутенное сообщение (`NO_ROUTE`) возвращается ошибкой, а не теряется молча. Недоступность брокера не расходует лимит попыток (`OUTBOX_PUBLISH_MAX_RETRIES`): пачка просто откладывается до следующего тика.

**Топология** объявляется и worker'ом, и consumer'ом при старте (`app/messaging/topology.py`), поэтому порядок запуска сервисов не важен.

**Retry.** Каждая доставка сообщения — одна попытка обработки. При ошибке consumer публикует копию в retry-очередь с фиксированным TTL и заголовком `x-attempt`; по истечении TTL RabbitMQ возвращает её в `payments.new`. На каждый шаг своя очередь, чтобы не было head-of-line blocking, как при per-message TTL. После `MAX_PROCESSING_ATTEMPTS` (по умолчанию 3) попыток сообщение уходит в `payments.dead`. Ответы webhook 4xx (кроме 408/429) и битые сообщения отправляются в DLQ сразу.

**Идемпотентность обработки.** Результат шлюза и доставка webhook отслеживаются раздельно (`status`/`processed_at` и `webhook_delivered_at`). Повторная попытка или сообщение, возвращённое из DLQ, продолжает с того шага, где упала предыдущая: шлюз не вызывается второй раз, а недоставленный webhook отправляется. Переход из `pending` делается условным `UPDATE ... WHERE status = 'pending'`, поэтому при конкурентной обработке побеждает один результат.

**Гарантии webhook:** at-least-once. Получатель должен дедуплицировать уведомления по `payment_id`.

Вероятность `90% / 10%` — финальный бизнес-результат эмуляции шлюза: retry применяются к сбоям обработки и доставки, а не к отказу шлюза.

### Повторная обработка из DLQ

Через RabbitMQ Management: очередь `payments.dead` → *Move messages* → в `payments.new`.

## Настройки

| Переменная | По умолчанию | |
|-----------|--------------|---|
| `API_KEY` | `secret-api-key` | ключ для `X-API-Key` |
| `MAX_PROCESSING_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `RETRY_BASE_DELAY_SECONDS` | `2.0` | задержка перед N-м повтором: `base * 2^(N-1)` |
| `WEBHOOK_TIMEOUT_SECONDS` | `10.0` | таймаут одного запроса webhook |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` | период опроса outbox |
| `OUTBOX_BATCH_SIZE` | `50` | сообщений за один тик |
| `OUTBOX_PUBLISH_MAX_RETRIES` | `5` | лимит для неустранимых ошибок публикации |

## Тесты

По умолчанию тесты поднимают Postgres и RabbitMQ через testcontainers (нужен запущенный Docker). Схема создаётся настоящими миграциями Alembic.

```bash
poetry install
poetry run pytest -v
```

Без Docker — на уже запущенных сервисах:

```bash
TEST_DATABASE_URL=postgresql+asyncpg://test:test@localhost:5432/test \
TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
poetry run pytest -v
```
