from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType

from app.config import settings

broker = RabbitBroker(settings.rabbitmq_url)

dlx_exchange = RabbitExchange(
    name="payments_dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

dlq_queue = RabbitQueue(
    name="payments.dead",
    durable=True,
    routing_key="payments.dead",
)

payments_exchange = RabbitExchange(
    name="payments_exchange",
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_queue = RabbitQueue(
    name="payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments_dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
