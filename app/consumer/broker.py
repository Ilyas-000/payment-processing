from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings
from app.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    PAYMENTS_QUEUE_ARGUMENTS,
)

broker = RabbitBroker(settings.rabbitmq_url)

# Must match app.messaging.topology exactly, otherwise RabbitMQ rejects the
# redeclaration with PRECONDITION_FAILED.
payments_exchange = RabbitExchange(
    name=PAYMENTS_EXCHANGE,
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_queue = RabbitQueue(
    name=PAYMENTS_QUEUE,
    durable=True,
    routing_key=PAYMENTS_QUEUE,
    arguments=PAYMENTS_QUEUE_ARGUMENTS,
)
