import logging

from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.consumer.broker import broker, dlx_exchange, dlq_queue
import app.consumer.handlers  # noqa: F401

logger = logging.getLogger(__name__)

app = FastStream(broker)


@broker.subscriber(dlq_queue, dlx_exchange, no_ack=True)
async def dlq_handler(body: dict, msg: RabbitMessage) -> None:
    logger.error("Message landed in DLQ: %s", body)
    await msg.ack()
