import logging

import httpx
from aio_pika import connect_robust
from faststream import FastStream

from app.config import settings
from app.consumer import runtime
from app.consumer.broker import broker
from app.database import async_session_factory
from app.logging_config import setup_logging
from app.messaging.topology import declare_topology

import app.consumer.handlers  # noqa: F401

setup_logging()

logger = logging.getLogger(__name__)

app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    # Declare the full topology (incl. retry queues and DLQ) before subscribing.
    # The DLQ intentionally has no consumer: dead messages stay there to be
    # inspected and replayed.
    connection = await connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await declare_topology(
            channel,
            settings.max_processing_attempts,
            settings.retry_base_delay_seconds,
        )

    runtime.session_factory = async_session_factory
    runtime.http_client = httpx.AsyncClient()


@app.on_shutdown
async def on_shutdown() -> None:
    if runtime.http_client is not None:
        await runtime.http_client.aclose()
