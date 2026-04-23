import logging

import httpx
from faststream import FastStream

from app.consumer import runtime
from app.consumer.broker import broker, dlq_queue, dlx_exchange
from app.database import async_session_factory
from app.logging_config import setup_logging

import app.consumer.handlers  # noqa: F401

setup_logging()

logger = logging.getLogger(__name__)

app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    runtime.session_factory = async_session_factory
    runtime.http_client = httpx.AsyncClient()


@app.on_shutdown
async def on_shutdown() -> None:
    if runtime.http_client is not None:
        await runtime.http_client.aclose()


@broker.subscriber(dlq_queue, dlx_exchange)
async def dlq_handler(body: dict) -> None:
    logger.error("Message landed in DLQ: %s", body)
