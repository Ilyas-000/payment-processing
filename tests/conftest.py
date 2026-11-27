import asyncio
import os
from pathlib import Path

import pytest
import pytest_asyncio
from aio_pika import connect_robust
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.messaging.topology import (
    DEAD_QUEUE,
    PAYMENTS_QUEUE,
    declare_topology,
    retry_queues,
)

ROOT = Path(__file__).resolve().parent.parent

# Small delays so retry tests finish fast. Queue names include the delay, so these
# never clash with queues declared by a locally running service.
TEST_MAX_ATTEMPTS = 3
TEST_RETRY_BASE_DELAY = 0.2


# By default infrastructure is started with testcontainers (requires Docker).
# Set TEST_DATABASE_URL / TEST_RABBITMQ_URL to run against existing services.

@pytest.fixture(scope="session")
def postgres_url():
    if url := os.getenv("TEST_DATABASE_URL"):
        yield url
        return
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        yield f"postgresql+asyncpg://test:test@{host}:{port}/test"


@pytest.fixture(scope="session")
def rabbitmq_url():
    if url := os.getenv("TEST_RABBITMQ_URL"):
        yield url
        return
    from testcontainers.rabbitmq import RabbitMqContainer

    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as rmq:
        host = rmq.get_container_host_ip()
        port = rmq.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


def _alembic_config(database_url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    cfg.attributes["database_url"] = database_url
    return cfg


@pytest_asyncio.fixture
async def db_engine(postgres_url):
    # Schema comes from the real migrations, not Base.metadata.create_all(),
    # so tests also catch drift between models and migrations.
    cfg = _alembic_config(postgres_url)
    # env.py calls asyncio.run(), so it has to run outside this event loop.
    await asyncio.to_thread(command.upgrade, cfg, "head")
    engine = create_async_engine(postgres_url)
    yield engine
    await engine.dispose()
    await asyncio.to_thread(command.downgrade, cfg, "base")


@pytest.fixture
def session_factory(db_engine):
    return async_sessionmaker(
        db_engine, class_=AsyncSession, expire_on_commit=False
    )


@pytest_asyncio.fixture
async def db_session(session_factory):
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(session_factory):
    from app.dependencies import get_db_session
    from app.main import app

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def retry_settings(monkeypatch):
    monkeypatch.setattr(settings, "max_processing_attempts", TEST_MAX_ATTEMPTS)
    monkeypatch.setattr(settings, "retry_base_delay_seconds", TEST_RETRY_BASE_DELAY)
    return settings


@pytest_asyncio.fixture
async def rabbit_channel(rabbitmq_url, retry_settings):
    """Channel with the full topology declared and all payment queues empty."""
    connection = await connect_robust(rabbitmq_url)
    channel = await connection.channel()
    await declare_topology(channel, TEST_MAX_ATTEMPTS, TEST_RETRY_BASE_DELAY)
    names = [PAYMENTS_QUEUE, DEAD_QUEUE] + [
        q.name for q in retry_queues(TEST_MAX_ATTEMPTS, TEST_RETRY_BASE_DELAY)
    ]
    for name in names:
        await (await channel.get_queue(name)).purge()
    yield channel
    await connection.close()
