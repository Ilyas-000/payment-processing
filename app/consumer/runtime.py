import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker

session_factory: async_sessionmaker | None = None
http_client: httpx.AsyncClient | None = None
