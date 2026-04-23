from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 50
    outbox_publish_max_retries: int = 5
    webhook_timeout_seconds: float = 10.0
    webhook_max_retries: int = 3


settings = Settings()
