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
    # Only non-transient errors (e.g. unroutable message) count towards this limit.
    # Broker outages never burn retries: the batch is simply retried on the next tick.
    outbox_publish_max_retries: int = 5
    outbox_publish_timeout_seconds: float = 5.0

    webhook_timeout_seconds: float = 10.0

    # Total processing attempts per message (first try + retries) before it goes to DLQ.
    max_processing_attempts: int = 3
    # Delay before retry N is base * 2 ** (N - 1): 2s, 4s, ...
    retry_base_delay_seconds: float = 2.0


settings = Settings()
