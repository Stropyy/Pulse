from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PULSE_ENV: str = "development"

    # PostgreSQL
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "pulse_user"
    POSTGRES_PASSWORD: str = "pulse_password"
    POSTGRES_DB: str = "pulse"
    DATABASE_URL: Optional[str] = None
    SYNC_DATABASE_URL: Optional[str] = None

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: Optional[str] = None

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # Worker Pool
    WORKER_CONCURRENCY: int = 8

    # Retries and Backoff
    RETRY_BASE_DELAY: float = 1.0
    RETRY_MAX_ATTEMPTS: int = 3
    RETRY_JITTER_MAX: float = 0.5

    # Recovery and Heartbeat
    HEARTBEAT_INTERVAL_SECONDS: float = 3.0
    HEARTBEAT_TTL_SECONDS: float = 10.0
    REAPER_INTERVAL_SECONDS: float = 5.0
    STALE_JOB_TIMEOUT_SECONDS: float = 30.0

    @property
    def async_database_url(self) -> str:
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def sync_database_url(self) -> str:
        if self.SYNC_DATABASE_URL:
            return self.SYNC_DATABASE_URL
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def async_redis_url(self) -> str:
        if self.REDIS_URL:
            return self.REDIS_URL
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
