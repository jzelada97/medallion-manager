"""12-factor configuration loaded from environment variables.

All credentials and connection details come from the environment. Never hardcode
secrets here (see .kiro/steering/00-critical-rules.md, Rule #2).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings, populated from env vars or a .env file."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "probando"
    kafka_group_id: str = "hr-etl-consumer"
    kafka_auto_offset_reset: str = "earliest"

    # MongoDB (Data Lake)
    mongo_uri: str = "mongodb://localhost:27017/"
    mongo_db: str = "hr_lake"
    mongo_raw_collection: str = "raw_messages"

    # PostgreSQL (Data Warehouse)
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "hr_warehouse"
    postgres_user: str = "hr_user"
    postgres_password: str = "changeme"

    # Redis (cache / matching buffer)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    redis_buffer_ttl: int = 300

    # App
    log_level: str = "INFO"
    consolidation_min_fragments: int = 2
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def postgres_dsn(self) -> str:
        """SQLAlchemy connection string for PostgreSQL."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
