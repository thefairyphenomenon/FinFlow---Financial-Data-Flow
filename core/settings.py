"""
core/settings.py
----------------
Central settings management using pydantic-settings.
This pattern is used in production FastAPI applications.

WHY THIS PATTERN:
- Type-safe configuration (no "string when you expected int" bugs)
- Automatically reads from environment variables
- Single source of truth for all config
"""

import yaml
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings


# ── Load YAML config ──────────────────────────────────────────────────────────
def load_yaml_config() -> dict:
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


CONFIG = load_yaml_config()


# ── Pydantic Settings Class ───────────────────────────────────────────────────
# Pydantic automatically reads matching UPPERCASE env vars.
# e.g. POSTGRES_PASSWORD env var → postgres_password field

class Settings(BaseSettings):
    # App
    app_env: str = Field(default="development", env="APP_ENV")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    secret_key: str = Field(default="dev-secret", env="SECRET_KEY")

    # Database
    postgres_user: str = Field(default="dataflow_user", env="POSTGRES_USER")
    postgres_password: str = Field(default="password", env="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="dataflow_db", env="POSTGRES_DB")
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")

    # Redis
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")

    # API Keys (free services)
    fred_api_key: str = Field(default="", env="FRED_API_KEY")
    alpha_vantage_key: str = Field(default="", env="ALPHA_VANTAGE_KEY")

    @property
    def database_url(self) -> str:
        """Constructs the PostgreSQL connection URL."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Singleton — import this everywhere
settings = Settings()
