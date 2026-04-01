# ABOUTME: Application settings loaded from environment variables.
# ABOUTME: Uses pydantic-settings with SWITCHYARD_ prefix for all config.
from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "SWITCHYARD_", "frozen": True}

    data_dir: str = "./data"
    upstream: str = ""
    sync_interval: int = 10
    manifest_ttl: int = 300
