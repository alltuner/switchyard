# ABOUTME: Application settings loaded from environment variables.
# ABOUTME: Provides a frozen Settings dataclass with sensible defaults.
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    data_dir: str = "./data"
    upstream: str = ""
    port: int = 5050
    sync_interval: int = 10
    manifest_ttl: int = 300

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            data_dir=os.environ.get("SWITCHYARD_DATA_DIR", cls.data_dir),
            upstream=os.environ.get("SWITCHYARD_UPSTREAM", cls.upstream),
            port=int(os.environ.get("SWITCHYARD_PORT", str(cls.port))),
            sync_interval=int(os.environ.get("SWITCHYARD_SYNC_INTERVAL", str(cls.sync_interval))),
            manifest_ttl=int(os.environ.get("SWITCHYARD_MANIFEST_TTL", str(cls.manifest_ttl))),
        )
