# ABOUTME: Tests for Settings dataclass and environment variable loading.
# ABOUTME: Verifies defaults and env var overrides.
import pytest

from switchyard.config import Settings


def test_defaults() -> None:
    settings = Settings()
    assert settings.data_dir == "./data"
    assert settings.upstream == ""
    assert settings.port == 5050
    assert settings.sync_interval == 10
    assert settings.manifest_ttl == 300


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWITCHYARD_DATA_DIR", "/tmp/registry")
    monkeypatch.setenv("SWITCHYARD_UPSTREAM", "https://central:5000")
    monkeypatch.setenv("SWITCHYARD_PORT", "9090")
    monkeypatch.setenv("SWITCHYARD_SYNC_INTERVAL", "30")
    monkeypatch.setenv("SWITCHYARD_MANIFEST_TTL", "600")

    settings = Settings.from_env()
    assert settings.data_dir == "/tmp/registry"
    assert settings.upstream == "https://central:5000"
    assert settings.port == 9090
    assert settings.sync_interval == 30
    assert settings.manifest_ttl == 600


def test_from_env_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWITCHYARD_UPSTREAM", "https://other:5000")

    settings = Settings.from_env()
    assert settings.upstream == "https://other:5000"
    assert settings.port == 5050  # default preserved


def test_frozen() -> None:
    settings = Settings()
    with pytest.raises(AttributeError):
        settings.port = 9999  # type: ignore[misc]
