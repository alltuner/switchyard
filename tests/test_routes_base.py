# ABOUTME: Tests for the /v2/ version check endpoint.
# ABOUTME: Verifies the required Docker-Distribution-API-Version header.
from __future__ import annotations

from starlette.testclient import TestClient


def test_version_check(registry: TestClient) -> None:
    response = registry.get("/v2/")
    assert response.status_code == 200
    assert response.headers["Docker-Distribution-API-Version"] == "registry/2.0"
