# ABOUTME: Tests for Docker Registry V2 catalog and tag listing endpoints.
# ABOUTME: Verifies repository listing and tag enumeration.
from __future__ import annotations

import json

from starlette.testclient import TestClient


MANIFEST_CT = "application/vnd.docker.distribution.manifest.v2+json"


def _push_manifest(client: TestClient, name: str, tag: str) -> None:
    body = json.dumps({"schemaVersion": 2, "tag": tag}).encode()
    resp = client.put(
        f"/v2/{name}/manifests/{tag}",
        content=body,
        headers={"Content-Type": MANIFEST_CT},
    )
    assert resp.status_code == 201


def test_catalog_empty(registry: TestClient) -> None:
    resp = registry.get("/v2/_catalog")
    assert resp.status_code == 200
    assert resp.json() == {"repositories": []}


def test_catalog_with_repos(registry: TestClient) -> None:
    _push_manifest(registry, "app-a", "latest")
    _push_manifest(registry, "app-b", "v1")

    resp = registry.get("/v2/_catalog")
    assert resp.status_code == 200
    repos = resp.json()["repositories"]
    assert "app-a" in repos
    assert "app-b" in repos


def test_tags_list(registry: TestClient) -> None:
    _push_manifest(registry, "myapp", "latest")
    _push_manifest(registry, "myapp", "v1")

    resp = registry.get("/v2/myapp/tags/list")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "myapp"
    assert sorted(data["tags"]) == ["latest", "v1"]


def test_tags_list_empty(registry: TestClient) -> None:
    resp = registry.get("/v2/nonexistent/tags/list")
    assert resp.status_code == 404
