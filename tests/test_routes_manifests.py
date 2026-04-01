# ABOUTME: Tests for Docker Registry V2 manifest endpoints.
# ABOUTME: Covers push, pull, HEAD, and sync queue integration.
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from starlette.testclient import TestClient


MANIFEST_CT = "application/vnd.docker.distribution.manifest.v2+json"


def _sample_manifest() -> bytes:
    return json.dumps({"schemaVersion": 2, "mediaType": MANIFEST_CT}).encode()


def test_get_manifest_missing(registry: TestClient) -> None:
    resp = registry.get("/v2/myapp/manifests/latest")
    assert resp.status_code == 404


def test_head_manifest_missing(registry: TestClient) -> None:
    resp = registry.head("/v2/myapp/manifests/latest")
    assert resp.status_code == 404


def test_put_and_get_manifest(registry: TestClient) -> None:
    body = _sample_manifest()
    digest = f"sha256:{hashlib.sha256(body).hexdigest()}"

    resp = registry.put(
        "/v2/myapp/manifests/latest",
        content=body,
        headers={"Content-Type": MANIFEST_CT},
    )
    assert resp.status_code == 201
    assert resp.headers["Docker-Content-Digest"] == digest

    # GET by tag
    resp = registry.get("/v2/myapp/manifests/latest")
    assert resp.status_code == 200
    assert resp.content == body
    assert resp.headers["Content-Type"] == MANIFEST_CT
    assert resp.headers["Docker-Content-Digest"] == digest

    # GET by digest
    resp = registry.get(f"/v2/myapp/manifests/{digest}")
    assert resp.status_code == 200
    assert resp.content == body


def test_head_manifest(registry: TestClient) -> None:
    body = _sample_manifest()
    digest = f"sha256:{hashlib.sha256(body).hexdigest()}"

    registry.put(
        "/v2/myapp/manifests/v1",
        content=body,
        headers={"Content-Type": MANIFEST_CT},
    )

    resp = registry.head("/v2/myapp/manifests/v1")
    assert resp.status_code == 200
    assert resp.headers["Docker-Content-Digest"] == digest
    assert resp.headers["Content-Length"] == str(len(body))


def test_put_manifest_creates_sync_marker(
    registry: TestClient, storage_path: Path
) -> None:
    body = _sample_manifest()
    registry.put(
        "/v2/myapp/manifests/latest",
        content=body,
        headers={"Content-Type": MANIFEST_CT},
    )

    pending_dir = storage_path / "pending" / "myapp"
    assert pending_dir.exists()
    markers = list(pending_dir.glob("*.json"))
    assert len(markers) == 1


def test_manifest_with_nested_name(registry: TestClient) -> None:
    body = _sample_manifest()

    resp = registry.put(
        "/v2/library/nginx/manifests/1.27",
        content=body,
        headers={"Content-Type": MANIFEST_CT},
    )
    assert resp.status_code == 201

    resp = registry.get("/v2/library/nginx/manifests/1.27")
    assert resp.status_code == 200
    assert resp.content == body
