# ABOUTME: Tests for the upstream registry client.
# ABOUTME: Uses responses to mock HTTP calls made by the python-dxf library.
from __future__ import annotations

import json

import responses

from switchyard.upstream import UpstreamClient


@responses.activate
async def test_check_blob_exists() -> None:
    responses.head(
        "https://central:5000/v2/myapp/blobs/sha256:abc123",
        status=200,
        headers={"Content-Length": "42"},
    )
    client = UpstreamClient("https://central:5000")
    assert await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@responses.activate
async def test_check_blob_missing() -> None:
    responses.head(
        "https://central:5000/v2/myapp/blobs/sha256:abc123",
        status=404,
    )
    client = UpstreamClient("https://central:5000")
    assert not await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@responses.activate
async def test_push_blob_skips_existing() -> None:
    responses.head(
        "https://central:5000/v2/myapp/blobs/sha256:abc123",
        status=200,
        headers={"Content-Length": "42"},
    )
    client = UpstreamClient("https://central:5000")
    await client.push_blob("myapp", "sha256:abc123", b"data")
    # Should only HEAD, no POST/PUT
    assert len(responses.calls) == 1
    await client.close()


@responses.activate
async def test_push_blob_uploads() -> None:
    responses.head(
        "https://central:5000/v2/myapp/blobs/sha256:abc123",
        status=404,
    )
    responses.post(
        "https://central:5000/v2/myapp/blobs/uploads/",
        status=202,
        headers={"Location": "https://central:5000/v2/myapp/blobs/uploads/uuid-1"},
    )
    responses.put(
        url="https://central:5000/v2/myapp/blobs/uploads/uuid-1",
        status=201,
    )

    client = UpstreamClient("https://central:5000")
    await client.push_blob("myapp", "sha256:abc123", b"blob data")
    assert len(responses.calls) == 3  # HEAD + POST + PUT
    await client.close()


@responses.activate
async def test_push_manifest() -> None:
    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
    })
    responses.put(
        "https://central:5000/v2/myapp/manifests/latest",
        status=201,
    )
    client = UpstreamClient("https://central:5000")
    await client.push_manifest(
        "myapp",
        "latest",
        manifest.encode(),
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    assert len(responses.calls) == 1
    await client.close()


@responses.activate
async def test_pull_manifest() -> None:
    body = json.dumps({"schemaVersion": 2})
    responses.get(
        "https://central:5000/v2/myapp/manifests/latest",
        body=body,
        status=200,
        headers={
            "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            "Docker-Content-Digest": "sha256:abc",
        },
    )
    client = UpstreamClient("https://central:5000")
    result = await client.pull_manifest("myapp", "latest")
    assert result is not None
    manifest_body, ct, digest = result
    assert manifest_body == body.encode()
    assert "manifest.v2" in ct
    assert digest == "sha256:abc"
    await client.close()


@responses.activate
async def test_pull_manifest_not_found() -> None:
    responses.get(
        "https://central:5000/v2/myapp/manifests/missing",
        status=404,
    )
    client = UpstreamClient("https://central:5000")
    result = await client.pull_manifest("myapp", "missing")
    assert result is None
    await client.close()
