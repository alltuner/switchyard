# ABOUTME: Tests for the upstream registry HTTP client.
# ABOUTME: Uses respx to mock upstream registry responses.
from __future__ import annotations

import httpx
import respx

from switchyard.upstream import UpstreamClient


@respx.mock
async def test_check_blob_exists() -> None:
    respx.head("https://central:5000/v2/myapp/blobs/sha256:abc123").mock(
        return_value=httpx.Response(200)
    )
    client = UpstreamClient("https://central:5000")
    assert await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@respx.mock
async def test_check_blob_missing() -> None:
    respx.head("https://central:5000/v2/myapp/blobs/sha256:abc123").mock(
        return_value=httpx.Response(404)
    )
    client = UpstreamClient("https://central:5000")
    assert not await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@respx.mock
async def test_push_blob_skips_existing() -> None:
    respx.head("https://central:5000/v2/myapp/blobs/sha256:abc123").mock(
        return_value=httpx.Response(200)
    )
    client = UpstreamClient("https://central:5000")
    await client.push_blob("myapp", "sha256:abc123", b"data")
    # Should only HEAD, no POST/PUT
    assert len(respx.calls) == 1
    await client.close()


@respx.mock
async def test_push_blob_uploads() -> None:
    respx.head("https://central:5000/v2/myapp/blobs/sha256:abc123").mock(
        return_value=httpx.Response(404)
    )
    respx.post("https://central:5000/v2/myapp/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": "/v2/myapp/blobs/uploads/uuid-1"})
    )
    respx.put(url__regex=r".*/blobs/uploads/uuid-1.*").mock(return_value=httpx.Response(201))

    client = UpstreamClient("https://central:5000")
    await client.push_blob("myapp", "sha256:abc123", b"blob data")
    assert len(respx.calls) == 3  # HEAD + POST + PUT
    await client.close()


@respx.mock
async def test_push_manifest() -> None:
    respx.put("https://central:5000/v2/myapp/manifests/latest").mock(
        return_value=httpx.Response(201)
    )
    client = UpstreamClient("https://central:5000")
    await client.push_manifest(
        "myapp",
        "latest",
        b'{"schemaVersion": 2}',
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    assert len(respx.calls) == 1
    await client.close()


@respx.mock
async def test_pull_manifest() -> None:
    body = b'{"schemaVersion": 2}'
    respx.get("https://central:5000/v2/myapp/manifests/latest").mock(
        return_value=httpx.Response(
            200,
            content=body,
            headers={
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
                "Docker-Content-Digest": "sha256:abc",
            },
        )
    )
    client = UpstreamClient("https://central:5000")
    result = await client.pull_manifest("myapp", "latest")
    assert result is not None
    manifest_body, ct, digest = result
    assert manifest_body == body
    assert "manifest.v2" in ct
    assert digest == "sha256:abc"
    await client.close()


@respx.mock
async def test_pull_manifest_not_found() -> None:
    respx.get("https://central:5000/v2/myapp/manifests/missing").mock(
        return_value=httpx.Response(404)
    )
    client = UpstreamClient("https://central:5000")
    result = await client.pull_manifest("myapp", "missing")
    assert result is None
    await client.close()
