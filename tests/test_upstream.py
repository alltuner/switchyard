# ABOUTME: Tests for the upstream registry client.
# ABOUTME: Uses respx to mock httpx calls to the Docker registry API.
from __future__ import annotations

import json
from collections.abc import AsyncIterator

import respx
from httpx import Response

from switchyard.upstream import UpstreamClient

BASE = "https://central:5000"


@respx.mock
async def test_check_blob_exists() -> None:
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(200, headers={"Content-Length": "42"})
    )
    client = UpstreamClient(BASE)
    assert await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@respx.mock
async def test_check_blob_missing() -> None:
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(404)
    )
    client = UpstreamClient(BASE)
    assert not await client.check_blob("myapp", "sha256:abc123")
    await client.close()


@respx.mock
async def test_push_blob_skips_existing() -> None:
    route = respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(200, headers={"Content-Length": "42"})
    )
    client = UpstreamClient(BASE)
    await client.push_blob("myapp", "sha256:abc123", b"data")
    # Should only HEAD, no POST/PUT
    assert route.call_count == 1
    await client.close()


@respx.mock
async def test_push_blob_uploads() -> None:
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": f"{BASE}/v2/myapp/blobs/uploads/uuid-1"})
    )
    respx.put(f"{BASE}/v2/myapp/blobs/uploads/uuid-1").mock(
        return_value=Response(201)
    )

    client = UpstreamClient(BASE)
    await client.push_blob("myapp", "sha256:abc123", b"blob data")
    assert respx.calls.call_count == 3  # HEAD + POST + PUT
    await client.close()


@respx.mock
async def test_push_blob_preserves_location_query_params() -> None:
    """Registry Location may include a _state token; digest must be appended, not replace it."""
    location = f"{BASE}/v2/myapp/blobs/uploads/uuid-1?_state=signed-token"
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": location})
    )
    put_route = respx.put(url__regex=r".*/v2/myapp/blobs/uploads/uuid-1.*").mock(
        return_value=Response(201)
    )

    client = UpstreamClient(BASE)
    await client.push_blob("myapp", "sha256:abc123", b"blob data")

    put_url = str(put_route.calls[0].request.url)
    assert "_state=signed-token" in put_url, f"_state param lost: {put_url}"
    assert "digest=sha256" in put_url, f"digest param missing: {put_url}"
    await client.close()


@respx.mock
async def test_push_blob_streaming_uploads() -> None:
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": f"{BASE}/v2/myapp/blobs/uploads/uuid-1"})
    )
    put_route = respx.put(f"{BASE}/v2/myapp/blobs/uploads/uuid-1").mock(
        return_value=Response(201)
    )

    async def blob_stream() -> AsyncIterator[bytes]:
        yield b"blob data"

    client = UpstreamClient(BASE)
    await client.push_blob_streaming("myapp", "sha256:abc123", blob_stream())
    assert respx.calls.call_count == 3  # HEAD + POST + PUT
    assert put_route.calls[0].request.headers["content-type"] == "application/octet-stream"
    await client.close()


@respx.mock
async def test_push_blob_streaming_preserves_location_query_params() -> None:
    """Registry Location may include a _state token; digest must be appended, not replace it."""
    location = f"{BASE}/v2/myapp/blobs/uploads/uuid-1?_state=signed-token"
    respx.head(f"{BASE}/v2/myapp/blobs/sha256:abc123").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": location})
    )
    put_route = respx.put(url__regex=r".*/v2/myapp/blobs/uploads/uuid-1.*").mock(
        return_value=Response(201)
    )

    async def blob_stream() -> AsyncIterator[bytes]:
        yield b"blob data"

    client = UpstreamClient(BASE)
    await client.push_blob_streaming("myapp", "sha256:abc123", blob_stream())

    put_url = str(put_route.calls[0].request.url)
    assert "_state=signed-token" in put_url, f"_state param lost: {put_url}"
    assert "digest=sha256" in put_url, f"digest param missing: {put_url}"
    await client.close()


@respx.mock
async def test_push_manifest() -> None:
    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
    })
    route = respx.put(f"{BASE}/v2/myapp/manifests/latest").mock(
        return_value=Response(201)
    )
    client = UpstreamClient(BASE)
    await client.push_manifest(
        "myapp",
        "latest",
        manifest.encode(),
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    assert route.call_count == 1
    # Verify Content-Type was set correctly
    assert route.calls[0].request.headers["content-type"] == (
        "application/vnd.docker.distribution.manifest.v2+json"
    )
    await client.close()


@respx.mock
async def test_push_manifest_oci_index() -> None:
    """Verify OCI image index content type is passed through correctly."""
    ct = "application/vnd.oci.image.index.v1+json"
    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": ct,
        "manifests": [],
    })
    route = respx.put(f"{BASE}/v2/myapp/manifests/latest").mock(
        return_value=Response(201)
    )
    client = UpstreamClient(BASE)
    await client.push_manifest("myapp", "latest", manifest.encode(), ct)
    assert route.calls[0].request.headers["content-type"] == ct
    await client.close()


@respx.mock
async def test_pull_manifest() -> None:
    body = json.dumps({"schemaVersion": 2})
    respx.get(f"{BASE}/v2/myapp/manifests/latest").mock(
        return_value=Response(
            200,
            content=body.encode(),
            headers={
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
                "Docker-Content-Digest": "sha256:abc",
            },
        )
    )
    client = UpstreamClient(BASE)
    result = await client.pull_manifest("myapp", "latest")
    assert result is not None
    manifest_body, ct, digest = result
    assert manifest_body == body.encode()
    assert "manifest.v2" in ct
    assert digest == "sha256:abc"
    await client.close()


@respx.mock
async def test_pull_manifest_not_found() -> None:
    respx.get(f"{BASE}/v2/myapp/manifests/missing").mock(
        return_value=Response(404)
    )
    client = UpstreamClient(BASE)
    result = await client.pull_manifest("myapp", "missing")
    assert result is None
    await client.close()
