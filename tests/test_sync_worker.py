# ABOUTME: Tests for the background sync worker.
# ABOUTME: Verifies that pending markers are processed and blobs/manifests pushed upstream.
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest
import respx
from httpx import Response

from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.sync_worker import (
    SyncMissingBlobsError,
    _extract_blob_digests,
    _extract_child_manifests,
    run_sync_loop,
    sync_one,
)
from switchyard.upstream import UpstreamClient

BASE = "https://central:5000"


def _make_index(child_digests: list[str]) -> bytes:
    """Build a minimal OCI image index referencing child manifests."""
    return json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": d,
                "size": 100,
                "platform": {"architecture": "amd64", "os": "linux"},
            }
            for d in child_digests
        ],
    }).encode()


def _make_manifest(layer_digests: list[str], config_digest: str = "") -> bytes:
    manifest: dict[str, object] = {"schemaVersion": 2}
    if config_digest:
        manifest["config"] = {"digest": config_digest, "mediaType": "application/json"}
    manifest["layers"] = [
        {"digest": d, "mediaType": "application/octet-stream"} for d in layer_digests
    ]
    return json.dumps(manifest).encode()


def test_extract_blob_digests() -> None:
    layer_digest = "sha256:layer1"
    config_digest = "sha256:config1"
    body = _make_manifest([layer_digest], config_digest)
    digests = _extract_blob_digests(body)
    assert config_digest in digests
    assert layer_digest in digests


def test_extract_blob_digests_no_config() -> None:
    body = _make_manifest(["sha256:layer1"])
    digests = _extract_blob_digests(body)
    assert digests == ["sha256:layer1"]


def test_extract_blob_digests_invalid_json() -> None:
    assert _extract_blob_digests(b"not json") == []


@respx.mock
async def test_sync_one_pushes_blobs_and_manifest(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    # Store a blob
    blob_data = b"layer content"
    blob_digest = f"sha256:{hashlib.sha256(blob_data).hexdigest()}"
    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, blob_data)
    await storage.store_blob_from_upload(upload_id, blob_digest)

    # Store a manifest referencing that blob
    manifest_body = _make_manifest([blob_digest])
    ct = "application/vnd.docker.distribution.manifest.v2+json"
    await storage.store_manifest("myapp", "latest", manifest_body, ct)

    # Enqueue
    await queue.enqueue("myapp", "latest")
    pending = await queue.list_pending()
    assert len(pending) == 1

    # Mock upstream: HEAD returns 404 (blob not there), POST+PUT for upload, PUT for manifest
    respx.head(f"{BASE}/v2/myapp/blobs/{blob_digest}").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": f"{BASE}/v2/myapp/blobs/uploads/u1"})
    )
    respx.put(f"{BASE}/v2/myapp/blobs/uploads/u1").mock(
        return_value=Response(201)
    )
    respx.put(f"{BASE}/v2/myapp/manifests/latest").mock(
        return_value=Response(201)
    )

    upstream = UpstreamClient(BASE)
    await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    # Marker should be cleared
    remaining = await queue.list_pending()
    assert len(remaining) == 0


@respx.mock
async def test_sync_one_skips_existing_blobs(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    blob_data = b"existing layer"
    blob_digest = f"sha256:{hashlib.sha256(blob_data).hexdigest()}"
    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, blob_data)
    await storage.store_blob_from_upload(upload_id, blob_digest)

    manifest_body = _make_manifest([blob_digest])
    ct = "application/vnd.docker.distribution.manifest.v2+json"
    await storage.store_manifest("myapp", "v1", manifest_body, ct)
    await queue.enqueue("myapp", "v1")

    # Blob already exists upstream
    respx.head(f"{BASE}/v2/myapp/blobs/{blob_digest}").mock(
        return_value=Response(200, headers={"Content-Length": str(len(blob_data))})
    )
    respx.put(f"{BASE}/v2/myapp/manifests/v1").mock(
        return_value=Response(201)
    )

    upstream = UpstreamClient(BASE)
    pending = await queue.list_pending()
    await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    # Should not have attempted POST for upload (blob exists)
    post_calls = [c for c in respx.calls if c.request.method == "POST"]
    assert len(post_calls) == 0


@respx.mock
async def test_sync_one_missing_manifest(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    await queue.enqueue("ghost", "latest")
    pending = await queue.list_pending()

    upstream = UpstreamClient(BASE)
    await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    # Marker should still be cleared (manifest gone, nothing to sync)
    remaining = await queue.list_pending()
    assert len(remaining) == 0


def test_extract_child_manifests_from_index() -> None:
    child1 = "sha256:aaaa"
    child2 = "sha256:bbbb"
    body = _make_index([child1, child2])
    assert _extract_child_manifests(body) == [child1, child2]


def test_extract_child_manifests_from_regular_manifest() -> None:
    body = _make_manifest(["sha256:layer1"], "sha256:config1")
    assert _extract_child_manifests(body) == []


def test_extract_child_manifests_invalid_json() -> None:
    assert _extract_child_manifests(b"not json") == []


@respx.mock
async def test_sync_one_pushes_child_manifests_before_index(tmp_path: Path) -> None:
    """Image index sync must push child manifests to upstream before the index itself."""
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    # Store a blob referenced by the child manifest
    blob_data = b"layer content"
    blob_digest = f"sha256:{hashlib.sha256(blob_data).hexdigest()}"
    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, blob_data)
    await storage.store_blob_from_upload(upload_id, blob_digest)

    # Store a child platform manifest
    child_body = _make_manifest([blob_digest])
    child_ct = "application/vnd.oci.image.manifest.v1+json"
    child_digest = f"sha256:{hashlib.sha256(child_body).hexdigest()}"
    await storage.store_manifest("myapp", child_digest, child_body, child_ct)

    # Store an image index referencing the child
    index_body = _make_index([child_digest])
    index_ct = "application/vnd.oci.image.index.v1+json"
    await storage.store_manifest("myapp", "latest", index_body, index_ct)

    await queue.enqueue("myapp", "latest")
    pending = await queue.list_pending()

    # Mock upstream
    respx.head(f"{BASE}/v2/myapp/blobs/{blob_digest}").mock(
        return_value=Response(404)
    )
    respx.post(f"{BASE}/v2/myapp/blobs/uploads/").mock(
        return_value=Response(202, headers={"Location": f"{BASE}/v2/myapp/blobs/uploads/u1"})
    )
    respx.put(f"{BASE}/v2/myapp/blobs/uploads/u1").mock(
        return_value=Response(201)
    )
    manifest_puts: list[str] = []

    def _record_manifest_put(request: respx.MockRequest) -> Response:
        # Extract the reference from the URL path
        ref = request.url.path.rsplit("/", 1)[-1]
        manifest_puts.append(ref)
        return Response(201)

    respx.put(url__regex=rf"{BASE}/v2/myapp/manifests/.+").mock(
        side_effect=_record_manifest_put
    )

    upstream = UpstreamClient(BASE)
    await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    # Child manifest must be pushed before the index
    assert len(manifest_puts) == 2
    assert manifest_puts[0] == child_digest
    assert manifest_puts[1] == "latest"

    remaining = await queue.list_pending()
    assert len(remaining) == 0


@respx.mock
async def test_sync_one_fails_when_child_manifest_blobs_missing(tmp_path: Path) -> None:
    """Sync must fail when a child manifest references blobs not stored locally."""
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    # Store a child manifest referencing a blob we DON'T store locally
    missing_blob = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
    child_body = _make_manifest([missing_blob])
    child_ct = "application/vnd.oci.image.manifest.v1+json"
    child_digest = f"sha256:{hashlib.sha256(child_body).hexdigest()}"
    await storage.store_manifest("myapp", child_digest, child_body, child_ct)

    # Store an image index referencing the child
    index_body = _make_index([child_digest])
    index_ct = "application/vnd.oci.image.index.v1+json"
    await storage.store_manifest("myapp", "latest", index_body, index_ct)

    await queue.enqueue("myapp", "latest")
    pending = await queue.list_pending()

    upstream = UpstreamClient(BASE)
    with pytest.raises(SyncMissingBlobsError):
        await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    # Marker should NOT be cleared (sync failed)
    remaining = await queue.list_pending()
    assert len(remaining) == 1


@respx.mock
async def test_sync_one_deduplicates_missing_blobs(tmp_path: Path) -> None:
    """When the same blob digest appears in multiple layers, it should only be
    reported once in the SyncMissingBlobsError."""
    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    missing_blob = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
    # Same blob referenced twice in two layers
    manifest_body = _make_manifest([missing_blob, missing_blob])
    ct = "application/vnd.oci.image.manifest.v1+json"
    await storage.store_manifest("myapp", "latest", manifest_body, ct)

    await queue.enqueue("myapp", "latest")
    pending = await queue.list_pending()

    upstream = UpstreamClient(BASE)
    with pytest.raises(SyncMissingBlobsError) as exc_info:
        await sync_one(pending[0], storage, queue, upstream)
    await upstream.close()

    assert len(exc_info.value.missing) == 1


@respx.mock
async def test_run_sync_loop_logs_missing_blobs_without_traceback(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    """SyncMissingBlobsError is an expected condition and should be logged as a
    warning without a full traceback."""
    import loguru
    import sys

    # Set up loguru to write to stderr so capfd captures it
    loguru.logger.remove()
    loguru.logger.add(sys.stderr, format="{level} | {message}")

    storage = Storage(str(tmp_path))
    await storage.init()
    queue = SyncQueue(str(tmp_path))
    await queue.init()

    missing_blob = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
    manifest_body = _make_manifest([missing_blob])
    ct = "application/vnd.oci.image.manifest.v1+json"
    await storage.store_manifest("myapp", "v1", manifest_body, ct)
    await queue.enqueue("myapp", "v1")

    upstream = UpstreamClient(BASE)

    # Run just one iteration by cancelling after a short delay
    async def cancel_after_one_iteration() -> None:
        # Give the loop time to process one marker
        await asyncio.sleep(0.1)
        raise asyncio.CancelledError

    task = asyncio.create_task(run_sync_loop(storage, queue, upstream, interval=60))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await upstream.close()

    captured = capfd.readouterr()
    stderr = captured.err

    # Should contain a WARNING, not an ERROR
    assert "WARNING" in stderr
    # Should NOT contain "Traceback" (no full stack trace)
    assert "Traceback" not in stderr
    # Should mention the missing blobs
    assert "Missing" in stderr or "missing" in stderr.lower()
