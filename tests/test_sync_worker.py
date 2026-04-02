# ABOUTME: Tests for the background sync worker.
# ABOUTME: Verifies that pending markers are processed and blobs/manifests pushed upstream.
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import respx
from httpx import Response

from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.sync_worker import _extract_blob_digests, sync_one
from switchyard.upstream import UpstreamClient

BASE = "https://central:5000"


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
