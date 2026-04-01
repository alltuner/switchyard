# ABOUTME: Tests for the local disk storage layer.
# ABOUTME: Covers blob, upload, and manifest operations using tmp_path.
from __future__ import annotations

import hashlib
from pathlib import Path

from switchyard.storage import Storage


async def _make_storage(tmp_path: Path) -> Storage:
    storage = Storage(str(tmp_path))
    await storage.init()
    return storage


# -- Blobs --


async def test_has_blob_missing(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    assert not await storage.has_blob("sha256:deadbeef")


async def test_store_and_retrieve_blob(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b"hello world"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, data)
    await storage.store_blob_from_upload(upload_id, digest)

    assert await storage.has_blob(digest)
    assert await storage.blob_size(digest) == len(data)

    chunks = []
    async for chunk in storage.stream_blob(digest):
        chunks.append(chunk)
    assert b"".join(chunks) == data


async def test_delete_blob(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b"to delete"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, data)
    await storage.store_blob_from_upload(upload_id, digest)

    assert await storage.delete_blob(digest)
    assert not await storage.has_blob(digest)
    assert not await storage.delete_blob(digest)


# -- Uploads --


async def test_create_and_append_upload(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    upload_id = await storage.create_upload()

    assert await storage.upload_size(upload_id) == 0
    size = await storage.append_upload(upload_id, b"chunk1")
    assert size == 6
    size = await storage.append_upload(upload_id, b"chunk2")
    assert size == 12


async def test_verify_upload_digest(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b"verify me"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, data)

    assert await storage.verify_upload_digest(upload_id, digest)
    assert not await storage.verify_upload_digest(upload_id, "sha256:wrong")


async def test_delete_upload(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    upload_id = await storage.create_upload()
    await storage.append_upload(upload_id, b"data")

    await storage.delete_upload(upload_id)
    assert await storage.upload_size(upload_id) is None


async def test_upload_missing_size(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    assert await storage.upload_size("nonexistent") is None


# -- Manifests --


async def test_has_manifest_missing(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    assert not await storage.has_manifest("myapp", "latest")


async def test_store_and_get_manifest(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b'{"schemaVersion": 2}'
    ct = "application/vnd.docker.distribution.manifest.v2+json"

    digest = await storage.store_manifest("myapp", "latest", data, ct)
    assert digest.startswith("sha256:")

    result = await storage.get_manifest("myapp", "latest")
    assert result is not None
    body, content_type = result
    assert body == data
    assert content_type == ct

    # Also accessible by digest
    result = await storage.get_manifest("myapp", digest)
    assert result is not None
    body, content_type = result
    assert body == data


async def test_store_manifest_nested_name(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b'{"schemaVersion": 2}'
    ct = "application/vnd.docker.distribution.manifest.v2+json"

    await storage.store_manifest("library/nginx", "1.27", data, ct)
    result = await storage.get_manifest("library/nginx", "1.27")
    assert result is not None


# -- Listing --


async def test_list_repos(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b'{"schemaVersion": 2}'
    ct = "application/json"

    await storage.store_manifest("app-a", "latest", data, ct)
    await storage.store_manifest("app-b", "v1", data, ct)

    repos = await storage.list_repos()
    assert "app-a" in repos
    assert "app-b" in repos


async def test_list_tags(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    data = b'{"schemaVersion": 2}'
    ct = "application/json"

    await storage.store_manifest("myapp", "latest", data, ct)
    await storage.store_manifest("myapp", "v1", data, ct)

    tags = await storage.list_tags("myapp")
    assert tags == ["latest", "v1"]


async def test_list_tags_empty(tmp_path: Path) -> None:
    storage = await _make_storage(tmp_path)
    assert await storage.list_tags("nonexistent") == []
