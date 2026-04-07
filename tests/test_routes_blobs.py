# ABOUTME: Tests for Docker Registry V2 blob upload and download endpoints.
# ABOUTME: Covers the full POST/PATCH/PUT upload cycle and HEAD/GET retrieval.
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from starlette.testclient import TestClient


def _push_blob(client: TestClient, name: str, data: bytes) -> str:
    """Push a blob through the full upload cycle. Returns the digest."""
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

    # Start upload
    resp = client.post(f"/v2/{name}/blobs/uploads/")
    assert resp.status_code == 202
    location = resp.headers["Location"]
    upload_uuid = resp.headers["Docker-Upload-UUID"]
    assert upload_uuid in location

    # Upload chunk
    resp = client.patch(
        location,
        content=data,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert resp.status_code == 202
    assert resp.headers["Range"] == f"0-{len(data) - 1}"

    # Complete upload
    resp = client.put(f"{location}?digest={digest}")
    assert resp.status_code == 201
    assert resp.headers["Docker-Content-Digest"] == digest

    return digest


def test_head_blob_missing(registry: TestClient) -> None:
    resp = registry.head("/v2/myapp/blobs/sha256:nonexistent")
    assert resp.status_code == 404


def test_get_blob_missing(registry: TestClient) -> None:
    resp = registry.get("/v2/myapp/blobs/sha256:nonexistent")
    assert resp.status_code == 404


def test_full_blob_upload_and_retrieve(registry: TestClient) -> None:
    data = b"hello blob world"
    digest = _push_blob(registry, "myapp", data)

    # HEAD should return size and digest
    resp = registry.head(f"/v2/myapp/blobs/{digest}")
    assert resp.status_code == 200
    assert resp.headers["Content-Length"] == str(len(data))
    assert resp.headers["Docker-Content-Digest"] == digest

    # GET should return the data
    resp = registry.get(f"/v2/myapp/blobs/{digest}")
    assert resp.status_code == 200
    assert resp.content == data
    assert resp.headers["Docker-Content-Digest"] == digest


def test_chunked_upload(registry: TestClient) -> None:
    chunk1 = b"first chunk "
    chunk2 = b"second chunk"
    full_data = chunk1 + chunk2
    digest = f"sha256:{hashlib.sha256(full_data).hexdigest()}"

    # Start
    resp = registry.post("/v2/myapp/blobs/uploads/")
    assert resp.status_code == 202
    location = resp.headers["Location"]

    # Chunk 1
    resp = registry.patch(
        location,
        content=chunk1,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert resp.status_code == 202
    assert resp.headers["Range"] == f"0-{len(chunk1) - 1}"

    # Chunk 2
    resp = registry.patch(
        location,
        content=chunk2,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert resp.status_code == 202
    assert resp.headers["Range"] == f"0-{len(full_data) - 1}"

    # Complete
    resp = registry.put(f"{location}?digest={digest}")
    assert resp.status_code == 201

    # Verify
    resp = registry.get(f"/v2/myapp/blobs/{digest}")
    assert resp.content == full_data


def test_monolithic_upload(registry: TestClient) -> None:
    data = b"monolithic blob"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

    resp = registry.post(
        f"/v2/myapp/blobs/uploads/?digest={digest}",
        content=data,
    )
    assert resp.status_code == 201
    assert resp.headers["Docker-Content-Digest"] == digest

    resp = registry.get(f"/v2/myapp/blobs/{digest}")
    assert resp.content == data


def test_upload_digest_mismatch(registry: TestClient) -> None:
    data = b"some data"

    resp = registry.post("/v2/myapp/blobs/uploads/")
    location = resp.headers["Location"]

    resp = registry.patch(location, content=data)
    assert resp.status_code == 202

    resp = registry.put(f"{location}?digest=sha256:wrong")
    assert resp.status_code == 400


def test_complete_upload_missing_digest(registry: TestClient) -> None:
    resp = registry.post("/v2/myapp/blobs/uploads/")
    location = resp.headers["Location"]

    resp = registry.put(location)
    assert resp.status_code == 400


def test_patch_nonexistent_upload(registry: TestClient) -> None:
    resp = registry.patch(
        "/v2/myapp/blobs/uploads/nonexistent-uuid",
        content=b"data",
    )
    assert resp.status_code == 404


def test_blob_with_nested_name(registry: TestClient) -> None:
    data = b"nested name blob"
    digest = _push_blob(registry, "library/nginx", data)

    resp = registry.get(f"/v2/library/nginx/blobs/{digest}")
    assert resp.status_code == 200
    assert resp.content == data


def test_completing_upload_nudges_pending_markers(
    registry: TestClient, storage_path: Path
) -> None:
    """Blob upload should reset backoff on pending sync markers."""
    from datetime import UTC, datetime, timedelta

    from switchyard.sync_queue import SyncMarker

    # Create a marker in backoff (simulating a deferred manifest sync)
    pending_dir = storage_path / "pending" / "myapp"
    pending_dir.mkdir(parents=True)
    future = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    marker = SyncMarker(name="myapp", reference="latest", retries=3, next_attempt=future)
    marker_path = pending_dir / "latest.json"
    marker_path.write_text(json.dumps({"name": "myapp", "reference": "latest", "retries": 3, "next_attempt": future, "created": datetime.now(UTC).isoformat()}))

    # Upload a blob (PUT completion path)
    _push_blob(registry, "myapp", b"some layer data")

    # Marker should have been nudged to now
    data = json.loads(marker_path.read_text())
    nudged_time = datetime.fromisoformat(data["next_attempt"])
    assert nudged_time <= datetime.now(UTC), "Marker should have been nudged to now"


def test_monolithic_upload_nudges_pending_markers(
    registry: TestClient, storage_path: Path
) -> None:
    """Monolithic blob upload should also nudge pending sync markers."""
    from datetime import UTC, datetime, timedelta

    pending_dir = storage_path / "pending" / "myapp"
    pending_dir.mkdir(parents=True)
    future = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    marker_path = pending_dir / "latest.json"
    marker_path.write_text(json.dumps({"name": "myapp", "reference": "latest", "retries": 2, "next_attempt": future, "created": datetime.now(UTC).isoformat()}))

    data = b"monolithic blob for nudge test"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    resp = registry.post(f"/v2/myapp/blobs/uploads/?digest={digest}", content=data)
    assert resp.status_code == 201

    marker_data = json.loads(marker_path.read_text())
    nudged_time = datetime.fromisoformat(marker_data["next_attempt"])
    assert nudged_time <= datetime.now(UTC), "Marker should have been nudged to now"
