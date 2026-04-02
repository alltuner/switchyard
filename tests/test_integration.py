# ABOUTME: End-to-end integration tests for the full push/pull/sync cycle.
# ABOUTME: Simulates a complete Docker image push, sync to upstream, and pull.
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import responses
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from switchyard.config import Settings
from switchyard.routes import base, blobs, catalog, manifests
from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.sync_worker import sync_one
from switchyard.upstream import UpstreamClient


def _make_app_with_upstream(
    storage: Storage, queue: SyncQueue, settings: Settings, upstream: UpstreamClient
) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        await storage.init()
        await queue.init()
        app.state.storage = storage
        app.state.queue = queue
        app.state.settings = settings
        app.state.upstream = upstream
        yield

    routes = [
        Route("/v2/", base.version_check, methods=["GET"]),
        Route("/v2/_catalog", catalog.catalog, methods=["GET"]),
        Route("/v2/{name:path}/blobs/{digest}", blobs.head_blob, methods=["HEAD"]),
        Route("/v2/{name:path}/blobs/{digest}", blobs.get_blob, methods=["GET"]),
        Route("/v2/{name:path}/blobs/uploads/", blobs.start_upload, methods=["POST"]),
        Route("/v2/{name:path}/blobs/uploads/{uuid}", blobs.patch_upload, methods=["PATCH"]),
        Route("/v2/{name:path}/blobs/uploads/{uuid}", blobs.complete_upload, methods=["PUT"]),
        Route("/v2/{name:path}/manifests/{reference}", manifests.head_manifest, methods=["HEAD"]),
        Route("/v2/{name:path}/manifests/{reference}", manifests.get_manifest, methods=["GET"]),
        Route("/v2/{name:path}/manifests/{reference}", manifests.put_manifest, methods=["PUT"]),
        Route("/v2/{name:path}/tags/list", catalog.list_tags, methods=["GET"]),
    ]

    return Starlette(routes=routes, lifespan=lifespan)


def _push_blob(client: TestClient, name: str, data: bytes) -> str:
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    resp = client.post(f"/v2/{name}/blobs/uploads/")
    assert resp.status_code == 202
    location = resp.headers["Location"]
    resp = client.patch(location, content=data)
    assert resp.status_code == 202
    resp = client.put(f"{location}?digest={digest}")
    assert resp.status_code == 201
    return digest


def _make_manifest(config_digest: str, layer_digests: list[str]) -> bytes:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"digest": config_digest, "mediaType": "application/json", "size": 0},
            "layers": [
                {"digest": d, "mediaType": "application/octet-stream", "size": 0}
                for d in layer_digests
            ],
        }
    ).encode()


class TestFullPushSyncCycle:
    """Push an image locally, sync it to a mock upstream, verify everything."""

    @responses.activate
    def test_push_and_sync(self, tmp_path: Path) -> None:
        storage = Storage(str(tmp_path))
        queue = SyncQueue(str(tmp_path))
        settings = Settings(data_dir=str(tmp_path), upstream="https://central:5000")
        upstream = UpstreamClient("https://central:5000")
        app = _make_app_with_upstream(storage, queue, settings, upstream)

        with TestClient(app) as client:
            # 1. Push blobs
            config_data = b'{"architecture": "amd64"}'
            layer_data = b"fake layer content here"
            config_digest = _push_blob(client, "myapp", config_data)
            layer_digest = _push_blob(client, "myapp", layer_data)

            # 2. Push manifest
            manifest_body = _make_manifest(config_digest, [layer_digest])
            manifest_ct = "application/vnd.docker.distribution.manifest.v2+json"
            resp = client.put(
                "/v2/myapp/manifests/latest",
                content=manifest_body,
                headers={"Content-Type": manifest_ct},
            )
            assert resp.status_code == 201
            assert resp.headers["Docker-Content-Digest"].startswith("sha256:")

            # 3. Verify local state
            resp = client.get("/v2/_catalog")
            assert "myapp" in resp.json()["repositories"]

            resp = client.get("/v2/myapp/tags/list")
            assert "latest" in resp.json()["tags"]

            resp = client.get(f"/v2/myapp/blobs/{layer_digest}")
            assert resp.content == layer_data

            resp = client.get("/v2/myapp/manifests/latest")
            assert resp.status_code == 200

            # 4. Verify sync marker was created
            pending_dir = tmp_path / "pending" / "myapp"
            assert pending_dir.exists()
            markers = list(pending_dir.glob("*.json"))
            assert len(markers) == 1

        # 5. Mock upstream and run sync
        # HEAD checks for both blobs (config + layer)
        responses.add(
            responses.HEAD,
            url="https://central:5000/v2/myapp/blobs/" + config_digest,
            status=404,
        )
        responses.add(
            responses.HEAD,
            url="https://central:5000/v2/myapp/blobs/" + layer_digest,
            status=404,
        )
        # POST to initiate upload for each blob
        responses.add(
            responses.POST,
            url="https://central:5000/v2/myapp/blobs/uploads/",
            status=202,
            headers={"Location": "https://central:5000/v2/myapp/blobs/uploads/u1"},
        )
        responses.add(
            responses.POST,
            url="https://central:5000/v2/myapp/blobs/uploads/",
            status=202,
            headers={"Location": "https://central:5000/v2/myapp/blobs/uploads/u2"},
        )
        # PUT to complete each blob upload
        responses.add(
            responses.PUT, url="https://central:5000/v2/myapp/blobs/uploads/u1", status=201
        )
        responses.add(
            responses.PUT, url="https://central:5000/v2/myapp/blobs/uploads/u2", status=201
        )
        # PUT manifest
        responses.add(
            responses.PUT, url="https://central:5000/v2/myapp/manifests/latest", status=201
        )

        async def _run_sync() -> None:
            await storage.init()
            await queue.init()
            pending = await queue.list_pending()
            assert len(pending) == 1
            await sync_one(pending[0], storage, queue, upstream)
            await upstream.close()

        asyncio.run(_run_sync())

        # Verify: 2 blob HEAD checks + 2 blob uploads (POST+PUT each) + 1 manifest PUT
        head_calls = [c for c in responses.calls if c.request.method == "HEAD"]
        post_calls = [c for c in responses.calls if c.request.method == "POST"]
        put_calls = [c for c in responses.calls if c.request.method == "PUT"]
        assert len(head_calls) == 2  # config + layer
        assert len(post_calls) == 2  # config + layer upload initiation
        assert len(put_calls) == 3  # config + layer upload completion + manifest

        # Marker should be cleared
        markers = list(pending_dir.glob("*.json"))
        assert len(markers) == 0


class TestPullProxyFromUpstream:
    """Pull an image that only exists on the upstream registry."""

    @responses.activate
    def test_pull_manifest_from_upstream(self, tmp_path: Path) -> None:
        storage = Storage(str(tmp_path))
        queue = SyncQueue(str(tmp_path))
        settings = Settings(data_dir=str(tmp_path), upstream="https://central:5000")
        upstream = UpstreamClient("https://central:5000")
        app = _make_app_with_upstream(storage, queue, settings, upstream)

        manifest_body = b'{"schemaVersion": 2}'
        manifest_ct = "application/vnd.docker.distribution.manifest.v2+json"
        manifest_digest = f"sha256:{hashlib.sha256(manifest_body).hexdigest()}"

        responses.get(
            "https://central:5000/v2/remote-app/manifests/latest",
            body=manifest_body,
            status=200,
            headers={
                "Content-Type": manifest_ct,
                "Docker-Content-Digest": manifest_digest,
            },
        )

        with TestClient(app) as client:
            # Pull manifest that doesn't exist locally
            resp = client.get("/v2/remote-app/manifests/latest")
            assert resp.status_code == 200
            assert resp.content == manifest_body
            assert resp.headers["Docker-Content-Digest"] == manifest_digest

            # Second pull should be served from local cache (no more upstream calls)
            resp = client.get("/v2/remote-app/manifests/latest")
            assert resp.status_code == 200
            assert resp.content == manifest_body
            # Only 1 call to upstream (first pull), second served from cache
            assert len(responses.calls) == 1
