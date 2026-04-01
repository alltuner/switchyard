# ABOUTME: Shared pytest fixtures for route testing.
# ABOUTME: Provides a configured Starlette test client backed by tmp_path storage.
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from switchyard.config import Settings
from switchyard.routes import base, blobs, catalog, manifests
from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue


def _make_app(storage: Storage, queue: SyncQueue, settings: Settings) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        await storage.init()
        await queue.init()
        app.state.storage = storage
        app.state.queue = queue
        app.state.settings = settings
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


@pytest.fixture
def registry(tmp_path: Path) -> TestClient:
    storage = Storage(str(tmp_path))
    queue = SyncQueue(str(tmp_path))
    settings = Settings(data_dir=str(tmp_path))
    app = _make_app(storage, queue, settings)
    # TestClient handles lifespan via context manager
    with TestClient(app) as client:
        yield client  # type: ignore[misc]


@pytest.fixture
def storage_path(registry: TestClient) -> Path:
    return Path(registry.app.state.settings.data_dir)  # type: ignore[union-attr]
