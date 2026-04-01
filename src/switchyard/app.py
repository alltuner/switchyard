# ABOUTME: Starlette application with route wiring and lifespan management.
# ABOUTME: Initializes storage, sync queue, and background sync worker.
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from loguru import logger
from starlette.applications import Starlette
from starlette.routing import Route

from switchyard.config import Settings
from switchyard.log import setup_logging
from switchyard.routes import base, blobs, catalog, manifests
from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.sync_worker import run_sync_loop
from switchyard.upstream import UpstreamClient

log = logger.bind(component="switchyard")


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    settings = Settings.from_env()
    setup_logging()

    storage = Storage(settings.data_dir)
    await storage.init()

    queue = SyncQueue(settings.data_dir)
    await queue.init()

    app.state.storage = storage
    app.state.queue = queue
    app.state.settings = settings

    sync_task = None
    upstream = None

    if settings.upstream:
        upstream = UpstreamClient(settings.upstream)
        app.state.upstream = upstream
        sync_task = asyncio.create_task(
            run_sync_loop(storage, queue, upstream, settings.sync_interval)
        )
        log.info("Upstream configured: {}", settings.upstream)
    else:
        app.state.upstream = None
        log.info("No upstream configured, running in local-only mode")

    yield

    if sync_task:
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            pass
    if upstream:
        await upstream.close()


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

app = Starlette(routes=routes, lifespan=lifespan)
