# ABOUTME: Docker Registry V2 manifest endpoints for push and pull.
# ABOUTME: Stores manifests locally and enqueues sync on push.
from __future__ import annotations

import hashlib

from starlette.requests import Request
from starlette.responses import Response

from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.upstream import UpstreamClient


def _get_storage(request: Request) -> Storage:
    return request.app.state.storage  # type: ignore[no-any-return]


def _get_queue(request: Request) -> SyncQueue:
    return request.app.state.queue  # type: ignore[no-any-return]


def _get_upstream(request: Request) -> UpstreamClient | None:
    return request.app.state.upstream  # type: ignore[no-any-return]


async def _fetch_upstream_manifest(
    name: str, reference: str, storage: Storage, upstream: UpstreamClient | None
) -> tuple[bytes, str, str] | None:
    """Try to pull a manifest from upstream and cache it locally."""
    if not upstream:
        return None
    result = await upstream.pull_manifest(name, reference)
    if result is None:
        return None
    body, content_type, digest = result
    await storage.store_manifest(name, reference, body, content_type)
    return body, content_type, digest


async def head_manifest(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    reference = request.path_params["reference"]

    result = await storage.get_manifest(name, reference)
    if result is None:
        upstream_result = await _fetch_upstream_manifest(
            name, reference, storage, _get_upstream(request)
        )
        if upstream_result is None:
            return Response(status_code=404)
        body, content_type, digest = upstream_result
    else:
        body, content_type = result
        digest = f"sha256:{hashlib.sha256(body).hexdigest()}"

    return Response(
        status_code=200,
        headers={
            "Docker-Content-Digest": digest,
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
        },
    )


async def get_manifest(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    reference = request.path_params["reference"]

    result = await storage.get_manifest(name, reference)
    if result is None:
        upstream_result = await _fetch_upstream_manifest(
            name, reference, storage, _get_upstream(request)
        )
        if upstream_result is None:
            return Response(status_code=404)
        body, content_type, digest = upstream_result
    else:
        body, content_type = result
        digest = f"sha256:{hashlib.sha256(body).hexdigest()}"

    return Response(
        content=body,
        status_code=200,
        media_type=content_type,
        headers={
            "Docker-Content-Digest": digest,
            "Content-Length": str(len(body)),
        },
    )


async def put_manifest(request: Request) -> Response:
    storage = _get_storage(request)
    queue = _get_queue(request)
    name = request.path_params["name"]
    reference = request.path_params["reference"]

    body = await request.body()
    content_type = request.headers.get(
        "content-type", "application/vnd.docker.distribution.manifest.v2+json"
    )

    digest = await storage.store_manifest(name, reference, body, content_type)
    await queue.enqueue(name, reference)

    return Response(
        status_code=201,
        headers={
            "Location": f"/v2/{name}/manifests/{reference}",
            "Docker-Content-Digest": digest,
            "Content-Length": "0",
        },
    )
