# ABOUTME: Docker Registry V2 blob endpoints for upload and download.
# ABOUTME: Handles the POST/PATCH/PUT upload flow and GET/HEAD for retrieval.
from __future__ import annotations

from collections.abc import AsyncIterator

from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from switchyard.storage import Storage
from switchyard.sync_queue import SyncQueue
from switchyard.upstream import UpstreamClient


def _get_storage(request: Request) -> Storage:
    return request.app.state.storage  # type: ignore[no-any-return]


def _get_queue(request: Request) -> SyncQueue:
    return request.app.state.queue  # type: ignore[no-any-return]


def _get_upstream(request: Request) -> UpstreamClient | None:
    return request.app.state.upstream  # type: ignore[no-any-return]


async def head_blob(request: Request) -> Response:
    storage = _get_storage(request)
    digest = request.path_params["digest"]

    size = await storage.blob_size(digest)
    if size is not None:
        return Response(
            status_code=200,
            headers={
                "Content-Length": str(size),
                "Docker-Content-Digest": digest,
                "Content-Type": "application/octet-stream",
            },
        )

    return Response(status_code=404)


async def get_blob(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    digest = request.path_params["digest"]

    # Serve from local storage
    if await storage.has_blob(digest):
        size = await storage.blob_size(digest)
        return StreamingResponse(
            storage.stream_blob(digest),
            media_type="application/octet-stream",
            headers={
                "Docker-Content-Digest": digest,
                "Content-Length": str(size),
            },
        )

    # Proxy from upstream and cache locally
    upstream = _get_upstream(request)
    if upstream:

        async def _proxy_and_cache() -> AsyncIterator[bytes]:
            upload_id = await storage.create_upload()
            try:
                async for chunk in upstream.pull_blob(name, digest):
                    await storage.append_upload(upload_id, chunk)
                    yield chunk
                await storage.store_blob_from_upload(upload_id, digest)
            except Exception:
                await storage.delete_upload(upload_id)
                raise

        try:
            return StreamingResponse(
                _proxy_and_cache(),
                media_type="application/octet-stream",
                headers={"Docker-Content-Digest": digest},
            )
        except Exception:
            pass

    return Response(status_code=404)


async def start_upload(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    upload_id = await storage.create_upload()

    # Check for monolithic upload (body present with digest query param)
    digest = request.query_params.get("digest")
    if digest:
        body = await request.body()
        if body:
            await storage.append_upload(upload_id, body)
            if not await storage.verify_upload_digest(upload_id, digest):
                await storage.delete_upload(upload_id)
                return Response(status_code=400, content="Digest mismatch")
            await storage.store_blob_from_upload(upload_id, digest)
            await _get_queue(request).nudge_pending()
            return Response(
                status_code=201,
                headers={
                    "Location": f"/v2/{name}/blobs/{digest}",
                    "Docker-Content-Digest": digest,
                    "Content-Length": "0",
                },
            )

    location = f"/v2/{name}/blobs/uploads/{upload_id}"
    return Response(
        status_code=202,
        headers={
            "Location": location,
            "Docker-Upload-UUID": upload_id,
            "Range": "0-0",
            "Content-Length": "0",
        },
    )


async def patch_upload(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    upload_id = request.path_params["uuid"]

    current_size = await storage.upload_size(upload_id)
    if current_size is None:
        return Response(status_code=404)

    # Stream the request body in chunks
    async for chunk in request.stream():
        if chunk:
            await storage.append_upload(upload_id, chunk)

    new_size = await storage.upload_size(upload_id)
    assert new_size is not None

    location = f"/v2/{name}/blobs/uploads/{upload_id}"
    return Response(
        status_code=202,
        headers={
            "Location": location,
            "Docker-Upload-UUID": upload_id,
            "Range": f"0-{new_size - 1}",
            "Content-Length": "0",
        },
    )


async def complete_upload(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    upload_id = request.path_params["uuid"]
    digest = request.query_params.get("digest", "")

    if not digest:
        return Response(status_code=400, content="Missing digest parameter")

    current_size = await storage.upload_size(upload_id)
    if current_size is None:
        return Response(status_code=404)

    # Handle final chunk in PUT body
    body = await request.body()
    if body:
        await storage.append_upload(upload_id, body)

    if not await storage.verify_upload_digest(upload_id, digest):
        await storage.delete_upload(upload_id)
        return Response(status_code=400, content="Digest mismatch")

    await storage.store_blob_from_upload(upload_id, digest)
    await _get_queue(request).nudge_pending()
    return Response(
        status_code=201,
        headers={
            "Location": f"/v2/{name}/blobs/{digest}",
            "Docker-Content-Digest": digest,
            "Content-Length": "0",
        },
    )
