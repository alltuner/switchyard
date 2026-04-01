# ABOUTME: Docker Registry V2 catalog and tag listing endpoints.
# ABOUTME: Lists repositories and tags from local storage.
from __future__ import annotations

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from switchyard.storage import Storage


def _get_storage(request: Request) -> Storage:
    return request.app.state.storage  # type: ignore[no-any-return]


async def catalog(request: Request) -> Response:
    storage = _get_storage(request)
    repos = await storage.list_repos()
    return JSONResponse({"repositories": repos})


async def list_tags(request: Request) -> Response:
    storage = _get_storage(request)
    name = request.path_params["name"]
    tags = await storage.list_tags(name)
    if not tags:
        return Response(status_code=404)
    return JSONResponse({"name": name, "tags": tags})
