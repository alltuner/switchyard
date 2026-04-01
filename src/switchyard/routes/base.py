# ABOUTME: Docker Registry V2 version check endpoint.
# ABOUTME: Returns 200 with the required API version header.
from __future__ import annotations

from starlette.requests import Request
from starlette.responses import JSONResponse, Response


async def version_check(request: Request) -> Response:
    return JSONResponse(
        {},
        headers={"Docker-Distribution-API-Version": "registry/2.0"},
    )
