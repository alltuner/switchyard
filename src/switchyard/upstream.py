# ABOUTME: HTTP client for communicating with the upstream Docker registry.
# ABOUTME: Handles pushing blobs/manifests and proxying pull requests.
from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
from loguru import logger

log = logger.bind(component="upstream")


class UpstreamClient:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(connect=10, read=300, write=300, pool=10),
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._client.aclose()

    # -- Blob operations --

    async def check_blob(self, name: str, digest: str) -> bool:
        resp = await self._client.head(f"/v2/{name}/blobs/{digest}")
        return resp.status_code == 200

    async def pull_blob(self, name: str, digest: str) -> AsyncIterator[bytes]:
        async with self._client.stream("GET", f"/v2/{name}/blobs/{digest}") as resp:
            resp.raise_for_status()
            async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                yield chunk

    async def push_blob(self, name: str, digest: str, data: bytes) -> None:
        """Push a blob using monolithic upload (POST + PUT)."""
        # Check if blob already exists
        if await self.check_blob(name, digest):
            log.debug("Blob {} already exists upstream, skipping", digest[:19])
            return

        # Initiate upload
        resp = await self._client.post(f"/v2/{name}/blobs/uploads/")
        resp.raise_for_status()
        location = resp.headers["Location"]

        # Complete with monolithic PUT
        if location.startswith("/"):
            upload_url = location
        else:
            upload_url = location

        resp = await self._client.put(
            upload_url,
            content=data,
            params={"digest": digest},
            headers={"Content-Type": "application/octet-stream"},
        )
        resp.raise_for_status()
        log.debug("Pushed blob {} upstream", digest[:19])

    async def push_blob_streaming(
        self, name: str, digest: str, stream: AsyncIterator[bytes]
    ) -> None:
        """Push a blob by streaming from local storage."""
        if await self.check_blob(name, digest):
            log.debug("Blob {} already exists upstream, skipping", digest[:19])
            return

        # Initiate upload
        resp = await self._client.post(f"/v2/{name}/blobs/uploads/")
        resp.raise_for_status()
        location = resp.headers["Location"]

        # Stream the blob content as a monolithic PUT
        async def _body() -> AsyncIterator[bytes]:
            async for chunk in stream:
                yield chunk

        resp = await self._client.put(
            location,
            content=_body(),
            params={"digest": digest},
            headers={"Content-Type": "application/octet-stream"},
        )
        resp.raise_for_status()
        log.debug("Pushed blob {} upstream (streamed)", digest[:19])

    # -- Manifest operations --

    async def check_manifest(self, name: str, reference: str) -> bool:
        resp = await self._client.head(f"/v2/{name}/manifests/{reference}")
        return resp.status_code == 200

    async def pull_manifest(self, name: str, reference: str) -> tuple[bytes, str, str] | None:
        """Pull a manifest. Returns (body, content_type, digest) or None."""
        resp = await self._client.get(
            f"/v2/{name}/manifests/{reference}",
            headers={
                "Accept": ", ".join(
                    [
                        "application/vnd.docker.distribution.manifest.v2+json",
                        "application/vnd.oci.image.manifest.v1+json",
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                        "application/vnd.oci.image.index.v1+json",
                    ]
                ),
            },
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()

        body = resp.content
        content_type = resp.headers.get("content-type", "application/json")
        digest = resp.headers.get("docker-content-digest", "")
        return body, content_type, digest

    async def push_manifest(
        self, name: str, reference: str, data: bytes, content_type: str
    ) -> None:
        resp = await self._client.put(
            f"/v2/{name}/manifests/{reference}",
            content=data,
            headers={"Content-Type": content_type},
        )
        resp.raise_for_status()
        log.debug("Pushed manifest {name}:{ref} upstream", name=name, ref=reference)
