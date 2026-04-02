# ABOUTME: Client for communicating with the upstream Docker registry.
# ABOUTME: Uses python-dxf for registry v2 operations, wrapped with asyncio.to_thread.
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import requests.exceptions
from dxf import DXF
from loguru import logger

log = logger.bind(component="upstream")

CHUNK_SIZE = 1024 * 1024  # 1MB
_SENTINEL = object()


class UpstreamClient:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        if "://" in self._base_url:
            self._insecure = self._base_url.startswith("http://")
            self._host = self._base_url.split("://", 1)[1]
        else:
            self._host = self._base_url
            self._insecure = False
        self._dxf_cache: dict[str, DXF] = {}

    def _get_dxf(self, repo: str) -> DXF:
        """Get or create a DXF instance for the given repo."""
        if repo not in self._dxf_cache:
            dxf = DXF(
                host=self._host,
                repo=repo,
                insecure=self._insecure,
                timeout=300,
            )
            dxf.__enter__()
            self._dxf_cache[repo] = dxf
        return self._dxf_cache[repo]

    async def close(self) -> None:
        for dxf in self._dxf_cache.values():
            dxf.__exit__(None, None, None)
        self._dxf_cache.clear()

    # -- Blob operations --

    async def check_blob(self, name: str, digest: str) -> bool:
        dxf = self._get_dxf(name)

        def _check() -> bool:
            try:
                dxf.blob_size(digest)
                return True
            except requests.exceptions.HTTPError:
                return False

        return await asyncio.to_thread(_check)

    async def pull_blob(self, name: str, digest: str) -> AsyncIterator[bytes]:
        dxf = self._get_dxf(name)
        chunks = await asyncio.to_thread(dxf.pull_blob, digest, chunk_size=CHUNK_SIZE)
        while True:
            chunk = await asyncio.to_thread(next, chunks, _SENTINEL)
            if chunk is _SENTINEL:
                break
            yield chunk

    async def push_blob(self, name: str, digest: str, data: bytes) -> None:
        """Push a blob using monolithic upload."""
        dxf = self._get_dxf(name)
        await asyncio.to_thread(dxf.push_blob, data=iter([data]), digest=digest)
        log.debug("Pushed blob {} upstream", digest[:19])

    async def push_blob_streaming(
        self, name: str, digest: str, stream: AsyncIterator[bytes]
    ) -> None:
        """Push a blob by collecting the stream and uploading."""
        chunks = [chunk async for chunk in stream]
        dxf = self._get_dxf(name)
        await asyncio.to_thread(dxf.push_blob, data=iter(chunks), digest=digest)
        log.debug("Pushed blob {} upstream (streamed)", digest[:19])

    # -- Manifest operations --

    async def check_manifest(self, name: str, reference: str) -> bool:
        dxf = self._get_dxf(name)

        def _check() -> bool:
            try:
                dxf.head_manifest_and_response(reference)
                return True
            except requests.exceptions.HTTPError:
                return False

        return await asyncio.to_thread(_check)

    async def pull_manifest(self, name: str, reference: str) -> tuple[bytes, str, str] | None:
        """Pull a manifest. Returns (body, content_type, digest) or None."""
        dxf = self._get_dxf(name)

        def _pull() -> tuple[bytes, str, str] | None:
            try:
                manifest_str, resp = dxf.get_manifest_and_response(reference)
                body = manifest_str.encode() if isinstance(manifest_str, str) else manifest_str
                content_type = resp.headers.get("content-type", "application/json")
                digest = resp.headers.get("docker-content-digest", "")
                return body, content_type, digest
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    return None
                raise

        return await asyncio.to_thread(_pull)

    async def push_manifest(
        self, name: str, reference: str, data: bytes, content_type: str
    ) -> None:
        dxf = self._get_dxf(name)
        manifest_json = data.decode() if isinstance(data, bytes) else data
        parsed = json.loads(manifest_json)
        if "mediaType" not in parsed:
            parsed["mediaType"] = content_type
            manifest_json = json.dumps(parsed)
        await asyncio.to_thread(dxf.set_manifest, reference, manifest_json)
        log.debug("Pushed manifest {name}:{ref} upstream", name=name, ref=reference)
