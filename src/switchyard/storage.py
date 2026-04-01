# ABOUTME: Local disk storage for Docker registry blobs, manifests, and uploads.
# ABOUTME: All file I/O is async via asyncio.to_thread to avoid blocking the event loop.
from __future__ import annotations

import asyncio
import hashlib
import shutil
import uuid
from pathlib import Path
from typing import AsyncIterator

from loguru import logger

log = logger.bind(component="storage")

CHUNK_SIZE = 1024 * 1024  # 1MB


class Storage:
    def __init__(self, data_dir: str) -> None:
        self._root = Path(data_dir)
        self._blobs = self._root / "blobs" / "sha256"
        self._manifests = self._root / "manifests"
        self._uploads = self._root / "uploads"

    async def init(self) -> None:
        """Create storage directories if they don't exist."""

        def _mkdir() -> None:
            self._blobs.mkdir(parents=True, exist_ok=True)
            self._manifests.mkdir(parents=True, exist_ok=True)
            self._uploads.mkdir(parents=True, exist_ok=True)

        await asyncio.to_thread(_mkdir)
        log.info("Initialized storage at {}", self._root)

    # -- Blobs --

    def _blob_path(self, digest: str) -> Path:
        """Return path for a blob. Digest format: 'sha256:abc123...'"""
        _, hex_digest = digest.split(":", 1)
        return self._blobs / hex_digest

    async def has_blob(self, digest: str) -> bool:
        path = self._blob_path(digest)
        return await asyncio.to_thread(path.exists)

    async def blob_size(self, digest: str) -> int | None:
        path = self._blob_path(digest)

        def _size() -> int | None:
            if path.exists():
                return path.stat().st_size
            return None

        return await asyncio.to_thread(_size)

    async def stream_blob(self, digest: str) -> AsyncIterator[bytes]:
        path = self._blob_path(digest)

        def _read_chunk(f: object, size: int) -> bytes:
            return f.read(size)  # type: ignore[union-attr]

        f = await asyncio.to_thread(open, path, "rb")
        try:
            while True:
                chunk = await asyncio.to_thread(_read_chunk, f, CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk
        finally:
            await asyncio.to_thread(f.close)

    async def store_blob_from_upload(self, upload_id: str, digest: str) -> int:
        """Move a completed upload to blob storage. Returns the blob size."""
        upload_path = self._uploads / upload_id
        blob_path = self._blob_path(digest)

        def _finalize() -> int:
            size = upload_path.stat().st_size
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(upload_path), str(blob_path))
            return size

        size = await asyncio.to_thread(_finalize)
        log.info("Stored blob {} ({} bytes)", digest[:19], size)
        return size

    async def delete_blob(self, digest: str) -> bool:
        path = self._blob_path(digest)

        def _delete() -> bool:
            if path.exists():
                path.unlink()
                return True
            return False

        return await asyncio.to_thread(_delete)

    # -- Uploads --

    async def create_upload(self) -> str:
        upload_id = str(uuid.uuid4())
        path = self._uploads / upload_id
        await asyncio.to_thread(path.touch)
        return upload_id

    async def append_upload(self, upload_id: str, data: bytes) -> int:
        """Append data to an upload. Returns the new total size."""
        path = self._uploads / upload_id

        def _append() -> int:
            with open(path, "ab") as f:
                f.write(data)
            return path.stat().st_size

        return await asyncio.to_thread(_append)

    async def upload_size(self, upload_id: str) -> int | None:
        path = self._uploads / upload_id

        def _size() -> int | None:
            if path.exists():
                return path.stat().st_size
            return None

        return await asyncio.to_thread(_size)

    async def verify_upload_digest(self, upload_id: str, expected_digest: str) -> bool:
        """Verify that the upload content matches the expected digest."""
        path = self._uploads / upload_id

        def _verify() -> bool:
            h = hashlib.sha256()
            with open(path, "rb") as f:
                while chunk := f.read(CHUNK_SIZE):
                    h.update(chunk)
            actual = f"sha256:{h.hexdigest()}"
            return actual == expected_digest

        return await asyncio.to_thread(_verify)

    async def delete_upload(self, upload_id: str) -> None:
        path = self._uploads / upload_id

        def _delete() -> None:
            if path.exists():
                path.unlink()

        await asyncio.to_thread(_delete)

    # -- Manifests --

    def _manifest_dir(self, name: str) -> Path:
        return self._manifests / name

    def _manifest_path(self, name: str, reference: str) -> Path:
        return self._manifest_dir(name) / reference

    def _content_type_path(self, name: str, reference: str) -> Path:
        return self._manifest_dir(name) / f"{reference}.content-type"

    async def has_manifest(self, name: str, reference: str) -> bool:
        path = self._manifest_path(name, reference)
        return await asyncio.to_thread(path.exists)

    async def get_manifest(self, name: str, reference: str) -> tuple[bytes, str] | None:
        """Return (body, content_type) or None."""
        manifest_path = self._manifest_path(name, reference)
        ct_path = self._content_type_path(name, reference)

        def _read() -> tuple[bytes, str] | None:
            if not manifest_path.exists():
                return None
            body = manifest_path.read_bytes()
            content_type = ct_path.read_text() if ct_path.exists() else "application/json"
            return body, content_type

        return await asyncio.to_thread(_read)

    async def store_manifest(
        self, name: str, reference: str, data: bytes, content_type: str
    ) -> str:
        """Store a manifest. Returns the digest."""
        manifest_path = self._manifest_path(name, reference)
        ct_path = self._content_type_path(name, reference)
        digest = f"sha256:{hashlib.sha256(data).hexdigest()}"

        def _write() -> None:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_bytes(data)
            ct_path.write_text(content_type)

        await asyncio.to_thread(_write)

        # Also store by digest for content-addressable lookups
        if reference != digest:
            digest_path = self._manifest_path(name, digest)
            digest_ct_path = self._content_type_path(name, digest)

            def _write_digest() -> None:
                digest_path.write_bytes(data)
                digest_ct_path.write_text(content_type)

            await asyncio.to_thread(_write_digest)

        log.info("Stored manifest {name}:{ref} ({digest})", name=name, ref=reference, digest=digest[:19])
        return digest

    async def list_repos(self) -> list[str]:
        def _list() -> list[str]:
            if not self._manifests.exists():
                return []
            repos = []
            for path in self._manifests.rglob("*"):
                if path.is_file() and not path.name.endswith(".content-type"):
                    rel = path.parent.relative_to(self._manifests)
                    repo = str(rel)
                    if repo not in repos:
                        repos.append(repo)
            return sorted(repos)

        return await asyncio.to_thread(_list)

    async def list_tags(self, name: str) -> list[str]:
        manifest_dir = self._manifest_dir(name)

        def _list() -> list[str]:
            if not manifest_dir.exists():
                return []
            tags = []
            for path in manifest_dir.iterdir():
                if path.is_file() and not path.name.endswith(".content-type"):
                    tag = path.name
                    # Skip digest references (sha256:...)
                    if not tag.startswith("sha256:"):
                        tags.append(tag)
            return sorted(tags)

        return await asyncio.to_thread(_list)
