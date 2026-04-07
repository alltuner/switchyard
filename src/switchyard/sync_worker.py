# ABOUTME: Background worker that syncs locally pushed images to the upstream registry.
# ABOUTME: Scans the pending queue, pushes blobs and manifests, handles retries.
from __future__ import annotations

import asyncio
import json

from loguru import logger

from switchyard.storage import Storage
from switchyard.sync_queue import SyncMarker, SyncQueue
from switchyard.upstream import UpstreamClient

log = logger.bind(component="sync")


class SyncMissingBlobsError(Exception):
    """Raised when blobs referenced by a manifest are not available locally."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__(f"Missing {len(missing)} blob(s): {', '.join(d[:19] for d in missing)}")


async def sync_one(
    marker: SyncMarker,
    storage: Storage,
    queue: SyncQueue,
    upstream: UpstreamClient,
) -> None:
    """Sync a single image (manifest + referenced blobs) to the upstream registry."""
    name = marker.name
    reference = marker.reference

    log.info("Syncing {name}:{ref}", name=name, ref=reference)

    # Read the manifest to find referenced blobs
    result = await storage.get_manifest(name, reference)
    if result is None:
        log.warning(
            "Manifest {name}:{ref} not found locally, removing marker",
            name=name,
            ref=reference,
        )
        await queue.mark_done(marker)
        return

    body, content_type = result

    # Collect all blobs and child manifests, checking for missing blobs before
    # pushing anything upstream.
    all_blobs: list[str] = _extract_blob_digests(body)
    children: list[tuple[str, bytes, str]] = []

    for child_digest in _extract_child_manifests(body):
        child = await storage.get_manifest(name, child_digest)
        if child is None:
            log.warning(
                "Child manifest {} referenced by index but missing locally",
                child_digest[:19],
            )
            continue
        child_body, child_ct = child
        children.append((child_digest, child_body, child_ct))
        all_blobs.extend(_extract_blob_digests(child_body))

    all_blobs = list(dict.fromkeys(all_blobs))
    missing = [d for d in all_blobs if not await storage.has_blob(d)]
    if missing:
        for digest in missing:
            log.warning("Blob {} referenced by manifest but missing locally", digest[:19])
        raise SyncMissingBlobsError(missing)

    # Push blobs
    pushed_blobs: set[str] = set()
    for digest in all_blobs:
        if digest not in pushed_blobs:
            await upstream.push_blob_streaming(name, digest, storage.stream_blob(digest))
            pushed_blobs.add(digest)

    # Push child manifests before the index
    for child_digest, child_body, child_ct in children:
        await upstream.push_manifest(name, child_digest, child_body, child_ct)

    # Push the top-level manifest
    await upstream.push_manifest(name, reference, body, content_type)

    await queue.mark_done(marker)
    log.info("Synced {name}:{ref} ({n} blobs)", name=name, ref=reference, n=len(pushed_blobs))


def _extract_blob_digests(manifest_body: bytes) -> list[str]:
    """Extract all blob digests referenced by a manifest."""
    try:
        manifest = json.loads(manifest_body)
    except json.JSONDecodeError:
        return []

    digests: list[str] = []

    # Config blob
    config = manifest.get("config", {})
    if isinstance(config, dict) and "digest" in config:
        digests.append(config["digest"])

    # Layer blobs
    layers = manifest.get("layers", [])
    for layer in layers:
        if isinstance(layer, dict) and "digest" in layer:
            digests.append(layer["digest"])

    return digests


_INDEX_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}


def _extract_child_manifests(manifest_body: bytes) -> list[str]:
    """Extract child manifest digests from an image index / manifest list."""
    try:
        manifest = json.loads(manifest_body)
    except json.JSONDecodeError:
        return []

    media_type = manifest.get("mediaType", "")
    if media_type not in _INDEX_MEDIA_TYPES:
        return []

    return [
        m["digest"]
        for m in manifest.get("manifests", [])
        if isinstance(m, dict) and "digest" in m
    ]


async def run_sync_loop(
    storage: Storage,
    queue: SyncQueue,
    upstream: UpstreamClient,
    interval: int,
) -> None:
    """Main sync loop. Runs until cancelled."""
    log.info("Sync worker started (interval={interval}s)", interval=interval)
    while True:
        try:
            pending = await queue.list_pending()
            for marker in pending:
                try:
                    await sync_one(marker, storage, queue, upstream)
                except SyncMissingBlobsError as exc:
                    log.warning(
                        "Sync deferred for {name}:{ref} — {n} blob(s) not yet"
                        " available locally, will retry after they arrive: {blobs}",
                        name=marker.name,
                        ref=marker.reference,
                        n=len(exc.missing),
                        blobs=", ".join(d[:19] for d in exc.missing),
                    )
                    await queue.mark_failed(marker)
                except Exception:
                    log.opt(exception=True).error(
                        "Failed to sync {name}:{ref}",
                        name=marker.name,
                        ref=marker.reference,
                    )
                    await queue.mark_failed(marker)
        except Exception:
            log.opt(exception=True).error("Error in sync loop iteration")

        await asyncio.sleep(interval)
