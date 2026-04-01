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
        log.warning("Manifest {name}:{ref} not found locally, removing marker", name=name, ref=reference)
        await queue.mark_done(marker)
        return

    body, content_type = result

    # Parse manifest to extract layer digests
    digests = _extract_blob_digests(body)

    # Push each blob that doesn't exist upstream
    for digest in digests:
        if await storage.has_blob(digest):
            await upstream.push_blob_streaming(name, digest, storage.stream_blob(digest))
        else:
            log.warning("Blob {} referenced by manifest but missing locally", digest[:19])

    # Push the manifest
    await upstream.push_manifest(name, reference, body, content_type)

    await queue.mark_done(marker)
    log.info("Synced {name}:{ref} ({n} blobs)", name=name, ref=reference, n=len(digests))


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
