# ABOUTME: File-based sync queue for pending image pushes to the upstream registry.
# ABOUTME: Marker files in a pending/ directory track what needs syncing with retry backoff.
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

log = logger.bind(component="sync-queue")

MAX_BACKOFF_SECONDS = 300


@dataclass
class SyncMarker:
    name: str
    reference: str
    retries: int = 0
    next_attempt: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    created: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def is_ready(self) -> bool:
        return datetime.fromisoformat(self.next_attempt) <= datetime.now(UTC)

    @property
    def path_key(self) -> str:
        """Filesystem-safe key for this marker."""
        # Reference might be a tag or sha256:digest, sanitize colons
        safe_ref = self.reference.replace(":", "_")
        return f"{self.name}/{safe_ref}.json"


class SyncQueue:
    def __init__(self, data_dir: str) -> None:
        self._pending = Path(data_dir) / "pending"

    async def init(self) -> None:
        def _mkdir() -> None:
            self._pending.mkdir(parents=True, exist_ok=True)

        await asyncio.to_thread(_mkdir)

    async def enqueue(self, name: str, reference: str) -> Path:
        marker = SyncMarker(name=name, reference=reference)
        path = self._pending / marker.path_key

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(asdict(marker), indent=2))

        await asyncio.to_thread(_write)
        log.info("Queued sync for {name}:{ref}", name=name, ref=reference)
        return path

    async def list_pending(self) -> list[SyncMarker]:
        def _scan() -> list[SyncMarker]:
            if not self._pending.exists():
                return []
            markers = []
            for path in self._pending.rglob("*.json"):
                try:
                    data = json.loads(path.read_text())
                    marker = SyncMarker(**data)
                    if marker.is_ready:
                        markers.append(marker)
                except (json.JSONDecodeError, TypeError, KeyError):
                    log.warning("Skipping malformed marker: {}", path)
            return markers

        return await asyncio.to_thread(_scan)

    async def mark_done(self, marker: SyncMarker) -> None:
        path = self._pending / marker.path_key

        def _delete() -> None:
            if path.exists():
                path.unlink()
            # Clean up empty parent dirs
            parent = path.parent
            if parent != self._pending and parent.exists() and not any(parent.iterdir()):
                parent.rmdir()

        await asyncio.to_thread(_delete)
        log.info("Sync complete for {name}:{ref}", name=marker.name, ref=marker.reference)

    async def nudge_pending(self) -> int:
        """Reset next_attempt to now for all markers in backoff. Returns count nudged."""
        now = datetime.now(UTC).isoformat()

        def _nudge() -> int:
            if not self._pending.exists():
                return 0
            count = 0
            for path in self._pending.rglob("*.json"):
                try:
                    data = json.loads(path.read_text())
                    marker = SyncMarker(**data)
                    if not marker.is_ready:
                        data["next_attempt"] = now
                        path.write_text(json.dumps(data, indent=2))
                        count += 1
                except (json.JSONDecodeError, TypeError, KeyError):
                    log.warning("Skipping malformed marker: {}", path)
            return count

        nudged = await asyncio.to_thread(_nudge)
        if nudged:
            log.info("Nudged {} pending marker(s) for immediate retry", nudged)
        return nudged

    async def mark_failed(self, marker: SyncMarker) -> None:
        marker.retries += 1
        backoff = min(5 * (2**marker.retries), MAX_BACKOFF_SECONDS)
        from datetime import timedelta

        marker.next_attempt = (datetime.now(UTC) + timedelta(seconds=backoff)).isoformat()
        path = self._pending / marker.path_key

        def _write() -> None:
            path.write_text(json.dumps(asdict(marker), indent=2))

        await asyncio.to_thread(_write)
        log.warning(
            "Sync failed for {name}:{ref} (retry {n}, next in {b}s)",
            name=marker.name,
            ref=marker.reference,
            n=marker.retries,
            b=backoff,
        )
