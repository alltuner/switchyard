# ABOUTME: Tests for the file-based sync queue.
# ABOUTME: Covers enqueue, list filtering, mark_done, mark_failed with backoff.
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from switchyard.sync_queue import SyncMarker, SyncQueue


async def _make_queue(tmp_path: Path) -> SyncQueue:
    queue = SyncQueue(str(tmp_path))
    await queue.init()
    return queue


async def test_enqueue_creates_marker_file(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    path = await queue.enqueue("myapp", "latest")

    assert path.exists()
    data = json.loads(path.read_text())
    assert data["name"] == "myapp"
    assert data["reference"] == "latest"
    assert data["retries"] == 0


async def test_list_pending_returns_ready_markers(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    await queue.enqueue("app-a", "v1")
    await queue.enqueue("app-b", "latest")

    pending = await queue.list_pending()
    names = [(m.name, m.reference) for m in pending]
    assert ("app-a", "v1") in names
    assert ("app-b", "latest") in names


async def test_list_pending_skips_future_markers(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    await queue.enqueue("myapp", "latest")

    # Manually set next_attempt to the future
    marker_path = tmp_path / "pending" / "myapp" / "latest.json"
    data = json.loads(marker_path.read_text())
    data["next_attempt"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    marker_path.write_text(json.dumps(data))

    pending = await queue.list_pending()
    assert len(pending) == 0


async def test_mark_done_removes_file(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    await queue.enqueue("myapp", "latest")

    pending = await queue.list_pending()
    assert len(pending) == 1

    await queue.mark_done(pending[0])

    pending = await queue.list_pending()
    assert len(pending) == 0

    # Parent dir should be cleaned up too
    assert not (tmp_path / "pending" / "myapp").exists()


async def test_mark_failed_increments_retries(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    await queue.enqueue("myapp", "latest")

    pending = await queue.list_pending()
    marker = pending[0]
    assert marker.retries == 0

    await queue.mark_failed(marker)
    assert marker.retries == 1

    # Should not be ready immediately (backoff = 10s)
    pending = await queue.list_pending()
    assert len(pending) == 0

    # Read back the file to verify
    marker_path = tmp_path / "pending" / "myapp" / "latest.json"
    data = json.loads(marker_path.read_text())
    assert data["retries"] == 1
    next_attempt = datetime.fromisoformat(data["next_attempt"])
    assert next_attempt > datetime.now(UTC)


async def test_backoff_formula(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    await queue.enqueue("myapp", "latest")
    pending = await queue.list_pending()
    marker = pending[0]

    # Retry 1: backoff = 5 * 2^1 = 10s
    # Retry 2: backoff = 5 * 2^2 = 20s
    # Retry 3: backoff = 5 * 2^3 = 40s
    # ...capped at 300s
    expected_backoffs = [10, 20, 40, 80, 160, 300, 300]
    for i, expected in enumerate(expected_backoffs):
        before = datetime.now(UTC)
        await queue.mark_failed(marker)
        assert marker.retries == i + 1
        next_attempt = datetime.fromisoformat(marker.next_attempt)
        delta = (next_attempt - before).total_seconds()
        # Allow 1s tolerance for test timing
        assert abs(delta - expected) < 1, f"retry {i + 1}: expected ~{expected}s, got {delta:.1f}s"


async def test_marker_path_sanitizes_digest_reference(tmp_path: Path) -> None:
    queue = await _make_queue(tmp_path)
    path = await queue.enqueue("myapp", "sha256:abc123")

    assert path.exists()
    # Colon should be replaced with underscore in filename
    assert "sha256_abc123.json" in str(path)


async def test_marker_is_ready() -> None:
    past = SyncMarker(
        name="a",
        reference="b",
        next_attempt=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
    )
    assert past.is_ready

    future = SyncMarker(
        name="a",
        reference="b",
        next_attempt=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    )
    assert not future.is_ready
