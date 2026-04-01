# ABOUTME: Entry point for running switchyard via `uv run switchyard` or `python -m switchyard`.
# ABOUTME: Starts granian with tuned settings for streaming Docker image blobs.
from __future__ import annotations

from granian import Granian
from granian.constants import HTTPModes, Interfaces, Loops
from granian.http import HTTP1Settings

from switchyard.config import Settings


def main() -> None:
    settings = Settings()

    server = Granian(
        target="switchyard.app:app",
        address="0.0.0.0",
        port=settings.port,
        interface=Interfaces.ASGI,
        loop=Loops.uvloop,
        http=HTTPModes.http1,
        backlog=2048,
        backpressure=32,
        blocking_threads=1,
        http1_settings=HTTP1Settings(
            header_read_timeout=120,
            keep_alive=True,
            max_buffer_size=1024 * 1024,
        ),
    )
    server.serve()


if __name__ == "__main__":
    main()
