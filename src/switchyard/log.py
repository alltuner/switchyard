# ABOUTME: Logging configuration using loguru.
# ABOUTME: Outputs structured logs to stdout for docker logs compatibility.
from __future__ import annotations

import sys

from loguru import logger


def setup_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[component]: <12}</cyan> | "
            "{message}"
        ),
        level="INFO",
        colorize=True,
    )


log = logger.bind(component="switchyard")
