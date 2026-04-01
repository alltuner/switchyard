# ABOUTME: Multi-stage Dockerfile for switchyard.
# ABOUTME: Builds with uv, runs with granian on a minimal Python image.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen --no-install-project
COPY src/ src/
RUN uv sync --no-dev --frozen --no-editable

FROM python:3.14-slim-bookworm

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV SWITCHYARD_DATA_DIR=/data

EXPOSE 5050
VOLUME /data

CMD ["switchyard"]
