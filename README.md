<h1 align="center">Switchyard</h1>

<p align="center">
  <strong>Local Docker registry with async upstream sync</strong><br>
  Push images at local disk speed, sync to your central registry in the background.
</p>

<p align="center">
  <a href="https://github.com/alltuner/switchyard">GitHub</a> &middot;
  <a href="https://github.com/sponsors/alltuner">Sponsor</a>
</p>

<p align="center">
  <img src="https://img.shields.io/github/license/alltuner/switchyard?color=5B2333" alt="License">
  <img src="https://img.shields.io/github/stars/alltuner/switchyard?color=5B2333" alt="Stars">
</p>

---

## How it works

```
docker push localhost:5050/myapp:latest
        │
        ▼
   ┌─────────┐       background sync     ┌─────────────────┐
   │Switchyard│  ─────────────────────►   │ Central Registry │
   │ (local)  │                           │ (self-hosted)    │
   │ :5050    │  ◄── reverse proxy ─────  │                  │
   └─────────┘    (pulls, cached)         └─────────────────┘
```

1. **Push** lands on your local machine instantly
2. **Background worker** syncs blobs and manifests to the central registry with retry and backoff
3. **Pull** checks local storage first, proxies to upstream on miss and caches the result
4. **Layer deduplication**: blobs that already exist upstream are never re-pushed

## Quick start

### Docker Compose (recommended)

Copy `compose.example.yaml` to `compose.yaml` and fill in your upstream registry URL:

```yaml
services:
  switchyard:
    image: ghcr.io/alltuner/switchyard:latest
    ports:
      - "5050:5050"
    volumes:
      - switchyard-data:/data
    environment:
      SWITCHYARD_UPSTREAM: "https://your-central-registry:5000"
    restart: unless-stopped

volumes:
  switchyard-data:
```

```bash
docker compose up -d
```

### From source

```bash
uv sync
uv run switchyard
```

### Docker daemon config

Add `localhost:5050` as an insecure registry in `~/.docker/daemon.json`:

```json
{
  "insecure-registries": ["localhost:5050"]
}
```

Restart Docker Desktop (or the daemon) after making this change.

### Push an image

```bash
docker tag alpine:latest localhost:5050/alpine:latest
docker push localhost:5050/alpine:latest
```

## Configuration

All configuration is via environment variables with the `SWITCHYARD_` prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `SWITCHYARD_DATA_DIR` | `./data` | Directory for blobs, manifests, and sync queue |
| `SWITCHYARD_UPSTREAM` | (empty) | Central registry URL. Empty = local-only mode |
| `SWITCHYARD_SYNC_INTERVAL` | `10` | Seconds between sync queue scans |
| `SWITCHYARD_MANIFEST_TTL` | `300` | Seconds before re-fetching a cached tag manifest from upstream |

## Sync queue

Pending syncs are stored as JSON files in `$SWITCHYARD_DATA_DIR/pending/`. You can inspect them with `ls`:

```bash
ls data/pending/
# myapp/latest.json
```

Failed syncs retry with exponential backoff (5s, 10s, 20s, ... capped at 5 minutes). Delete a marker file to cancel a pending sync.

## Development

```bash
uv sync --all-groups
uv run pytest -v
uv run ruff check src/ tests/
```

## Tech stack

- Python 3.14+, async throughout
- [Starlette](https://www.starlette.io/) for the HTTP layer
- [Granian](https://github.com/emmett-framework/granian) (Rust ASGI server) with uvloop
- [httpx](https://www.python-httpx.org/) for upstream communication (HTTP/2, brotli, zstd)
- File-based storage and sync queue (no database)

## Support the project

Switchyard is an open source project built by [David Poblador i Garcia](https://davidpoblador.com/) through [All Tuner Labs](https://www.alltuner.com/).

If this project is useful to you, consider supporting its development.

❤️ **Sponsor development**
https://github.com/sponsors/alltuner

☕ **One-time support**
https://buymeacoffee.com/alltuner

Your support helps fund the continued development of Switchyard and other open source developer tools such as [Factory Floor](https://github.com/alltuner/factoryfloor).

## License

[MIT](LICENSE)

---

<p align="center">
  Built by <a href="https://davidpoblador.com">David Poblador i Garcia</a> with the support of <a href="https://alltuner.com">All Tuner Labs</a>.<br>
  Made with ❤️ in Poblenou, Barcelona.
</p>
