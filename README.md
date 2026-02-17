# Nexus KB API Server

Nexus KB is a read-focused backend for mailing-list archives and patch-series workflows. This image serves the public/admin API and runs background pipeline work against Postgres and Meilisearch. The API stays read-oriented while ingest, threading, lineage, and search indexing run in worker jobs.

## Crate Layout

- `crates/nexus-api`: HTTP API (`/api/v1/*`, `/admin/v1/*`).
- `crates/nexus-jobs`: Worker runtime and pipeline stages.
- `crates/nexus-db`: Postgres models, queries, migrations.
- `crates/nexus-core`: Shared config/types.
- `crates/nexus-cli`: Operator/developer helpers.

## Deployment (Two Containers, One Image)

Use one packaged image for both processes:

- `ghcr.io/<owner>/nexus-api-server:latest`

Run **both** containers in production: one API container and one worker container.

```bash
IMAGE=ghcr.io/<owner>/nexus-api-server:latest

docker network create nexus-kb || true

# API container (serves HTTP)
docker run -d --name nexus-api \
  --network nexus-kb \
  -p 3000:3000 \
  --env-file ./nexus-api.env \
  "$IMAGE"

# Worker container (runs jobs; required for ingest/index freshness)
docker run -d --name nexus-worker \
  --network nexus-kb \
  --env-file ./nexus-api.env \
  -v /srv/nexus/mailing-lists:/opt/nexus/mailing-lists:ro \
  "$IMAGE" /usr/local/bin/worker
```

`nexus-api.env` should contain only variables in the matrix below.

## Environment Variables (Runtime In Use)

This list includes only variables currently read by runtime code (`nexus-core` config + API CORS env).

### Core and API

| Variable | Required | Default if unset | Production guidance |
| --- | --- | --- | --- |
| `NEXUS__DATABASE__URL` | yes | none | Set to your Postgres DSN. |
| `NEXUS__DATABASE__MAX_CONNECTIONS` | no | `20` | Increase based on DB capacity. |
| `NEXUS__APP__HOST` | no | `0.0.0.0` | Keep default in containers. |
| `NEXUS__APP__PORT` | no | `3000` | Keep `3000` unless remapping internally. |
| `NEXUS__APP__BUILD_SHA` | no | `dev` | Set from CI/release metadata. |
| `NEXUS__APP__BUILD_TIME` | no | startup UTC timestamp | Set from CI for stable build metadata. |
| `NEXUS__APP__SCHEMA_VERSION` | no | `phase0` | Set from release metadata if needed. |
| `NEXUS__ADMIN__TOKEN` | no | `nexus-dev-admin` | Set a long random secret in production. |
| `NEXUS_CORS_ALLOWED_ORIGINS` | no | `*` | Prefer explicit origins in production (for example `https://app.example.com`). |

### Mail Mirror and Meilisearch

| Variable | Required | Default if unset | Production guidance |
| --- | --- | --- | --- |
| `NEXUS__MAIL__MIRROR_ROOT` | required for ingest/list-sync jobs | `/opt/nexus/mailing-lists` | Mount mirror path read-only in worker. |
| `NEXUS__MAIL__COMMIT_BATCH_SIZE` | no | `250` | Tune by ingest throughput and memory budget. |
| `NEXUS__MEILI__URL` | no | `http://127.0.0.1:7700` | Point to your Meilisearch service URL. |
| `NEXUS__MEILI__MASTER_KEY` | no | `nexus-dev-key` | Set to your real Meili master key. |
| `NEXUS__MEILI__UPSERT_BATCH_SIZE` | no | `100` | Tune for index/write latency tradeoff. |

### Embeddings (Optional Feature)

| Variable | Required | Default if unset | Production guidance |
| --- | --- | --- | --- |
| `NEXUS__EMBEDDINGS__ENABLED` | no | `false` | Set `true` only when embedding endpoint is configured. |
| `NEXUS__EMBEDDINGS__BASE_URL` | required if embeddings enabled | `https://openrouter.ai/api/v1` | Use your embedding provider endpoint. |
| `NEXUS__EMBEDDINGS__API_KEY` | required if embeddings enabled | empty | Set provider API key via secret manager. |
| `NEXUS__EMBEDDINGS__MODEL` | required if embeddings enabled | `qwen/qwen3-embedding-4b` | Pin to deployed embedding model. |
| `NEXUS__EMBEDDINGS__DIMENSIONS` | no | `768` | Must match model output dimensions. |
| `NEXUS__EMBEDDINGS__EMBEDDER_NAME` | no | `qwen3` | Keep stable for index naming/versioning. |
| `NEXUS__EMBEDDINGS__QUERY_CACHE_TTL_SECS` | no | `120` | Increase if query patterns are repetitive. |
| `NEXUS__EMBEDDINGS__QUERY_CACHE_MAX_ENTRIES` | no | `10000` | Size to memory budget. |
| `NEXUS__EMBEDDINGS__BATCH_SIZE` | no | `32` | Tune to provider/API limits. |
| `NEXUS__EMBEDDINGS__OPENROUTER_REFERER` | no | unset | Optional header when using OpenRouter. |
| `NEXUS__EMBEDDINGS__OPENROUTER_TITLE` | no | unset | Optional header when using OpenRouter. |

### Worker Runtime

| Variable | Required | Default if unset | Production guidance |
| --- | --- | --- | --- |
| `NEXUS__WORKER__POLL_MS` | no | `1500` | Queue poll interval. |
| `NEXUS__WORKER__CLAIM_BATCH` | no | `16` | Jobs claimed per poll. |
| `NEXUS__WORKER__LEASE_MS` | no | `45000` | Running job lease duration. |
| `NEXUS__WORKER__HEARTBEAT_MS` | no | `15000` | Lease heartbeat cadence. |
| `NEXUS__WORKER__SWEEP_MS` | no | `10000` | Stale lease sweeper interval. |
| `NEXUS__WORKER__BASE_BACKOFF_MS` | no | `15000` | Retry backoff base. |
| `NEXUS__WORKER__MAX_BACKOFF_MS` | no | `3600000` | Retry backoff cap. |
| `NEXUS__WORKER__INGEST_PARSE_CONCURRENCY` | no | `8` | Parser parallelism target. |
| `NEXUS__WORKER__INGEST_PARSE_CPU_RATIO` | no | `0.6` | CPU fraction for parser worker sizing. |
| `NEXUS__WORKER__INGEST_PARSE_WORKERS_MIN` | no | `2` | Parser worker lower bound. |
| `NEXUS__WORKER__INGEST_PARSE_WORKERS_MAX` | no | `32` | Parser worker upper bound. |
| `NEXUS__WORKER__MAX_INFLIGHT_JOBS` | no | `1` | Global concurrent jobs in worker process. |
| `NEXUS__WORKER__INGEST_WRITE_MODE` | no | `copy` | `copy` (default) or `batched_sql`. |
| `NEXUS__WORKER__PROGRESS_CHECKPOINT_INTERVAL` | no | `10000` | Progress checkpoint cadence. |
| `NEXUS__WORKER__DB_RELAXED_DURABILITY` | no | `false` | Enable only with accepted durability tradeoff. |
