# Nexus API Server (Phase 0)

Hard-reset backend focused on job orchestration + ingestion tickets 1-5.

## Crates

- `crates/nexus-api`: Axum API exposing `/api/v1/*` and `/admin/v1/*`.
- `crates/nexus-db`: Postgres schema + stores for jobs/catalog/ingest.
- `crates/nexus-jobs`: worker runtime, repo scanner, RFC822/MIME parser, ingestion pipeline.
- `crates/nexus-cli`: minimal CLI helper (`seed-pilot`).
- `crates/nexus-core`: shared config.

## Container-first dev workflow (recommended)

Use the root `compose.yml` as the canonical dev workflow for API + worker + dependencies.

From repo root:

```bash
podman compose -f compose.yml up -d --build api worker postgres meilisearch
```

or:

```bash
docker compose -f compose.yml up -d --build api worker postgres meilisearch
```

Notes:

- API and worker use `nexus-api-server/Dockerfile.dev`.
- Source is bind-mounted from host (`./nexus-api-server`), and both services run under `cargo watch`.
- Rust edits on host trigger rebuild/restart in the corresponding container.
- Dev defaults are injected by compose (no shell sourcing required).

## Required environment

- `NEXUS__DATABASE__URL`

Optional defaults:

- `NEXUS__APP__HOST=0.0.0.0`
- `NEXUS__APP__PORT=3000`
- `NEXUS__ADMIN__TOKEN=nexus-dev-admin`
- `NEXUS__MAIL__MIRROR_ROOT=/opt/nexus/mailing-lists`
- `NEXUS__MAIL__COMMIT_BATCH_SIZE=250`
- `NEXUS__WORKER__BACKFILL_MODE=full_pipeline|ingest_only`
- `NEXUS__WORKER__INGEST_PARSE_CONCURRENCY=8`
- `NEXUS__WORKER__MAX_INFLIGHT_JOBS=1`
- `NEXUS__WORKER__MAX_INFLIGHT_INGEST_JOBS=1`
- `NEXUS__WORKER__BACKFILL_BATCH_SIZE=10000`
- `NEXUS__WORKER__INGEST_WRITE_MODE=copy|batched_sql`
- `NEXUS__WORKER__DB_RELAXED_DURABILITY=false`
- `NEXUS_CORS_ALLOWED_ORIGINS` (optional, comma-separated, defaults to `http://127.0.0.1:3001,http://localhost:3001,http://host.containers.internal:3001,http://host.docker.internal:3001`)

## Run API

```bash
cargo run -p nexus-api
```

Cross-origin requests from the web app are supported with local dev defaults for
`http://127.0.0.1:3001`, `http://localhost:3001`, `http://host.containers.internal:3001`, and
`http://host.docker.internal:3001` so a Next frontend on port 3001 can call live API routes.

Override the allowed origins as needed:

```bash
NEXUS_CORS_ALLOWED_ORIGINS=http://localhost:3001,http://127.0.0.1:3001,http://host.containers.internal:3001,https://nexus.local cargo run -p nexus-api
```

## Run worker

```bash
cargo run -p nexus-jobs --bin worker
```

## Docker (standalone, not compose)

Build:

```bash
docker build -t nexus-api-server:debug -f Dockerfile .
```

Build dev image (compose-compatible tooling image):

```bash
docker build -t nexus-api-server:dev -f Dockerfile.dev .
```

Run API long-lived:

```bash
docker run --name nexus-api --rm -p 3000:3000 \
  -e NEXUS__DATABASE__URL=postgresql://nexus:nexus@host.docker.internal:5432/nexus_kb \
  -e NEXUS__MAIL__MIRROR_ROOT=/opt/nexus/mailing-lists \
  nexus-api-server:debug
```

For Podman, replace `host.docker.internal` with `host.containers.internal`.

Run worker using the same image:

```bash
docker run --name nexus-worker --rm \
  -e NEXUS__DATABASE__URL=postgresql://nexus:nexus@host.docker.internal:5432/nexus_kb \
  -e NEXUS__MAIL__MIRROR_ROOT=/opt/nexus/mailing-lists \
  --entrypoint /srv/nexus-api-server/target/release/worker \
  nexus-api-server:debug
```

Backfill-oriented worker profile example:

```bash
NEXUS__WORKER__BACKFILL_MODE=ingest_only \
NEXUS__WORKER__INGEST_PARSE_CONCURRENCY=8 \
NEXUS__WORKER__MAX_INFLIGHT_JOBS=4 \
NEXUS__WORKER__MAX_INFLIGHT_INGEST_JOBS=2 \
NEXUS__WORKER__BACKFILL_BATCH_SIZE=10000 \
NEXUS__WORKER__INGEST_WRITE_MODE=copy \
NEXUS__WORKER__DB_RELAXED_DURABILITY=true \
cargo run -p nexus-jobs --bin worker
```

## CLI

```bash
cargo run -p nexus-cli -- seed-pilot
```

## Public endpoints

- `GET /api/v1/healthz`
- `GET /api/v1/readyz`
- `GET /api/v1/version`
- `GET /api/v1/openapi.json`
- `GET /api/v1/lists?page=&page_size=`
- `GET /api/v1/lists/{list_key}`
- `GET /api/v1/lists/{list_key}/stats?window=30d`
- `GET /api/v1/lists/{list_key}/threads?sort=&page=&page_size=&from=&to=&author=&has_diff=`
- `GET /api/v1/lists/{list_key}/threads/{thread_id}`
- `GET /api/v1/lists/{list_key}/threads/{thread_id}/messages?view=full|snippets&page=&page_size=`
- `GET /api/v1/messages/{message_id}`
- `GET /api/v1/messages/{message_id}/body?include_diff=true|false&strip_quotes=true|false`
- `GET /api/v1/messages/{message_id}/raw`
- `GET /api/v1/r/{msgid}`
- `GET /api/v1/series?list_key=&sort=last_seen_desc&page=&page_size=`
- `GET /api/v1/series/{series_id}`
- `GET /api/v1/series/{series_id}/versions/{series_version_id}?assembled=true|false`
- `GET /api/v1/series/{series_id}/compare?v1=&v2=&mode=summary|per_patch|per_file`
- `GET /api/v1/patch-items/{patch_item_id}`
- `GET /api/v1/patch-items/{patch_item_id}/files`
- `GET /api/v1/patch-items/{patch_item_id}/files/{path}/diff`
- `GET /api/v1/patch-items/{patch_item_id}/diff`
- `GET /api/v1/series/{series_id}/versions/{series_version_id}/export/mbox?assembled=true|false&include_cover=true|false`

## Admin endpoints (token required)

- `POST /admin/v1/jobs/enqueue`
- `GET /admin/v1/jobs`
- `GET /admin/v1/jobs/{job_id}`
- `POST /admin/v1/jobs/{job_id}/cancel`
- `POST /admin/v1/jobs/{job_id}/retry`
- `POST /admin/v1/ingest/sync?list_key=<key>`
- `POST /admin/v1/ingest/reset-watermark?list_key=<key>&repo_key=<key>`
- `POST /admin/v1/threading/rebuild?list_key=<key>&from=<iso8601>&to=<iso8601>`
- `POST /admin/v1/lineage/rebuild?list_key=<key>&from=<iso8601>&to=<iso8601>`
