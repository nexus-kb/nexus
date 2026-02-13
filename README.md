# Nexus API Server (Phase 0)

Hard-reset backend focused on job orchestration + ingestion tickets 1-5.

## Crates

- `crates/nexus-api`: Axum API exposing `/api/v1/*` and `/admin/v1/*`.
- `crates/nexus-db`: Postgres schema + stores for jobs/catalog/ingest.
- `crates/nexus-jobs`: worker runtime, repo scanner, RFC822/MIME parser, ingestion pipeline.
- `crates/nexus-cli`: minimal CLI helper (`seed-pilot`).
- `crates/nexus-core`: shared config.

## Required environment

- `NEXUS__DATABASE__URL`

Optional defaults:

- `NEXUS__APP__HOST=0.0.0.0`
- `NEXUS__APP__PORT=3000`
- `NEXUS__ADMIN__TOKEN=nexus-dev-admin`
- `NEXUS__MAIL__MIRROR_ROOT=/opt/nexus/mailing-lists`
- `NEXUS__MAIL__COMMIT_BATCH_SIZE=250`

## Run API

```bash
cargo run -p nexus-api
```

## Run worker

```bash
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
- `GET /api/v1/lists/{list_key}/threads?sort=&limit=&cursor=&from=&to=&author=&has_diff=`
- `GET /api/v1/lists/{list_key}/threads/{thread_id}`
- `GET /api/v1/lists/{list_key}/threads/{thread_id}/messages?view=full|snippets`
- `GET /api/v1/messages/{message_id}`
- `GET /api/v1/messages/{message_id}/body?include_diff=true|false&strip_quotes=true|false`
- `GET /api/v1/messages/{message_id}/raw`
- `GET /api/v1/r/{msgid}`
- `GET /api/v1/series?list_key=&limit=&cursor=`
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
