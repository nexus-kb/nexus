# Nexus API Server (Axum + Postgres)

Workspace layout:
- `crates/nexus-api`: Axum HTTP server (jobs + mailing list APIs + webhook).
- `crates/nexus-db`: Postgres access layer, migrations, job repository, bulk ingest helpers.
- `crates/nexus-jobs`: Job runner library and `worker` binary (mail sync + threading).
- `crates/nexus-cli`: Management CLI (import mailing lists, reset DB).
- `crates/nexus-core`: Shared config types and error helpers.

## Configuration
Environment variables (prefix `NEXUS__`):
- `NEXUS__DATABASE__URL` (required): Postgres connection string.
- `NEXUS__DATABASE__MAX_CONNECTIONS` (default 10)
- `NEXUS__APP__HOST` (default `0.0.0.0`)
- `NEXUS__APP__PORT` (default `3000`)
- `NEXUS__MAIL__MIRROR_ROOT` (default `/Users/tansanrao/work/nexus/mirrors`)
- `NEXUS__MAIL__WEBHOOK_SECRET` (optional; if set, webhook requires `x-nexus-webhook-secret`)

Config is loaded the same way by API, worker, and CLI.

## Running
```bash
# API server
cargo run -p nexus-api

# Worker (job runner)
cargo run -p nexus-jobs --bin worker
```

## CLI
```bash
# Import/update mailing list metadata from the public-inbox manifest
NEXUS__DATABASE__URL=postgres://... \
cargo run -p nexus-cli -- import-mailing-lists

# Reset DB (drops all data, re-runs migrations after confirmation)
NEXUS__DATABASE__URL=postgres://... \
cargo run -p nexus-cli -- reset-db
```

## API
### Health
- `GET /hello`
- `GET /health`

### Jobs
- `POST /jobs`
- `GET /jobs`
- `GET /jobs/{id}`
- `GET /jobs/stats`

### Mailing lists
- `GET /mailing-lists`
- `GET /mailing-lists/enabled`
- `PATCH /mailing-lists/{slug}` (body: `{"enabled": true|false}`)

### Webhooks
- `POST /webhooks/grokmirror` (triggers sync job for each enabled mailing list)

## Job queue design
- Jobs stored in `jobs` table (see migration `crates/nexus-db/migrations/00001_create_jobs.sql`).
- Enqueue inserts row and `NOTIFY nexus_jobs` to wake workers.
- Workers use `LISTEN/NOTIFY` plus interval polling; reservations use
  `SELECT ... FOR UPDATE SKIP LOCKED` to avoid double processing.
- Attempts/backoff handled in `JobStore::mark_failed`.

## Mail ingestion pipeline
- `nexus-cli import-mailing-lists` loads public-inbox manifest into `mailing_lists` + `mailing_list_epochs` (lists start disabled).
- Grokmirror webhook enqueues `sync_mailing_list` jobs for enabled lists.
- Worker parses new mail from mirror repos using gitoxide (no git subprocess), batches 10k emails, and bulk ingests via UNLOGGED staging tables + set-based upserts.
- After sync, worker enqueues `thread_mailing_list` and rebuilds threads using JWZ-style References + subject fallback.

## Development notes
- Dependencies pinned to latest stable (Axum 0.8.x, sqlx 0.8.x, tokio 1.48).
- `cargo fmt && cargo check` is clean.
