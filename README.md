# Nexus API Server (Axum + Postgres)

Workspace layout:
- `crates/nexus-api`: Axum HTTP server (`/hello`, `/health`).
- `crates/nexus-db`: Postgres access layer, migrations, job repository.
- `crates/nexus-jobs`: Job runner library and `worker` binary.
- `crates/nexus-core`: Shared config types and error helpers.

## Configuration
Environment variables (prefix `NEXUS__`):
- `NEXUS__DATABASE__URL` (required): Postgres connection string.
- `NEXUS__DATABASE__MAX_CONNECTIONS` (default 10)
- `NEXUS__APP__HOST` (default `0.0.0.0`)
- `NEXUS__APP__PORT` (default `3000`)

Config is loaded the same way by both API and worker binaries.

## Running
```bash
# API server
cargo run -p nexus-api

# Worker (job runner)
cargo run -p nexus-jobs --bin worker
```

## Job queue design
- Jobs stored in `jobs` table (see migration `crates/nexus-db/migrations/20250101_create_jobs.sql`).
- Enqueue inserts row and `NOTIFY nexus_jobs` to wake workers.
- Workers use `LISTEN/NOTIFY` plus interval polling; reservations use
  `SELECT ... FOR UPDATE SKIP LOCKED` to avoid double processing.
- Attempts/backoff handled in `JobStore::mark_failed`.

## Development notes
- Dependencies pinned to latest stable (Axum 0.8.x, sqlx 0.8.x, tokio 1.48).
- `cargo fmt && cargo check` is clean.
