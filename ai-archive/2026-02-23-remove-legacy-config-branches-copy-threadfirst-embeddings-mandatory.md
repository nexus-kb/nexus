# 2026-02-23 Remove Legacy Config Branches (Copy-only, Thread-first, Embeddings mandatory)

## Summary
Implemented the config and runtime simplification pass to remove legacy conditional paths that are no longer needed in the current compose/default deployment model.

Implemented outcomes:
- COPY-only ingest writes (removed batched SQL ingest path).
- Thread-first-only lineage discovery (removed message-legacy execution path).
- Removed relaxed durability toggle (no `synchronous_commit=off` branch).
- Removed all embeddings-disabled branches (embeddings are mandatory).
- Removed unused worker parser sizing knobs (`ingest_parse_cpu_ratio`, `ingest_parse_workers_min`, `ingest_parse_workers_max`).

## Code Changes

### Config surface cleanup (`crates/nexus-core`)
- Removed `EmbeddingsConfig.enabled`.
- Removed `IngestWriteMode` enum and `WorkerConfig.ingest_write_mode`.
- Removed `LineageDiscoveryMode` enum and `WorkerConfig.lineage_discovery_mode`.
- Removed `WorkerConfig.db_relaxed_durability`.
- Removed `WorkerConfig.ingest_parse_cpu_ratio`.
- Removed `WorkerConfig.ingest_parse_workers_min`.
- Removed `WorkerConfig.ingest_parse_workers_max`.
- Updated settings validation: embeddings config is always required (`api_key`, `base_url`, `model` non-empty).

### Embeddings always-on runtime
- `OpenAiEmbeddingsClient::from_settings` now always returns a client (no `Option`).
- Updated API state and jobs handler fields to store non-optional embedding clients.
- Removed API/job branches that returned disabled-feature skips/errors based on `embeddings.enabled`.

### Ingest path simplification
- Deleted `crates/nexus-db/src/ingest/batch_sql.rs`.
- Removed module wiring for `batch_sql`.
- `write_ingest_rows` now always calls `ingest_messages_copy_batch`.
- Removed `relaxed_durability` parameter plumbing and removed `SET LOCAL synchronous_commit = off` branch from COPY ingest.

### Lineage path simplification
- Simplified pipeline lineage and lineage rebuild handlers to thread-first logic only.
- Removed message-legacy branch-specific DB queries and lineage extraction entrypoints:
  - removed `process_patch_extract_window` exports/implementation.
  - removed `LineageStore::load_messages_for_anchors`.
  - removed `PipelineStore::query_ingest_window_message_pks`.
  - removed `ThreadingStore::list_message_pks_for_rebuild`.

## Docs and Environment Updates
- Updated `compose.yml`:
  - removed `NEXUS__EMBEDDINGS__ENABLED`.
  - removed worker `NEXUS__WORKER__INGEST_WRITE_MODE` override.
- Updated `nexus-api-server/README.md` env var tables to remove deleted variables and describe embeddings as mandatory.
- Updated architecture/state docs in `ai-docs/` to remove references to:
  - `batched_sql` ingest mode,
  - `message_legacy` lineage mode,
  - `embeddings.enabled=false` guard behavior.

## Validation
- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-targets`

All validation commands passed.
