# 2026-03-03 — Async safety, payload hardening, and cache concurrency improvements

## Scope
Implemented core parts of the planned Rust refactor focused on:
- async/runtime correctness in worker ingest paths,
- eliminating unsafe payload serialization fallbacks,
- reducing orchestration duplication between worker and pipeline flow,
- improving API query embedding cache concurrency behavior.

Public HTTP API and documented admin endpoint shapes were not changed.
Pipeline stage order/state semantics were not changed.

## Implemented changes

### 1) Scanner moved to async process I/O (no blocking calls on async path)
Files:
- `crates/nexus-jobs/src/scanner.rs`
- `crates/nexus-jobs/src/pipeline/ingest.rs`

Changes:
- Replaced `std::process::Command` + blocking `BufRead::read_line` with `tokio::process::Command` + async `AsyncBufReadExt::read_line`.
- `CommitOidChunkStream::next_chunk` is now async (`next_chunk().await`).
- `stream_new_commit_oid_chunks` is now async and awaited in ingest stage.
- `commit_exists` now runs asynchronously.
- Added `kill_on_drop(true)` and non-blocking `start_kill()` in drop path to avoid blocking worker tasks.

Why:
- Removes event-loop blocking while scanning git commit OIDs during ingest.
- Improves responsiveness/cancellation behavior under large repo scans.

### 2) Removed silent `{}` payload fallback in job enqueue paths
Files:
- `crates/nexus-jobs/src/pipeline/pipeline_flow.rs`
- `crates/nexus-jobs/src/engine/mod.rs`
- `crates/nexus-jobs/src/pipeline/embedding_generate.rs`

Changes:
- Replaced all `serde_json::to_value(...).unwrap_or_else(|_| json!({}))` enqueue payload writes.
- Enqueue payloads are now built directly with explicit `serde_json::json!({...})` objects.

Why:
- Prevents silently enqueuing malformed/empty payload JSON.
- Makes payload contents explicit and predictable at enqueue sites.

### 3) Reduced orchestration duplication (worker delegates to pipeline flow helpers)
Files:
- `crates/nexus-jobs/src/engine/mod.rs`
- `crates/nexus-jobs/src/pipeline/pipeline_flow.rs`

Changes:
- Exposed pipeline activation helpers as `pub(crate)` on `Phase0JobHandler`.
- Worker maintenance and failure/cancel continuation paths now delegate to handler activation helpers.
- Removed duplicated activation/enqueue implementations from worker engine.

Why:
- Single implementation path for "activate next run and enqueue ingest" behavior.
- Lowers drift risk between maintenance/failure and stage chaining logic.

### 4) Added cooperative cancellation checks in long-running Meili/embedding loops
Files:
- `crates/nexus-jobs/src/pipeline/embedding_generate.rs`
- `crates/nexus-jobs/src/pipeline/lexical.rs`

Changes:
- `wait_for_task` now checks `ctx.is_cancel_requested()` each poll iteration.
- `embed_text_batch_with_retry` now checks cancellation between retries.
- `handle_embedding_generate_batch` and `handle_pipeline_lexical` now short-circuit to `Cancelled` outcomes when cancellation is detected during these loops.

Why:
- Improves cancellation latency and prevents long waits during Meili task polling or embedding retry backoff.

### 5) Query embedding cache moved from mutexed hashmap to concurrent TTL cache
Files:
- `crates/nexus-api/Cargo.toml`
- `crates/nexus-api/src/state.rs`
- `Cargo.lock`

Changes:
- Added `moka` (`future` feature) to `nexus-api` crate.
- Replaced `Arc<Mutex<HashMap<...>>>` + O(n) retention/eviction logic with `moka::future::Cache<String, Vec<f32>>`.
- Preserved existing cache API (`get`/`insert`) and config-driven TTL/max entries.

Why:
- Removes lock contention and full-map cleanup work from hot query embedding reads.
- Maintains bounded size + TTL semantics with production-grade concurrent cache behavior.

## Verification
Executed from `nexus-api-server`:

1. `cargo check --workspace --all-targets --all-features`
- Result: success

2. `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- Result: success

3. `cargo test --workspace --all-targets --all-features --locked`
- Result: success
- Key outcomes: all tests in `nexus-api`, `nexus-core`, `nexus-db`, `nexus-jobs` passed.

## Notes
- No public API contract updates were required.
- No documented job state machine transitions were changed.
- Additional broad pedantic cleanup (casts/import cleanup/large-function decomposition across all handlers) remains available as a follow-up pass.
