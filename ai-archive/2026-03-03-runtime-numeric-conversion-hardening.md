# 2026-03-03 Runtime numeric conversion hardening

## Scope
Third refactor slice focused on integer conversion safety in runtime worker/database paths.

## Implemented

1. Added reusable checked/saturating conversion helpers for pipeline runtime
- File: `crates/nexus-jobs/src/pipeline/helpers.rs`
- Added helpers for `usize -> i64`, `usize -> u64`, `u64 -> i64`, `i64 -> u64`, and `u128 -> u64` conversions.
- Reused these helpers where queue sizes, chunk counts, and elapsed durations were previously cast with `as`.

2. Removed unchecked `as` conversions in pipeline flows
- `embedding_backfill`: chunk limits, processed counters, and metrics conversion now use helpers.
- `meili_bootstrap`: chunk limits, docs/processed counters, and cancellation/final metrics now use helpers.
- `lineage`: batch/checkpoint limits, chunk work-item count, and checkpoint elapsed-ms conversion now use helpers.
- `rebuild`: epoch window totals, batch limit, and chunk work-item count now use helpers.

3. Hardened worker engine conversion points
- `engine/mod.rs`: `max_inflight_jobs` conversion for `claim_limit` now uses checked conversion.
- `engine/helpers.rs`: retry `backoff_ms (u64)` to chrono milliseconds (`i64`) now uses checked conversion to avoid wrap on extreme values.

4. Hardened database conversion points
- `nexus-db/src/jobs.rs`:
  - `compute_backoff` now performs checked conversions for `attempt -> u32` and bounded milliseconds `u64 -> i64`.
- `nexus-db/src/ingest/helpers.rs`:
  - Added checked `usize -> i64` helper for SQL bind values.
  - Replaced unchecked binds in `reserve_ids` and `run_text_integrity_probe`.

5. Small API-state capacity safety hardening
- `nexus-api/src/state.rs`: query embedding cache `max_capacity` now uses checked `usize -> u64` conversion.

## Validation
Executed and passed:
- `cargo fmt --all`
- `cargo check --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-targets --all-features --locked`
