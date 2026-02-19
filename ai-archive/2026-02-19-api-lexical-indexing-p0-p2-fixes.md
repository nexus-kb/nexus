# 2026-02-19 API lexical indexing P0-P2 fixes

## Scope

Implemented P0-P2 fixes for slow lexical-indexing impacted-thread queries on large mailing lists.

## Implemented changes

1. P0 query-shape/correctness hotfix
   - Updated impacted-thread SQL to join on `(mailing_list_id, message_pk)` and filter `tm.mailing_list_id = $1`.
   - Reworked selection to `DISTINCT ON (tm.thread_id)` with ordered `thread_id` semantics.
   - File:
     - `crates/nexus-db/src/pipeline.rs`

2. P1 impacted-ID materialization once per stage
   - Added full impacted-ID loaders:
     - `query_impacted_thread_ids`
     - `query_impacted_series_ids`
     - `query_impacted_patch_item_ids`
   - Lexical stage now fetches impacted IDs once per family, then chunks in Rust for Meili upserts.
   - Embedding stage now fetches impacted IDs once per scope, then chunks in Rust for batch enqueue.
   - Files:
     - `crates/nexus-db/src/pipeline.rs`
     - `crates/nexus-jobs/src/pipeline.rs`

3. P1 operational DB tuning
   - Added pool `after_connect` session setup: `SET jit = off`.
   - File:
     - `crates/nexus-db/src/db.rs`

4. P2 index hygiene
   - Added migration `0013_partition_index_hygiene.sql` to:
     - drop redundant per-partition threading indexes from legacy partition ensure functions,
     - drop duplicate global indexes covered by PK/UNIQUE indexes,
     - replace `ensure_threads_partition`, `ensure_thread_nodes_partition`, and `ensure_thread_messages_partition` so future partitions do not recreate duplicate indexes.
   - File:
     - `crates/nexus-db/migrations/0013_partition_index_hygiene.sql`

## Validation

- `cargo fmt`
- `cargo check`
- `cargo check -p nexus-db -p nexus-jobs`
- `cargo test -p nexus-db --lib`
- `cargo test -p nexus-jobs --lib`

All succeeded.
