# 2026-02-23 API Server Full Reorganization and Cleanup

## Summary
Executed a full structural cleanup and dead-path removal pass across `nexus-api`, `nexus-jobs`, and `nexus-db` with behavior-preserving intent:

- Replaced large monolithic modules with domain/stage-oriented module trees.
- Removed internal dead alias path `pipeline_search` from job dispatch.
- Removed unused `MeiliClientError::is_transient` method.
- Preserved public/admin API route semantics and job/state-machine behavior.

No public API shape changes were introduced, so `ai-docs/` contract docs were not modified.

## Major Refactors

### `crates/nexus-api`
- Split `handlers/public.rs` into:
  - `handlers/public/mod.rs`
  - `handlers/public/types.rs`
  - `handlers/public/lists.rs`
  - `handlers/public/messages.rs`
  - `handlers/public/patch_items.rs`
  - `handlers/public/threads.rs`
  - `handlers/public/series.rs`
  - `handlers/public/search.rs`
  - `handlers/public/openapi.rs`
  - `handlers/public/helpers_and_tests.rs`
- Split `handlers/admin.rs` into:
  - `handlers/admin/mod.rs`
  - `handlers/admin/jobs.rs`
  - `handlers/admin/diagnostics.rs`
  - `handlers/admin/meili_bootstrap.rs`
  - `handlers/admin/ingest.rs`
  - `handlers/admin/rebuild.rs`
  - `handlers/admin/pipeline.rs`
  - `handlers/admin/embeddings.rs`
  - `handlers/admin/helpers_and_tests.rs`
- Maintained existing router binding names (`public::*`, `admin::*`).

### `crates/nexus-jobs`
- Split `pipeline.rs` into staged modules:
  - `pipeline/mod.rs`
  - `pipeline/ingest.rs`
  - `pipeline/threading.rs`
  - `pipeline/lineage.rs`
  - `pipeline/lexical.rs`
  - `pipeline/pipeline_flow.rs`
  - `pipeline/rebuild.rs`
  - `pipeline/meili_bootstrap.rs`
  - `pipeline/embedding_backfill.rs`
  - `pipeline/embedding_generate.rs`
  - `pipeline/helpers.rs`
- Split `engine.rs` into:
  - `engine/mod.rs`
  - `engine/helpers.rs`
  - `engine/tests.rs`
- Split `lineage.rs` into:
  - `lineage/mod.rs`
  - `lineage/core.rs`
  - `lineage/similarity.rs`
  - `lineage/tests.rs`
- Split `mail.rs` into:
  - `mail/mod.rs`
  - `mail/helpers.rs`
  - `mail/tests.rs`

### `crates/nexus-db`
- Split `lineage.rs` into:
  - `lineage/mod.rs`
  - `lineage/source.rs`
  - `lineage/read_messages.rs`
  - `lineage/read_series.rs`
  - `lineage/upsert_series_items.rs`
  - `lineage/upsert_files.rs`
  - `lineage/logical.rs`
- Split `ingest.rs` into:
  - `ingest/mod.rs`
  - `ingest/message.rs`
  - `ingest/batch_sql.rs`
  - `ingest/batch_copy.rs`
  - `ingest/helpers.rs`
  - `ingest/tests.rs`

## Dead/Unused Cleanup
- Removed `pipeline_search` dispatch alias in `Phase0JobHandler::handle`.
- Removed unused method `MeiliClientError::is_transient`.
- Rebalanced admin module boundaries to eliminate dead-code warnings from accidental overlap.

## Post-Refactor Hygiene
- Resolved all strict lint findings under `-D warnings` across workspace targets:
  - Introduced `BackfillProgressUpdate` to replace a too-many-arguments DB progress updater.
  - Flattened split test modules (`engine/tests.rs`, `mail/tests.rs`, `lineage/tests.rs`, `ingest/tests.rs`) to avoid module-inception warnings.
  - Replaced modulo checks with `is_multiple_of`, removed clone-on-copy usages, and collapsed nested-if flow where applicable.
  - Introduced `SearchRequestHashInput` in public handlers to remove a high-arity helper and keep cursor hash behavior deterministic.

## Verification
- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-targets`

All commands succeeded.

## Size/Complexity Target Verification
Validated file sizes across `crates/nexus-api/src`, `crates/nexus-jobs/src`, and `crates/nexus-db/src`:
- No Rust source file exceeds 800 LOC after refactor.
