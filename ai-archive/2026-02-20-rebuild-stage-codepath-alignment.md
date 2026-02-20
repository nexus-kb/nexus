# 2026-02-20 — Rebuild Stage Codepath Alignment

## Context

Operator request: ensure manual rebuild jobs (`threading_rebuild_list`, `lineage_rebuild_list`) execute the same threading/lineage logic as pipeline stages.

## Findings (before change)

- `threading_rebuild_list` reused `apply_threading_for_anchors` (same core JWZ build/apply), but selected work via message-PK pagination (`list_message_pks_for_rebuild`) instead of pipeline epoch-window repo-scoped traversal.
- `lineage_rebuild_list` always used message-level extraction (`process_patch_extract_window`) and ignored `worker.lineage_discovery_mode`, while `pipeline_lineage` defaults to `thread_first` and can rollback to `message_legacy`.

## Changes Implemented

### 1) Threading rebuild now follows pipeline epoch-window work selection

Files:
- `crates/nexus-jobs/src/pipeline.rs`
- `crates/nexus-db/src/threading.rs`

Details:
- Added rebuild-range DB helpers in `ThreadingStore`:
  - `list_repo_ids_for_rebuild(...)`
  - `list_message_pks_for_rebuild_repo_ids(...)`
- Refactored `handle_threading_rebuild_list` to:
  - load/order repos with `order_repos_for_ingest`
  - derive touched repos from optional `from_seen_at`/`to_seen_at`
  - compute `start_epoch = min_touched_epoch - 1`
  - build epoch windows with `build_threading_epoch_windows`
  - process each window by repo IDs and run `apply_threading_for_anchors`
- Preserved inline Meili thread-doc upsert behavior for impacted threads.

### 2) Lineage rebuild now follows pipeline lineage discovery mode

Files:
- `crates/nexus-jobs/src/pipeline.rs`
- `crates/nexus-db/src/threading.rs`

Details:
- Added thread-chunk rebuild helper in `ThreadingStore`:
  - `list_thread_ids_for_rebuild(...)`
- Refactored `handle_lineage_rebuild_list` to branch exactly by `worker.lineage_discovery_mode`:
  - `thread_first`: `list_thread_ids_for_rebuild` + `process_patch_extract_threads`
  - `message_legacy`: `list_message_pks_for_rebuild` + `process_patch_extract_window`
- Kept the same downstream extraction pipeline in both modes:
  - patch-id compute + diff parse.

### 3) Observability updates retained

- Progress logs for rebuild handlers remain in place and now include mode-specific counters (`discovery_mode`, `work_item_kind`, epoch-window counters).

## Documentation Updates

- `ai-docs/nexus-kb-job-state-machine.md`
  - updated admin job definitions to reflect stage parity with pipeline logic.
- `ai-docs/nexus-kb-api-endpoint-map.md`
  - added execution-parity notes under rebuild endpoints.

## Validation

Commands:
- `cargo fmt`
- `cargo test -p nexus-jobs --lib`

Result:
- tests passed (`59 passed, 0 failed`).
