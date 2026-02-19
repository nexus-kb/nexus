# 2026-02-19 Disable patch-item Meili search indexing

## Scope

Disabled `patch_item` indexing/query usage in Meilisearch while keeping patch-item parsing and persistence in Postgres unchanged.

## Implemented changes

1. Lexical pipeline indexing scope
   - Removed `MeiliIndexKind::PatchItemDocs` from `pipeline_lexical` indexing loop.
   - `pipeline_lexical` now upserts only:
     - `thread_docs`
     - `patch_series_docs`
   - File:
     - `crates/nexus-jobs/src/pipeline.rs`

2. Public search API scope behavior
   - Search now rejects `scope=patch_item` with HTTP `422` and error:
     - `{"error":"patch_item search is temporarily disabled"}`
   - This keeps the temporary disable explicit for API callers while preserving future re-enable flexibility.
   - File:
     - `crates/nexus-api/src/handlers/public.rs`

3. Public list scope hints + OpenAPI summary
   - Removed `patch_item` from list detail `facets_hint.available_scopes`.
   - Updated OpenAPI search summary text from `threads/series/patches` to `threads/series`.
   - File:
     - `crates/nexus-api/src/handlers/public.rs`

4. Documentation updates (source-of-truth docs)
   - Updated API endpoint map search section to `scope=thread|series` and documented `patch_item` scope disable response.
   - Updated job state machine lexical stage to reflect thread/series-only Meili indexing.
   - Files:
     - `../ai-docs/nexus-kb-api-endpoint-map.md`
     - `../ai-docs/nexus-kb-job-state-machine.md`

## Validation plan executed

- `cargo fmt`
- `cargo check`
- `cargo test -p nexus-api`
- `cargo test -p nexus-jobs --lib`
