# 2026-03-03 — Schema hardening: thread scope + job attempt consistency

## Scope
Implemented the approved in-place schema refactor plan for `nexus-api-server`:
- list-scoped thread references for lineage series versions,
- DB-enforced job/attempt lifecycle invariants,
- partition helper/index hygiene cleanup,
- removal of unused high-write-cost indexes,
- worker/API code updates to align with the new schema guarantees.

Public endpoint shapes remain unchanged.

## Implemented changes

### 1) New migration `0026_schema_hardening_thread_scope_and_jobs.sql`
File:
- `crates/nexus-db/migrations/0026_schema_hardening_thread_scope_and_jobs.sql`

What changed:
- Added `patch_series_versions.thread_mailing_list_id`.
- Backfilled `thread_mailing_list_id` from `threads`.
- Nullified unresolved legacy refs where `thread_id` existed without a resolvable list scope.
- Added `patch_series_versions_thread_ref_pair_check` to enforce nullable-pair semantics.
- Added composite FK `patch_series_versions_thread_ref_fk` on `(thread_mailing_list_id, thread_id) -> threads(mailing_list_id, id)` with `ON DELETE SET NULL`.
- Added index `idx_patch_series_versions_thread_ref`.

Job/attempt hardening:
- Reconciled duplicate and stale `job_attempts.status='running'` rows.
- Added `job_attempts_status_check` and `job_attempts_status_finished_at_check`.
- Added partial unique index `idx_job_attempts_one_running_per_job`.
- Added trigger function `jobs_finalize_running_attempts_on_state_change` and trigger `jobs_finalize_running_attempts_trg` to auto-finalize lingering running attempts whenever `jobs.state` transitions to a terminal state.

Partition/index hygiene:
- Removed legacy per-partition `list_message_instances_<id>_(seen_at|message_pk)_idx` duplicates.
- Simplified `ensure_list_message_instances_partition()` to create only the partition table (parent partitioned indexes now provide coverage).
- Dropped currently-unused indexes:
  - `idx_messages_references_ids_gin`
  - `idx_messages_in_reply_to_ids_gin`
  - `idx_patch_series_author_subject`
  - `idx_patch_items_patch_id`
  - `idx_patch_item_files_path`
  - `idx_message_patch_facts_patch_id`

### 2) Lineage model/query changes for list-scoped thread refs
Files:
- `crates/nexus-db/src/lineage/mod.rs`
- `crates/nexus-db/src/lineage/read_series.rs`
- `crates/nexus-db/src/lineage/upsert_series_items.rs`
- `crates/nexus-jobs/src/lineage/core.rs`
- `crates/nexus-api/src/handlers/public/series.rs`

What changed:
- Added `thread_mailing_list_id` to lineage record/input structs.
- Included `thread_mailing_list_id` in series version read/upsert SQL.
- `series_detail` now resolves thread refs via `(thread_mailing_list_id, thread_id)` instead of `thread_id` alone.
- Lineage extraction now writes `thread_mailing_list_id` from pipeline list context.

### 3) Thread query scoping cleanup in embeddings/search paths
Files:
- `crates/nexus-db/src/embeddings.rs`
- `crates/nexus-db/src/search.rs`
- `crates/nexus-jobs/src/pipeline/embedding_generate.rs`

What changed:
- Thread participants/snippet/body CTEs now group/join by both `mailing_list_id` and `thread_id`.
- Added `lookup_thread_message_body_in_list(mailing_list_id, thread_id)`.
- Embedding failure diagnostics now use list-scoped lookup when payload includes `list_key`.

### 4) Atomic job + attempt finalization path in Rust
Files:
- `crates/nexus-db/src/jobs.rs`
- `crates/nexus-jobs/src/engine/helpers.rs`
- `crates/nexus-jobs/src/engine/mod.rs`

What changed:
- Added combined finalization methods on `JobStore`:
  - `finalize_succeeded_attempt`
  - `finalize_cancelled_attempt`
  - `finalize_terminal_attempt`
  - `finalize_retryable_attempt`
- Worker finalization now uses combined methods (single SQL statement per outcome via CTE).
- Pre-execution cancel path also uses combined finalization.

Why:
- Prevents drift where jobs reach terminal state but `job_attempts` remains `running`.
- Keeps job and attempt transitions consistent even under retries/cancellation races.

## Documentation updates
Files:
- `../ai-docs/nexus-kb-redesign-doc.md`
- `../ai-docs/nexus-kb-api-endpoint-map.md`
- `../ai-docs/nexus-kb-job-state-machine.md`

Updates:
- Documented `patch_series_versions` list-scoped thread reference columns/FK/index.
- Clarified `thread_id` is list-scoped in API conventions.
- Documented DB-enforced job attempt invariants and reconciliation trigger.

## Verification
From `nexus-api-server`:

1. `cargo check`
- Result: success

2. `cargo test -p nexus-db -p nexus-jobs -p nexus-api --lib`
- Result: success

Runtime/dev-stack verification:

3. `podman compose -f compose.yml logs --tail=200 api worker`
- Confirmed migration `26` executed successfully.

4. Post-migration DB checks (`nexus-kb-postgres`):
- `_sqlx_migrations` latest: `26 | schema hardening thread scope and jobs | t`
- stale running-attempt drift count: `0`
- invalid thread ref pair count: `0`
- duplicate running attempts per job: `0`
- new indexes present: `idx_patch_series_versions_thread_ref`, `idx_job_attempts_one_running_per_job`
- dropped indexes absent (all listed above)

5. API readiness:
- `GET /api/v1/healthz` -> `{ "ok": true }`
- `GET /api/v1/readyz` -> `{ "ok": true, "deps": { "postgres": "ok", "meili": "ok" } }`

## Notes
- The migration executed on the live dev Postgres container via startup migration flow and completed in ~5.4s.
- Public response payloads are unchanged; series thread refs are now resolved with explicit list scope internally.
