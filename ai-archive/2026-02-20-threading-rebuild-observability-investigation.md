# 2026-02-20 Threading rebuild observability investigation

## Request investigated

Investigate why `POST /admin/v1/threading/rebuild?list_key=bpf` appears to create a job but shows no obvious worker activity in logs.

## Reproduction in dev

1. Triggered rebuild:
   - `POST /admin/v1/threading/rebuild?list_key=bpf`
2. Observed DB state transitions:
   - `jobs.id=1526`: `queued -> running -> succeeded`
   - start: `2026-02-20T14:53:29Z`
   - end: `2026-02-20T14:54:18Z`
3. Worker log output during run:
   - before fix, no mid-run rebuild logs were emitted.
   - only final lines appeared at completion:
     - `threading_rebuild_list applied update chunks ...`
     - `job attempt completed job_id=1526 job_type=threading_rebuild_list ...`

## Findings

1. The rebuild job is executed correctly by worker.
   - It did not remain stuck in queued.
   - It reached terminal success with expected metrics.

2. The apparent “no activity” issue was observability-related.
   - `handle_threading_rebuild_list` logged only once at the end.
   - Long runs can look idle because there were no periodic progress logs.

3. Worker scheduling was functioning as expected.
   - Worker is single-flight (`max_inflight_jobs=1`) and often busy with embedding jobs.
   - Rebuild job priority (`11`) still took precedence over embedding batch jobs (`2`) once claim cycle advanced.

## Fix implemented

Added periodic progress logging for manual rebuild jobs in `crates/nexus-jobs/src/pipeline.rs`:

1. `threading_rebuild_list`:
   - logs `threading_rebuild_list progress` every 10 chunks or every 30 seconds.
   - includes `job_id`, `list_key`, `processed_chunks`, `processed_messages`,
     `affected_threads`, `threads_rebuilt`, `threads_unchanged_skipped`,
     `meili_docs_upserted`.

2. `lineage_rebuild_list`:
   - logs `lineage_rebuild_list progress` every 10 chunks or every 30 seconds.
   - includes `job_id`, `list_key`, `processed_chunks`, `processed_messages`,
     `series_versions_written`, `patch_items_written`, `patch_item_files_written`.

## Validation after fix

1. Tests:
   - `cargo fmt`
   - `cargo test -p nexus-jobs --lib` (pass)

2. Runtime:
   - Triggered another rebuild (`jobs.id=1527`).
   - Observed periodic logs while still running:
     - `threading_rebuild_list progress ... processed_chunks=10 ...`
     - `threading_rebuild_list progress ... processed_chunks=20 ...`
   - Job then completed with final summary + job-attempt completion logs.

## Practical prod takeaway

If prod shows rebuild jobs in `running` with few logs, it may still be working. With this change deployed, periodic progress logs make that visible without waiting for terminal completion.
