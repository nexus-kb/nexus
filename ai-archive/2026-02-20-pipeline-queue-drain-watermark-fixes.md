# 2026-02-20 Pipeline queue-drain and watermark fixes

## Problem investigated

Repeated `POST /admin/v1/ingest/grokmirror` triggers could leave many `pipeline_runs` stuck in `pending` (and in some deployments, a buildup of queued ingest jobs) while other stages continued.

Observed root causes:

1. Batch-local activation only
   - Run chaining activated pending runs only within the same `batch_id`.
   - If new batches were created while another batch was active, those new batches could be stranded.

2. Activation conflict on per-list running uniqueness
   - `activate_next_pending_run(batch_id)` selected the first pending row in a batch without filtering out lists that already had a running run.
   - This could hit `idx_pipeline_runs_active_per_list` and abort activation.

3. Grokmirror duplicate open runs per list
   - Webhook path created new pending runs even when a list already had `pending` or `running` work.

4. Watermark scan source
   - Commit scanner used `git rev-list --all`; metadata refs can cause repeated historical deltas unrelated to branch tips.

## Implemented fixes

1. Pipeline store activation logic
   - Updated batch activation query to skip lists with an existing `running` run.
   - Added global activation method:
     - `activate_next_pending_run_any()`
     - selects oldest activatable pending run across all batches.
   - Added open-run lookup:
     - `get_open_run_for_list()` for `pending|running` dedupe checks.
   - File:
     - `crates/nexus-db/src/pipeline.rs`

2. Grokmirror trigger idempotency and launch behavior
   - `ingest_grokmirror` now reuses existing open run per list (does not create duplicate pending runs).
   - Auto-activation of a newly created run only happens when no run is currently active.
   - `ingest_sync` now also checks for open run (`pending|running`) instead of only `running`.
   - File:
     - `crates/nexus-api/src/handlers/admin.rs`

3. Worker queue draining improvements
   - Stage terminal chaining now:
     - tries next pending run in same batch,
     - then falls back to global pending queue.
   - Maintenance sweep now recovers orphan pending queues:
     - when no active run exists, it activates one global pending run.
   - Maintenance sweep also coalesces duplicate pending runs per list (keeps newest pending).
   - Worker claim limit is now bounded to `min(claim_batch, max_inflight_jobs)` so the worker
     does not mark more jobs `running` than it can execute concurrently.
   - Applied in both runtime paths:
     - worker engine pipeline finalization helpers,
     - lexical-stage chaining helper.
   - Files:
     - `crates/nexus-jobs/src/engine.rs`
     - `crates/nexus-jobs/src/pipeline.rs`

4. Watermark delta scan source
   - Changed commit scanner from `git rev-list --all` to `git rev-list --branches`.
   - Avoids metadata-ref-driven historical resurfacing during incremental scans.
   - File:
     - `crates/nexus-jobs/src/scanner.rs`

## Verification performed

1. Build/test
   - `cargo fmt`
   - `cargo check`
   - `cargo test -p nexus-api`
   - `cargo test -p nexus-jobs --lib`

2. Runtime reproduction and validation
   - Reproduced failure mode with multiple grokmirror triggers:
     - pending batches accumulated and activation errors appeared (`idx_pipeline_runs_active_per_list`).
   - After fixes and hot reload:
     - maintenance immediately recovered orphan pending runs by global activation,
     - batch chaining proceeded and crossed batch boundaries via global fallback,
     - repeated grokmirror trigger during active run did not create new pipeline runs and returned existing open run ids.
   - Queue claim stress probe:
     - inserted 200 synthetic queued jobs (`job_type=probe_claim`) and sampled states at high frequency during worker drain.
     - observed `MAX_RUNNING=1` while `max_inflight_jobs=1` and default `claim_batch=16`.
     - confirms claim capping prevents over-claiming into `running`.

## Operational impact

- Pipeline queue now drains reliably even with overlapping grokmirror triggers.
- Webhook behavior is idempotent per list while a run is already open.
- Job state transitions now preserve `running` semantics under claim pressure (`running <= max_inflight_jobs`).
- Watermark incremental scans avoid metadata refs, reducing repeated historical rescans.
