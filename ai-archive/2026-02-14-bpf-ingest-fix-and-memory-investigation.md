# 2026-02-14 BPF Ingest Fix + Worker Memory Investigation

## Scope
- Fix `repo_ingest_run` write-path failures observed during `bpf` full ingest.
- Validate end-to-end ingest + threading on `bpf`.
- Investigate post-job worker RSS retention.

## Code Changes
- File: `crates/nexus-db/src/ingest.rs`
  - Deduped batched `message_id_map` rows before `ON CONFLICT DO UPDATE` in `ingest_messages_batch` to prevent:
    - `ON CONFLICT DO UPDATE command cannot affect row a second time`
  - Fixed tuple `IN (...)` SQL builder generation in:
    - `load_existing_message_id_rows`
    - `promote_message_id_rows`
  - These changes remove the malformed SQL path that produced:
    - `syntax error at or near ","`

## Validation
- Build/format:
  - `cargo fmt`
  - `cargo check -q`
- Runtime validation (containers):
  - Started fresh stack and worker in `ingest_only` mode with `copy` write mode.
  - Triggered `POST /admin/v1/ingest/sync?list_key=bpf`.
  - Job result (`repo_ingest_run`) succeeded:
    - `commit_count=169328`
    - `rows_written=169322`
    - `batch_size=10000`
    - `ingest_write_mode=copy`
  - Triggered `POST /admin/v1/threading/rebuild?list_key=bpf`.
  - Job result (`threading_rebuild_list`) succeeded:
    - `processed_messages=169251`
    - `threads_rebuilt=36850`
  - Data checks after completion:
    - `list_message_instances=169322`
    - `messages=169251`
    - `threads_1=20663`
    - `thread_nodes_1=171662`
    - `thread_messages_1=169026`

## Memory Investigation Notes
- Observed previous worker RSS after heavy ingest: ~2.73 GB.
- Smaps showed mostly anonymous private memory (heap), not page cache.
- Restarted idle worker baseline returned to ~4-9 MB RSS, indicating no persistent logical in-memory state leak.
- Evidence points to allocator high-water behavior after large COPY payload allocations.
- Measured lower retained RSS with runtime/allocator tuning:
  - `TOKIO_WORKER_THREADS=4`
  - `MALLOC_ARENA_MAX=2`
  - `MALLOC_TRIM_THRESHOLD_=131072`
  - `MALLOC_MMAP_THRESHOLD_=131072`

## Outcome
- BPF ingest and threading now complete end-to-end successfully on current code.
- Worker post-job RSS behavior is understood and reproducible; retention is primarily allocator/arena behavior after large transient allocations.
