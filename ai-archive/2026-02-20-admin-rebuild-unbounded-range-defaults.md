# 2026-02-20 Admin rebuild unbounded-range defaults

## Goal

Ensure admin rebuild/backfill APIs treat unspecified ranges as full-list/full-scope runs, without accidentally applying watermark-like bounds.

## Changes implemented

1. Range parsing hardening in admin handlers (`crates/nexus-api/src/handlers/admin.rs`)
   - Added `deserialize_optional_datetime_query` for `threading_rebuild` and `lineage_rebuild` query fields.
   - `from` / `to` now accept:
     - RFC3339 timestamps
     - `YYYY-MM-DD`
   - Blank query values (e.g. `from=&to=`) are normalized to `None` instead of causing parse errors.
   - Omitted query values continue to map to `None`.

2. Embedding backfill range normalization
   - Replaced direct parsing with `parse_optional_timestamp_query(...)`.
   - `from` / `to` now behave consistently with rebuild endpoints:
     - blank or omitted => `None` (unbounded backfill)
     - invalid non-empty values => `422`.

3. Parsing helpers
   - Added `parse_timestamp(raw)` and `parse_optional_timestamp_query(raw)` for centralized, consistent parsing logic.

4. Tests added (`nexus-api` unit tests)
   - `parse_optional_timestamp_query_accepts_rfc3339`
   - `parse_optional_timestamp_query_accepts_date`
   - `parse_optional_timestamp_query_treats_empty_as_unbounded`
   - `parse_optional_timestamp_query_rejects_invalid_input`

## Verification

1. Test suite
   - `cargo fmt`
   - `cargo test -p nexus-api` (pass)

2. Runtime checks (live API)
   - `POST /admin/v1/threading/rebuild?list_key=bpf&from=&to=`
     - accepted
     - job payload persisted with `from_seen_at=null`, `to_seen_at=null`
   - `POST /admin/v1/lineage/rebuild?list_key=bpf&from=&to=`
     - accepted
     - job payload persisted with `from_seen_at=null`, `to_seen_at=null`
   - `POST /admin/v1/search/embeddings/backfill?scope=thread&list_key=bpf&from=&to=`
     - accepted
     - backfill run persisted with `from_seen_at=null`, `to_seen_at=null`

## Result

Admin-triggered threading/lineage rebuilds and embedding backfills now reliably run unbounded when range is not specified (including blank query params), preventing accidental narrowing to watermark-like windows.
