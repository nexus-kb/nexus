# 2026-03-03 Handler safety and HTTP client reuse

## Scope
Second refactor slice after `630498b`, focused on API handler safety, async HTTP efficiency, and cast/clone cleanup.

## Implemented

1. Reused a shared async HTTP client in API state
- Added `http_client: reqwest::Client` to `ApiState` and initialized it once in `ApiState::new`.
- Switched Meilisearch calls in public search, admin diagnostics storage fetch, and readiness checks to use `state.http_client` instead of constructing a new client per request.

2. Removed unsafe integer `as` casts from API handlers
- Added `limit_to_usize(i64) -> usize` helpers in both public/admin handler helper modules.
- Replaced `limit as usize` pagination conversions in public endpoints (`lists`, `threads`, `series`) and admin endpoints (`jobs`, `pipeline`, `meili_bootstrap`).
- Added `usize_to_i64(usize) -> i64` helper for summary counters and removed `count() as i64` casts in `series_compare`.

3. Hardened numeric conversions from external JSON
- In admin Meilisearch parsing, replaced `u64 as i64` conversions with checked conversion via `i64::try_from`.
- Prevents accidental wrap/truncation behavior from large numeric payloads.

4. Eliminated silent serialization fallbacks in hash generation
- Updated request/cursor hash helpers to hash stable JSON string representations instead of `serde_json::to_vec(...).unwrap_or_default()` fallback behavior.

5. Reduced clone/ownership anti-patterns and small cleanups
- Removed unnecessary `next_cursor.clone()` in page-info builders.
- Updated `slice_by_offsets` to use checked integer conversions.
- Removed redundant clone in Cloudflare IP selection logic.
- Simplified `path_and_query` fallback with `map_or`.
- In search pagination, used `saturating_add` for cursor offset math.

## Validation
Executed and passed:
- `cargo fmt --all`
- `cargo check --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-targets --all-features --locked`
