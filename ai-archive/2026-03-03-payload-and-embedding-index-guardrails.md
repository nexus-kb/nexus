# 2026-03-03 Payload and embedding index guardrails

## Scope
Follow-up hardening after runtime numeric-conversion refactor.

## Implemented

1. Removed remaining silent enqueue payload fallback
- File: `crates/nexus-api/src/handlers/admin/ingest.rs`
- Replaced `serde_json::to_value(payload).unwrap_or_default()` with explicit JSON payload construction:
  - `json!({ "run_id": payload.run_id })`
- Avoids accidental `{}` payload enqueue on serialization failure paths.

2. Hardened embedding response index conversion
- File: `crates/nexus-core/src/embeddings.rs`
- Replaced unchecked `u64 -> usize` cast for embedding entry indexes with checked conversion:
  - `usize::try_from(index)` with protocol error on overflow.
- Preserves bounds-check behavior while avoiding truncation/wrap on narrow targets.

## Validation
Executed and passed:
- `cargo fmt --all`
- `cargo check --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-targets --all-features --locked`
