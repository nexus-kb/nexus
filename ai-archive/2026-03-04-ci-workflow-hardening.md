# 2026-03-04 - CI Workflow Hardening (API)

## Scope
Refactored `.github/workflows/tests.yml` for `nexus-api-server` to align with strict CI/CD best practices while preserving existing job contracts (`Verify`, `Publish GHCR image`).

## Why
The linked failing run passed `Verify` and failed in publish at Trivy scan. The previous flow pushed the image before scanning, which made failures noisy and less actionable.

## Implemented
1. Hardened action references
   - Pinned third-party actions to immutable SHAs.

2. Strengthened verify job
   - Added Rust toolchain setup with `rustfmt` and `clippy`.
   - Added `Swatinem/rust-cache`.
   - Added explicit quality gates:
     - `cargo fmt --all -- --check`
     - `cargo clippy --workspace --all-targets --locked -- -D warnings`
     - `cargo test --workspace --locked`
   - Added `timeout-minutes`.

3. Refactored publish job to pre-push security gating
   - Build local candidate image (`load: true`, `local/nexus-api-server:scan`).
   - Run strict blocking Trivy gates before push:
     - Gate 1: critical vulnerabilities (`scanners=vuln`, `severity=CRITICAL`, `ignore-unfixed=true`, `vuln-type=os,library`).
     - Gate 2: secrets (`scanners=secret`).
   - Build and push to GHCR only after both gates pass.

4. Added security reporting
   - Added always-on SARIF generation via Trivy.
   - Added SARIF upload via `github/codeql-action/upload-sarif`.
   - Added publish summary with vulnerability gate, secret gate, and push outcome.

## Permissions / Contract Changes
- `publish` job now requires `security-events: write` in addition to `contents: read` and `packages: write`.

## Validation
- Local repo commands passed before workflow finalization:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo test --workspace --locked`

## Notes
- Job names were kept unchanged to avoid branch protection status-check churn.
- Security policy is strict by design: blocking for critical vulnerabilities and secrets.
