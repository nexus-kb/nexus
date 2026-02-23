use serde_json::json;

use super::{
    MAX_LOG_REASON_CHARS, extract_job_payload_context, summarize_outcome, truncate_for_log,
};
use crate::JobExecutionOutcome;
use nexus_db::JobStoreMetrics;

fn metrics() -> JobStoreMetrics {
    JobStoreMetrics {
        duration_ms: 123,
        rows_written: 9,
        bytes_read: 88,
        commit_count: 7,
        parse_errors: 1,
    }
}

#[test]
fn payload_context_extracts_common_fields() {
    let payload = json!({
        "list_key": "bpf",
        "repo_key": "linux.git",
        "run_id": 42,
        "scope": "thread",
        "ids": [11, 22, 33]
    });

    let context = extract_job_payload_context(&payload);
    assert_eq!(context.list_key.as_deref(), Some("bpf"));
    assert_eq!(context.repo_key.as_deref(), Some("linux.git"));
    assert_eq!(context.run_id, Some(42));
    assert_eq!(context.scope.as_deref(), Some("thread"));
    assert_eq!(context.batch_count, Some(3));
}

#[test]
fn payload_context_uses_commit_oid_batch_count_when_ids_missing() {
    let payload = json!({
        "list_key": "lkml",
        "commit_oids": ["a", "b"]
    });

    let context = extract_job_payload_context(&payload);
    assert_eq!(context.list_key.as_deref(), Some("lkml"));
    assert_eq!(context.batch_count, Some(2));
}

#[test]
fn summarize_outcome_success_has_no_error_fields() {
    let summary = summarize_outcome(&JobExecutionOutcome::Success {
        result_json: json!({"ok": true}),
        metrics: metrics(),
    });

    assert_eq!(summary.outcome, "succeeded");
    assert!(summary.kind.is_none());
    assert!(summary.reason.is_none());
    assert!(summary.backoff_ms.is_none());
    assert_eq!(summary.metrics.rows_written, 9);
}

#[test]
fn summarize_outcome_retryable_includes_kind_reason_and_backoff() {
    let summary = summarize_outcome(&JobExecutionOutcome::Retryable {
        reason: "temporary meili timeout".to_string(),
        kind: "transient".to_string(),
        backoff_ms: 15_000,
        metrics: metrics(),
    });

    assert_eq!(summary.outcome, "retryable");
    assert_eq!(summary.kind.as_deref(), Some("transient"));
    assert_eq!(summary.reason.as_deref(), Some("temporary meili timeout"));
    assert_eq!(summary.backoff_ms, Some(15_000));
}

#[test]
fn summarize_outcome_terminal_includes_kind_and_reason() {
    let summary = summarize_outcome(&JobExecutionOutcome::Terminal {
        reason: "invalid payload shape".to_string(),
        kind: "payload".to_string(),
        metrics: metrics(),
    });

    assert_eq!(summary.outcome, "terminal");
    assert_eq!(summary.kind.as_deref(), Some("payload"));
    assert_eq!(summary.reason.as_deref(), Some("invalid payload shape"));
    assert!(summary.backoff_ms.is_none());
}

#[test]
fn summarize_outcome_cancelled_uses_cancelled_kind() {
    let summary = summarize_outcome(&JobExecutionOutcome::Cancelled {
        reason: "cancel requested".to_string(),
        metrics: metrics(),
    });

    assert_eq!(summary.outcome, "cancelled");
    assert_eq!(summary.kind.as_deref(), Some("cancelled"));
    assert_eq!(summary.reason.as_deref(), Some("cancel requested"));
    assert!(summary.backoff_ms.is_none());
}

#[test]
fn truncates_and_normalizes_reason_for_logs() {
    let raw = "line one\nline two\tline three";
    let normalized = truncate_for_log(raw, MAX_LOG_REASON_CHARS);
    assert_eq!(normalized, "line one line two line three");

    let long = "x".repeat(MAX_LOG_REASON_CHARS + 20);
    let truncated = truncate_for_log(&long, 24);
    assert_eq!(truncated.len(), 24);
    assert!(truncated.ends_with("..."));
}
