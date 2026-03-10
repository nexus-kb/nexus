use nexus_core::config::{
    AdminConfig, AppConfig, DatabaseConfig, EmbeddingsConfig, MailConfig, MainlineConfig,
    MeiliConfig, Settings, WorkerConfig,
};
use nexus_db::{CatalogStore, Db, EnqueueJobParams, JobState, PipelineStore};
use serde_json::json;

use super::{
    MAX_LOG_REASON_CHARS, Phase0Worker, extract_job_payload_context, summarize_outcome,
    truncate_for_log,
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

fn test_settings(database_url: String) -> Settings {
    Settings {
        database: DatabaseConfig {
            url: database_url,
            max_connections: 4,
        },
        app: AppConfig::default(),
        admin: AdminConfig::default(),
        mail: MailConfig::default(),
        mainline: MainlineConfig::default(),
        meili: MeiliConfig::default(),
        embeddings: EmbeddingsConfig::default(),
        worker: WorkerConfig::default(),
    }
}

#[tokio::test]
async fn start_attempt_unique_violation_terminalizes_job_and_pipeline_run()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
        return Ok(());
    };

    let settings = test_settings(database_url);
    let db = Db::connect(&settings.database).await?;
    db.migrate().await?;

    let jobs = nexus_db::JobStore::new(db.pool().clone());
    let catalog = CatalogStore::new(db.pool().clone());
    let pipeline = PipelineStore::new(db.pool().clone());
    let worker = Phase0Worker::new(settings.clone(), db.clone());

    let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let list_key = format!("engine-start-attempt-{unique}");
    let list = catalog.ensure_mailing_list(&list_key).await?;
    let run = pipeline
        .create_running_run(list.id, &list_key, "test")
        .await?;
    pipeline.set_run_current_stage(run.id, "lexical").await?;

    let job = jobs
        .enqueue(EnqueueJobParams {
            job_type: "pipeline_lexical".to_string(),
            payload_json: serde_json::json!({ "run_id": run.id }),
            priority: 10,
            dedupe_scope: Some(format!("tests:{unique}")),
            dedupe_key: Some("engine-start-attempt".to_string()),
            run_after: None,
            max_attempts: Some(3),
        })
        .await?;

    sqlx::query(
        r#"UPDATE jobs
        SET state = 'running',
            claimed_by = 'test-worker',
            lease_until = now() + interval '30 seconds',
            attempt = 1
        WHERE id = $1"#,
    )
    .bind(job.id)
    .execute(db.pool())
    .await?;
    sqlx::query(
        r#"INSERT INTO job_attempts (job_id, attempt, status)
        VALUES ($1, 1, 'running')"#,
    )
    .bind(job.id)
    .execute(db.pool())
    .await?;

    let claimed_job = jobs.get(job.id).await?.expect("job must exist");
    worker.process_job(claimed_job).await;

    let updated_job = jobs.get(job.id).await?.expect("job must exist");
    assert_eq!(updated_job.state, JobState::FailedTerminal);
    assert_eq!(updated_job.last_error_kind.as_deref(), Some("queue_state"));
    assert!(
        updated_job
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("stale running attempt row blocks retry")
    );

    let attempts = jobs.list_attempts(job.id, 10, None).await?;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, "failed");
    assert!(attempts[0].finished_at.is_some());

    let updated_run = pipeline.get_run(run.id).await?.expect("run must exist");
    assert_eq!(updated_run.state, "failed");
    assert!(
        updated_run
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("stage pipeline_lexical failed")
    );

    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job.id)
        .execute(db.pool())
        .await?;
    sqlx::query("DELETE FROM pipeline_runs WHERE id = $1")
        .bind(run.id)
        .execute(db.pool())
        .await?;
    sqlx::query("DELETE FROM mailing_lists WHERE id = $1")
        .bind(list.id)
        .execute(db.pool())
        .await?;

    Ok(())
}
