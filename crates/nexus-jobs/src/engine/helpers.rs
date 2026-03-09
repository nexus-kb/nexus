use super::*;
use nexus_db::is_running_attempt_unique_violation;

pub(super) fn is_pipeline_job_type(job_type: &str) -> bool {
    matches!(
        job_type,
        "pipeline_ingest"
            | "pipeline_threading"
            | "pipeline_lineage"
            | "pipeline_lexical"
            | "pipeline_search"
    )
}

pub(super) fn metrics_to_json(metrics: JobStoreMetrics) -> serde_json::Value {
    serde_json::json!({
        "duration_ms": metrics.duration_ms,
        "rows_written": metrics.rows_written,
        "bytes_read": metrics.bytes_read,
        "commit_count": metrics.commit_count,
        "parse_errors": metrics.parse_errors,
    })
}

pub(super) fn log_job_attempt_completion(job: &Job, outcome: &JobExecutionOutcome) {
    let context = extract_job_payload_context(&job.payload_json);
    let summary = summarize_outcome(outcome);

    let list_key = context.list_key.as_deref().unwrap_or("-");
    let repo_key = context.repo_key.as_deref().unwrap_or("-");
    let run_id = context
        .run_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let scope = context.scope.as_deref().unwrap_or("-");
    let batch_count = context
        .batch_count
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());

    if summary.kind.is_some() || summary.reason.is_some() || summary.backoff_ms.is_some() {
        let kind = summary.kind.as_deref().unwrap_or("-");
        let reason = summary.reason.as_deref().unwrap_or("-");
        let backoff_ms = summary
            .backoff_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());

        info!(
            target: "worker_job",
            job_id = job.id,
            job_type = %job.job_type,
            attempt = job.attempt,
            max_attempts = job.max_attempts,
            outcome = summary.outcome,
            duration_ms = %summary.metrics.duration_ms,
            rows_written = summary.metrics.rows_written,
            bytes_read = summary.metrics.bytes_read,
            commit_count = summary.metrics.commit_count,
            parse_errors = summary.metrics.parse_errors,
            list_key = %list_key,
            repo_key = %repo_key,
            run_id = %run_id,
            scope = %scope,
            batch_count = %batch_count,
            kind = %kind,
            reason = %reason,
            backoff_ms = %backoff_ms,
            "job attempt completed"
        );
    } else {
        info!(
            target: "worker_job",
            job_id = job.id,
            job_type = %job.job_type,
            attempt = job.attempt,
            max_attempts = job.max_attempts,
            outcome = summary.outcome,
            duration_ms = %summary.metrics.duration_ms,
            rows_written = summary.metrics.rows_written,
            bytes_read = summary.metrics.bytes_read,
            commit_count = summary.metrics.commit_count,
            parse_errors = summary.metrics.parse_errors,
            list_key = %list_key,
            repo_key = %repo_key,
            run_id = %run_id,
            scope = %scope,
            batch_count = %batch_count,
            "job attempt completed"
        );
    }
}

pub(super) fn extract_job_payload_context(payload: &serde_json::Value) -> JobPayloadContext {
    let list_key = payload
        .get("list_key")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let repo_key = payload
        .get("repo_key")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let run_id = payload.get("run_id").and_then(serde_json::Value::as_i64);
    let scope = payload
        .get("scope")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);

    let batch_count = ["ids", "commit_oids", "anchor_message_pks", "patch_item_ids"]
        .iter()
        .find_map(|key| {
            payload
                .get(*key)
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
        });

    JobPayloadContext {
        list_key,
        repo_key,
        run_id,
        scope,
        batch_count,
    }
}

pub(super) fn summarize_outcome(outcome: &JobExecutionOutcome) -> JobOutcomeSummary {
    match outcome {
        JobExecutionOutcome::Success { metrics, .. } => JobOutcomeSummary {
            outcome: "succeeded",
            kind: None,
            reason: None,
            backoff_ms: None,
            metrics: metrics.clone(),
        },
        JobExecutionOutcome::Retryable {
            reason,
            kind,
            backoff_ms,
            metrics,
        } => JobOutcomeSummary {
            outcome: "retryable",
            kind: Some(kind.clone()),
            reason: Some(truncate_for_log(reason, MAX_LOG_REASON_CHARS)),
            backoff_ms: Some(*backoff_ms),
            metrics: metrics.clone(),
        },
        JobExecutionOutcome::Terminal {
            reason,
            kind,
            metrics,
        } => JobOutcomeSummary {
            outcome: "terminal",
            kind: Some(kind.clone()),
            reason: Some(truncate_for_log(reason, MAX_LOG_REASON_CHARS)),
            backoff_ms: None,
            metrics: metrics.clone(),
        },
        JobExecutionOutcome::Cancelled { reason, metrics } => JobOutcomeSummary {
            outcome: "cancelled",
            kind: Some("cancelled".to_string()),
            reason: Some(truncate_for_log(reason, MAX_LOG_REASON_CHARS)),
            backoff_ms: None,
            metrics: metrics.clone(),
        },
    }
}

pub(super) fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let compact = value
        .replace(['\r', '\n', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if compact.chars().count() <= max_chars {
        return compact;
    }

    if max_chars <= 3 {
        return "...".chars().take(max_chars).collect();
    }

    let prefix: String = compact.chars().take(max_chars - 3).collect();
    format!("{prefix}...")
}

pub(super) async fn finalize_job(
    jobs: &JobStore,
    _settings: &Settings,
    job: &Job,
    outcome: &JobExecutionOutcome,
    attempt_id: i64,
    attempt: i32,
    max_attempts: i32,
) -> Result<(), sqlx::Error> {
    match outcome {
        JobExecutionOutcome::Success {
            result_json,
            metrics,
        } => {
            jobs.finalize_succeeded_attempt(
                job.id,
                attempt_id,
                Some(result_json.clone()),
                Some(metrics_to_json(metrics.clone())),
            )
            .await?;
        }
        JobExecutionOutcome::Cancelled { reason, metrics } => {
            jobs.finalize_cancelled_attempt(
                job.id,
                attempt_id,
                reason,
                Some(metrics_to_json(metrics.clone())),
            )
            .await?;
        }
        JobExecutionOutcome::Terminal {
            reason,
            kind,
            metrics,
        } => {
            jobs.finalize_terminal_attempt(
                job.id,
                attempt_id,
                reason,
                kind,
                Some(metrics_to_json(metrics.clone())),
            )
            .await?;
        }
        JobExecutionOutcome::Retryable {
            reason,
            kind,
            backoff_ms,
            metrics,
        } => {
            if attempt >= max_attempts {
                jobs.finalize_terminal_attempt(
                    job.id,
                    attempt_id,
                    reason,
                    kind,
                    Some(metrics_to_json(metrics.clone())),
                )
                .await?;
            } else {
                let backoff_ms_i64 = i64::try_from((*backoff_ms).max(1)).unwrap_or(i64::MAX);
                let run_after = Utc::now() + chrono::Duration::milliseconds(backoff_ms_i64);

                jobs.finalize_retryable_attempt(
                    job.id,
                    attempt_id,
                    RetryDecision {
                        reason: reason.clone(),
                        kind: kind.clone(),
                        run_after,
                    },
                    Some(metrics_to_json(metrics.clone())),
                )
                .await?;
            }
        }
    }

    Ok(())
}

pub(super) async fn finalize_job_without_attempt(
    jobs: &JobStore,
    job: &Job,
    outcome: &JobExecutionOutcome,
) -> Result<(), sqlx::Error> {
    match outcome {
        JobExecutionOutcome::Success { result_json, .. } => {
            jobs.mark_succeeded(job.id, Some(result_json.clone()))
                .await?;
        }
        JobExecutionOutcome::Cancelled { reason, .. } => {
            jobs.mark_cancelled(job.id, reason).await?;
        }
        JobExecutionOutcome::Terminal { reason, kind, .. } => {
            jobs.mark_terminal(job.id, reason, kind).await?;
        }
        JobExecutionOutcome::Retryable {
            reason,
            kind,
            backoff_ms,
            ..
        } => {
            if job.attempt >= job.max_attempts {
                jobs.mark_terminal(job.id, reason, kind).await?;
            } else {
                let backoff_ms_i64 = i64::try_from((*backoff_ms).max(1)).unwrap_or(i64::MAX);
                let run_after = Utc::now() + chrono::Duration::milliseconds(backoff_ms_i64);

                jobs.mark_retryable(
                    job.id,
                    RetryDecision {
                        reason: reason.clone(),
                        kind: kind.clone(),
                        run_after,
                    },
                )
                .await?;
            }
        }
    }

    Ok(())
}

pub(super) fn start_attempt_failure_outcome(
    settings: &Settings,
    job: &Job,
    err: &sqlx::Error,
) -> JobExecutionOutcome {
    let metrics = JobStoreMetrics {
        duration_ms: 0,
        rows_written: 0,
        bytes_read: 0,
        commit_count: 0,
        parse_errors: 0,
    };

    if is_running_attempt_unique_violation(err) {
        return JobExecutionOutcome::Terminal {
            reason: format!(
                "failed to start job attempt: stale running attempt row blocks retry: {err}"
            ),
            kind: "queue_state".to_string(),
            metrics,
        };
    }

    let backoff = JobStore::compute_backoff(
        settings.worker.base_backoff_ms,
        settings.worker.max_backoff_ms,
        job.attempt,
    );
    let backoff_ms = u64::try_from(backoff.num_milliseconds().max(1)).unwrap_or(1);

    JobExecutionOutcome::Retryable {
        reason: format!("failed to start job attempt: {err}"),
        kind: "db".to_string(),
        backoff_ms,
        metrics,
    }
}
