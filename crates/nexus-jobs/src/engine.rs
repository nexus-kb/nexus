use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use nexus_core::config::Settings;
use nexus_db::{
    CatalogStore, Db, EmbeddingsStore, IngestStore, Job, JobState, JobStore, JobStoreMetrics,
    LineageStore, PipelineStore, RetryDecision, SearchStore, ThreadingStore,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info, warn};

use crate::JobExecutionOutcome;
use crate::payloads::{IngestCommitBatchPayload, RepoIngestRunPayload};
use crate::pipeline::Phase0JobHandler;

const MAX_LOG_REASON_CHARS: usize = 180;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub poll_ms: u64,
    pub claim_batch: i64,
    pub lease_ms: i64,
    pub heartbeat_ms: u64,
    pub sweep_ms: u64,
    pub max_inflight_jobs: usize,
    pub max_inflight_ingest_jobs: usize,
}

impl From<&nexus_core::config::WorkerConfig> for WorkerConfig {
    fn from(value: &nexus_core::config::WorkerConfig) -> Self {
        let max_inflight_jobs = value.max_inflight_jobs.max(1);
        Self {
            poll_ms: value.poll_ms,
            claim_batch: value.claim_batch,
            lease_ms: value.lease_ms,
            heartbeat_ms: value.heartbeat_ms,
            sweep_ms: value.sweep_ms,
            max_inflight_jobs,
            max_inflight_ingest_jobs: value.max_inflight_ingest_jobs.max(1).min(max_inflight_jobs),
        }
    }
}

#[derive(Clone)]
pub struct ExecutionContext {
    pub job_id: i64,
    worker_id: String,
    jobs: JobStore,
    lease_ms: i64,
}

impl ExecutionContext {
    pub async fn heartbeat(&self) -> Result<bool, sqlx::Error> {
        self.jobs
            .heartbeat(self.job_id, &self.worker_id, self.lease_ms)
            .await
    }

    pub async fn is_cancel_requested(&self) -> Result<bool, sqlx::Error> {
        self.jobs.is_cancel_requested(self.job_id).await
    }
}

#[derive(Clone)]
pub struct Phase0Worker {
    settings: Settings,
    db: Db,
    jobs: JobStore,
    handler: Phase0JobHandler,
    cfg: WorkerConfig,
    worker_id: String,
    ingest_job_semaphore: Arc<Semaphore>,
    repo_ingest_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct JobPayloadContext {
    list_key: Option<String>,
    repo_key: Option<String>,
    run_id: Option<i64>,
    scope: Option<String>,
    batch_count: Option<usize>,
}

#[derive(Debug, Clone)]
struct JobOutcomeSummary {
    outcome: &'static str,
    kind: Option<String>,
    reason: Option<String>,
    backoff_ms: Option<u64>,
    metrics: JobStoreMetrics,
}

impl Phase0Worker {
    pub fn new(settings: Settings, db: Db) -> Self {
        let jobs = JobStore::new(db.pool().clone());
        let catalog = CatalogStore::new(db.pool().clone());
        let ingest = IngestStore::new(db.pool().clone());
        let threading = ThreadingStore::new(db.pool().clone());
        let lineage = LineageStore::new(db.pool().clone());
        let pipeline = PipelineStore::new(db.pool().clone());
        let search = SearchStore::new(db.pool().clone());
        let embeddings = EmbeddingsStore::new(db.pool().clone());
        let handler = Phase0JobHandler::new(
            settings.clone(),
            catalog,
            ingest,
            threading,
            lineage,
            pipeline,
            search,
            embeddings,
            jobs.clone(),
        );
        let cfg = WorkerConfig::from(&settings.worker);
        let ingest_limit = cfg.max_inflight_ingest_jobs;

        Self {
            settings,
            db,
            jobs,
            handler,
            cfg,
            worker_id: format!("phase0-worker-{}", std::process::id()),
            ingest_job_semaphore: Arc::new(Semaphore::new(ingest_limit)),
            repo_ingest_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(self) -> Result<(), sqlx::Error> {
        let mut listener = self.db.listener().await?;
        listener.listen("nexus_jobs").await?;

        let mut poll_tick = interval(Duration::from_millis(self.cfg.poll_ms));
        poll_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut sweep_tick = interval(Duration::from_millis(self.cfg.sweep_ms));
        sweep_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(worker_id = %self.worker_id, "phase0 worker started");

        loop {
            tokio::select! {
                _ = poll_tick.tick() => {
                    self.drain_once().await?;
                }
                _ = sweep_tick.tick() => {
                    self.run_maintenance().await?;
                }
                _ = listener.recv() => {
                    self.drain_once().await?;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("phase0 worker received shutdown signal");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn run_maintenance(&self) -> Result<(), sqlx::Error> {
        let promoted = self.jobs.promote_ready_jobs().await?;
        let (requeued, terminal) = self.jobs.requeue_stuck_jobs().await?;

        if promoted > 0 || requeued > 0 || terminal > 0 {
            info!(promoted, requeued, terminal, "job maintenance sweep");
        }

        Ok(())
    }

    async fn drain_once(&self) -> Result<(), sqlx::Error> {
        self.jobs.promote_ready_jobs().await?;
        let claimed = self
            .jobs
            .claim_jobs(self.cfg.claim_batch, &self.worker_id, self.cfg.lease_ms)
            .await?;

        if claimed.is_empty() {
            return Ok(());
        }

        let mut joinset = JoinSet::new();
        let mut iter = claimed.into_iter();

        for _ in 0..self.cfg.max_inflight_jobs {
            let Some(job) = iter.next() else {
                break;
            };
            let worker = self.clone();
            joinset.spawn(async move {
                worker.process_job(job).await;
            });
        }

        while let Some(result) = joinset.join_next().await {
            if let Err(err) = result {
                error!(error = %err, "job task failed");
            }

            if let Some(job) = iter.next() {
                let worker = self.clone();
                joinset.spawn(async move {
                    worker.process_job(job).await;
                });
            }
        }

        Ok(())
    }

    async fn process_job(&self, job: Job) {
        let _ingest_permit = self.acquire_ingest_permit(&job).await;
        let _repo_guard = self.acquire_repo_ingest_lock(&job).await;

        let attempt = match self.jobs.start_attempt(job.id, job.attempt).await {
            Ok(v) => v,
            Err(err) => {
                error!(job_id = job.id, error = %err, "failed to start job attempt");
                return;
            }
        };

        if job.cancel_requested {
            let cancel_metrics = JobStoreMetrics {
                duration_ms: 0,
                rows_written: 0,
                bytes_read: 0,
                commit_count: 0,
                parse_errors: 0,
            };
            if let Err(err) = self
                .jobs
                .mark_cancelled(job.id, "cancel requested before execution")
                .await
            {
                error!(job_id = job.id, error = %err, "failed to mark cancelled");
            }
            let _ = self
                .jobs
                .finish_attempt(
                    attempt.id,
                    "cancelled",
                    Some("cancel requested before execution"),
                    Some(metrics_to_json(cancel_metrics.clone())),
                )
                .await;
            let outcome = JobExecutionOutcome::Cancelled {
                reason: "cancel requested before execution".to_string(),
                metrics: cancel_metrics,
            };
            log_job_attempt_completion(&job, &outcome);
            return;
        }

        let context = ExecutionContext {
            job_id: job.id,
            worker_id: self.worker_id.clone(),
            jobs: self.jobs.clone(),
            lease_ms: self.cfg.lease_ms,
        };

        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let jobs = self.jobs.clone();
        let worker_id = self.worker_id.clone();
        let heartbeat_ms = self.cfg.heartbeat_ms;
        let lease_ms = self.cfg.lease_ms;
        let heartbeat_job_id = job.id;

        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(heartbeat_ms));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = jobs.heartbeat(heartbeat_job_id, &worker_id, lease_ms).await {
                            warn!(job_id = heartbeat_job_id, error = %err, "heartbeat failed");
                        }
                    }
                    _ = &mut stop_rx => break,
                }
            }
        });

        let outcome = self.handler.handle(job.clone(), context).await;

        let _ = stop_tx.send(());
        let _ = heartbeat_task.await;

        match finalize_job(
            &self.jobs,
            &self.settings,
            &job,
            &outcome,
            attempt.id,
            job.attempt,
            job.max_attempts,
        )
        .await
        {
            Ok(()) => {}
            Err(err) => {
                error!(job_id = job.id, error = %err, "failed to finalize job result");
            }
        }

        log_job_attempt_completion(&job, &outcome);
    }

    async fn acquire_ingest_permit(&self, job: &Job) -> Option<OwnedSemaphorePermit> {
        if !is_ingest_job_type(&job.job_type) {
            return None;
        }

        match self.ingest_job_semaphore.clone().acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(err) => {
                warn!(job_id = job.id, error = %err, "ingest concurrency limiter unavailable");
                None
            }
        }
    }

    async fn acquire_repo_ingest_lock(
        &self,
        job: &Job,
    ) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        if !is_ingest_job_type(&job.job_type) {
            return None;
        }

        let repo_key = match job.job_type.as_str() {
            "ingest_commit_batch" => {
                let payload: IngestCommitBatchPayload =
                    match serde_json::from_value(job.payload_json.clone()) {
                        Ok(v) => v,
                        Err(err) => {
                            warn!(
                                job_id = job.id,
                                error = %err,
                                "failed to parse ingest payload for repo lock"
                            );
                            return None;
                        }
                    };
                format!("{}:{}", payload.list_key, payload.repo_key)
            }
            "repo_ingest_run" => {
                let payload: RepoIngestRunPayload =
                    match serde_json::from_value(job.payload_json.clone()) {
                        Ok(v) => v,
                        Err(err) => {
                            warn!(
                                job_id = job.id,
                                error = %err,
                                "failed to parse repo_ingest_run payload for repo lock"
                            );
                            return None;
                        }
                    };
                format!("{}:{}", payload.list_key, payload.repo_key)
            }
            _ => return None,
        };
        let lock = {
            let mut locks = self.repo_ingest_locks.lock().await;
            locks
                .entry(repo_key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        Some(lock.lock_owned().await)
    }
}

fn is_ingest_job_type(job_type: &str) -> bool {
    matches!(
        job_type,
        "ingest_commit_batch" | "repo_ingest_run" | "pipeline_stage_ingest"
    )
}

fn metrics_to_json(metrics: JobStoreMetrics) -> serde_json::Value {
    serde_json::json!({
        "duration_ms": metrics.duration_ms,
        "rows_written": metrics.rows_written,
        "bytes_read": metrics.bytes_read,
        "commit_count": metrics.commit_count,
        "parse_errors": metrics.parse_errors,
    })
}

fn log_job_attempt_completion(job: &Job, outcome: &JobExecutionOutcome) {
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

fn extract_job_payload_context(payload: &serde_json::Value) -> JobPayloadContext {
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

fn summarize_outcome(outcome: &JobExecutionOutcome) -> JobOutcomeSummary {
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

fn truncate_for_log(value: &str, max_chars: usize) -> String {
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

async fn finalize_job(
    jobs: &JobStore,
    settings: &Settings,
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
            jobs.mark_succeeded(job.id, Some(result_json.clone()))
                .await?;
            jobs.finish_attempt(
                attempt_id,
                "succeeded",
                None,
                Some(metrics_to_json(metrics.clone())),
            )
            .await?;
        }
        JobExecutionOutcome::Cancelled { reason, metrics } => {
            jobs.mark_cancelled(job.id, reason).await?;
            jobs.finish_attempt(
                attempt_id,
                "cancelled",
                Some(reason),
                Some(metrics_to_json(metrics.clone())),
            )
            .await?;
        }
        JobExecutionOutcome::Terminal {
            reason,
            kind,
            metrics,
        } => {
            jobs.mark_terminal(job.id, reason, kind).await?;
            jobs.finish_attempt(
                attempt_id,
                "failed",
                Some(reason),
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
                jobs.mark_terminal(job.id, reason, kind).await?;
                jobs.finish_attempt(
                    attempt_id,
                    "failed",
                    Some(reason),
                    Some(metrics_to_json(metrics.clone())),
                )
                .await?;
            } else {
                let run_after =
                    Utc::now() + chrono::Duration::milliseconds((*backoff_ms as i64).max(1));

                jobs.mark_retryable(
                    job.id,
                    RetryDecision {
                        reason: reason.clone(),
                        kind: kind.clone(),
                        run_after,
                    },
                )
                .await?;
                jobs.finish_attempt(
                    attempt_id,
                    "failed",
                    Some(reason),
                    Some(metrics_to_json(metrics.clone())),
                )
                .await?;
            }
        }
    }

    // Keep `scheduled` jobs moving if this finalization happened during a long processing window.
    let _ = jobs.promote_ready_jobs().await;

    // If all retries are consumed and the state remained retryable due race, clamp to terminal.
    if let Some(updated_job) = jobs.get(job.id).await?
        && updated_job.state == JobState::FailedRetryable
        && updated_job.attempt >= updated_job.max_attempts
    {
        jobs.mark_terminal(job.id, "max attempts reached", "transient")
            .await?;
    }

    // Keep backoff policy deterministic for calls that rely on worker defaults.
    let _ = nexus_db::JobStore::compute_backoff(
        settings.worker.base_backoff_ms,
        settings.worker.max_backoff_ms,
        attempt,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
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
}
