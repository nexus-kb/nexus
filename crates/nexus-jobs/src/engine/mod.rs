use std::time::Duration;

use chrono::Utc;
use nexus_core::config::Settings;
use nexus_db::{
    CatalogStore, Db, EmbeddingsStore, IngestStore, Job, JobState, JobStore, JobStoreMetrics,
    LineageStore, PipelineStore, RetryDecision, SearchStore, ThreadingStore,
};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info, warn};

use crate::JobExecutionOutcome;
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
}

impl From<&nexus_core::config::WorkerConfig> for WorkerConfig {
    fn from(value: &nexus_core::config::WorkerConfig) -> Self {
        Self {
            poll_ms: value.poll_ms,
            claim_batch: value.claim_batch,
            lease_ms: value.lease_ms,
            heartbeat_ms: value.heartbeat_ms,
            sweep_ms: value.sweep_ms,
            max_inflight_jobs: value.max_inflight_jobs.max(1),
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
    pipeline: PipelineStore,
    handler: Phase0JobHandler,
    cfg: WorkerConfig,
    worker_id: String,
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
            pipeline.clone(),
            search,
            embeddings,
            jobs.clone(),
        );
        let cfg = WorkerConfig::from(&settings.worker);

        Self {
            settings,
            db,
            jobs,
            pipeline,
            handler,
            cfg,
            worker_id: format!("phase0-worker-{}", std::process::id()),
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
        let collapsed_pending = self.pipeline.collapse_duplicate_pending_runs().await?;

        if promoted > 0 || requeued > 0 || terminal > 0 || collapsed_pending > 0 {
            info!(
                promoted,
                requeued, terminal, collapsed_pending, "job maintenance sweep"
            );
        }

        // Recovery path for orphaned pending pipeline runs (for example after overlapping
        // webhook triggers): if no run is currently active, kick one pending run.
        if self.pipeline.get_any_active_run().await?.is_none() {
            let activated = self.handler.activate_and_enqueue_next_list_global().await?;
            if activated {
                info!("activated pending pipeline run from global queue during maintenance");
            }
        }

        Ok(())
    }

    async fn drain_once(&self) -> Result<(), sqlx::Error> {
        self.jobs.promote_ready_jobs().await?;
        let max_inflight_i64 = i64::try_from(self.cfg.max_inflight_jobs).unwrap_or(i64::MAX);
        let claim_limit = self.cfg.claim_batch.max(1).min(max_inflight_i64);
        let claimed = self
            .jobs
            .claim_jobs(claim_limit, &self.worker_id, self.cfg.lease_ms)
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
            if let Err(err) = self.finalize_pipeline_run_if_needed(&job, &outcome).await {
                error!(
                    job_id = job.id,
                    error = %err,
                    "failed to finalize pipeline run after pre-execution cancellation"
                );
            }
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

        let finalize_result = finalize_job(
            &self.jobs,
            &self.settings,
            &job,
            &outcome,
            attempt.id,
            job.attempt,
            job.max_attempts,
        )
        .await;
        match finalize_result {
            Ok(()) => {
                if let Err(err) = self.finalize_pipeline_run_if_needed(&job, &outcome).await {
                    error!(
                        job_id = job.id,
                        error = %err,
                        "failed to finalize pipeline run after job finalization"
                    );
                }
            }
            Err(err) => {
                error!(job_id = job.id, error = %err, "failed to finalize job result");
            }
        }

        log_job_attempt_completion(&job, &outcome);
    }

    async fn finalize_pipeline_run_if_needed(
        &self,
        job: &Job,
        outcome: &JobExecutionOutcome,
    ) -> Result<(), sqlx::Error> {
        if !is_pipeline_job_type(&job.job_type) {
            return Ok(());
        }
        let Some(run_id) = job
            .payload_json
            .get("run_id")
            .and_then(serde_json::Value::as_i64)
        else {
            return Ok(());
        };

        let run = match self.pipeline.get_run(run_id).await? {
            Some(run) => run,
            None => return Ok(()),
        };
        if run.state != "running" {
            return Ok(());
        }

        match outcome {
            JobExecutionOutcome::Terminal { reason, .. } => {
                self.mark_pipeline_run_failed_and_continue(&run, &job.job_type, reason)
                    .await?;
            }
            JobExecutionOutcome::Retryable { reason, .. } if job.attempt >= job.max_attempts => {
                self.mark_pipeline_run_failed_and_continue(&run, &job.job_type, reason)
                    .await?;
            }
            JobExecutionOutcome::Cancelled { reason, .. } => {
                self.mark_pipeline_run_cancelled_and_continue(&run, &job.job_type, reason)
                    .await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn mark_pipeline_run_failed_and_continue(
        &self,
        run: &nexus_db::PipelineRun,
        job_type: &str,
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        let failure_reason = format!("stage {job_type} failed: {reason}");
        self.pipeline
            .mark_run_failed(run.id, &failure_reason)
            .await?;
        if let Some(batch_id) = run.batch_id {
            self.handler
                .activate_and_enqueue_next_list(batch_id)
                .await?;
        }
        Ok(())
    }

    async fn mark_pipeline_run_cancelled_and_continue(
        &self,
        run: &nexus_db::PipelineRun,
        job_type: &str,
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        let cancel_reason = format!("stage {job_type} cancelled: {reason}");
        self.pipeline
            .mark_run_cancelled(run.id, &cancel_reason)
            .await?;
        if let Some(batch_id) = run.batch_id {
            self.handler
                .activate_and_enqueue_next_list(batch_id)
                .await?;
        }
        Ok(())
    }
}

mod helpers;
#[cfg(test)]
mod tests;

use helpers::*;
