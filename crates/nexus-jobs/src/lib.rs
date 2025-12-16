use async_trait::async_trait;
use nexus_db::{Db, Job, JobStore, worker_id};
use std::cmp::min;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tracing::{error, info, warn};

pub type Result<T> = std::result::Result<T, JobRunnerError>;

#[derive(thiserror::Error, Debug)]
pub enum JobRunnerError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

#[derive(Clone)]
pub struct JobRunnerConfig {
    pub queue: String,
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub default_backoff: Duration,
}

impl Default for JobRunnerConfig {
    fn default() -> Self {
        Self {
            queue: "default".to_string(),
            concurrency: 8,
            poll_interval: Duration::from_secs(3),
            batch_size: 10,
            default_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
pub enum JobResult {
    Success,
    Retry { backoff: Duration, reason: String },
    Fail { reason: String },
}

#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    async fn handle(&self, job: Job) -> JobResult;
}

pub struct JobRunner {
    db: Db,
    cfg: JobRunnerConfig,
}

impl JobRunner {
    pub fn new(db: Db, cfg: JobRunnerConfig) -> Self {
        Self { db, cfg }
    }

    pub async fn run<H: JobHandler + Clone + Send + Sync + 'static>(
        self,
        handler: H,
    ) -> Result<()> {
        let worker_id = worker_id();
        let mut listener = self.db.listener().await?;
        listener.listen("nexus_jobs").await?;
        let semaphore = Arc::new(Semaphore::new(self.cfg.concurrency));
        let mut tick = interval(self.cfg.poll_interval);
        let handler = Arc::new(handler);

        info!(queue = %self.cfg.queue, worker_id, "job runner started");

        loop {
            tokio::select! {
                _ = listener.recv() => {
                    // Wake quickly when NOTIFY arrives.
                    self.drain(handler.clone(), &worker_id, &semaphore).await?;
                }
                _ = tick.tick() => {
                    // Poll fallback in case notifications were missed during disconnects.
                    self.drain(handler.clone(), &worker_id, &semaphore).await?;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal received");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn drain<H: JobHandler + Clone + Send + Sync + 'static>(
        &self,
        handler: Arc<H>,
        worker_id: &str,
        semaphore: &Arc<Semaphore>,
    ) -> Result<()> {
        let permits = semaphore.available_permits();
        if permits == 0 {
            return Ok(());
        }
        let take = min(permits as i64, self.cfg.batch_size);

        let store = JobStore::new(self.db.pool().clone());
        let jobs = store
            .fetch_and_claim(&self.cfg.queue, take, worker_id)
            .await?;

        for job in jobs {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let store = JobStore::new(self.db.pool().clone());
            let handler = handler.clone();

            tokio::spawn(async move {
                let outcome = handler.handle(job.clone()).await;
                match outcome {
                    JobResult::Success => {
                        if let Err(e) = store.mark_succeeded(job.id).await {
                            error!(error = %e, job_id = job.id, "mark succeeded failed");
                        }
                    }
                    JobResult::Retry { backoff, reason } => {
                        warn!(job_id = job.id, %reason, "job retry requested");
                        if let Err(e) = store.mark_failed(job.id, &reason, backoff).await {
                            error!(error = %e, job_id = job.id, "mark retry failed");
                        }
                    }
                    JobResult::Fail { reason } => {
                        error!(job_id = job.id, %reason, "job failed");
                        if let Err(e) = store
                            .mark_failed(job.id, &reason, Duration::from_secs(0))
                            .await
                        {
                            error!(error = %e, job_id = job.id, "mark fail failed");
                        }
                    }
                }
                drop(permit);
            });
        }

        Ok(())
    }
}
