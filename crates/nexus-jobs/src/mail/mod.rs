//! Mailing list sync and threading job handlers.

mod parse;
pub mod patch;
mod sync;
mod threading;

use async_trait::async_trait;
use chrono::Utc;
use nexus_db::{Db, Job, JobStore, MailingListEpochStore, MailingListStore};
use serde::Deserialize;
use sqlx::PgPool;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::threading::{ThreadingCache, build_email_threads};
use crate::{JobHandler, JobResult};

type MailJobResult<T> = Result<T, MailJobError>;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MailJobPayload {
    SyncMailingList { slug: String },
    ThreadMailingList { slug: String },
}

#[derive(Debug, thiserror::Error)]
enum MailJobError {
    #[error("mailing list not found: {0}")]
    MailingListNotFound(String),
    #[error("mirror root is not configured")]
    MissingMirrorRoot,
    #[error("mailing list is locked")]
    Locked,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("gitoxide error: {0}")]
    Gitoxide(String),
}

/// Job handler for mailing list ingest and threading work.
#[derive(Clone)]
pub struct MailJobHandler {
    db: Db,
    mirror_root: PathBuf,
    queue: String,
    default_backoff: Duration,
}

impl MailJobHandler {
    /// Create a new handler for mailing list jobs.
    pub fn new(db: Db, mirror_root: PathBuf) -> Self {
        Self {
            db,
            mirror_root,
            queue: "default".to_string(),
            default_backoff: Duration::from_secs(5),
        }
    }

    async fn handle_sync(&self, slug: &str) -> MailJobResult<SyncOutcome> {
        if self.mirror_root.as_os_str().is_empty() {
            return Err(MailJobError::MissingMirrorRoot);
        }

        let list_store = MailingListStore::new(self.db.pool().clone());
        let list = list_store
            .find_by_slug(slug)
            .await?
            .ok_or_else(|| MailJobError::MailingListNotFound(slug.to_string()))?;

        let pool = self.db.pool().clone();
        let pool_for_lock = pool.clone();
        let list_id = list.id;

        let outcome = with_list_lock(&pool_for_lock, list_id, move || {
            let pool = pool.clone();
            let list = list.clone();
            async move {
                let list_store = MailingListStore::new(pool.clone());
                let epoch_store = MailingListEpochStore::new(pool.clone());
                let ingest_store = nexus_db::MailIngestStore::new(pool.clone());
                let epochs = epoch_store.list_by_mailing_list(list_id).await?;

                let mut total_commits = 0usize;
                let mut total_emails = 0usize;

                for epoch in epochs {
                    let repo_path = self.mirror_root.join(&list.slug).join(&epoch.repo_relpath);
                    if !repo_path.exists() {
                        warn!(
                            list = %list.slug,
                            epoch = epoch.epoch,
                            repo = %repo_path.display(),
                            "mirror repo missing"
                        );
                        continue;
                    }

                    let (commits, _emails) = sync::sync_epoch(
                        &repo_path,
                        &ingest_store,
                        &epoch_store,
                        epoch,
                        list_id,
                        &list.slug,
                        &mut total_emails,
                    )
                    .await?;
                    total_commits += commits;
                }

                list_store
                    .update_last_synced_at(list_id, Utc::now())
                    .await?;

                Ok(SyncOutcome {
                    commits_processed: total_commits,
                    emails_ingested: total_emails,
                })
            }
        })
        .await?;

        Ok(outcome)
    }

    async fn handle_thread(&self, slug: &str) -> MailJobResult<ThreadOutcome> {
        let list_store = MailingListStore::new(self.db.pool().clone());
        let list = list_store
            .find_by_slug(slug)
            .await?
            .ok_or_else(|| MailJobError::MailingListNotFound(slug.to_string()))?;
        let pool = self.db.pool().clone();
        let pool_for_lock = pool.clone();
        let list_id = list.id;

        let outcome = with_list_lock(&pool_for_lock, list_id, move || {
            let pool = pool.clone();
            async move {
                let cache = ThreadingCache::load(&pool, list_id).await?;
                let threads = build_email_threads(cache.email_data, cache.references);
                let (thread_count, _membership_count) =
                    threading::persist_threads(&pool, list_id, threads).await?;
                list_store
                    .update_last_threaded_at(list_id, Utc::now())
                    .await?;

                Ok(ThreadOutcome {
                    threads_created: thread_count,
                })
            }
        })
        .await?;

        Ok(outcome)
    }

    async fn enqueue_thread_job(&self, slug: &str) -> MailJobResult<()> {
        let store = JobStore::new(self.db.pool().clone());
        let payload = serde_json::json!({
            "type": "thread_mailing_list",
            "slug": slug,
        });
        store.enqueue(&self.queue, payload, None, None).await?;
        Ok(())
    }
}

#[async_trait]
impl JobHandler for MailJobHandler {
    async fn handle(&self, job: Job) -> JobResult {
        let payload: MailJobPayload = match serde_json::from_value(job.payload.clone()) {
            Ok(payload) => payload,
            Err(err) => {
                return JobResult::Fail {
                    reason: format!("invalid payload: {err}"),
                };
            }
        };

        let result = match payload {
            MailJobPayload::SyncMailingList { slug } => match self.handle_sync(&slug).await {
                Ok(outcome) => {
                    info!(
                        list = %slug,
                        commits = outcome.commits_processed,
                        emails = outcome.emails_ingested,
                        "mailing list sync complete"
                    );
                    if let Err(err) = self.enqueue_thread_job(&slug).await {
                        error!(list = %slug, error = %err, "failed to enqueue threading job");
                    }
                    Ok(())
                }
                Err(err) => Err(err),
            },
            MailJobPayload::ThreadMailingList { slug } => match self.handle_thread(&slug).await {
                Ok(outcome) => {
                    info!(
                        list = %slug,
                        threads = outcome.threads_created,
                        "mailing list threading complete"
                    );
                    Ok(())
                }
                Err(err) => Err(err),
            },
        };

        match result {
            Ok(()) => JobResult::Success,
            Err(MailJobError::Locked) => JobResult::Retry {
                backoff: self.default_backoff,
                reason: "mailing list locked".to_string(),
            },
            Err(MailJobError::MailingListNotFound(slug)) => JobResult::Fail {
                reason: format!("mailing list not found: {slug}"),
            },
            Err(err) => JobResult::Retry {
                backoff: self.default_backoff,
                reason: err.to_string(),
            },
        }
    }
}

struct SyncOutcome {
    commits_processed: usize,
    emails_ingested: usize,
}

struct ThreadOutcome {
    threads_created: usize,
}

async fn with_list_lock<F, Fut, T>(pool: &PgPool, mailing_list_id: i32, f: F) -> MailJobResult<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = MailJobResult<T>>,
{
    let key = mailing_list_id as i64;
    let mut conn = pool.acquire().await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await?;
    if !locked {
        return Err(MailJobError::Locked);
    }

    let result = f().await;

    let _ = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await;

    result
}
