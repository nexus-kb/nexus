use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::Duration;

use crate::Result;

/// Background job record stored in Postgres.
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Job {
    pub id: i64,
    pub queue: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub run_at: DateTime<Utc>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Aggregate count per job status.
#[derive(Debug, Serialize)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

/// Summary statistics for the jobs table.
#[derive(Debug, Serialize)]
pub struct JobStats {
    pub total: i64,
    pub by_status: Vec<StatusCount>,
}

/// Job persistence and queue helpers.
#[derive(Clone)]
pub struct JobStore {
    pool: PgPool,
}

impl JobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(
        &self,
        queue: &str,
        payload: serde_json::Value,
        run_at: Option<DateTime<Utc>>,
        max_attempts: Option<i32>,
    ) -> Result<Job> {
        let job = sqlx::query_as::<_, Job>(
            r#"INSERT INTO jobs (queue, payload, run_at, max_attempts)
            VALUES ($1, $2, COALESCE($3, NOW()), COALESCE($4, 5))
            RETURNING id, queue, payload, status, run_at, attempts, max_attempts, locked_at, locked_by, last_error, created_at, updated_at
            "#,
        )
        .bind(queue)
        .bind(payload)
        .bind(run_at)
        .bind(max_attempts)
        .fetch_one(&self.pool)
        .await?;

        // Notify listeners that work is available; payload is just the queue name to keep NOTIFY cheap.
        let _ = sqlx::query("SELECT pg_notify('nexus_jobs', $1)")
            .bind(queue)
            .execute(&self.pool)
            .await?;

        Ok(job)
    }

    pub async fn fetch_and_claim(
        &self,
        queue: &str,
        limit: i64,
        worker_id: &str,
    ) -> Result<Vec<Job>> {
        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;

        // Reserve a batch of due jobs using SKIP LOCKED so multiple workers can safely compete.
        let jobs = sqlx::query_as::<_, Job>(
            r#"
            WITH grabbed AS (
                SELECT id
                FROM jobs
                WHERE status = 'queued'
                  AND queue = $1
                  AND run_at <= NOW()
                ORDER BY run_at
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            ), updated AS (
                UPDATE jobs j
                SET status = 'running', locked_at = NOW(), locked_by = $3
                WHERE j.id IN (SELECT id FROM grabbed)
                RETURNING j.*
            )
            SELECT * FROM updated
            "#,
        )
        .bind(queue)
        .bind(limit)
        .bind(worker_id)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(jobs)
    }

    pub async fn mark_succeeded(&self, job_id: i64) -> Result<()> {
        sqlx::query("UPDATE jobs SET status = 'succeeded', locked_at = NULL, locked_by = NULL WHERE id = $1")
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, job_id: i64, error: &str, backoff: Duration) -> Result<()> {
        // Increment attempts, requeue with backoff if we have retries left, otherwise mark failed.
        sqlx::query(
            r#"
            UPDATE jobs
            SET attempts = attempts + 1,
                status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'queued' END,
                run_at = CASE WHEN attempts + 1 >= max_attempts THEN run_at ELSE NOW() + ($2::bigint * INTERVAL '1 millisecond') END,
                locked_at = NULL,
                locked_by = NULL,
                last_error = $1
            WHERE id = $3
            "#,
        )
        .bind(error)
        .bind(backoff.as_millis() as i64)
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_id(&self, job_id: i64) -> Result<Option<Job>> {
        sqlx::query_as::<_, Job>(r#"SELECT * FROM jobs WHERE id = $1"#)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn list(&self, status: Option<&str>, limit: i64, offset: i64) -> Result<Vec<Job>> {
        if let Some(status) = status {
            sqlx::query_as::<_, Job>(
                r#"SELECT * FROM jobs WHERE status = $1 ORDER BY id DESC LIMIT $2 OFFSET $3"#,
            )
            .bind(status)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, Job>(r#"SELECT * FROM jobs ORDER BY id DESC LIMIT $1 OFFSET $2"#)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
        }
    }

    pub async fn stats(&self) -> Result<JobStats> {
        let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM jobs")
            .fetch_one(&self.pool)
            .await?;

        let by_status =
            sqlx::query(r#"SELECT status, COUNT(*) as count FROM jobs GROUP BY status"#)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| StatusCount {
                    status: row.get::<String, _>("status"),
                    count: row.get::<i64, _>("count"),
                })
                .collect();

        Ok(JobStats { total, by_status })
    }
}

/// Send a NOTIFY to wake queue listeners.
pub async fn notify_queue(pool: &PgPool, queue: &str) -> Result<()> {
    sqlx::query("SELECT pg_notify('nexus_jobs', $1)")
        .bind(queue)
        .execute(pool)
        .await?;
    Ok(())
}

/// Human-friendly worker ID based on the current process id.
pub fn worker_id() -> String {
    format!("worker-{}", std::process::id())
}
