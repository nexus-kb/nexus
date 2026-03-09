use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};
use utoipa::ToSchema;

use crate::{Job, JobState, Result};

const UNIQUE_VIOLATION_SQLSTATE: &str = "23505";
const RUNNING_ATTEMPT_CONSTRAINT: &str = "idx_job_attempts_one_running_per_job";

#[derive(Debug, Clone)]
pub struct EnqueueJobParams {
    pub job_type: String,
    pub payload_json: serde_json::Value,
    pub priority: i32,
    pub dedupe_scope: Option<String>,
    pub dedupe_key: Option<String>,
    pub run_after: Option<DateTime<Utc>>,
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct ListJobsParams {
    pub state: Option<JobState>,
    pub job_type: Option<String>,
    pub limit: i64,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct JobAttempt {
    pub id: i64,
    pub job_id: i64,
    pub attempt: i32,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub error: Option<String>,
    pub metrics_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct JobStateCount {
    pub state: JobState,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct JobTypeStateCount {
    pub job_type: String,
    pub total: i64,
    pub scheduled: i64,
    pub queued: i64,
    pub running: i64,
    pub succeeded: i64,
    pub failed_retryable: i64,
    pub failed_terminal: i64,
    pub cancelled: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct RunningJobSnapshot {
    pub id: i64,
    pub job_type: String,
    pub claimed_by: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub is_stuck: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JobStoreMetrics {
    pub duration_ms: u128,
    pub rows_written: u64,
    pub bytes_read: u64,
    pub commit_count: u64,
    pub parse_errors: u64,
}

#[derive(Debug, Clone)]
pub struct RetryDecision {
    pub reason: String,
    pub kind: String,
    pub run_after: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum RetryJobResult {
    Updated(Box<Job>),
    RunningConflict,
    NotFound,
}

#[derive(Clone)]
pub struct JobStore {
    pool: PgPool,
}

impl JobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(&self, params: EnqueueJobParams) -> Result<Job> {
        let dedupe_scope = params.dedupe_scope.unwrap_or_else(|| "global".to_string());
        let max_attempts = params.max_attempts.unwrap_or(8);
        let run_after = params.run_after.unwrap_or_else(Utc::now);
        let state = if run_after > Utc::now() {
            JobState::Scheduled
        } else {
            JobState::Queued
        };

        let job = if let Some(dedupe_key) = params.dedupe_key {
            sqlx::query_as::<_, Job>(
                r#"INSERT INTO jobs
                (job_type, state, priority, run_after, dedupe_scope, dedupe_key, max_attempts, payload_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (job_type, dedupe_scope, dedupe_key)
                DO UPDATE SET updated_at = now()
                RETURNING *"#,
            )
            .bind(&params.job_type)
            .bind(state)
            .bind(params.priority)
            .bind(run_after)
            .bind(&dedupe_scope)
            .bind(dedupe_key)
            .bind(max_attempts)
            .bind(&params.payload_json)
            .fetch_one(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, Job>(
                r#"INSERT INTO jobs
                (job_type, state, priority, run_after, dedupe_scope, max_attempts, payload_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING *"#,
            )
            .bind(&params.job_type)
            .bind(state)
            .bind(params.priority)
            .bind(run_after)
            .bind(&dedupe_scope)
            .bind(max_attempts)
            .bind(&params.payload_json)
            .fetch_one(&self.pool)
            .await?
        };

        let _ = sqlx::query("SELECT pg_notify('nexus_jobs', $1)")
            .bind(&job.job_type)
            .execute(&self.pool)
            .await;

        Ok(job)
    }

    pub async fn list(&self, params: ListJobsParams) -> Result<Vec<Job>> {
        let mut qb = QueryBuilder::new("SELECT * FROM jobs WHERE true");

        if let Some(state) = params.state {
            qb.push(" AND state = ").push_bind(state);
        }
        if let Some(job_type) = params.job_type {
            qb.push(" AND job_type = ").push_bind(job_type);
        }
        if let Some(cursor) = params.cursor {
            qb.push(" AND id < ").push_bind(cursor);
        }

        qb.push(" ORDER BY id DESC LIMIT ")
            .push_bind(params.limit.clamp(1, 500));

        qb.build_query_as::<Job>().fetch_all(&self.pool).await
    }

    pub async fn get(&self, job_id: i64) -> Result<Option<Job>> {
        sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn list_attempts(
        &self,
        job_id: i64,
        limit: i64,
        cursor: Option<i64>,
    ) -> Result<Vec<JobAttempt>> {
        let mut qb = QueryBuilder::new(
            r#"SELECT id, job_id, attempt, started_at, finished_at, status, error, metrics_json
            FROM job_attempts
            WHERE job_id = "#,
        );
        qb.push_bind(job_id);
        if let Some(cursor) = cursor {
            qb.push(" AND id < ").push_bind(cursor);
        }
        qb.push(" ORDER BY attempt DESC, id DESC LIMIT ")
            .push_bind(clamp_attempt_limit(limit));

        qb.build_query_as::<JobAttempt>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn list_state_counts(&self) -> Result<Vec<JobStateCount>> {
        sqlx::query_as::<_, JobStateCount>(
            r#"SELECT state, COUNT(*)::bigint AS count
            FROM jobs
            GROUP BY state"#,
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_job_type_state_counts(&self, limit: i64) -> Result<Vec<JobTypeStateCount>> {
        sqlx::query_as::<_, JobTypeStateCount>(
            r#"SELECT
                job_type,
                COUNT(*)::bigint AS total,
                COUNT(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled,
                COUNT(*) FILTER (WHERE state = 'queued')::bigint AS queued,
                COUNT(*) FILTER (WHERE state = 'running')::bigint AS running,
                COUNT(*) FILTER (WHERE state = 'succeeded')::bigint AS succeeded,
                COUNT(*) FILTER (WHERE state = 'failed_retryable')::bigint AS failed_retryable,
                COUNT(*) FILTER (WHERE state = 'failed_terminal')::bigint AS failed_terminal,
                COUNT(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled
            FROM jobs
            GROUP BY job_type
            ORDER BY total DESC, job_type ASC
            LIMIT $1"#,
        )
        .bind(clamp_job_type_limit(limit))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_running_jobs(&self, limit: i64) -> Result<Vec<RunningJobSnapshot>> {
        sqlx::query_as::<_, RunningJobSnapshot>(
            r#"SELECT
                id,
                job_type,
                claimed_by,
                lease_until,
                attempt,
                max_attempts,
                CASE
                    WHEN lease_until IS NULL THEN false
                    WHEN lease_until < now() THEN true
                    ELSE false
                END AS is_stuck
            FROM jobs
            WHERE state = 'running'
            ORDER BY priority DESC, id ASC
            LIMIT $1"#,
        )
        .bind(clamp_running_limit(limit))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn request_cancel(&self, job_id: i64) -> Result<Option<Job>> {
        sqlx::query_as::<_, Job>(
            r#"UPDATE jobs
            SET state = CASE WHEN state IN ('queued', 'scheduled', 'failed_retryable') THEN 'cancelled'::job_state ELSE state END,
                cancel_requested = CASE WHEN state = 'running' THEN true ELSE cancel_requested END,
                updated_at = now()
            WHERE id = $1
            RETURNING *"#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn retry(&self, job_id: i64, run_after: DateTime<Utc>) -> Result<RetryJobResult> {
        let mut tx = self.pool.begin().await?;
        let state = sqlx::query_scalar::<_, JobState>(
            r#"SELECT state
            FROM jobs
            WHERE id = $1
            FOR UPDATE"#,
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(state) = state else {
            tx.rollback().await?;
            return Ok(RetryJobResult::NotFound);
        };

        if state == JobState::Running {
            tx.rollback().await?;
            return Ok(RetryJobResult::RunningConflict);
        }

        let job = sqlx::query_as::<_, Job>(
            r#"UPDATE jobs
            SET state = CASE WHEN $2 > now() THEN 'scheduled'::job_state ELSE 'queued'::job_state END,
                run_after = $2,
                claimed_by = NULL,
                lease_until = NULL,
                cancel_requested = false,
                updated_at = now(),
                last_error = NULL,
                last_error_kind = NULL
            WHERE id = $1
            RETURNING *"#,
        )
        .bind(job_id)
        .bind(run_after)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(RetryJobResult::Updated(Box::new(job)))
    }

    pub async fn promote_ready_jobs(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"UPDATE jobs
            SET state = 'queued', updated_at = now()
            WHERE state IN ('scheduled', 'failed_retryable')
              AND run_after <= now()"#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn requeue_stuck_jobs(&self) -> Result<(u64, u64)> {
        let requeued = sqlx::query(
            r#"UPDATE jobs
            SET state = 'queued',
                claimed_by = NULL,
                lease_until = NULL,
                updated_at = now(),
                last_error = 'lease expired',
                last_error_kind = 'transient'
            WHERE state = 'running'
              AND lease_until < now()
              AND attempt < max_attempts"#,
        )
        .execute(&self.pool)
        .await?
        .rows_affected();

        let terminal = sqlx::query(
            r#"UPDATE jobs
            SET state = 'failed_terminal',
                claimed_by = NULL,
                lease_until = NULL,
                updated_at = now(),
                last_error = 'lease expired and max attempts reached',
                last_error_kind = 'transient'
            WHERE state = 'running'
              AND lease_until < now()
              AND attempt >= max_attempts"#,
        )
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok((requeued, terminal))
    }

    pub async fn claim_jobs(&self, limit: i64, worker_id: &str, lease_ms: i64) -> Result<Vec<Job>> {
        sqlx::query_as::<_, Job>(
            r#"WITH picked AS (
                SELECT id
                FROM jobs
                WHERE state = 'queued'
                  AND run_after <= now()
                ORDER BY priority DESC, run_after ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE jobs j
            SET state = 'running',
                claimed_by = $2,
                lease_until = now() + ($3::bigint * interval '1 millisecond'),
                attempt = attempt + 1,
                updated_at = now()
            FROM picked
            WHERE j.id = picked.id
            RETURNING j.*"#,
        )
        .bind(limit)
        .bind(worker_id)
        .bind(lease_ms)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn heartbeat(&self, job_id: i64, worker_id: &str, lease_ms: i64) -> Result<bool> {
        let result = sqlx::query(
            r#"UPDATE jobs
            SET lease_until = now() + ($3::bigint * interval '1 millisecond'),
                updated_at = now()
            WHERE id = $1
              AND state = 'running'
              AND claimed_by = $2"#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(lease_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn is_cancel_requested(&self, job_id: i64) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT cancel_requested FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&self.pool)
            .await
    }

    pub async fn start_attempt(&self, job_id: i64, attempt: i32) -> Result<JobAttempt> {
        sqlx::query_as::<_, JobAttempt>(
            r#"INSERT INTO job_attempts (job_id, attempt, status)
            VALUES ($1, $2, 'running')
            RETURNING id, job_id, attempt, started_at, finished_at, status, error, metrics_json"#,
        )
        .bind(job_id)
        .bind(attempt)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn finish_attempt(
        &self,
        attempt_id: i64,
        status: &str,
        error: Option<&str>,
        metrics_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE job_attempts
            SET finished_at = now(),
                status = $2,
                error = $3,
                metrics_json = $4
            WHERE id = $1"#,
        )
        .bind(attempt_id)
        .bind(status)
        .bind(error)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn finalize_succeeded_attempt(
        &self,
        job_id: i64,
        attempt_id: i64,
        result_json: Option<serde_json::Value>,
        metrics_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"WITH updated_job AS (
                UPDATE jobs
                SET state = 'succeeded',
                    result_json = $3,
                    claimed_by = NULL,
                    lease_until = NULL,
                    cancel_requested = false,
                    updated_at = now()
                WHERE id = $1
                RETURNING id
            )
            UPDATE job_attempts ja
            SET finished_at = now(),
                status = 'succeeded',
                error = NULL,
                metrics_json = $4
            WHERE ja.id = $2
              AND ja.job_id IN (SELECT id FROM updated_job)"#,
        )
        .bind(job_id)
        .bind(attempt_id)
        .bind(result_json)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finalize_cancelled_attempt(
        &self,
        job_id: i64,
        attempt_id: i64,
        reason: &str,
        metrics_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"WITH updated_job AS (
                UPDATE jobs
                SET state = 'cancelled',
                    claimed_by = NULL,
                    lease_until = NULL,
                    updated_at = now(),
                    last_error = $3,
                    last_error_kind = 'cancelled'
                WHERE id = $1
                RETURNING id
            )
            UPDATE job_attempts ja
            SET finished_at = now(),
                status = 'cancelled',
                error = $3,
                metrics_json = $4
            WHERE ja.id = $2
              AND ja.job_id IN (SELECT id FROM updated_job)"#,
        )
        .bind(job_id)
        .bind(attempt_id)
        .bind(reason)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finalize_terminal_attempt(
        &self,
        job_id: i64,
        attempt_id: i64,
        reason: &str,
        kind: &str,
        metrics_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"WITH updated_job AS (
                UPDATE jobs
                SET state = 'failed_terminal',
                    claimed_by = NULL,
                    lease_until = NULL,
                    updated_at = now(),
                    last_error = $3,
                    last_error_kind = $4
                WHERE id = $1
                RETURNING id
            )
            UPDATE job_attempts ja
            SET finished_at = now(),
                status = 'failed',
                error = $3,
                metrics_json = $5
            WHERE ja.id = $2
              AND ja.job_id IN (SELECT id FROM updated_job)"#,
        )
        .bind(job_id)
        .bind(attempt_id)
        .bind(reason)
        .bind(kind)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finalize_retryable_attempt(
        &self,
        job_id: i64,
        attempt_id: i64,
        retry: RetryDecision,
        metrics_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"WITH updated_job AS (
                UPDATE jobs
                SET state = 'failed_retryable',
                    run_after = $3,
                    claimed_by = NULL,
                    lease_until = NULL,
                    updated_at = now(),
                    last_error = $4,
                    last_error_kind = $5
                WHERE id = $1
                RETURNING id
            )
            UPDATE job_attempts ja
            SET finished_at = now(),
                status = 'failed',
                error = $4,
                metrics_json = $6
            WHERE ja.id = $2
              AND ja.job_id IN (SELECT id FROM updated_job)"#,
        )
        .bind(job_id)
        .bind(attempt_id)
        .bind(retry.run_after)
        .bind(retry.reason)
        .bind(retry.kind)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_succeeded(
        &self,
        job_id: i64,
        result_json: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE jobs
            SET state = 'succeeded',
                result_json = $2,
                claimed_by = NULL,
                lease_until = NULL,
                cancel_requested = false,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_retryable(&self, job_id: i64, retry: RetryDecision) -> Result<()> {
        sqlx::query(
            r#"UPDATE jobs
            SET state = 'failed_retryable',
                run_after = $2,
                claimed_by = NULL,
                lease_until = NULL,
                updated_at = now(),
                last_error = $3,
                last_error_kind = $4
            WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(retry.run_after)
        .bind(retry.reason)
        .bind(retry.kind)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_terminal(&self, job_id: i64, reason: &str, kind: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE jobs
            SET state = 'failed_terminal',
                claimed_by = NULL,
                lease_until = NULL,
                updated_at = now(),
                last_error = $2,
                last_error_kind = $3
            WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(reason)
        .bind(kind)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_cancelled(&self, job_id: i64, reason: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE jobs
            SET state = 'cancelled',
                claimed_by = NULL,
                lease_until = NULL,
                updated_at = now(),
                last_error = $2,
                last_error_kind = 'cancelled'
            WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn compute_backoff(base_ms: u64, max_ms: u64, attempt: i32) -> Duration {
        let exponent = u32::try_from(attempt.max(1))
            .unwrap_or(u32::MAX)
            .saturating_sub(1);
        let raw = base_ms.saturating_mul(2u64.saturating_pow(exponent));
        let max_i64_ms = u64::try_from(i64::MAX).unwrap_or(u64::MAX);
        let bounded_ms = raw.min(max_ms).min(max_i64_ms);
        let millis = i64::try_from(bounded_ms).unwrap_or(i64::MAX);
        Duration::milliseconds(millis)
    }
}

pub fn is_running_attempt_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err.code().as_deref() == Some(UNIQUE_VIOLATION_SQLSTATE)
                && db_err.constraint() == Some(RUNNING_ATTEMPT_CONSTRAINT)
        }
        _ => false,
    }
}

fn clamp_attempt_limit(limit: i64) -> i64 {
    limit.clamp(1, 200)
}

fn clamp_job_type_limit(limit: i64) -> i64 {
    limit.clamp(1, 500)
}

fn clamp_running_limit(limit: i64) -> i64 {
    limit.clamp(1, 200)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use nexus_core::config::DatabaseConfig;

    use crate::Db;

    use super::{RetryJobResult, clamp_attempt_limit, clamp_job_type_limit, clamp_running_limit};

    #[test]
    fn attempt_limit_is_clamped() {
        assert_eq!(clamp_attempt_limit(0), 1);
        assert_eq!(clamp_attempt_limit(1), 1);
        assert_eq!(clamp_attempt_limit(50), 50);
        assert_eq!(clamp_attempt_limit(500), 200);
    }

    #[test]
    fn job_type_limit_is_clamped() {
        assert_eq!(clamp_job_type_limit(0), 1);
        assert_eq!(clamp_job_type_limit(500), 500);
        assert_eq!(clamp_job_type_limit(501), 500);
    }

    #[test]
    fn running_limit_is_clamped() {
        assert_eq!(clamp_running_limit(0), 1);
        assert_eq!(clamp_running_limit(25), 25);
        assert_eq!(clamp_running_limit(1000), 200);
    }

    #[tokio::test]
    async fn retry_rejects_running_jobs() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let db = Db::connect(&DatabaseConfig {
            url: database_url,
            max_connections: 4,
        })
        .await?;
        db.migrate().await?;

        let jobs = super::JobStore::new(db.pool().clone());
        let unique = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let job = jobs
            .enqueue(super::EnqueueJobParams {
                job_type: "test_retry_running_conflict".to_string(),
                payload_json: serde_json::json!({ "test": unique }),
                priority: 1,
                dedupe_scope: Some(format!("tests:{unique}")),
                dedupe_key: Some("retry-conflict".to_string()),
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

        let result = jobs.retry(job.id, Utc::now()).await?;
        assert!(matches!(result, RetryJobResult::RunningConflict));

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id)
            .execute(db.pool())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn running_attempt_is_closed_when_job_requeues_from_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let db = Db::connect(&DatabaseConfig {
            url: database_url,
            max_connections: 4,
        })
        .await?;
        db.migrate().await?;

        let jobs = super::JobStore::new(db.pool().clone());
        let unique = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let job = jobs
            .enqueue(super::EnqueueJobParams {
                job_type: "test_requeue_attempt_finalization".to_string(),
                payload_json: serde_json::json!({ "test": unique }),
                priority: 1,
                dedupe_scope: Some(format!("tests:{unique}")),
                dedupe_key: Some("requeue-attempt-finalization".to_string()),
                run_after: None,
                max_attempts: Some(4),
            })
            .await?;

        sqlx::query(
            r#"UPDATE jobs
            SET state = 'running',
                claimed_by = 'test-worker',
                lease_until = now() - interval '2 minutes',
                attempt = 1,
                last_error = NULL,
                last_error_kind = NULL
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

        let (requeued, terminal) = jobs.requeue_stuck_jobs().await?;
        assert_eq!(requeued, 1);
        assert_eq!(terminal, 0);

        let first_attempt: super::JobAttempt = sqlx::query_as(
            r#"SELECT id, job_id, attempt, started_at, finished_at, status, error, metrics_json
            FROM job_attempts
            WHERE job_id = $1 AND attempt = 1"#,
        )
        .bind(job.id)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(first_attempt.status, "failed");
        assert!(first_attempt.finished_at.is_some());
        assert_eq!(first_attempt.error.as_deref(), Some("lease expired"));

        let claimed = jobs.claim_jobs(1, "test-worker-2", 45_000).await?;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job.id);
        assert_eq!(claimed[0].attempt, 2);

        let second_attempt = jobs.start_attempt(job.id, claimed[0].attempt).await?;
        assert_eq!(second_attempt.attempt, 2);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id)
            .execute(db.pool())
            .await?;

        Ok(())
    }
}
