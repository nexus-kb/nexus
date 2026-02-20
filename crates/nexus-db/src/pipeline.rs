use chrono::{DateTime, Utc};
use sqlx::{PgPool, QueryBuilder};

use crate::{PipelineRun, Result};

const UNIQUE_VIOLATION_SQLSTATE: &str = "23505";
const ACTIVATION_RETRY_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, Default)]
pub struct ListPipelineRunsParams {
    pub list_key: Option<String>,
    pub state: Option<String>,
    pub limit: i64,
    pub cursor: Option<i64>,
}

#[derive(Clone)]
pub struct PipelineStore {
    pool: PgPool,
}

impl PipelineStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    // ── Run lifecycle ──────────────────────────────────────────────

    /// Create a new pending run positioned in a batch.
    pub async fn create_pending_run(
        &self,
        mailing_list_id: i64,
        list_key: &str,
        source: &str,
        batch_id: i64,
        batch_position: i32,
    ) -> Result<PipelineRun> {
        match sqlx::query_as::<_, PipelineRun>(
            r#"INSERT INTO pipeline_runs
            (mailing_list_id, list_key, state, current_stage, source, batch_id, batch_position)
            VALUES ($1, $2, 'pending', 'ingest', $3, $4, $5)
            RETURNING *"#,
        )
        .bind(mailing_list_id)
        .bind(list_key)
        .bind(source)
        .bind(batch_id)
        .bind(batch_position)
        .fetch_one(&self.pool)
        .await
        {
            Ok(run) => Ok(run),
            Err(err) if is_unique_violation(&err) => {
                if let Some(existing) = self.get_open_run_for_list(list_key).await? {
                    Ok(existing)
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Create a run that starts immediately in `running` state (for admin single-list sync).
    pub async fn create_running_run(
        &self,
        mailing_list_id: i64,
        list_key: &str,
        source: &str,
    ) -> Result<PipelineRun> {
        match sqlx::query_as::<_, PipelineRun>(
            r#"INSERT INTO pipeline_runs
            (mailing_list_id, list_key, state, current_stage, source)
            VALUES ($1, $2, 'running', 'ingest', $3)
            RETURNING *"#,
        )
        .bind(mailing_list_id)
        .bind(list_key)
        .bind(source)
        .fetch_one(&self.pool)
        .await
        {
            Ok(run) => Ok(run),
            Err(err) if is_unique_violation(&err) => {
                if let Some(existing) = self.get_open_run_for_list(list_key).await? {
                    Ok(existing)
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Activate the next pending run in a batch: set to running and return it.
    pub async fn activate_next_pending_run(&self, batch_id: i64) -> Result<Option<PipelineRun>> {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            match sqlx::query_as::<_, PipelineRun>(
                r#"UPDATE pipeline_runs
                SET state = 'running',
                    started_at = now(),
                    updated_at = now()
                WHERE id = (
                    SELECT pr.id
                    FROM pipeline_runs pr
                    WHERE pr.batch_id = $1
                      AND pr.state = 'pending'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pipeline_runs active
                          WHERE active.mailing_list_id = pr.mailing_list_id
                            AND active.state = 'running'
                      )
                    ORDER BY pr.batch_position ASC, pr.id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *"#,
            )
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            {
                Ok(run) => return Ok(run),
                Err(err) if is_unique_violation(&err) && attempts < ACTIVATION_RETRY_ATTEMPTS => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Activate the next pending run globally (oldest by batch/id) and return it.
    pub async fn activate_next_pending_run_any(&self) -> Result<Option<PipelineRun>> {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            match sqlx::query_as::<_, PipelineRun>(
                r#"UPDATE pipeline_runs
                SET state = 'running',
                    started_at = now(),
                    updated_at = now()
                WHERE id = (
                    SELECT pr.id
                    FROM pipeline_runs pr
                    WHERE pr.state = 'pending'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pipeline_runs active
                          WHERE active.mailing_list_id = pr.mailing_list_id
                            AND active.state = 'running'
                      )
                    ORDER BY pr.batch_id ASC NULLS LAST, pr.batch_position ASC NULLS LAST, pr.id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *"#,
            )
            .fetch_optional(&self.pool)
            .await
            {
                Ok(run) => return Ok(run),
                Err(err) if is_unique_violation(&err) && attempts < ACTIVATION_RETRY_ATTEMPTS => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn get_run(&self, run_id: i64) -> Result<Option<PipelineRun>> {
        sqlx::query_as::<_, PipelineRun>("SELECT * FROM pipeline_runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn get_active_run_for_list(&self, list_key: &str) -> Result<Option<PipelineRun>> {
        sqlx::query_as::<_, PipelineRun>(
            r#"SELECT *
            FROM pipeline_runs
            WHERE list_key = $1
              AND state = 'running'
            ORDER BY id DESC
            LIMIT 1"#,
        )
        .bind(list_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_open_run_for_list(&self, list_key: &str) -> Result<Option<PipelineRun>> {
        sqlx::query_as::<_, PipelineRun>(
            r#"SELECT *
            FROM pipeline_runs
            WHERE list_key = $1
              AND state IN ('running', 'pending')
            ORDER BY CASE state WHEN 'running' THEN 0 ELSE 1 END, id ASC
            LIMIT 1"#,
        )
        .bind(list_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_any_active_run(&self) -> Result<Option<PipelineRun>> {
        sqlx::query_as::<_, PipelineRun>(
            r#"SELECT *
            FROM pipeline_runs
            WHERE state = 'running'
            ORDER BY id DESC
            LIMIT 1"#,
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_runs(&self, params: ListPipelineRunsParams) -> Result<Vec<PipelineRun>> {
        let mut qb = QueryBuilder::new("SELECT * FROM pipeline_runs WHERE true");

        if let Some(list_key) = params.list_key {
            qb.push(" AND list_key = ").push_bind(list_key);
        }
        if let Some(state) = params.state {
            qb.push(" AND state = ").push_bind(state);
        }
        if let Some(cursor) = params.cursor {
            qb.push(" AND id < ").push_bind(cursor);
        }

        qb.push(" ORDER BY id DESC LIMIT ")
            .push_bind(params.limit.clamp(1, 500));

        qb.build_query_as::<PipelineRun>()
            .fetch_all(&self.pool)
            .await
    }

    // ── Run state transitions ──────────────────────────────────────

    pub async fn set_run_current_stage(&self, run_id: i64, stage: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET current_stage = $2,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(stage)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_run_ingest_window(
        &self,
        run_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET ingest_window_from = $2,
                ingest_window_to = $3,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(from)
        .bind(to)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_run_progress(
        &self,
        run_id: i64,
        progress_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET progress_json = $2,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(progress_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_succeeded(&self, run_id: i64) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET state = 'succeeded',
                completed_at = now(),
                last_error = NULL,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_failed(&self, run_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET state = 'failed',
                completed_at = now(),
                last_error = $2,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_cancelled(&self, run_id: i64, reason: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET state = 'cancelled',
                completed_at = now(),
                last_error = $2,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_run_to_pending(&self, run_id: i64, reason: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_runs
            SET state = 'pending',
                started_at = NULL,
                completed_at = NULL,
                last_error = $2,
                updated_at = now()
            WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Keep only the newest pending run per list; cancel older pending duplicates.
    pub async fn collapse_duplicate_pending_runs(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (PARTITION BY mailing_list_id ORDER BY id DESC) AS rn
                FROM pipeline_runs
                WHERE state = 'pending'
            )
            UPDATE pipeline_runs pr
            SET state = 'cancelled',
                completed_at = COALESCE(pr.completed_at, now()),
                last_error = COALESCE(pr.last_error, 'superseded by newer pending pipeline run'),
                updated_at = now()
            FROM ranked r
            WHERE pr.id = r.id
              AND r.rn > 1"#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    // ── Ingest window queries (replaces artifact tables) ───────────

    /// Paginated message PKs within the ingest window for a mailing list.
    pub async fn query_ingest_window_message_pks(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after_pk: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT message_pk
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND seen_at >= $2 AND seen_at <= $3
              AND message_pk > $4
            ORDER BY message_pk ASC
            LIMIT $5"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .bind(after_pk)
        .bind(limit.clamp(1, 50_000))
        .fetch_all(&self.pool)
        .await
    }

    /// Message PKs in ingest window scoped to specific repo IDs.
    pub async fn query_ingest_window_message_pks_for_repo_ids(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        repo_ids: &[i64],
    ) -> Result<Vec<i64>> {
        if repo_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT message_pk
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND seen_at >= $2 AND seen_at <= $3
              AND repo_id = ANY($4)
            ORDER BY message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .bind(repo_ids)
        .fetch_all(&self.pool)
        .await
    }

    /// Distinct repo IDs touched in the ingest window for a mailing list.
    pub async fn query_ingest_window_repo_ids(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT repo_id
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND seen_at >= $2 AND seen_at <= $3
              AND repo_id IS NOT NULL
            ORDER BY repo_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
    }

    /// All thread IDs impacted by messages in the ingest window.
    pub async fn query_impacted_thread_ids(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT tm.thread_id
            FROM list_message_instances lmi
            JOIN thread_messages tm
              ON tm.mailing_list_id = lmi.mailing_list_id
             AND tm.message_pk = lmi.message_pk
            WHERE lmi.mailing_list_id = $1
              AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
              AND tm.thread_id > 0
            GROUP BY tm.thread_id
            ORDER BY tm.thread_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
    }

    /// All patch series IDs impacted by messages in the ingest window.
    pub async fn query_impacted_series_ids(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT psv.patch_series_id
            FROM patch_series_versions psv
            JOIN patch_items pi ON pi.patch_series_version_id = psv.id
            JOIN list_message_instances lmi ON lmi.message_pk = pi.message_pk
            WHERE lmi.mailing_list_id = $1
              AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
            ORDER BY psv.patch_series_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
    }

    /// All patch item IDs impacted by messages in the ingest window.
    pub async fn query_impacted_patch_item_ids(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT pi.id AS patch_item_id
            FROM patch_items pi
            JOIN list_message_instances lmi ON lmi.message_pk = pi.message_pk
            WHERE lmi.mailing_list_id = $1
              AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
            ORDER BY patch_item_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
    }

    /// Chunked thread IDs impacted by messages in the ingest window.
    pub async fn query_impacted_thread_ids_chunk(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT tm.thread_id
            FROM list_message_instances lmi
            JOIN thread_messages tm
              ON tm.mailing_list_id = lmi.mailing_list_id
             AND tm.message_pk = lmi.message_pk
            WHERE lmi.mailing_list_id = $1
              AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
              AND tm.thread_id > $4
            GROUP BY tm.thread_id
            ORDER BY tm.thread_id ASC
            LIMIT $5"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .bind(after_id)
        .bind(limit.clamp(1, 50_000))
        .fetch_all(&self.pool)
        .await
    }

    /// Chunked thread IDs in the ingest window.
    pub async fn query_ingest_window_thread_ids_chunk(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after_thread_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        self.query_impacted_thread_ids_chunk(mailing_list_id, from, to, after_thread_id, limit)
            .await
    }

    /// Chunked patch series IDs impacted by messages in the ingest window.
    pub async fn query_impacted_series_ids_chunk(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT patch_series_id
            FROM (
                SELECT DISTINCT psv.patch_series_id
                FROM patch_series_versions psv
                JOIN patch_items pi ON pi.patch_series_version_id = psv.id
                JOIN list_message_instances lmi ON lmi.message_pk = pi.message_pk
                WHERE lmi.mailing_list_id = $1
                  AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
                  AND psv.patch_series_id > $4
            ) impacted
            ORDER BY patch_series_id ASC
            LIMIT $5"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .bind(after_id)
        .bind(limit.clamp(1, 50_000))
        .fetch_all(&self.pool)
        .await
    }

    /// Chunked patch item IDs impacted by messages in the ingest window.
    pub async fn query_impacted_patch_item_ids_chunk(
        &self,
        mailing_list_id: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT patch_item_id
            FROM (
                SELECT DISTINCT pi.id AS patch_item_id
                FROM patch_items pi
                JOIN list_message_instances lmi ON lmi.message_pk = pi.message_pk
                WHERE lmi.mailing_list_id = $1
                  AND lmi.seen_at >= $2 AND lmi.seen_at <= $3
                  AND pi.id > $4
            ) impacted
            ORDER BY patch_item_id ASC
            LIMIT $5"#,
        )
        .bind(mailing_list_id)
        .bind(from)
        .bind(to)
        .bind(after_id)
        .bind(limit.clamp(1, 50_000))
        .fetch_all(&self.pool)
        .await
    }

    /// Generate the next batch ID from a sequence.
    pub async fn next_batch_id(&self) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT nextval('pipeline_run_batches_seq')")
            .fetch_one(&self.pool)
            .await
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err.code().as_deref() == Some(UNIQUE_VIOLATION_SQLSTATE)
        }
        _ => false,
    }
}
