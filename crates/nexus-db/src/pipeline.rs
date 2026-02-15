use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};

use crate::{PipelineRun, PipelineStageRun, Result};

#[derive(Debug, Clone, Default)]
pub struct ListPipelineRunsParams {
    pub list_key: Option<String>,
    pub state: Option<String>,
    pub limit: i64,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PipelineArtifactCount {
    pub artifact_kind: String,
    pub count: i64,
}

#[derive(Clone)]
pub struct PipelineStore {
    pool: PgPool,
}

impl PipelineStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_or_get_active_run(
        &self,
        mailing_list_id: i64,
        list_key: &str,
        source: &str,
    ) -> Result<PipelineRun> {
        if let Some(existing) = self.get_active_run_for_list(list_key).await? {
            return Ok(existing);
        }

        let inserted = sqlx::query_as::<_, PipelineRun>(
            r#"INSERT INTO pipeline_runs
            (mailing_list_id, list_key, state, current_stage, source)
            VALUES ($1, $2, 'running', 'ingest', $3)
            RETURNING *"#,
        )
        .bind(mailing_list_id)
        .bind(list_key)
        .bind(source)
        .fetch_one(&self.pool)
        .await;

        match inserted {
            Ok(run) => Ok(run),
            Err(err) if is_unique_violation(&err) => {
                self.get_active_run_for_list(list_key).await?.ok_or(err)
            }
            Err(err) => Err(err),
        }
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

    pub async fn get_run(&self, run_id: i64) -> Result<Option<PipelineRun>> {
        sqlx::query_as::<_, PipelineRun>("SELECT * FROM pipeline_runs WHERE id = $1")
            .bind(run_id)
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

    pub async fn ensure_stage_running(&self, run_id: i64, stage: &str) -> Result<PipelineStageRun> {
        sqlx::query_as::<_, PipelineStageRun>(
            r#"INSERT INTO pipeline_stage_runs (run_id, stage, state, progress_json)
            VALUES ($1, $2, 'running', '{}'::jsonb)
            ON CONFLICT (run_id, stage)
            DO UPDATE SET state = 'running',
                          completed_at = NULL,
                          last_error = NULL,
                          updated_at = now()
            RETURNING *"#,
        )
        .bind(run_id)
        .bind(stage)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_stage_progress(
        &self,
        run_id: i64,
        stage: &str,
        progress_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_stage_runs
            SET progress_json = $3,
                updated_at = now()
            WHERE run_id = $1 AND stage = $2"#,
        )
        .bind(run_id)
        .bind(stage)
        .bind(progress_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_stage_succeeded(
        &self,
        run_id: i64,
        stage: &str,
        result_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_stage_runs
            SET state = 'succeeded',
                completed_at = now(),
                result_json = $3,
                last_error = NULL,
                updated_at = now()
            WHERE run_id = $1 AND stage = $2"#,
        )
        .bind(run_id)
        .bind(stage)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_stage_failed(&self, run_id: i64, stage: &str, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE pipeline_stage_runs
            SET state = 'failed',
                completed_at = now(),
                last_error = $3,
                updated_at = now()
            WHERE run_id = $1 AND stage = $2"#,
        )
        .bind(run_id)
        .bind(stage)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_stage_runs(&self, run_id: i64) -> Result<Vec<PipelineStageRun>> {
        sqlx::query_as::<_, PipelineStageRun>(
            r#"SELECT *
            FROM pipeline_stage_runs
            WHERE run_id = $1
            ORDER BY stage ASC"#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
    }

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

    pub async fn is_list_ingest_stage_active(&self, list_key: &str) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (
                SELECT 1
                FROM pipeline_runs r
                WHERE r.list_key = $1
                  AND r.state = 'running'
                  AND r.current_stage = 'ingest'
            )"#,
        )
        .bind(list_key)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn insert_artifacts(&self, run_id: i64, kind: &str, ids: &[i64]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let mut deduped = ids.to_vec();
        deduped.sort_unstable();
        deduped.dedup();
        deduped.retain(|id| *id > 0);
        if deduped.is_empty() {
            return Ok(0);
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO pipeline_stage_artifacts (run_id, artifact_kind, artifact_id) ",
        );
        qb.push_values(deduped.iter(), |mut b, artifact_id| {
            b.push_bind(run_id).push_bind(kind).push_bind(*artifact_id);
        });
        qb.push(" ON CONFLICT (run_id, artifact_kind, artifact_id) DO NOTHING");

        let result = qb.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    pub async fn list_artifact_ids_chunk(
        &self,
        run_id: i64,
        kind: &str,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT artifact_id
            FROM pipeline_stage_artifacts
            WHERE run_id = $1
              AND artifact_kind = $2
              AND artifact_id > $3
            ORDER BY artifact_id ASC
            LIMIT $4"#,
        )
        .bind(run_id)
        .bind(kind)
        .bind(after_id)
        .bind(limit.clamp(1, 10_000))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_artifact_counts(&self, run_id: i64) -> Result<Vec<PipelineArtifactCount>> {
        sqlx::query_as::<_, PipelineArtifactCount>(
            r#"SELECT artifact_kind, COUNT(*)::bigint AS count
            FROM pipeline_stage_artifacts
            WHERE run_id = $1
            GROUP BY artifact_kind
            ORDER BY artifact_kind ASC"#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
        _ => false,
    }
}
