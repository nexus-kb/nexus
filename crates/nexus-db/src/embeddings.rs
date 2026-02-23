use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::{EmbeddingBackfillRun, MeiliBootstrapRun, Result};

#[derive(Debug, Clone)]
pub struct EmbeddingInputRow {
    pub doc_id: i64,
    pub text: String,
    pub source_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct EmbeddingVectorUpsert {
    pub doc_id: i64,
    pub vector: Vec<f32>,
    pub source_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BackfillProgressUpdate {
    pub cursor_id: i64,
    pub processed_count: i64,
    pub embedded_count: i64,
    pub failed_count: i64,
    pub total_candidates: i64,
    pub progress_json: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct ListMeiliBootstrapRunsParams {
    pub list_key: Option<String>,
    pub state: Option<String>,
    pub limit: i64,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ThreadEmbeddingRow {
    doc_id: i64,
    subject: String,
    participants: Vec<String>,
    body_corpus: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct SeriesEmbeddingRow {
    doc_id: i64,
    canonical_subject: String,
    cover_body: Option<String>,
    patch_subjects: Vec<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct SearchVectorRow {
    doc_id: i64,
    vector: Vec<f32>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ThreadSourceRow {
    message_pk: i64,
    body_id: i64,
}

#[derive(Clone)]
pub struct EmbeddingsStore {
    pool: PgPool,
}

impl EmbeddingsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_backfill_run(
        &self,
        scope: &str,
        list_key: Option<&str>,
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
        model_key: &str,
    ) -> Result<EmbeddingBackfillRun> {
        sqlx::query_as::<_, EmbeddingBackfillRun>(
            r#"INSERT INTO embedding_backfill_runs
               (scope, list_key, from_seen_at, to_seen_at, state, model_key, progress_json)
               VALUES ($1, $2, $3, $4, 'running', $5, '{}'::jsonb)
               RETURNING *"#,
        )
        .bind(scope)
        .bind(list_key)
        .bind(from_seen_at)
        .bind(to_seen_at)
        .bind(model_key)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_backfill_run(&self, run_id: i64) -> Result<Option<EmbeddingBackfillRun>> {
        sqlx::query_as::<_, EmbeddingBackfillRun>(
            "SELECT * FROM embedding_backfill_runs WHERE id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn update_backfill_progress(
        &self,
        run_id: i64,
        progress: BackfillProgressUpdate,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE embedding_backfill_runs
               SET cursor_id = $2,
                   processed_count = $3,
                   embedded_count = $4,
                   failed_count = $5,
                   total_candidates = $6,
                   progress_json = $7,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(progress.cursor_id)
        .bind(progress.processed_count)
        .bind(progress.embedded_count)
        .bind(progress.failed_count)
        .bind(progress.total_candidates)
        .bind(progress.progress_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_backfill_succeeded(
        &self,
        run_id: i64,
        result_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE embedding_backfill_runs
               SET state = 'succeeded',
                   completed_at = now(),
                   result_json = $2,
                   last_error = NULL,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_backfill_failed(&self, run_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE embedding_backfill_runs
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

    pub async fn mark_backfill_cancelled(&self, run_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE embedding_backfill_runs
               SET state = 'cancelled',
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

    pub async fn create_meili_bootstrap_run(
        &self,
        scope: &str,
        list_key: Option<&str>,
        embedder_name: &str,
        model_key: &str,
    ) -> Result<MeiliBootstrapRun> {
        sqlx::query_as::<_, MeiliBootstrapRun>(
            r#"INSERT INTO meili_bootstrap_runs
               (scope, list_key, state, embedder_name, model_key, progress_json)
               VALUES ($1, $2, 'queued', $3, $4, '{}'::jsonb)
               RETURNING *"#,
        )
        .bind(scope)
        .bind(list_key)
        .bind(embedder_name)
        .bind(model_key)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn set_meili_bootstrap_job_id(&self, run_id: i64, job_id: i64) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
               SET job_id = $2,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_meili_bootstrap_run(&self, run_id: i64) -> Result<Option<MeiliBootstrapRun>> {
        sqlx::query_as::<_, MeiliBootstrapRun>("SELECT * FROM meili_bootstrap_runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn list_meili_bootstrap_runs(
        &self,
        params: ListMeiliBootstrapRunsParams,
    ) -> Result<Vec<MeiliBootstrapRun>> {
        let mut qb = sqlx::QueryBuilder::new("SELECT * FROM meili_bootstrap_runs WHERE true");
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
            .push_bind(params.limit.clamp(1, 200));
        qb.build_query_as::<MeiliBootstrapRun>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_active_meili_bootstrap_run(
        &self,
        scope: &str,
        list_key: Option<&str>,
    ) -> Result<Option<MeiliBootstrapRun>> {
        sqlx::query_as::<_, MeiliBootstrapRun>(
            r#"SELECT *
               FROM meili_bootstrap_runs
               WHERE scope = $1
                 AND list_key IS NOT DISTINCT FROM $2
                 AND state IN ('queued', 'running')
               ORDER BY id DESC
               LIMIT 1"#,
        )
        .bind(scope)
        .bind(list_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn mark_meili_bootstrap_running(&self, run_id: i64) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
               SET state = 'running',
                   started_at = COALESCE(started_at, now()),
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_meili_bootstrap_progress(
        &self,
        run_id: i64,
        thread_cursor_id: i64,
        series_cursor_id: i64,
        total_candidates_thread: i64,
        total_candidates_series: i64,
        processed_thread: i64,
        processed_series: i64,
        docs_upserted: i64,
        vectors_attached: i64,
        placeholders_written: i64,
        failed_batches: i64,
        progress_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
               SET thread_cursor_id = $2,
                   series_cursor_id = $3,
                   total_candidates_thread = $4,
                   total_candidates_series = $5,
                   processed_thread = $6,
                   processed_series = $7,
                   docs_upserted = $8,
                   vectors_attached = $9,
                   placeholders_written = $10,
                   failed_batches = $11,
                   progress_json = $12,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(thread_cursor_id)
        .bind(series_cursor_id)
        .bind(total_candidates_thread)
        .bind(total_candidates_series)
        .bind(processed_thread)
        .bind(processed_series)
        .bind(docs_upserted)
        .bind(vectors_attached)
        .bind(placeholders_written)
        .bind(failed_batches)
        .bind(progress_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_meili_bootstrap_succeeded(
        &self,
        run_id: i64,
        result_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
               SET state = 'succeeded',
                   completed_at = now(),
                   result_json = $2,
                   last_error = NULL,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_meili_bootstrap_failed(&self, run_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
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

    pub async fn mark_meili_bootstrap_cancelled(&self, run_id: i64, reason: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE meili_bootstrap_runs
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

    pub async fn count_candidate_ids(
        &self,
        scope: &str,
        list_key: Option<&str>,
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
    ) -> Result<i64> {
        match scope {
            "thread" => {
                sqlx::query_scalar::<_, i64>(
                    r#"SELECT COUNT(*)::bigint
                       FROM threads t
                       JOIN mailing_lists ml
                         ON ml.id = t.mailing_list_id
                       WHERE ($1::text IS NULL OR ml.list_key = $1)
                         AND ($2::timestamptz IS NULL OR t.last_activity_at >= $2)
                         AND ($3::timestamptz IS NULL OR t.last_activity_at < $3)"#,
                )
                .bind(list_key)
                .bind(from_seen_at)
                .bind(to_seen_at)
                .fetch_one(&self.pool)
                .await
            }
            "series" => {
                sqlx::query_scalar::<_, i64>(
                    r#"SELECT COUNT(*)::bigint
                       FROM patch_series ps
                       WHERE ($1::text IS NULL OR EXISTS (
                           SELECT 1
                           FROM patch_series_lists psl
                           JOIN mailing_lists ml
                             ON ml.id = psl.mailing_list_id
                           WHERE psl.patch_series_id = ps.id
                             AND ml.list_key = $1
                       ))
                         AND ($2::timestamptz IS NULL OR ps.last_seen_at >= $2)
                         AND ($3::timestamptz IS NULL OR ps.last_seen_at < $3)"#,
                )
                .bind(list_key)
                .bind(from_seen_at)
                .bind(to_seen_at)
                .fetch_one(&self.pool)
                .await
            }
            other => Err(sqlx::Error::Protocol(format!(
                "unsupported embedding scope: {other}"
            ))),
        }
    }

    pub async fn list_candidate_ids(
        &self,
        scope: &str,
        list_key: Option<&str>,
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        match scope {
            "thread" => {
                sqlx::query_scalar::<_, i64>(
                    r#"SELECT t.id
                       FROM threads t
                       JOIN mailing_lists ml
                         ON ml.id = t.mailing_list_id
                       WHERE t.id > $1
                         AND ($2::text IS NULL OR ml.list_key = $2)
                         AND ($3::timestamptz IS NULL OR t.last_activity_at >= $3)
                         AND ($4::timestamptz IS NULL OR t.last_activity_at < $4)
                       ORDER BY t.id ASC
                       LIMIT $5"#,
                )
                .bind(after_id)
                .bind(list_key)
                .bind(from_seen_at)
                .bind(to_seen_at)
                .bind(limit.clamp(1, 10_000))
                .fetch_all(&self.pool)
                .await
            }
            "series" => {
                sqlx::query_scalar::<_, i64>(
                    r#"SELECT ps.id
                       FROM patch_series ps
                       WHERE ps.id > $1
                         AND ($2::text IS NULL OR EXISTS (
                             SELECT 1
                             FROM patch_series_lists psl
                             JOIN mailing_lists ml
                               ON ml.id = psl.mailing_list_id
                             WHERE psl.patch_series_id = ps.id
                               AND ml.list_key = $2
                         ))
                         AND ($3::timestamptz IS NULL OR ps.last_seen_at >= $3)
                         AND ($4::timestamptz IS NULL OR ps.last_seen_at < $4)
                       ORDER BY ps.id ASC
                       LIMIT $5"#,
                )
                .bind(after_id)
                .bind(list_key)
                .bind(from_seen_at)
                .bind(to_seen_at)
                .bind(limit.clamp(1, 10_000))
                .fetch_all(&self.pool)
                .await
            }
            other => Err(sqlx::Error::Protocol(format!(
                "unsupported embedding scope: {other}"
            ))),
        }
    }

    pub async fn build_embedding_inputs(
        &self,
        scope: &str,
        ids: &[i64],
    ) -> Result<Vec<EmbeddingInputRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        match scope {
            "thread" => self.build_thread_embedding_inputs(ids).await,
            "series" => self.build_series_embedding_inputs(ids).await,
            other => Err(sqlx::Error::Protocol(format!(
                "unsupported embedding scope: {other}"
            ))),
        }
    }

    pub async fn upsert_vectors(
        &self,
        scope: &str,
        model_key: &str,
        dimensions: i32,
        entries: &[EmbeddingVectorUpsert],
    ) -> Result<u64> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut rows_written = 0u64;
        for entry in entries {
            let result = sqlx::query(
                r#"INSERT INTO search_doc_embeddings
                   (scope, doc_id, model_key, dimensions, vector, source_hash)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (scope, doc_id, model_key)
                   DO UPDATE SET dimensions = EXCLUDED.dimensions,
                                 vector = EXCLUDED.vector,
                                 source_hash = EXCLUDED.source_hash,
                                 updated_at = now()"#,
            )
            .bind(scope)
            .bind(entry.doc_id)
            .bind(model_key)
            .bind(dimensions)
            .bind(&entry.vector)
            .bind(&entry.source_hash)
            .execute(&self.pool)
            .await?;
            rows_written += result.rows_affected();
        }

        Ok(rows_written)
    }

    pub async fn get_vectors(
        &self,
        scope: &str,
        model_key: &str,
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<f32>>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query_as::<_, SearchVectorRow>(
            r#"SELECT doc_id, vector
               FROM search_doc_embeddings
               WHERE scope = $1
                 AND model_key = $2
                 AND doc_id = ANY($3)"#,
        )
        .bind(scope)
        .bind(model_key)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.doc_id, row.vector))
            .collect::<HashMap<_, _>>())
    }

    pub async fn lookup_thread_message_body(&self, thread_id: i64) -> Result<Option<(i64, i64)>> {
        let row = sqlx::query_as::<_, ThreadSourceRow>(
            r#"SELECT
                   tm.message_pk,
                   m.body_id
               FROM thread_messages tm
               JOIN messages m
                 ON m.id = tm.message_pk
               WHERE tm.thread_id = $1
               ORDER BY tm.sort_key, tm.message_pk
               LIMIT 1"#,
        )
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|value| (value.message_pk, value.body_id)))
    }

    async fn build_thread_embedding_inputs(&self, ids: &[i64]) -> Result<Vec<EmbeddingInputRow>> {
        let rows = sqlx::query_as::<_, ThreadEmbeddingRow>(
            r#"WITH targets AS (
                   SELECT t.id, t.mailing_list_id, t.subject_norm
                   FROM threads t
                   WHERE t.id = ANY($1)
               ),
               participants AS (
                   SELECT
                       tm.thread_id AS thread_id,
                       COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT m.from_email), NULL), ARRAY[]::text[]) AS participant_emails
                   FROM thread_messages tm
                   JOIN targets t
                     ON t.id = tm.thread_id
                    AND t.mailing_list_id = tm.mailing_list_id
                   JOIN messages m
                     ON m.id = tm.message_pk
                   GROUP BY tm.thread_id
               ),
               bodies AS (
                   SELECT
                       tm.thread_id AS thread_id,
                       STRING_AGG(nexus_safe_prefix(mb.search_text, 1000), ' ' ORDER BY tm.sort_key) AS body_corpus
                   FROM thread_messages tm
                   JOIN targets t
                     ON t.id = tm.thread_id
                    AND t.mailing_list_id = tm.mailing_list_id
                   JOIN messages m
                     ON m.id = tm.message_pk
                   JOIN message_bodies mb
                     ON mb.id = m.body_id
                   GROUP BY tm.thread_id
               )
               SELECT
                   t.id AS doc_id,
                   t.subject_norm AS subject,
                   COALESCE(p.participant_emails, ARRAY[]::text[]) AS participants,
                   b.body_corpus
               FROM targets t
               LEFT JOIN participants p
                 ON p.thread_id = t.id
               LEFT JOIN bodies b
                 ON b.thread_id = t.id
               ORDER BY t.id ASC"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let participants_joined = row.participants.join(" ");
                let body = row.body_corpus.unwrap_or_default();
                let body_collapsed = collapse_whitespace(&body);
                let body_capped = limit_chars(body_collapsed.as_str(), 1_000);
                let text = normalize_embedding_text(&[
                    row.subject.as_str(),
                    participants_joined.as_str(),
                    body_capped.as_str(),
                ]);
                if text.is_empty() {
                    return None;
                }
                Some(EmbeddingInputRow {
                    doc_id: row.doc_id,
                    source_hash: hash_text(&text),
                    text,
                })
            })
            .collect())
    }

    async fn build_series_embedding_inputs(&self, ids: &[i64]) -> Result<Vec<EmbeddingInputRow>> {
        let rows = sqlx::query_as::<_, SeriesEmbeddingRow>(
            r#"SELECT
                   ps.id AS doc_id,
                   ps.canonical_subject_norm AS canonical_subject,
                   mb.body_text AS cover_body,
                   COALESCE(
                       ARRAY_REMOVE(
                           ARRAY_AGG(DISTINCT pi.subject_raw) FILTER (WHERE pi.item_type = 'patch'),
                           NULL
                       ),
                       ARRAY[]::text[]
                   ) AS patch_subjects
               FROM patch_series ps
               LEFT JOIN patch_series_versions psv
                 ON psv.id = ps.latest_version_id
               LEFT JOIN patch_items pi
                 ON pi.patch_series_version_id = psv.id
               LEFT JOIN messages m
                 ON m.id = psv.cover_message_pk
               LEFT JOIN message_bodies mb
                 ON mb.id = m.body_id
               WHERE ps.id = ANY($1)
               GROUP BY ps.id, ps.canonical_subject_norm, mb.body_text
               ORDER BY ps.id ASC"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let patch_subjects = row.patch_subjects.join(" ");
                let cover = limit_chars(row.cover_body.as_deref().unwrap_or(""), 8_000);
                let patch_subjects_limited = limit_chars(&patch_subjects, 8_000);
                let text = normalize_embedding_text(&[
                    row.canonical_subject.as_str(),
                    cover.as_str(),
                    patch_subjects_limited.as_str(),
                ]);
                if text.is_empty() {
                    return None;
                }
                Some(EmbeddingInputRow {
                    doc_id: row.doc_id,
                    source_hash: hash_text(&text),
                    text,
                })
            })
            .collect())
    }
}

fn normalize_embedding_text(parts: &[&str]) -> String {
    let joined = parts
        .iter()
        .filter(|part| !part.trim().is_empty())
        .map(|part| part.trim())
        .collect::<Vec<_>>()
        .join("\n\n");
    limit_chars(collapse_whitespace(joined.as_str()).as_str(), 16_000)
}

fn collapse_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn hash_text(raw: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hasher.finalize().to_vec()
}

fn limit_chars(raw: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    raw.chars().take(max_chars).collect::<String>()
}
