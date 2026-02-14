use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::Result;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MailingList {
    pub id: i64,
    pub list_key: String,
    pub posting_address: Option<String>,
    pub description: Option<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MailingListRepo {
    pub id: i64,
    pub mailing_list_id: i64,
    pub repo_key: String,
    pub repo_relpath: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListCatalogItemRecord {
    pub list_key: String,
    pub description: Option<String>,
    pub posting_address: Option<String>,
    pub latest_activity_at: Option<DateTime<Utc>>,
    pub thread_count_30d: i64,
    pub message_count_30d: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListDetailRecord {
    pub list_key: String,
    pub description: Option<String>,
    pub posting_address: Option<String>,
    pub active_repo_count: i64,
    pub repo_count: i64,
    pub latest_repo_watermark_updated_at: Option<DateTime<Utc>>,
    pub message_count: i64,
    pub thread_count: i64,
    pub patch_series_count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListStatsTotalsRecord {
    pub messages: i64,
    pub threads: i64,
    pub patch_series: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListTopAuthorRecord {
    pub from_email: String,
    pub from_name: Option<String>,
    pub message_count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListActivityByDayRecord {
    pub day_utc: DateTime<Utc>,
    pub messages: i64,
    pub threads: i64,
}

#[derive(Clone)]
pub struct CatalogStore {
    pool: PgPool,
}

impl CatalogStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_mailing_list(&self, list_key: &str) -> Result<MailingList> {
        let list = sqlx::query_as::<_, MailingList>(
            r#"INSERT INTO mailing_lists (list_key)
            VALUES ($1)
            ON CONFLICT (list_key) DO UPDATE SET list_key = EXCLUDED.list_key
            RETURNING id, list_key, posting_address, description, active, created_at"#,
        )
        .bind(list_key)
        .fetch_one(&self.pool)
        .await?;

        sqlx::query("SELECT ensure_list_message_instances_partition($1)")
            .bind(list.id)
            .execute(&self.pool)
            .await?;
        sqlx::query("SELECT ensure_threads_partition($1)")
            .bind(list.id)
            .execute(&self.pool)
            .await?;
        sqlx::query("SELECT ensure_thread_nodes_partition($1)")
            .bind(list.id)
            .execute(&self.pool)
            .await?;
        sqlx::query("SELECT ensure_thread_messages_partition($1)")
            .bind(list.id)
            .execute(&self.pool)
            .await?;

        Ok(list)
    }

    pub async fn get_mailing_list(&self, list_key: &str) -> Result<Option<MailingList>> {
        sqlx::query_as::<_, MailingList>(
            "SELECT id, list_key, posting_address, description, active, created_at FROM mailing_lists WHERE list_key = $1",
        )
        .bind(list_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn count_mailing_lists(&self) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::bigint FROM mailing_lists WHERE active = true",
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn list_mailing_lists(
        &self,
        page: i64,
        page_size: i64,
    ) -> Result<Vec<ListCatalogItemRecord>> {
        let page_size = page_size.clamp(1, 200);
        let page = page.max(1);
        let offset = (page - 1) * page_size;

        sqlx::query_as::<_, ListCatalogItemRecord>(
            r#"SELECT
                ml.list_key,
                ml.description,
                ml.posting_address,
                latest.latest_activity_at,
                COALESCE(t30.thread_count_30d, 0)::bigint AS thread_count_30d,
                COALESCE(m30.message_count_30d, 0)::bigint AS message_count_30d
            FROM mailing_lists ml
            LEFT JOIN LATERAL (
                SELECT MAX(t.last_activity_at) AS latest_activity_at
                FROM threads t
                WHERE t.mailing_list_id = ml.id
            ) latest ON true
            LEFT JOIN LATERAL (
                SELECT COUNT(*)::bigint AS thread_count_30d
                FROM threads t
                WHERE t.mailing_list_id = ml.id
                  AND t.last_activity_at >= now() - interval '30 days'
            ) t30 ON true
            LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT lmi.message_pk)::bigint AS message_count_30d
                FROM list_message_instances lmi
                WHERE lmi.mailing_list_id = ml.id
                  AND lmi.seen_at >= now() - interval '30 days'
            ) m30 ON true
            WHERE ml.active = true
            ORDER BY ml.list_key ASC
            LIMIT $1 OFFSET $2"#,
        )
        .bind(page_size)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_mailing_list_detail(
        &self,
        list_key: &str,
    ) -> Result<Option<ListDetailRecord>> {
        sqlx::query_as::<_, ListDetailRecord>(
            r#"SELECT
                ml.list_key,
                ml.description,
                ml.posting_address,
                COALESCE(repo.active_repo_count, 0)::bigint AS active_repo_count,
                COALESCE(repo.repo_count, 0)::bigint AS repo_count,
                repo.latest_repo_watermark_updated_at,
                COALESCE(msg.message_count, 0)::bigint AS message_count,
                COALESCE(threads.thread_count, 0)::bigint AS thread_count,
                COALESCE(series.patch_series_count, 0)::bigint AS patch_series_count
            FROM mailing_lists ml
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE r.active)::bigint AS active_repo_count,
                    COUNT(*)::bigint AS repo_count,
                    MAX(rw.updated_at) AS latest_repo_watermark_updated_at
                FROM mailing_list_repos r
                LEFT JOIN repo_watermarks rw
                  ON rw.repo_id = r.id
                WHERE r.mailing_list_id = ml.id
            ) repo ON true
            LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT lmi.message_pk)::bigint AS message_count
                FROM list_message_instances lmi
                WHERE lmi.mailing_list_id = ml.id
            ) msg ON true
            LEFT JOIN LATERAL (
                SELECT COUNT(*)::bigint AS thread_count
                FROM threads t
                WHERE t.mailing_list_id = ml.id
            ) threads ON true
            LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT psl.patch_series_id)::bigint AS patch_series_count
                FROM patch_series_lists psl
                WHERE psl.mailing_list_id = ml.id
            ) series ON true
            WHERE ml.list_key = $1
              AND ml.active = true
            LIMIT 1"#,
        )
        .bind(list_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_list_stats_totals(
        &self,
        mailing_list_id: i64,
        since: DateTime<Utc>,
    ) -> Result<ListStatsTotalsRecord> {
        sqlx::query_as::<_, ListStatsTotalsRecord>(
            r#"SELECT
                (
                    SELECT COUNT(DISTINCT lmi.message_pk)::bigint
                    FROM list_message_instances lmi
                    WHERE lmi.mailing_list_id = $1
                      AND lmi.seen_at >= $2
                ) AS messages,
                (
                    SELECT COUNT(*)::bigint
                    FROM threads t
                    WHERE t.mailing_list_id = $1
                      AND t.last_activity_at >= $2
                ) AS threads,
                (
                    SELECT COUNT(DISTINCT psl.patch_series_id)::bigint
                    FROM patch_series_lists psl
                    WHERE psl.mailing_list_id = $1
                      AND psl.last_seen_at >= $2
                ) AS patch_series"#,
        )
        .bind(mailing_list_id)
        .bind(since)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_list_top_authors(
        &self,
        mailing_list_id: i64,
        since: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<ListTopAuthorRecord>> {
        sqlx::query_as::<_, ListTopAuthorRecord>(
            r#"SELECT
                m.from_email,
                MAX(m.from_name) AS from_name,
                COUNT(DISTINCT lmi.message_pk)::bigint AS message_count
            FROM list_message_instances lmi
            JOIN messages m
              ON m.id = lmi.message_pk
            WHERE lmi.mailing_list_id = $1
              AND lmi.seen_at >= $2
            GROUP BY m.from_email
            ORDER BY message_count DESC, m.from_email ASC
            LIMIT $3"#,
        )
        .bind(mailing_list_id)
        .bind(since)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_list_activity_by_day(
        &self,
        mailing_list_id: i64,
        since: DateTime<Utc>,
    ) -> Result<Vec<ListActivityByDayRecord>> {
        sqlx::query_as::<_, ListActivityByDayRecord>(
            r#"WITH message_activity AS (
                SELECT
                    date_trunc('day', lmi.seen_at) AS day_utc,
                    COUNT(DISTINCT lmi.message_pk)::bigint AS messages
                FROM list_message_instances lmi
                WHERE lmi.mailing_list_id = $1
                  AND lmi.seen_at >= $2
                GROUP BY day_utc
            ),
            thread_activity AS (
                SELECT
                    date_trunc('day', t.last_activity_at) AS day_utc,
                    COUNT(*)::bigint AS threads
                FROM threads t
                WHERE t.mailing_list_id = $1
                  AND t.last_activity_at >= $2
                GROUP BY day_utc
            )
            SELECT
                COALESCE(m.day_utc, t.day_utc) AS day_utc,
                COALESCE(m.messages, 0)::bigint AS messages,
                COALESCE(t.threads, 0)::bigint AS threads
            FROM message_activity m
            FULL OUTER JOIN thread_activity t
              ON t.day_utc = m.day_utc
            ORDER BY day_utc ASC"#,
        )
        .bind(mailing_list_id)
        .bind(since)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn ensure_repo(
        &self,
        mailing_list_id: i64,
        repo_key: &str,
        repo_relpath: &str,
    ) -> Result<MailingListRepo> {
        let repo = sqlx::query_as::<_, MailingListRepo>(
            r#"INSERT INTO mailing_list_repos (mailing_list_id, repo_key, repo_relpath)
            VALUES ($1, $2, $3)
            ON CONFLICT (mailing_list_id, repo_key)
            DO UPDATE SET repo_relpath = EXCLUDED.repo_relpath, active = true
            RETURNING id, mailing_list_id, repo_key, repo_relpath, active, created_at"#,
        )
        .bind(mailing_list_id)
        .bind(repo_key)
        .bind(repo_relpath)
        .fetch_one(&self.pool)
        .await?;

        sqlx::query(
            "INSERT INTO repo_watermarks (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO NOTHING",
        )
        .bind(repo.id)
        .execute(&self.pool)
        .await?;

        Ok(repo)
    }

    pub async fn list_repos_for_list(&self, list_key: &str) -> Result<Vec<MailingListRepo>> {
        sqlx::query_as::<_, MailingListRepo>(
            r#"SELECT r.id, r.mailing_list_id, r.repo_key, r.repo_relpath, r.active, r.created_at
            FROM mailing_list_repos r
            JOIN mailing_lists l ON l.id = r.mailing_list_id
            WHERE l.list_key = $1 AND r.active = true
            ORDER BY r.repo_key"#,
        )
        .bind(list_key)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_repo(
        &self,
        list_key: &str,
        repo_key: &str,
    ) -> Result<Option<MailingListRepo>> {
        sqlx::query_as::<_, MailingListRepo>(
            r#"SELECT r.id, r.mailing_list_id, r.repo_key, r.repo_relpath, r.active, r.created_at
            FROM mailing_list_repos r
            JOIN mailing_lists l ON l.id = r.mailing_list_id
            WHERE l.list_key = $1 AND r.repo_key = $2"#,
        )
        .bind(list_key)
        .bind(repo_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_watermark(&self, repo_id: i64) -> Result<Option<String>> {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT last_indexed_commit_oid FROM repo_watermarks WHERE repo_id = $1",
        )
        .bind(repo_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_watermark(&self, repo_id: i64, commit_oid: Option<&str>) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO repo_watermarks (repo_id, last_indexed_commit_oid, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (repo_id)
            DO UPDATE SET last_indexed_commit_oid = EXCLUDED.last_indexed_commit_oid,
                          updated_at = now()"#,
        )
        .bind(repo_id)
        .bind(commit_oid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_watermark(&self, list_key: &str, repo_key: &str) -> Result<bool> {
        let updated = sqlx::query(
            r#"UPDATE repo_watermarks w
            SET last_indexed_commit_oid = NULL, updated_at = now()
            FROM mailing_list_repos r
            JOIN mailing_lists l ON l.id = r.mailing_list_id
            WHERE w.repo_id = r.id
              AND l.list_key = $1
              AND r.repo_key = $2"#,
        )
        .bind(list_key)
        .bind(repo_key)
        .execute(&self.pool)
        .await?;
        Ok(updated.rows_affected() > 0)
    }
}
