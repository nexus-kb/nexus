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
