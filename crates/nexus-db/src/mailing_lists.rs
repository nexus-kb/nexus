use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::Result;

/// Mailing list record.
#[derive(Debug, Serialize, Deserialize, JsonSchema, sqlx::FromRow, Clone)]
pub struct MailingList {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub last_synced_at: Option<DateTime<Utc>>,
    pub last_threaded_at: Option<DateTime<Utc>>,
}

/// CRUD helpers for mailing lists.
#[derive(Clone)]
pub struct MailingListStore {
    pool: PgPool,
}

impl MailingListStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_all_enabled(&self) -> Result<Vec<MailingList>> {
        sqlx::query_as::<_, MailingList>(
            r#"SELECT id, name, slug, description, enabled, created_at, last_synced_at, last_threaded_at
            FROM mailing_lists
            WHERE enabled = TRUE
            ORDER BY slug"#,
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list(
        &self,
        enabled: Option<bool>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MailingList>> {
        sqlx::query_as::<_, MailingList>(
            r#"SELECT id, name, slug, description, enabled, created_at, last_synced_at, last_threaded_at
            FROM mailing_lists
            WHERE ($1::bool IS NULL OR enabled = $1)
            ORDER BY slug
            LIMIT $2 OFFSET $3"#,
        )
        .bind(enabled)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn find_by_slug(&self, slug: &str) -> Result<Option<MailingList>> {
        sqlx::query_as::<_, MailingList>(
            r#"SELECT id, name, slug, description, enabled, created_at, last_synced_at, last_threaded_at
            FROM mailing_lists
            WHERE slug = $1"#,
        )
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn set_enabled(&self, slug: &str, enabled: bool) -> Result<Option<MailingList>> {
        sqlx::query_as::<_, MailingList>(
            r#"UPDATE mailing_lists
            SET enabled = $1
            WHERE slug = $2
            RETURNING id, name, slug, description, enabled, created_at, last_synced_at, last_threaded_at"#,
        )
        .bind(enabled)
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn update_last_synced_at(
        &self,
        mailing_list_id: i32,
        synced_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query("UPDATE mailing_lists SET last_synced_at = $1 WHERE id = $2")
            .bind(synced_at)
            .bind(mailing_list_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_last_threaded_at(
        &self,
        mailing_list_id: i32,
        threaded_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query("UPDATE mailing_lists SET last_threaded_at = $1 WHERE id = $2")
            .bind(threaded_at)
            .bind(mailing_list_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
