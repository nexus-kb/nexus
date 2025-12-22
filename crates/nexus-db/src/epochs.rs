use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::Result;

/// Public-inbox epoch metadata for a mailing list.
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct MailingListEpoch {
    pub id: i32,
    pub mailing_list_id: i32,
    pub epoch: i16,
    pub repo_relpath: String,
    pub fingerprint: Option<String>,
    pub modified_at: Option<DateTime<Utc>>,
    pub reference_epoch: Option<i16>,
    pub last_indexed_commit: Option<String>,
    pub last_indexed_at: Option<DateTime<Utc>>,
}

/// Database access for epoch metadata.
#[derive(Clone)]
pub struct MailingListEpochStore {
    pool: PgPool,
}

impl MailingListEpochStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_by_mailing_list(
        &self,
        mailing_list_id: i32,
    ) -> Result<Vec<MailingListEpoch>> {
        sqlx::query_as::<_, MailingListEpoch>(
            r#"SELECT id, mailing_list_id, epoch, repo_relpath, fingerprint, modified_at, reference_epoch, last_indexed_commit, last_indexed_at
            FROM mailing_list_epochs
            WHERE mailing_list_id = $1
            ORDER BY epoch"#,
        )
        .bind(mailing_list_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn update_last_indexed_commit(
        &self,
        epoch_id: i32,
        last_indexed_commit: &str,
        indexed_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE mailing_list_epochs
            SET last_indexed_commit = $1, last_indexed_at = $2
            WHERE id = $3"#,
        )
        .bind(last_indexed_commit)
        .bind(indexed_at)
        .bind(epoch_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
