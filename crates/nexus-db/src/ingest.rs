use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::{MailingListRepo, Result};

#[derive(Debug, Clone)]
pub struct ParsedBodyInput {
    pub raw_rfc822: Vec<u8>,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
    pub search_text: String,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedMessageInput {
    pub content_hash_sha256: Vec<u8>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub to_raw: Option<String>,
    pub cc_raw: Option<String>,
    pub message_ids: Vec<String>,
    pub message_id_primary: String,
    pub in_reply_to_ids: Vec<String>,
    pub references_ids: Vec<String>,
    pub mime_type: Option<String>,
    pub body: ParsedBodyInput,
}

#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub message_inserted: bool,
    pub instance_inserted: bool,
}

#[derive(Clone)]
pub struct IngestStore {
    pool: PgPool,
}

impl IngestStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ingest_message(
        &self,
        repo: &MailingListRepo,
        git_commit_oid: &str,
        input: &ParsedMessageInput,
    ) -> Result<WriteOutcome> {
        let mut tx = self.pool.begin().await?;

        // Commit-level idempotency for this list.
        let already_ingested = sqlx::query_scalar::<_, i64>(
            r#"SELECT 1
            FROM list_message_instances
            WHERE mailing_list_id = $1 AND git_commit_oid = $2
            LIMIT 1"#,
        )
        .bind(repo.mailing_list_id)
        .bind(git_commit_oid)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();

        if already_ingested {
            tx.commit().await?;
            return Ok(WriteOutcome {
                message_inserted: false,
                instance_inserted: false,
            });
        }

        let mut message_inserted = false;

        let existing_message_pk =
            sqlx::query_scalar::<_, i64>("SELECT id FROM messages WHERE content_hash_sha256 = $1")
                .bind(&input.content_hash_sha256)
                .fetch_optional(&mut *tx)
                .await?;

        let message_pk = if let Some(id) = existing_message_pk {
            id
        } else {
            let body_id = sqlx::query_scalar::<_, i64>(
                r#"INSERT INTO message_bodies
                (raw_rfc822, body_text, diff_text, search_text, has_diff, has_attachments)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id"#,
            )
            .bind(&input.body.raw_rfc822)
            .bind(&input.body.body_text)
            .bind(&input.body.diff_text)
            .bind(&input.body.search_text)
            .bind(input.body.has_diff)
            .bind(input.body.has_attachments)
            .fetch_one(&mut *tx)
            .await?;

            let message_pk = sqlx::query_scalar::<_, i64>(
                r#"INSERT INTO messages
                (content_hash_sha256, subject_raw, subject_norm, from_name, from_email,
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary,
                 in_reply_to_ids, references_ids, mime_type, body_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id"#,
            )
            .bind(&input.content_hash_sha256)
            .bind(&input.subject_raw)
            .bind(&input.subject_norm)
            .bind(&input.from_name)
            .bind(&input.from_email)
            .bind(input.date_utc)
            .bind(&input.to_raw)
            .bind(&input.cc_raw)
            .bind(&input.message_ids)
            .bind(&input.message_id_primary)
            .bind(&input.in_reply_to_ids)
            .bind(&input.references_ids)
            .bind(&input.mime_type)
            .bind(body_id)
            .fetch_one(&mut *tx)
            .await?;

            message_inserted = true;
            message_pk
        };

        for message_id in &input.message_ids {
            sqlx::query(
                r#"INSERT INTO message_id_map (message_id, message_pk, is_primary)
                VALUES ($1, $2, $3)
                ON CONFLICT (message_id, message_pk)
                DO UPDATE SET is_primary = message_id_map.is_primary OR EXCLUDED.is_primary"#,
            )
            .bind(message_id)
            .bind(message_pk)
            .bind(*message_id == input.message_id_primary)
            .execute(&mut *tx)
            .await?;
        }

        let instance_result = sqlx::query(
            r#"INSERT INTO list_message_instances
            (mailing_list_id, message_pk, repo_id, git_commit_oid)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (mailing_list_id, git_commit_oid) DO NOTHING"#,
        )
        .bind(repo.mailing_list_id)
        .bind(message_pk)
        .bind(repo.id)
        .bind(git_commit_oid)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(WriteOutcome {
            message_inserted,
            instance_inserted: instance_result.rows_affected() > 0,
        })
    }
}
