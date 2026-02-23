use super::*;

impl IngestStore {
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
            let existing_message_pk = sqlx::query_scalar::<_, i64>(
                r#"SELECT message_pk
                FROM list_message_instances
                WHERE mailing_list_id = $1 AND git_commit_oid = $2
                ORDER BY message_pk ASC
                LIMIT 1"#,
            )
            .bind(repo.mailing_list_id)
            .bind(git_commit_oid)
            .fetch_optional(&mut *tx)
            .await?;

            tx.commit().await?;
            return Ok(WriteOutcome {
                message_inserted: false,
                instance_inserted: false,
                message_pk: existing_message_pk,
            });
        }

        let mut message_inserted = false;

        #[derive(sqlx::FromRow)]
        struct ExistingMessageIdentityRow {
            id: i64,
            message_id_primary: String,
        }

        let existing_message = sqlx::query_as::<_, ExistingMessageIdentityRow>(
            "SELECT id, message_id_primary FROM messages WHERE content_hash_sha256 = $1",
        )
        .bind(&input.content_hash_sha256)
        .fetch_optional(&mut *tx)
        .await?;

        let (message_pk, canonical_primary_message_id) =
            if let Some(existing_message) = existing_message {
                (existing_message.id, existing_message.message_id_primary)
            } else {
                let body_id = sqlx::query_scalar::<_, i64>(
                    r#"INSERT INTO message_bodies
                (body_text, diff_text, search_text, has_diff, has_attachments)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id"#,
                )
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
                (message_pk, input.message_id_primary.clone())
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
            .bind(message_id == &canonical_primary_message_id)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(patch_facts) = &input.patch_facts {
            upsert_message_patch_facts(&mut tx, message_pk, patch_facts).await?;
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
            message_pk: Some(message_pk),
        })
    }
}
