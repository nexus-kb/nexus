use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

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
    pub message_pk: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IngestCommitRow {
    pub git_commit_oid: String,
    pub parsed_message: ParsedMessageInput,
}

#[derive(Debug, Clone, Default)]
pub struct BatchWriteOutcome {
    pub inserted_instances: u64,
    pub message_pks: Vec<i64>,
    pub skipped_commits: Vec<String>,
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
            message_pk: Some(message_pk),
        })
    }

    pub async fn ingest_messages_batch(
        &self,
        repo: &MailingListRepo,
        rows: &[IngestCommitRow],
        relaxed_durability: bool,
    ) -> Result<BatchWriteOutcome> {
        if rows.is_empty() {
            return Ok(BatchWriteOutcome::default());
        }

        let mut tx = self.pool.begin().await?;
        if relaxed_durability {
            sqlx::query("SET LOCAL synchronous_commit = off")
                .execute(&mut *tx)
                .await?;
        }

        let mut unique_indexes = Vec::with_capacity(rows.len());
        let mut seen_commit_oids: HashSet<&str> = HashSet::new();
        for (idx, row) in rows.iter().enumerate() {
            if seen_commit_oids.insert(row.git_commit_oid.as_str()) {
                unique_indexes.push(idx);
            }
        }

        let commit_oids: Vec<String> = unique_indexes
            .iter()
            .map(|idx| rows[*idx].git_commit_oid.clone())
            .collect();

        #[derive(sqlx::FromRow)]
        struct ExistingInstanceRow {
            git_commit_oid: String,
            message_pk: i64,
        }

        let existing_instance_rows = sqlx::query_as::<_, ExistingInstanceRow>(
            r#"SELECT git_commit_oid, message_pk
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND git_commit_oid = ANY($2)"#,
        )
        .bind(repo.mailing_list_id)
        .bind(&commit_oids)
        .fetch_all(&mut *tx)
        .await?;

        let mut existing_instances = HashMap::with_capacity(existing_instance_rows.len());
        for row in existing_instance_rows {
            existing_instances.insert(row.git_commit_oid, row.message_pk);
        }

        let mut message_pk_by_commit: HashMap<String, i64> =
            HashMap::with_capacity(unique_indexes.len());
        let mut skipped_commits = Vec::new();
        let mut skipped_commit_set = HashSet::new();
        let mut pending_indexes = Vec::new();

        for idx in &unique_indexes {
            let row = &rows[*idx];
            if let Some(message_pk) = existing_instances.get(&row.git_commit_oid).copied() {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
                skipped_commit_set.insert(row.git_commit_oid.clone());
                skipped_commits.push(row.git_commit_oid.clone());
            } else {
                pending_indexes.push(*idx);
            }
        }

        #[derive(sqlx::FromRow)]
        struct ExistingMessageRow {
            id: i64,
            content_hash_sha256: Vec<u8>,
        }

        let mut existing_messages_by_hash: HashMap<Vec<u8>, i64> = HashMap::new();
        if !pending_indexes.is_empty() {
            let mut content_hashes = Vec::new();
            let mut seen_hashes: HashSet<Vec<u8>> = HashSet::new();
            for idx in &pending_indexes {
                let hash = &rows[*idx].parsed_message.content_hash_sha256;
                if seen_hashes.insert(hash.clone()) {
                    content_hashes.push(hash.clone());
                }
            }

            if !content_hashes.is_empty() {
                let mut qb = QueryBuilder::<Postgres>::new(
                    "SELECT id, content_hash_sha256 FROM messages WHERE content_hash_sha256 IN (",
                );
                let mut separated = qb.separated(", ");
                for hash in &content_hashes {
                    separated.push_bind(hash);
                }
                separated.push_unseparated(")");

                let existing_messages = qb
                    .build_query_as::<ExistingMessageRow>()
                    .fetch_all(&mut *tx)
                    .await?;

                for message in existing_messages {
                    existing_messages_by_hash.insert(message.content_hash_sha256, message.id);
                }
            }
        }

        let mut insert_indexes = Vec::new();
        let mut pending_insert_hashes: HashSet<Vec<u8>> = HashSet::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .copied()
            {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if pending_insert_hashes.insert(row.parsed_message.content_hash_sha256.clone()) {
                insert_indexes.push(*idx);
            }
        }

        #[derive(sqlx::FromRow)]
        struct InsertedBodyRow {
            id: i64,
        }

        #[derive(sqlx::FromRow)]
        struct InsertedMessageRow {
            id: i64,
            content_hash_sha256: Vec<u8>,
        }

        let mut inserted_messages_by_hash = HashMap::new();
        if !insert_indexes.is_empty() {
            let mut insert_bodies_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO message_bodies (raw_rfc822, body_text, diff_text, search_text, has_diff, has_attachments) ",
            );
            insert_bodies_qb.push_values(insert_indexes.iter(), |mut b, idx| {
                let row = &rows[*idx];
                b.push_bind(row.parsed_message.body.raw_rfc822.as_slice())
                    .push_bind(row.parsed_message.body.body_text.as_deref())
                    .push_bind(row.parsed_message.body.diff_text.as_deref())
                    .push_bind(row.parsed_message.body.search_text.as_str())
                    .push_bind(row.parsed_message.body.has_diff)
                    .push_bind(row.parsed_message.body.has_attachments);
            });
            insert_bodies_qb.push(" RETURNING id");

            let inserted_bodies = insert_bodies_qb
                .build_query_as::<InsertedBodyRow>()
                .fetch_all(&mut *tx)
                .await?;

            let mut insert_messages_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO messages (content_hash_sha256, subject_raw, subject_norm, from_name, from_email, \
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary, in_reply_to_ids, references_ids, \
                 mime_type, body_id) ",
            );

            insert_messages_qb.push_values(
                insert_indexes.iter().zip(inserted_bodies.iter()),
                |mut b, (idx, inserted_body)| {
                    let row = &rows[*idx];
                    b.push_bind(row.parsed_message.content_hash_sha256.as_slice())
                        .push_bind(row.parsed_message.subject_raw.as_str())
                        .push_bind(row.parsed_message.subject_norm.as_str())
                        .push_bind(row.parsed_message.from_name.as_deref())
                        .push_bind(row.parsed_message.from_email.as_str())
                        .push_bind(row.parsed_message.date_utc)
                        .push_bind(row.parsed_message.to_raw.as_deref())
                        .push_bind(row.parsed_message.cc_raw.as_deref())
                        .push_bind(&row.parsed_message.message_ids)
                        .push_bind(row.parsed_message.message_id_primary.as_str())
                        .push_bind(&row.parsed_message.in_reply_to_ids)
                        .push_bind(&row.parsed_message.references_ids)
                        .push_bind(row.parsed_message.mime_type.as_deref())
                        .push_bind(inserted_body.id);
                },
            );
            insert_messages_qb.push(" RETURNING id, content_hash_sha256");

            let inserted_messages = insert_messages_qb
                .build_query_as::<InsertedMessageRow>()
                .fetch_all(&mut *tx)
                .await?;

            for row in inserted_messages {
                inserted_messages_by_hash.insert(row.content_hash_sha256, row.id);
            }
        }

        for idx in &pending_indexes {
            let row = &rows[*idx];
            if message_pk_by_commit.contains_key(&row.git_commit_oid) {
                continue;
            }

            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .copied()
                .or_else(|| {
                    inserted_messages_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .copied()
                })
            {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if skipped_commit_set.insert(row.git_commit_oid.clone()) {
                skipped_commits.push(row.git_commit_oid.clone());
            }
        }

        let mut message_id_rows = Vec::new();
        let mut instance_rows = Vec::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            let Some(message_pk) = message_pk_by_commit.get(&row.git_commit_oid).copied() else {
                continue;
            };

            for message_id in &row.parsed_message.message_ids {
                message_id_rows.push((
                    message_id.clone(),
                    message_pk,
                    *message_id == row.parsed_message.message_id_primary,
                ));
            }

            instance_rows.push((
                repo.mailing_list_id,
                message_pk,
                repo.id,
                row.git_commit_oid.clone(),
            ));
        }

        let message_id_rows = dedupe_message_id_rows(message_id_rows);
        if !message_id_rows.is_empty() {
            let mut insert_message_ids_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO message_id_map (message_id, message_pk, is_primary) ",
            );
            insert_message_ids_qb.push_values(message_id_rows.iter(), |mut b, row| {
                b.push_bind(&row.message_id)
                    .push_bind(row.message_pk)
                    .push_bind(row.is_primary);
            });
            insert_message_ids_qb.push(
                " ON CONFLICT (message_id, message_pk) \
                 DO UPDATE SET is_primary = message_id_map.is_primary OR EXCLUDED.is_primary",
            );
            insert_message_ids_qb.build().execute(&mut *tx).await?;
        }

        #[derive(sqlx::FromRow)]
        struct InsertedInstanceRow {
            git_commit_oid: String,
        }

        let mut inserted_instances = 0u64;
        if !instance_rows.is_empty() {
            let mut insert_instances_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO list_message_instances (mailing_list_id, message_pk, repo_id, git_commit_oid) ",
            );
            insert_instances_qb.push_values(instance_rows.iter(), |mut b, row| {
                b.push_bind(row.0)
                    .push_bind(row.1)
                    .push_bind(row.2)
                    .push_bind(&row.3);
            });
            insert_instances_qb
                .push(" ON CONFLICT (mailing_list_id, git_commit_oid) DO NOTHING RETURNING git_commit_oid");

            let inserted = insert_instances_qb
                .build_query_as::<InsertedInstanceRow>()
                .fetch_all(&mut *tx)
                .await?;

            inserted_instances = inserted.len() as u64;

            let inserted_commit_set: HashSet<String> =
                inserted.into_iter().map(|row| row.git_commit_oid).collect();
            for row in &instance_rows {
                if !inserted_commit_set.contains(&row.3) && skipped_commit_set.insert(row.3.clone())
                {
                    skipped_commits.push(row.3.clone());
                }
            }
        }

        let mut message_pks: Vec<i64> = message_pk_by_commit.values().copied().collect();
        message_pks.sort_unstable();
        message_pks.dedup();

        tx.commit().await?;

        Ok(BatchWriteOutcome {
            inserted_instances,
            message_pks,
            skipped_commits,
        })
    }

    pub async fn ingest_messages_copy_batch(
        &self,
        repo: &MailingListRepo,
        rows: &[IngestCommitRow],
        relaxed_durability: bool,
    ) -> Result<BatchWriteOutcome> {
        if rows.is_empty() {
            return Ok(BatchWriteOutcome::default());
        }

        let mut tx = self.pool.begin().await?;
        if relaxed_durability {
            sqlx::query("SET LOCAL synchronous_commit = off")
                .execute(&mut *tx)
                .await?;
        }

        let mut unique_indexes = Vec::with_capacity(rows.len());
        let mut seen_commit_oids: HashSet<&str> = HashSet::new();
        for (idx, row) in rows.iter().enumerate() {
            if seen_commit_oids.insert(row.git_commit_oid.as_str()) {
                unique_indexes.push(idx);
            }
        }

        let commit_oids: Vec<String> = unique_indexes
            .iter()
            .map(|idx| rows[*idx].git_commit_oid.clone())
            .collect();

        #[derive(sqlx::FromRow)]
        struct ExistingInstanceRow {
            git_commit_oid: String,
            message_pk: i64,
        }

        let existing_instance_rows = sqlx::query_as::<_, ExistingInstanceRow>(
            r#"SELECT git_commit_oid, message_pk
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND git_commit_oid = ANY($2)"#,
        )
        .bind(repo.mailing_list_id)
        .bind(&commit_oids)
        .fetch_all(&mut *tx)
        .await?;

        let mut existing_instances = HashMap::with_capacity(existing_instance_rows.len());
        for row in existing_instance_rows {
            existing_instances.insert(row.git_commit_oid, row.message_pk);
        }

        let mut message_pk_by_commit: HashMap<String, i64> =
            HashMap::with_capacity(unique_indexes.len());
        let mut skipped_commits = Vec::new();
        let mut skipped_commit_set = HashSet::new();
        let mut pending_indexes = Vec::new();

        for idx in &unique_indexes {
            let row = &rows[*idx];
            if let Some(message_pk) = existing_instances.get(&row.git_commit_oid).copied() {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
                skipped_commit_set.insert(row.git_commit_oid.clone());
                skipped_commits.push(row.git_commit_oid.clone());
            } else {
                pending_indexes.push(*idx);
            }
        }

        #[derive(sqlx::FromRow)]
        struct ExistingMessageRow {
            id: i64,
            content_hash_sha256: Vec<u8>,
        }

        let mut existing_messages_by_hash: HashMap<Vec<u8>, i64> = HashMap::new();
        if !pending_indexes.is_empty() {
            let mut content_hashes = Vec::new();
            let mut seen_hashes: HashSet<Vec<u8>> = HashSet::new();
            for idx in &pending_indexes {
                let hash = &rows[*idx].parsed_message.content_hash_sha256;
                if seen_hashes.insert(hash.clone()) {
                    content_hashes.push(hash.clone());
                }
            }

            if !content_hashes.is_empty() {
                let mut qb = QueryBuilder::<Postgres>::new(
                    "SELECT id, content_hash_sha256 FROM messages WHERE content_hash_sha256 IN (",
                );
                let mut separated = qb.separated(", ");
                for hash in &content_hashes {
                    separated.push_bind(hash);
                }
                separated.push_unseparated(")");

                let existing_messages = qb
                    .build_query_as::<ExistingMessageRow>()
                    .fetch_all(&mut *tx)
                    .await?;

                for message in existing_messages {
                    existing_messages_by_hash.insert(message.content_hash_sha256, message.id);
                }
            }
        }

        let mut insert_indexes = Vec::new();
        let mut pending_insert_hashes: HashSet<Vec<u8>> = HashSet::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .copied()
            {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if pending_insert_hashes.insert(row.parsed_message.content_hash_sha256.clone()) {
                insert_indexes.push(*idx);
            }
        }

        let body_ids = reserve_ids(&mut tx, "message_bodies_id_seq", insert_indexes.len()).await?;
        let message_ids = reserve_ids(&mut tx, "messages_id_seq", insert_indexes.len()).await?;

        let mut body_rows = Vec::with_capacity(insert_indexes.len());
        let mut message_rows = Vec::with_capacity(insert_indexes.len());
        let mut inserted_messages_by_hash = HashMap::new();
        for (offset, idx) in insert_indexes.iter().enumerate() {
            let row = &rows[*idx];
            let body_id = body_ids[offset];
            let message_id = message_ids[offset];

            body_rows.push(CopyMessageBodyRow {
                id: body_id,
                raw_rfc822: row.parsed_message.body.raw_rfc822.clone(),
                body_text: row.parsed_message.body.body_text.clone(),
                diff_text: row.parsed_message.body.diff_text.clone(),
                search_text: row.parsed_message.body.search_text.clone(),
                has_diff: row.parsed_message.body.has_diff,
                has_attachments: row.parsed_message.body.has_attachments,
            });

            message_rows.push(CopyMessageRow {
                id: message_id,
                content_hash_sha256: row.parsed_message.content_hash_sha256.clone(),
                subject_raw: row.parsed_message.subject_raw.clone(),
                subject_norm: row.parsed_message.subject_norm.clone(),
                from_name: row.parsed_message.from_name.clone(),
                from_email: row.parsed_message.from_email.clone(),
                date_utc: row.parsed_message.date_utc,
                to_raw: row.parsed_message.to_raw.clone(),
                cc_raw: row.parsed_message.cc_raw.clone(),
                message_ids: row.parsed_message.message_ids.clone(),
                message_id_primary: row.parsed_message.message_id_primary.clone(),
                in_reply_to_ids: row.parsed_message.in_reply_to_ids.clone(),
                references_ids: row.parsed_message.references_ids.clone(),
                mime_type: row.parsed_message.mime_type.clone(),
                body_id,
            });

            inserted_messages_by_hash
                .insert(row.parsed_message.content_hash_sha256.clone(), message_id);
        }

        for idx in &pending_indexes {
            let row = &rows[*idx];
            if message_pk_by_commit.contains_key(&row.git_commit_oid) {
                continue;
            }

            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .copied()
                .or_else(|| {
                    inserted_messages_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .copied()
                })
            {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if skipped_commit_set.insert(row.git_commit_oid.clone()) {
                skipped_commits.push(row.git_commit_oid.clone());
            }
        }

        let mut message_id_rows = Vec::new();
        let mut instance_rows = Vec::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            let Some(message_pk) = message_pk_by_commit.get(&row.git_commit_oid).copied() else {
                continue;
            };

            for message_id in &row.parsed_message.message_ids {
                message_id_rows.push((
                    message_id.clone(),
                    message_pk,
                    *message_id == row.parsed_message.message_id_primary,
                ));
            }

            instance_rows.push(CopyInstanceRow {
                mailing_list_id: repo.mailing_list_id,
                message_pk,
                repo_id: repo.id,
                git_commit_oid: row.git_commit_oid.clone(),
            });
        }

        let message_id_rows = dedupe_message_id_rows(message_id_rows);
        let existing_message_id_rows =
            load_existing_message_id_rows(&mut tx, &message_id_rows).await?;

        let mut existing_primary_lookup = HashMap::with_capacity(existing_message_id_rows.len());
        for row in existing_message_id_rows {
            existing_primary_lookup.insert((row.message_id, row.message_pk), row.is_primary);
        }

        let mut message_id_insert_rows = Vec::new();
        let mut message_id_promote_rows = Vec::new();
        for row in message_id_rows {
            let key = (row.message_id.clone(), row.message_pk);
            if let Some(existing_primary) = existing_primary_lookup.get(&key).copied() {
                if row.is_primary && !existing_primary {
                    message_id_promote_rows.push(key);
                }
            } else {
                message_id_insert_rows.push(row);
            }
        }

        if !body_rows.is_empty() {
            copy_into_table(
                &mut tx,
                "COPY message_bodies (id, raw_rfc822, body_text, diff_text, search_text, has_diff, has_attachments) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                build_message_bodies_copy_payload(&body_rows),
            )
            .await?;
        }

        if !message_rows.is_empty() {
            copy_into_table(
                &mut tx,
                "COPY messages (id, content_hash_sha256, subject_raw, subject_norm, from_name, from_email, \
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary, in_reply_to_ids, references_ids, \
                 mime_type, body_id) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                build_messages_copy_payload(&message_rows),
            )
            .await?;
        }

        if !message_id_promote_rows.is_empty() {
            promote_message_id_rows(&mut tx, &message_id_promote_rows).await?;
        }

        if !message_id_insert_rows.is_empty() {
            copy_into_table(
                &mut tx,
                "COPY message_id_map (message_id, message_pk, is_primary) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                build_message_id_copy_payload(&message_id_insert_rows),
            )
            .await?;
        }

        let inserted_instances = if instance_rows.is_empty() {
            0
        } else {
            copy_into_table(
                &mut tx,
                "COPY list_message_instances (mailing_list_id, message_pk, repo_id, git_commit_oid) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                build_instances_copy_payload(&instance_rows),
            )
            .await?
        };

        let mut message_pks: Vec<i64> = message_pk_by_commit.values().copied().collect();
        message_pks.sort_unstable();
        message_pks.dedup();

        tx.commit().await?;

        Ok(BatchWriteOutcome {
            inserted_instances,
            message_pks,
            skipped_commits,
        })
    }
}

#[derive(Debug, Clone)]
struct CopyMessageBodyRow {
    id: i64,
    raw_rfc822: Vec<u8>,
    body_text: Option<String>,
    diff_text: Option<String>,
    search_text: String,
    has_diff: bool,
    has_attachments: bool,
}

#[derive(Debug, Clone)]
struct CopyMessageRow {
    id: i64,
    content_hash_sha256: Vec<u8>,
    subject_raw: String,
    subject_norm: String,
    from_name: Option<String>,
    from_email: String,
    date_utc: Option<DateTime<Utc>>,
    to_raw: Option<String>,
    cc_raw: Option<String>,
    message_ids: Vec<String>,
    message_id_primary: String,
    in_reply_to_ids: Vec<String>,
    references_ids: Vec<String>,
    mime_type: Option<String>,
    body_id: i64,
}

#[derive(Debug, Clone)]
struct CopyMessageIdRow {
    message_id: String,
    message_pk: i64,
    is_primary: bool,
}

#[derive(Debug, Clone)]
struct CopyInstanceRow {
    mailing_list_id: i64,
    message_pk: i64,
    repo_id: i64,
    git_commit_oid: String,
}

#[derive(Debug, sqlx::FromRow)]
struct ExistingMessageIdRow {
    message_id: String,
    message_pk: i64,
    is_primary: bool,
}

async fn reserve_ids(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sequence_name: &str,
    count: usize,
) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    sqlx::query_scalar::<_, i64>(
        "SELECT nextval($1::regclass)::bigint FROM generate_series(1, $2::bigint)",
    )
    .bind(sequence_name)
    .bind(count as i64)
    .fetch_all(&mut **tx)
    .await
}

async fn copy_into_table(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    statement: &str,
    payload: String,
) -> Result<u64> {
    if payload.is_empty() {
        return Ok(0);
    }

    let mut copy = (&mut **tx).copy_in_raw(statement).await?;
    copy.send(payload.as_bytes()).await?;
    copy.finish().await
}

fn build_message_bodies_copy_payload(rows: &[CopyMessageBodyRow]) -> String {
    let mut out = String::new();
    for row in rows {
        let fields = [
            row.id.to_string(),
            encode_bytea_hex(&row.raw_rfc822),
            encode_nullable_text(row.body_text.as_deref()),
            encode_nullable_text(row.diff_text.as_deref()),
            encode_nullable_text(Some(&row.search_text)),
            encode_bool(row.has_diff),
            encode_bool(row.has_attachments),
        ];
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out
}

fn build_messages_copy_payload(rows: &[CopyMessageRow]) -> String {
    let mut out = String::new();
    for row in rows {
        let fields = [
            row.id.to_string(),
            encode_bytea_hex(&row.content_hash_sha256),
            encode_nullable_text(Some(&row.subject_raw)),
            encode_nullable_text(Some(&row.subject_norm)),
            encode_nullable_text(row.from_name.as_deref()),
            encode_nullable_text(Some(&row.from_email)),
            encode_nullable_datetime(row.date_utc),
            encode_nullable_text(row.to_raw.as_deref()),
            encode_nullable_text(row.cc_raw.as_deref()),
            encode_nullable_text(Some(&encode_text_array(&row.message_ids))),
            encode_nullable_text(Some(&row.message_id_primary)),
            encode_nullable_text(Some(&encode_text_array(&row.in_reply_to_ids))),
            encode_nullable_text(Some(&encode_text_array(&row.references_ids))),
            encode_nullable_text(row.mime_type.as_deref()),
            row.body_id.to_string(),
        ];
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out
}

fn build_message_id_copy_payload(rows: &[CopyMessageIdRow]) -> String {
    let mut out = String::new();
    for row in rows {
        let fields = [
            encode_nullable_text(Some(&row.message_id)),
            row.message_pk.to_string(),
            encode_bool(row.is_primary),
        ];
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out
}

fn build_instances_copy_payload(rows: &[CopyInstanceRow]) -> String {
    let mut out = String::new();
    for row in rows {
        let fields = [
            row.mailing_list_id.to_string(),
            row.message_pk.to_string(),
            row.repo_id.to_string(),
            encode_nullable_text(Some(&row.git_commit_oid)),
        ];
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out
}

fn encode_nullable_text(value: Option<&str>) -> String {
    match value {
        Some(v) => {
            let mut escaped = String::with_capacity(v.len() + 2);
            escaped.push('"');
            for ch in v.chars() {
                if ch == '"' {
                    escaped.push('"');
                }
                escaped.push(ch);
            }
            escaped.push('"');
            escaped
        }
        None => "\\N".to_string(),
    }
}

fn encode_nullable_datetime(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|v| encode_nullable_text(Some(&v.to_rfc3339())))
        .unwrap_or_else(|| "\\N".to_string())
}

fn encode_bool(value: bool) -> String {
    if value {
        "t".to_string()
    } else {
        "f".to_string()
    }
}

fn encode_bytea_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2 + 2);
    text.push('\\');
    text.push('x');
    for byte in bytes {
        let _ = write!(&mut text, "{byte:02x}");
    }
    encode_nullable_text(Some(&text))
}

fn encode_text_array(values: &[String]) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }

    let mut out = String::from("{");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in value.chars() {
            if ch == '\\' || ch == '"' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push('}');
    out
}

fn dedupe_message_id_rows(rows: Vec<(String, i64, bool)>) -> Vec<CopyMessageIdRow> {
    let mut deduped = HashMap::<(String, i64), bool>::new();
    for (message_id, message_pk, is_primary) in rows {
        let entry = deduped.entry((message_id, message_pk)).or_insert(false);
        *entry = *entry || is_primary;
    }

    deduped
        .into_iter()
        .map(|((message_id, message_pk), is_primary)| CopyMessageIdRow {
            message_id,
            message_pk,
            is_primary,
        })
        .collect()
}

async fn load_existing_message_id_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[CopyMessageIdRow],
) -> Result<Vec<ExistingMessageIdRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut found = Vec::new();
    for chunk in rows.chunks(3000) {
        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT message_id, message_pk, is_primary FROM message_id_map WHERE (message_id, message_pk) IN (",
        );
        for (idx, row) in chunk.iter().enumerate() {
            if idx > 0 {
                qb.push(", ");
            }
            qb.push("(")
                .push_bind(&row.message_id)
                .push(", ")
                .push_bind(row.message_pk)
                .push(")");
        }
        qb.push(")");

        let mut existing = qb
            .build_query_as::<ExistingMessageIdRow>()
            .fetch_all(&mut **tx)
            .await?;
        found.append(&mut existing);
    }

    Ok(found)
}

async fn promote_message_id_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[(String, i64)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    for chunk in rows.chunks(3000) {
        let mut qb = QueryBuilder::<Postgres>::new(
            "UPDATE message_id_map SET is_primary = true WHERE is_primary = false AND (message_id, message_pk) IN (",
        );
        for (idx, (message_id, message_pk)) in chunk.iter().enumerate() {
            if idx > 0 {
                qb.push(", ");
            }
            qb.push("(")
                .push_bind(message_id)
                .push(", ")
                .push_bind(*message_pk)
                .push(")");
        }
        qb.push(")");
        qb.build().execute(&mut **tx).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{encode_bytea_hex, encode_nullable_text, encode_text_array};

    #[test]
    fn encode_bytea_hex_uses_postgres_hex_prefix() {
        assert_eq!(encode_bytea_hex(&[0x00, 0x7f, 0xff]), "\"\\x007fff\"");
    }

    #[test]
    fn encode_text_array_escapes_quotes_and_backslashes() {
        let values = vec![
            "a/b".to_string(),
            "with\"quote".to_string(),
            "with\\slash".to_string(),
        ];
        assert_eq!(
            encode_text_array(&values),
            "{\"a/b\",\"with\\\"quote\",\"with\\\\slash\"}"
        );
    }

    #[test]
    fn encode_nullable_text_handles_commas_quotes_and_newlines() {
        assert_eq!(
            encode_nullable_text(Some("a,\"b\"\nline")),
            "\"a,\"\"b\"\"\nline\""
        );
        assert_eq!(encode_nullable_text(None), "\\N");
    }
}
