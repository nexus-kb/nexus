use chrono::{DateTime, Utc};
use csv::{QuoteStyle, Terminator, WriterBuilder};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use crate::{MailingListRepo, Result};

const COPY_NULL_SENTINEL: &str = "\u{001f}NEXUS_NULL\u{001f}";
const UTF8_INTEGRITY_SAMPLE_LIMIT: usize = 4_000;
const UTF8_INTEGRITY_OFFENDER_LIMIT: usize = 5;

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
            message_id_primary: String,
        }

        #[derive(Debug, Clone)]
        struct MessageIdentity {
            id: i64,
            message_id_primary: String,
        }

        let mut existing_messages_by_hash: HashMap<Vec<u8>, MessageIdentity> = HashMap::new();
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
                    "SELECT id, content_hash_sha256, message_id_primary \
                     FROM messages WHERE content_hash_sha256 IN (",
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
                    existing_messages_by_hash.insert(
                        message.content_hash_sha256,
                        MessageIdentity {
                            id: message.id,
                            message_id_primary: message.message_id_primary,
                        },
                    );
                }
            }
        }

        let mut insert_indexes = Vec::new();
        let mut pending_insert_hashes: HashSet<Vec<u8>> = HashSet::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            if let Some(message) =
                existing_messages_by_hash.get(&row.parsed_message.content_hash_sha256)
            {
                let message_pk = message.id;
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
        let mut inserted_primary_by_hash = HashMap::new();
        if !insert_indexes.is_empty() {
            let mut insert_bodies_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO message_bodies (body_text, diff_text, search_text, has_diff, has_attachments) ",
            );
            insert_bodies_qb.push_values(insert_indexes.iter(), |mut b, idx| {
                let row = &rows[*idx];
                b.push_bind(row.parsed_message.body.body_text.as_deref())
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

            for idx in &insert_indexes {
                let row = &rows[*idx];
                inserted_primary_by_hash.insert(
                    row.parsed_message.content_hash_sha256.clone(),
                    row.parsed_message.message_id_primary.clone(),
                );
            }
        }

        for idx in &pending_indexes {
            let row = &rows[*idx];
            if message_pk_by_commit.contains_key(&row.git_commit_oid) {
                continue;
            }

            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.id)
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
            let canonical_primary_message_id = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.message_id_primary.as_str())
                .or_else(|| {
                    inserted_primary_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .map(String::as_str)
                })
                .unwrap_or(row.parsed_message.message_id_primary.as_str());

            for message_id in &row.parsed_message.message_ids {
                message_id_rows.push((
                    message_id.clone(),
                    message_pk,
                    message_id == canonical_primary_message_id,
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

        validate_text_integrity(&mut tx, &message_pks).await?;

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
            message_id_primary: String,
        }

        #[derive(Debug, Clone)]
        struct MessageIdentity {
            id: i64,
            message_id_primary: String,
        }

        let mut existing_messages_by_hash: HashMap<Vec<u8>, MessageIdentity> = HashMap::new();
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
                    "SELECT id, content_hash_sha256, message_id_primary \
                     FROM messages WHERE content_hash_sha256 IN (",
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
                    existing_messages_by_hash.insert(
                        message.content_hash_sha256,
                        MessageIdentity {
                            id: message.id,
                            message_id_primary: message.message_id_primary,
                        },
                    );
                }
            }
        }

        let mut insert_indexes = Vec::new();
        let mut pending_insert_hashes: HashSet<Vec<u8>> = HashSet::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            if let Some(message) =
                existing_messages_by_hash.get(&row.parsed_message.content_hash_sha256)
            {
                let message_pk = message.id;
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
        let mut inserted_primary_by_hash = HashMap::new();
        for (offset, idx) in insert_indexes.iter().enumerate() {
            let row = &rows[*idx];
            let body_id = body_ids[offset];
            let message_id = message_ids[offset];

            body_rows.push(CopyMessageBodyRow {
                id: body_id,
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
            inserted_primary_by_hash.insert(
                row.parsed_message.content_hash_sha256.clone(),
                row.parsed_message.message_id_primary.clone(),
            );
        }

        for idx in &pending_indexes {
            let row = &rows[*idx];
            if message_pk_by_commit.contains_key(&row.git_commit_oid) {
                continue;
            }

            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.id)
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
            let canonical_primary_message_id = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.message_id_primary.as_str())
                .or_else(|| {
                    inserted_primary_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .map(String::as_str)
                })
                .unwrap_or(row.parsed_message.message_id_primary.as_str());

            for message_id in &row.parsed_message.message_ids {
                message_id_rows.push((
                    message_id.clone(),
                    message_pk,
                    message_id == canonical_primary_message_id,
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
            let payload = build_message_bodies_copy_payload(&body_rows)?;
            copy_into_table(
                &mut tx,
                "COPY message_bodies (id, body_text, diff_text, search_text, has_diff, has_attachments) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        if !message_rows.is_empty() {
            let payload = build_messages_copy_payload(&message_rows)?;
            copy_into_table(
                &mut tx,
                "COPY messages (id, content_hash_sha256, subject_raw, subject_norm, from_name, from_email, \
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary, in_reply_to_ids, references_ids, \
                 mime_type, body_id) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        if !message_id_promote_rows.is_empty() {
            promote_message_id_rows(&mut tx, &message_id_promote_rows).await?;
        }

        if !message_id_insert_rows.is_empty() {
            let payload = build_message_id_copy_payload(&message_id_insert_rows)?;
            copy_into_table(
                &mut tx,
                "COPY message_id_map (message_id, message_pk, is_primary) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        let inserted_instances = if instance_rows.is_empty() {
            0
        } else {
            let payload = build_instances_copy_payload(&instance_rows)?;
            copy_into_table(
                &mut tx,
                "COPY list_message_instances (mailing_list_id, message_pk, repo_id, git_commit_oid) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?
        };

        let mut message_pks: Vec<i64> = message_pk_by_commit.values().copied().collect();
        message_pks.sort_unstable();
        message_pks.dedup();

        validate_text_integrity(&mut tx, &message_pks).await?;

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
    payload: Vec<u8>,
) -> Result<u64> {
    if payload.is_empty() {
        return Ok(0);
    }

    let mut copy = (&mut **tx).copy_in_raw(statement).await?;
    copy.send(payload).await?;
    copy.finish().await
}

fn build_message_bodies_copy_payload(rows: &[CopyMessageBodyRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let id = row.id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(id)),
                row.body_text.as_deref().map(Cow::Borrowed),
                row.diff_text.as_deref().map(Cow::Borrowed),
                Some(Cow::Borrowed(row.search_text.as_str())),
                Some(Cow::Borrowed(encode_bool(row.has_diff))),
                Some(Cow::Borrowed(encode_bool(row.has_attachments))),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

fn build_messages_copy_payload(rows: &[CopyMessageRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let id = row.id.to_string();
        let content_hash = encode_bytea_hex(&row.content_hash_sha256);
        let date_utc = encode_nullable_datetime(row.date_utc);
        let message_ids = encode_text_array(&row.message_ids);
        let in_reply_to_ids = encode_text_array(&row.in_reply_to_ids);
        let references_ids = encode_text_array(&row.references_ids);
        let body_id = row.body_id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(id)),
                Some(Cow::Owned(content_hash)),
                Some(Cow::Borrowed(row.subject_raw.as_str())),
                Some(Cow::Borrowed(row.subject_norm.as_str())),
                row.from_name.as_deref().map(Cow::Borrowed),
                Some(Cow::Borrowed(row.from_email.as_str())),
                date_utc.as_deref().map(Cow::Borrowed),
                row.to_raw.as_deref().map(Cow::Borrowed),
                row.cc_raw.as_deref().map(Cow::Borrowed),
                Some(Cow::Owned(message_ids)),
                Some(Cow::Borrowed(row.message_id_primary.as_str())),
                Some(Cow::Owned(in_reply_to_ids)),
                Some(Cow::Owned(references_ids)),
                row.mime_type.as_deref().map(Cow::Borrowed),
                Some(Cow::Owned(body_id)),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

fn build_message_id_copy_payload(rows: &[CopyMessageIdRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let message_pk = row.message_pk.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Borrowed(row.message_id.as_str())),
                Some(Cow::Owned(message_pk)),
                Some(Cow::Borrowed(encode_bool(row.is_primary))),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

fn build_instances_copy_payload(rows: &[CopyInstanceRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let mailing_list_id = row.mailing_list_id.to_string();
        let message_pk = row.message_pk.to_string();
        let repo_id = row.repo_id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(mailing_list_id)),
                Some(Cow::Owned(message_pk)),
                Some(Cow::Owned(repo_id)),
                Some(Cow::Borrowed(row.git_commit_oid.as_str())),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

fn make_copy_writer() -> csv::Writer<Vec<u8>> {
    WriterBuilder::new()
        .has_headers(false)
        .quote_style(QuoteStyle::Always)
        .terminator(Terminator::Any(b'\n'))
        .from_writer(Vec::new())
}

fn write_copy_record<'a, const N: usize>(
    writer: &mut csv::Writer<Vec<u8>>,
    fields: [Option<Cow<'a, str>>; N],
) -> Result<()> {
    let encoded_fields: Vec<Cow<'a, str>> = fields
        .into_iter()
        .map(|field| field.unwrap_or_else(|| Cow::Borrowed(COPY_NULL_SENTINEL)))
        .collect();
    writer
        .write_record(encoded_fields.iter().map(|value| value.as_ref()))
        .map_err(|err| protocol_error(format!("failed to encode COPY row as CSV: {err}")))?;
    Ok(())
}

fn finish_copy_payload(writer: csv::Writer<Vec<u8>>) -> Result<Vec<u8>> {
    let payload = writer.into_inner().map_err(|err| {
        protocol_error(format!("failed to finalize COPY payload: {}", err.error()))
    })?;
    let quoted_sentinel = format!("\"{COPY_NULL_SENTINEL}\"");
    let payload = String::from_utf8(payload)
        .map_err(|err| protocol_error(format!("COPY payload is not UTF-8: {err}")))?;
    Ok(payload.replace(&quoted_sentinel, "\\N").into_bytes())
}

fn protocol_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}

fn encode_nullable_datetime(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(|v| v.to_rfc3339())
}

fn encode_bool(value: bool) -> &'static str {
    if value { "t" } else { "f" }
}

fn encode_bytea_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2 + 2);
    text.push('\\');
    text.push('x');
    for byte in bytes {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
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

async fn validate_text_integrity(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
) -> Result<()> {
    if message_pks.is_empty() {
        return Ok(());
    }

    let mut sample = message_pks.to_vec();
    sample.sort_unstable();
    sample.dedup();
    if sample.len() > UTF8_INTEGRITY_SAMPLE_LIMIT {
        sample.truncate(UTF8_INTEGRITY_SAMPLE_LIMIT);
    }

    let probe_error = match run_text_integrity_probe(tx, &sample).await {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };

    let offenders =
        collect_text_integrity_offenders(tx, &sample, UTF8_INTEGRITY_OFFENDER_LIMIT).await?;
    let offenders_text = if offenders.is_empty() {
        "none".to_string()
    } else {
        offenders
            .iter()
            .map(|(message_pk, body_id)| format!("message_pk={message_pk} body_id={body_id:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    Err(protocol_error(format!(
        "utf8 integrity validation failed: stage=text_integrity_probe sampled_ids={} offenders=[{}] probe_error={}",
        sample.len(),
        offenders_text,
        probe_error
    )))
}

async fn run_text_integrity_probe(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
) -> Result<()> {
    sqlx::query("SAVEPOINT copy_validate_probe")
        .execute(&mut **tx)
        .await?;

    let probe = sqlx::query(
        r#"SELECT
            m.id,
            m.body_id,
            LENGTH(nexus_safe_prefix(mb.search_text, 256)),
            LENGTH(nexus_safe_prefix(m.from_email, 128)),
            LENGTH(nexus_safe_prefix(m.subject_norm, 256))
        FROM messages m
        JOIN message_bodies mb
          ON mb.id = m.body_id
        WHERE m.id = ANY($1)
        ORDER BY m.id
        LIMIT $2"#,
    )
    .bind(message_pks)
    .bind(message_pks.len() as i64)
    .fetch_all(&mut **tx)
    .await;

    match probe {
        Ok(_) => {
            sqlx::query("RELEASE SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
        Err(err) => {
            sqlx::query("ROLLBACK TO SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            Err(err)
        }
    }
}

async fn collect_text_integrity_offenders(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
    max_offenders: usize,
) -> Result<Vec<(i64, Option<i64>)>> {
    if max_offenders == 0 {
        return Ok(Vec::new());
    }

    let mut offenders = Vec::new();
    for message_pk in message_pks {
        if offenders.len() >= max_offenders {
            break;
        }

        sqlx::query("SAVEPOINT copy_validate_row")
            .execute(&mut **tx)
            .await?;
        let check_result = sqlx::query(
            r#"SELECT
                LENGTH(nexus_safe_prefix(mb.search_text, 256)),
                LENGTH(nexus_safe_prefix(m.from_email, 128)),
                LENGTH(nexus_safe_prefix(m.subject_norm, 256))
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1"#,
        )
        .bind(*message_pk)
        .fetch_optional(&mut **tx)
        .await;

        match check_result {
            Ok(_) => {
                sqlx::query("RELEASE SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
            }
            Err(_) => {
                sqlx::query("ROLLBACK TO SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
                sqlx::query("RELEASE SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
                let body_id =
                    sqlx::query_scalar::<_, i64>("SELECT body_id FROM messages WHERE id = $1")
                        .bind(*message_pk)
                        .fetch_optional(&mut **tx)
                        .await?;
                offenders.push((*message_pk, body_id));
            }
        }
    }

    Ok(offenders)
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
    use super::{
        CopyMessageBodyRow, CopyMessageRow, build_message_bodies_copy_payload,
        build_messages_copy_payload, encode_bytea_hex, encode_text_array,
    };

    #[test]
    fn encode_bytea_hex_uses_postgres_hex_prefix() {
        assert_eq!(encode_bytea_hex(&[0x00, 0x7f, 0xff]), "\\x007fff");
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
    fn copy_payload_handles_csv_sensitive_content_and_nulls() {
        let payload = build_message_bodies_copy_payload(&[CopyMessageBodyRow {
            id: 42,
            body_text: Some("a,\"b\"\nline".to_string()),
            diff_text: None,
            search_text: "\\N".to_string(),
            has_diff: true,
            has_attachments: false,
        }])
        .expect("payload should be encoded");
        let encoded = String::from_utf8(payload).expect("payload should be utf-8");
        assert!(encoded.contains(",\\N,"));
        assert!(encoded.contains("\"\\N\""));
        assert!(encoded.contains("\"a,\"\"b\"\"\nline\""));
    }

    #[test]
    fn copy_messages_payload_round_trips_arrays_and_bytea() {
        let payload = build_messages_copy_payload(&[CopyMessageRow {
            id: 7,
            content_hash_sha256: vec![0x00, 0x7f, 0xff],
            subject_raw: "[PATCH] csv".to_string(),
            subject_norm: "csv".to_string(),
            from_name: None,
            from_email: "alice@example.com".to_string(),
            date_utc: None,
            to_raw: Some("list@example.com".to_string()),
            cc_raw: None,
            message_ids: vec![
                "id@example.com".to_string(),
                "with\"quote".to_string(),
                "with\\slash".to_string(),
            ],
            message_id_primary: "id@example.com".to_string(),
            in_reply_to_ids: vec!["parent@example.com".to_string()],
            references_ids: vec!["root@example.com".to_string()],
            mime_type: Some("text/plain".to_string()),
            body_id: 11,
        }])
        .expect("payload should be encoded");

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(payload.as_slice());
        let record = reader
            .records()
            .next()
            .expect("one row")
            .expect("valid csv row");
        assert_eq!(record.get(1), Some("\\x007fff"));
        assert_eq!(
            record.get(9),
            Some("{\"id@example.com\",\"with\\\"quote\",\"with\\\\slash\"}")
        );
        assert_eq!(record.get(4), Some("\\N"));
    }
}
