use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::{HashMap, HashSet};
use tracing::warn;

use crate::Result;

/// Parsed recipient information for ingest.
#[derive(Debug, Clone)]
pub struct EmailRecipient {
    pub email: String,
    pub name: Option<String>,
}

/// Parsed email input ready for ingest.
#[derive(Debug, Clone)]
pub struct EmailInput {
    pub mailing_list_id: i32,
    pub message_id: String,
    pub blob_oid: String,
    pub epoch: i16,
    pub author_email: String,
    pub author_name: Option<String>,
    pub subject: String,
    pub normalized_subject: String,
    pub date: DateTime<Utc>,
    pub in_reply_to: Option<String>,
    pub body: Option<String>,
    pub patch_metadata: Option<serde_json::Value>,
    pub references: Vec<String>,
    pub to: Vec<EmailRecipient>,
    pub cc: Vec<EmailRecipient>,
}

/// Bulk ingest helper for parsed mail batches.
#[derive(Clone)]
pub struct MailIngestStore {
    pool: PgPool,
}

impl MailIngestStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ingest_batch(&self, mailing_list_id: i32, batch: &[EmailInput]) -> Result<usize> {
        if batch.is_empty() {
            return Ok(0);
        }

        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;
        let authors = collect_unique_authors(batch);
        let mut author_emails = Vec::with_capacity(authors.len());
        let mut author_names = Vec::with_capacity(authors.len());
        for (email, name) in authors {
            author_emails.push(email);
            author_names.push(name);
        }

        if !author_emails.is_empty() {
            sqlx::query(
                r#"INSERT INTO authors (email, name, first_seen, last_seen)
                SELECT email, name, NOW(), NOW()
                FROM UNNEST($1::text[], $2::text[]) AS t(email, name)
                ON CONFLICT (email) DO UPDATE
                    SET name = COALESCE(EXCLUDED.name, authors.name),
                        last_seen = NOW()"#,
            )
            .bind(&author_emails)
            .bind(&author_names)
            .execute(&mut *tx)
            .await?;
        }

        let author_rows: Vec<(String, i32)> = if author_emails.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as("SELECT email, id FROM authors WHERE email = ANY($1)")
                .bind(&author_emails)
                .fetch_all(&mut *tx)
                .await?
        };
        let author_id_map: HashMap<String, i32> = author_rows.into_iter().collect();

        let mut list_ids = Vec::with_capacity(batch.len());
        let mut message_ids = Vec::with_capacity(batch.len());
        let mut blob_oids = Vec::with_capacity(batch.len());
        let mut author_ids = Vec::with_capacity(batch.len());
        let mut subjects = Vec::with_capacity(batch.len());
        let mut normalized_subjects = Vec::with_capacity(batch.len());
        let mut dates = Vec::with_capacity(batch.len());
        let mut in_reply_tos = Vec::with_capacity(batch.len());
        let mut epochs = Vec::with_capacity(batch.len());
        let mut patch_metadatas = Vec::with_capacity(batch.len());

        for email in batch {
            let author_id = match author_id_map.get(&email.author_email) {
                Some(id) => *id,
                None => {
                    warn!(
                        author = %email.author_email,
                        message_id = %email.message_id,
                        "author not found for email; skipping"
                    );
                    continue;
                }
            };
            list_ids.push(mailing_list_id);
            message_ids.push(email.message_id.clone());
            blob_oids.push(email.blob_oid.clone());
            author_ids.push(author_id);
            subjects.push(email.subject.clone());
            normalized_subjects.push(email.normalized_subject.clone());
            dates.push(email.date);
            in_reply_tos.push(email.in_reply_to.clone());
            epochs.push(email.epoch);
            patch_metadatas.push(email.patch_metadata.clone());
        }

        let inserted_count = if message_ids.is_empty() {
            0
        } else {
            sqlx::query_scalar::<_, i64>(
                r#"WITH data AS (
                    SELECT * FROM UNNEST(
                        $1::int[],
                        $2::text[],
                        $3::text[],
                        $4::int[],
                        $5::text[],
                        $6::text[],
                        $7::timestamptz[],
                        $8::text[],
                        $9::int2[],
                        $10::jsonb[]
                    ) AS t (
                        mailing_list_id,
                        message_id,
                        blob_oid,
                        author_id,
                        subject,
                        normalized_subject,
                        date,
                        in_reply_to,
                        epoch,
                        patch_metadata
                    )
                ),
                inserted AS (
                    INSERT INTO emails (mailing_list_id, message_id, blob_oid, author_id, subject, normalized_subject, date, in_reply_to, epoch, patch_metadata)
                    SELECT mailing_list_id, message_id, blob_oid, author_id, subject, normalized_subject, date, in_reply_to, epoch, patch_metadata
                    FROM data
                    ON CONFLICT (mailing_list_id, message_id) DO NOTHING
                    RETURNING id, author_id, date
                ),
                activity AS (
                    INSERT INTO author_mailing_list_activity (author_id, mailing_list_id, first_email_date, last_email_date, created_count, participated_count)
                    SELECT author_id, $11, MIN(date), MAX(date), COUNT(*), COUNT(*)
                    FROM inserted
                    GROUP BY author_id
                    ON CONFLICT (author_id, mailing_list_id) DO UPDATE
                        SET first_email_date = LEAST(author_mailing_list_activity.first_email_date, EXCLUDED.first_email_date),
                            last_email_date = GREATEST(author_mailing_list_activity.last_email_date, EXCLUDED.last_email_date),
                            created_count = author_mailing_list_activity.created_count + EXCLUDED.created_count,
                            participated_count = author_mailing_list_activity.participated_count + EXCLUDED.participated_count
                    RETURNING 1
                )
                SELECT COUNT(*) FROM inserted"#,
            )
            .bind(&list_ids)
            .bind(&message_ids)
            .bind(&blob_oids)
            .bind(&author_ids)
            .bind(&subjects)
            .bind(&normalized_subjects)
            .bind(&dates)
            .bind(&in_reply_tos)
            .bind(&epochs)
            .bind(&patch_metadatas)
            .bind(mailing_list_id)
            .fetch_one(&mut *tx)
            .await? as usize
        };

        let unique_message_ids: Vec<String> = message_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let email_id_rows: Vec<(String, i32)> = if unique_message_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as(
                "SELECT message_id, id FROM emails WHERE mailing_list_id = $1 AND message_id = ANY($2)",
            )
            .bind(mailing_list_id)
            .bind(&unique_message_ids)
            .fetch_all(&mut *tx)
            .await?
        };
        let email_id_map: HashMap<String, i32> = email_id_rows.into_iter().collect();

        let mut recipient_set: HashSet<(i32, i32, String)> = HashSet::new();
        for email in batch {
            let Some(&email_id) = email_id_map.get(&email.message_id) else {
                continue;
            };
            for recipient in email
                .to
                .iter()
                .map(|r| (r, "to"))
                .chain(email.cc.iter().map(|r| (r, "cc")))
            {
                if let Some(&author_id) = author_id_map.get(&recipient.0.email) {
                    recipient_set.insert((email_id, author_id, recipient.1.to_string()));
                }
            }
        }

        let mut recipient_list_ids = Vec::with_capacity(recipient_set.len());
        let mut recipient_email_ids = Vec::with_capacity(recipient_set.len());
        let mut recipient_author_ids = Vec::with_capacity(recipient_set.len());
        let mut recipient_types = Vec::with_capacity(recipient_set.len());
        for (email_id, author_id, recipient_type) in recipient_set {
            recipient_list_ids.push(mailing_list_id);
            recipient_email_ids.push(email_id);
            recipient_author_ids.push(author_id);
            recipient_types.push(recipient_type);
        }

        if !recipient_email_ids.is_empty() {
            sqlx::query(
                r#"INSERT INTO email_recipients (mailing_list_id, email_id, author_id, recipient_type)
                SELECT * FROM UNNEST($1::int[], $2::int[], $3::int[], $4::text[])"#,
            )
            .bind(&recipient_list_ids)
            .bind(&recipient_email_ids)
            .bind(&recipient_author_ids)
            .bind(&recipient_types)
            .execute(&mut *tx)
            .await?;
        }

        let mut reference_map: HashMap<(i32, String), i32> = HashMap::new();
        for email in batch {
            let Some(&email_id) = email_id_map.get(&email.message_id) else {
                continue;
            };
            for (position, reference) in email.references.iter().enumerate() {
                reference_map
                    .entry((email_id, reference.clone()))
                    .or_insert(position as i32);
            }
        }

        let mut reference_list_ids = Vec::with_capacity(reference_map.len());
        let mut reference_email_ids = Vec::with_capacity(reference_map.len());
        let mut reference_message_ids = Vec::with_capacity(reference_map.len());
        let mut reference_positions = Vec::with_capacity(reference_map.len());
        for ((email_id, reference), position) in reference_map {
            reference_list_ids.push(mailing_list_id);
            reference_email_ids.push(email_id);
            reference_message_ids.push(reference);
            reference_positions.push(position);
        }

        if !reference_email_ids.is_empty() {
            sqlx::query(
                r#"INSERT INTO email_references (mailing_list_id, email_id, referenced_message_id, position)
                SELECT * FROM UNNEST($1::int[], $2::int[], $3::text[], $4::int[])
                ON CONFLICT (mailing_list_id, email_id, referenced_message_id) DO NOTHING"#,
            )
            .bind(&reference_list_ids)
            .bind(&reference_email_ids)
            .bind(&reference_message_ids)
            .bind(&reference_positions)
            .execute(&mut *tx)
            .await?;
        }

        let bodies = collect_email_bodies(batch);
        let mut body_email_ids = Vec::with_capacity(bodies.len());
        let mut body_list_ids = Vec::with_capacity(bodies.len());
        let mut body_values = Vec::with_capacity(bodies.len());
        for (message_id, body) in bodies {
            let Some(&email_id) = email_id_map.get(&message_id) else {
                continue;
            };
            body_email_ids.push(email_id);
            body_list_ids.push(mailing_list_id);
            body_values.push(body);
        }

        if !body_email_ids.is_empty() {
            sqlx::query(
                r#"INSERT INTO email_bodies (email_id, mailing_list_id, body)
                SELECT * FROM UNNEST($1::int[], $2::int[], $3::text[])
                ON CONFLICT (email_id, mailing_list_id) DO UPDATE SET body = EXCLUDED.body"#,
            )
            .bind(&body_email_ids)
            .bind(&body_list_ids)
            .bind(&body_values)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(inserted_count)
    }
}

fn collect_unique_authors(batch: &[EmailInput]) -> HashMap<String, Option<String>> {
    let mut authors = HashMap::new();
    for email in batch {
        upsert_author(
            &mut authors,
            &email.author_email,
            email.author_name.as_deref(),
        );

        for recipient in email.to.iter().chain(email.cc.iter()) {
            upsert_author(&mut authors, &recipient.email, recipient.name.as_deref());
        }
    }
    authors
}

fn upsert_author(authors: &mut HashMap<String, Option<String>>, email: &str, name: Option<&str>) {
    if email.is_empty() {
        return;
    }
    let cleaned_name = name.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    authors
        .entry(email.to_string())
        .and_modify(|existing| {
            if existing.is_none() {
                *existing = cleaned_name.clone();
            }
        })
        .or_insert(cleaned_name);
}

fn collect_email_bodies(batch: &[EmailInput]) -> HashMap<String, String> {
    let mut bodies = HashMap::new();
    for email in batch {
        let Some(body) = email.body.as_ref() else {
            continue;
        };
        if body.is_empty() {
            continue;
        }
        bodies
            .entry(email.message_id.clone())
            .or_insert_with(|| body.clone());
    }
    bodies
}

#[cfg(test)]
mod tests {
    use super::{EmailInput, EmailRecipient, collect_email_bodies, collect_unique_authors};
    use chrono::Utc;

    fn sample_email(message_id: &str, author_email: &str, author_name: Option<&str>) -> EmailInput {
        EmailInput {
            mailing_list_id: 1,
            message_id: message_id.to_string(),
            blob_oid: "deadbeef".to_string(),
            epoch: 0,
            author_email: author_email.to_string(),
            author_name: author_name.map(|value| value.to_string()),
            subject: "Hello".to_string(),
            normalized_subject: "hello".to_string(),
            date: Utc::now(),
            in_reply_to: None,
            body: None,
            patch_metadata: None,
            references: Vec::new(),
            to: Vec::new(),
            cc: Vec::new(),
        }
    }

    #[test]
    fn collect_unique_authors_prefers_named() {
        let mut first = sample_email("a@x", "author@example.com", None);
        first.to.push(EmailRecipient {
            email: "friend@example.com".to_string(),
            name: None,
        });
        let mut second = sample_email("b@x", "author@example.com", Some("Author Name"));
        second.cc.push(EmailRecipient {
            email: "friend@example.com".to_string(),
            name: Some("Friend Name".to_string()),
        });

        let authors = collect_unique_authors(&[first, second]);
        assert_eq!(
            authors.get("author@example.com"),
            Some(&Some("Author Name".to_string()))
        );
        assert_eq!(
            authors.get("friend@example.com"),
            Some(&Some("Friend Name".to_string()))
        );
    }

    #[test]
    fn collect_email_bodies_dedupes_by_message_id() {
        let mut first = sample_email("a@x", "author@example.com", None);
        first.body = Some("first".to_string());
        let mut second = sample_email("a@x", "author@example.com", None);
        second.body = Some("second".to_string());

        let bodies = collect_email_bodies(&[first, second]);
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies.get("a@x"), Some(&"first".to_string()));
    }
}
