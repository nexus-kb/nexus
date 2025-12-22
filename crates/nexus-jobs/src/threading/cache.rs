use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use super::container::EmailData;

#[derive(Debug)]
pub struct ThreadingCache {
    pub email_data: HashMap<i32, EmailData>,
    pub references: HashMap<i32, Vec<String>>,
}

impl ThreadingCache {
    pub async fn load(pool: &PgPool, mailing_list_id: i32) -> Result<Self, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct EmailRow {
            id: i32,
            message_id: String,
            subject: String,
            normalized_subject: String,
            in_reply_to: Option<String>,
            date: DateTime<Utc>,
        }

        let rows: Vec<EmailRow> = sqlx::query_as(
            r#"SELECT id, message_id, subject, COALESCE(normalized_subject, '') AS normalized_subject, in_reply_to, date
            FROM emails
            WHERE mailing_list_id = $1"#,
        )
        .bind(mailing_list_id)
        .fetch_all(pool)
        .await?;

        let mut email_data = HashMap::with_capacity(rows.len());
        for row in rows {
            email_data.insert(
                row.id,
                EmailData {
                    id: row.id,
                    message_id: row.message_id,
                    subject: row.subject,
                    normalized_subject: row.normalized_subject,
                    in_reply_to: row.in_reply_to,
                    date: row.date,
                },
            );
        }

        let rows = sqlx::query(
            r#"SELECT email_id, referenced_message_id
            FROM email_references
            WHERE mailing_list_id = $1
            ORDER BY email_id, position"#,
        )
        .bind(mailing_list_id)
        .fetch_all(pool)
        .await?;

        let mut references: HashMap<i32, Vec<String>> = HashMap::new();
        for row in rows {
            let email_id: i32 = row.get("email_id");
            let message_id: String = row.get("referenced_message_id");
            references.entry(email_id).or_default().push(message_id);
        }

        Ok(Self {
            email_data,
            references,
        })
    }
}
