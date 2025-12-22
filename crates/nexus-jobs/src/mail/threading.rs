//! Thread persistence for mailing list threading runs.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::threading::ThreadInfo;

use super::MailJobResult;

struct PreparedThread {
    root_message_id: String,
    subject: String,
    start_date: DateTime<Utc>,
    last_date: DateTime<Utc>,
    message_count: i32,
    membership_hash: Vec<u8>,
    membership_map: HashMap<i32, i32>,
}

fn prepare_thread_batch_data(threads: Vec<ThreadInfo>) -> Vec<PreparedThread> {
    let mut prepared = Vec::with_capacity(threads.len());
    for thread in threads {
        let mut membership_map = HashMap::new();
        for (email_id, depth) in thread.emails {
            membership_map.entry(email_id).or_insert(depth);
        }

        let mut sorted_email_ids: Vec<i32> = membership_map.keys().copied().collect();
        sorted_email_ids.sort_unstable();

        let mut hasher = Sha256::new();
        for email_id in &sorted_email_ids {
            hasher.update(email_id.to_le_bytes());
        }
        let membership_hash = hasher.finalize().to_vec();

        prepared.push(PreparedThread {
            root_message_id: thread.root_message_id,
            subject: thread.subject,
            start_date: thread.start_date,
            last_date: thread.last_date,
            message_count: membership_map.len() as i32,
            membership_hash,
            membership_map,
        });
    }
    prepared
}

/// Persist thread metadata and membership for a full list rethread.
pub async fn persist_threads(
    pool: &PgPool,
    mailing_list_id: i32,
    threads: Vec<ThreadInfo>,
) -> MailJobResult<(usize, usize)> {
    if threads.is_empty() {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM thread_memberships WHERE mailing_list_id = $1")
            .bind(mailing_list_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM threads WHERE mailing_list_id = $1")
            .bind(mailing_list_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok((0, 0));
    }

    let prepared = prepare_thread_batch_data(threads);
    let thread_count = prepared.len();
    let root_message_ids: Vec<String> = prepared
        .iter()
        .map(|thread| thread.root_message_id.clone())
        .collect();

    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"DELETE FROM thread_memberships
        WHERE mailing_list_id = $1
          AND thread_id IN (
              SELECT id FROM threads
              WHERE mailing_list_id = $1
                AND root_message_id <> ALL($2)
          )"#,
    )
    .bind(mailing_list_id)
    .bind(&root_message_ids)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"DELETE FROM threads
        WHERE mailing_list_id = $1
          AND root_message_id <> ALL($2)"#,
    )
    .bind(mailing_list_id)
    .bind(&root_message_ids)
    .execute(&mut *tx)
    .await?;

    let existing_threads: Vec<(String, i32, Option<Vec<u8>>)> = sqlx::query_as(
        r#"SELECT root_message_id, id, membership_hash
        FROM threads
        WHERE mailing_list_id = $1 AND root_message_id = ANY($2)"#,
    )
    .bind(mailing_list_id)
    .bind(&root_message_ids)
    .fetch_all(&mut *tx)
    .await?;

    let existing_map: HashMap<String, (i32, Option<Vec<u8>>)> = existing_threads
        .into_iter()
        .map(|(root_id, thread_id, hash)| (root_id, (thread_id, hash)))
        .collect();

    let mut threads_to_upsert = Vec::new();
    let mut thread_id_map: HashMap<String, i32> = HashMap::new();
    let mut threads_to_clear = HashSet::new();

    for thread in prepared {
        match existing_map.get(&thread.root_message_id) {
            Some((thread_id, Some(hash)))
                if hash.as_slice() == thread.membership_hash.as_slice() =>
            {
                continue;
            }
            Some((thread_id, _)) => {
                thread_id_map.insert(thread.root_message_id.clone(), *thread_id);
                threads_to_clear.insert(*thread_id);
                threads_to_upsert.push(thread);
            }
            None => {
                threads_to_upsert.push(thread);
            }
        }
    }

    if threads_to_upsert.is_empty() {
        tx.commit().await?;
        return Ok((thread_count, 0));
    }

    let mut list_ids = Vec::with_capacity(threads_to_upsert.len());
    let mut root_ids = Vec::with_capacity(threads_to_upsert.len());
    let mut subjects = Vec::with_capacity(threads_to_upsert.len());
    let mut start_dates = Vec::with_capacity(threads_to_upsert.len());
    let mut last_dates = Vec::with_capacity(threads_to_upsert.len());
    let mut message_counts = Vec::with_capacity(threads_to_upsert.len());
    let mut membership_hashes = Vec::with_capacity(threads_to_upsert.len());

    for thread in &threads_to_upsert {
        list_ids.push(mailing_list_id);
        root_ids.push(thread.root_message_id.clone());
        subjects.push(thread.subject.clone());
        start_dates.push(thread.start_date);
        last_dates.push(thread.last_date);
        message_counts.push(thread.message_count);
        membership_hashes.push(thread.membership_hash.clone());
    }

    let inserted_threads: Vec<(String, i32)> = sqlx::query_as(
        r#"INSERT INTO threads
           (mailing_list_id, root_message_id, subject, start_date, last_date, message_count, membership_hash)
           SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[], $6::int[], $7::bytea[])
           ON CONFLICT (mailing_list_id, root_message_id)
           DO UPDATE SET
               subject = EXCLUDED.subject,
               start_date = EXCLUDED.start_date,
               last_date = EXCLUDED.last_date,
               message_count = EXCLUDED.message_count,
               membership_hash = EXCLUDED.membership_hash
           RETURNING root_message_id, id"#,
    )
    .bind(&list_ids)
    .bind(&root_ids)
    .bind(&subjects)
    .bind(&start_dates)
    .bind(&last_dates)
    .bind(&message_counts)
    .bind(&membership_hashes)
    .fetch_all(&mut *tx)
    .await?;

    for (root_id, thread_id) in inserted_threads {
        thread_id_map.insert(root_id, thread_id);
    }

    if !threads_to_clear.is_empty() {
        let thread_ids: Vec<i32> = threads_to_clear.into_iter().collect();
        sqlx::query(
            r#"DELETE FROM thread_memberships
            WHERE mailing_list_id = $1 AND thread_id = ANY($2)"#,
        )
        .bind(mailing_list_id)
        .bind(&thread_ids)
        .execute(&mut *tx)
        .await?;
    }

    let mut membership_list_ids = Vec::new();
    let mut membership_thread_ids = Vec::new();
    let mut membership_email_ids = Vec::new();
    let mut membership_depths = Vec::new();

    for thread in &threads_to_upsert {
        let Some(&thread_id) = thread_id_map.get(&thread.root_message_id) else {
            continue;
        };
        for (email_id, depth) in &thread.membership_map {
            membership_list_ids.push(mailing_list_id);
            membership_thread_ids.push(thread_id);
            membership_email_ids.push(*email_id);
            membership_depths.push(*depth);
        }
    }

    let membership_count = membership_email_ids.len();
    if membership_count > 0 {
        sqlx::query(
            r#"INSERT INTO thread_memberships (mailing_list_id, thread_id, email_id, depth)
               SELECT * FROM UNNEST($1::int[], $2::int[], $3::int[], $4::int[])
               ON CONFLICT (mailing_list_id, thread_id, email_id) DO NOTHING"#,
        )
        .bind(&membership_list_ids)
        .bind(&membership_thread_ids)
        .bind(&membership_email_ids)
        .bind(&membership_depths)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok((thread_count, membership_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::threading::ThreadInfo;
    use chrono::Utc;

    #[test]
    fn membership_hash_ignores_email_order() {
        let now = Utc::now();
        let mut first = ThreadInfo::new("root".to_string(), "subject".to_string(), now, now);
        first.emails = vec![(3, 0), (1, 0), (2, 1)];

        let mut second = ThreadInfo::new("root".to_string(), "subject".to_string(), now, now);
        second.emails = vec![(2, 1), (3, 0), (1, 0)];

        let first_prepared = prepare_thread_batch_data(vec![first]);
        let second_prepared = prepare_thread_batch_data(vec![second]);

        assert_eq!(
            first_prepared[0].membership_hash,
            second_prepared[0].membership_hash
        );
    }
}
