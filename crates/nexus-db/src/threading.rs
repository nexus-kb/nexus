use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};

use crate::Result;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadSourceMessage {
    pub message_pk: i64,
    pub message_id_primary: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub references_ids: Vec<String>,
    pub in_reply_to_ids: Vec<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct MessageIdResolution {
    message_id: String,
    message_pk: i64,
}

#[derive(Debug, Clone)]
pub struct ThreadSummaryWrite {
    pub root_node_key: String,
    pub root_message_pk: Option<i64>,
    pub subject_norm: String,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: i32,
    pub membership_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ThreadNodeWrite {
    pub node_key: String,
    pub message_pk: Option<i64>,
    pub parent_node_key: Option<String>,
    pub depth: i32,
    pub sort_key: Vec<u8>,
    pub is_dummy: bool,
}

#[derive(Debug, Clone)]
pub struct ThreadMessageWrite {
    pub message_pk: i64,
    pub parent_message_pk: Option<i64>,
    pub depth: i32,
    pub sort_key: Vec<u8>,
    pub is_dummy: bool,
}

#[derive(Debug, Clone)]
pub struct ThreadComponentWrite {
    pub summary: ThreadSummaryWrite,
    pub nodes: Vec<ThreadNodeWrite>,
    pub messages: Vec<ThreadMessageWrite>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ThreadingApplyStats {
    pub threads_rebuilt: u64,
    pub nodes_written: u64,
    pub dummy_nodes_written: u64,
    pub messages_written: u64,
    pub stale_threads_removed: u64,
}

#[derive(Clone)]
pub struct ThreadingStore {
    pool: PgPool,
}

impl ThreadingStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load_source_messages(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
    ) -> Result<Vec<ThreadSourceMessage>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, ThreadSourceMessage>(
            r#"SELECT
                m.id AS message_pk,
                m.message_id_primary,
                m.subject_raw,
                m.subject_norm,
                m.date_utc,
                m.references_ids,
                m.in_reply_to_ids
            FROM messages m
            WHERE m.id = ANY($2)
              AND EXISTS (
                SELECT 1
                FROM list_message_instances lmi
                WHERE lmi.mailing_list_id = $1
                  AND lmi.message_pk = m.id
              )
            ORDER BY m.id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn resolve_message_ids(
        &self,
        mailing_list_id: i64,
        message_ids: &[String],
    ) -> Result<Vec<(String, i64)>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, MessageIdResolution>(
            r#"SELECT DISTINCT ON (mim.message_id)
                mim.message_id,
                mim.message_pk
            FROM message_id_map mim
            JOIN list_message_instances lmi
              ON lmi.message_pk = mim.message_pk
             AND lmi.mailing_list_id = $1
            WHERE mim.message_id = ANY($2)
            ORDER BY mim.message_id ASC, mim.is_primary DESC, mim.message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.message_id, row.message_pk))
            .collect())
    }

    pub async fn expand_ancestor_closure(
        &self,
        mailing_list_id: i64,
        seeds: &[i64],
    ) -> Result<Vec<i64>> {
        if seeds.is_empty() {
            return Ok(Vec::new());
        }

        let mut known: BTreeSet<i64> = seeds.iter().copied().collect();
        let mut frontier: Vec<i64> = known.iter().copied().collect();

        while !frontier.is_empty() {
            let batch = std::mem::take(&mut frontier);
            let source_messages = self.load_source_messages(mailing_list_id, &batch).await?;

            let mut lookup_message_ids = BTreeSet::new();
            for message in &source_messages {
                if !message.references_ids.is_empty() {
                    for id in &message.references_ids {
                        if !id.trim().is_empty() {
                            lookup_message_ids.insert(id.clone());
                        }
                    }
                } else if let Some(first) = message.in_reply_to_ids.first()
                    && !first.trim().is_empty()
                {
                    lookup_message_ids.insert(first.clone());
                }
            }

            if lookup_message_ids.is_empty() {
                continue;
            }

            let lookup: Vec<String> = lookup_message_ids.into_iter().collect();
            let resolutions = self.resolve_message_ids(mailing_list_id, &lookup).await?;

            for (_, message_pk) in resolutions {
                if known.insert(message_pk) {
                    frontier.push(message_pk);
                }
            }
        }

        Ok(known.into_iter().collect())
    }

    pub async fn find_impacted_thread_ids(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
    ) -> Result<Vec<i64>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT thread_id
            FROM thread_messages
            WHERE mailing_list_id = $1
              AND message_pk = ANY($2)
            ORDER BY thread_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_message_pks_for_threads(
        &self,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<i64>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT message_pk
            FROM thread_messages
            WHERE mailing_list_id = $1
              AND thread_id = ANY($2)
            ORDER BY message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(thread_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_message_pks_for_rebuild(
        &self,
        mailing_list_id: i64,
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
        after_message_pk: i64,
        limit: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT lmi.message_pk
            FROM list_message_instances lmi
            WHERE lmi.mailing_list_id = $1
              AND ($2::timestamptz IS NULL OR lmi.seen_at >= $2)
              AND ($3::timestamptz IS NULL OR lmi.seen_at < $3)
              AND lmi.message_pk > $4
            ORDER BY lmi.message_pk ASC
            LIMIT $5"#,
        )
        .bind(mailing_list_id)
        .bind(from_seen_at)
        .bind(to_seen_at)
        .bind(after_message_pk)
        .bind(limit.clamp(1, 10_000))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn apply_components(
        &self,
        mailing_list_id: i64,
        previous_thread_ids: &[i64],
        components: &[ThreadComponentWrite],
    ) -> Result<ThreadingApplyStats> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(mailing_list_id)
            .execute(&mut *tx)
            .await?;

        let mut stats = ThreadingApplyStats::default();
        let mut rewritten_thread_ids = BTreeSet::new();

        for component in components {
            let thread_id = sqlx::query_scalar::<_, i64>(
                r#"INSERT INTO threads
                (mailing_list_id, root_node_key, root_message_pk, subject_norm, created_at,
                 last_activity_at, message_count, membership_hash, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                ON CONFLICT (mailing_list_id, root_node_key)
                DO UPDATE SET root_message_pk = EXCLUDED.root_message_pk,
                              subject_norm = EXCLUDED.subject_norm,
                              created_at = EXCLUDED.created_at,
                              last_activity_at = EXCLUDED.last_activity_at,
                              message_count = EXCLUDED.message_count,
                              membership_hash = EXCLUDED.membership_hash,
                              updated_at = now()
                RETURNING id"#,
            )
            .bind(mailing_list_id)
            .bind(&component.summary.root_node_key)
            .bind(component.summary.root_message_pk)
            .bind(&component.summary.subject_norm)
            .bind(component.summary.created_at)
            .bind(component.summary.last_activity_at)
            .bind(component.summary.message_count)
            .bind(&component.summary.membership_hash)
            .fetch_one(&mut *tx)
            .await?;

            rewritten_thread_ids.insert(thread_id);

            sqlx::query(
                "DELETE FROM thread_messages WHERE mailing_list_id = $1 AND thread_id = $2",
            )
            .bind(mailing_list_id)
            .bind(thread_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query("DELETE FROM thread_nodes WHERE mailing_list_id = $1 AND thread_id = $2")
                .bind(mailing_list_id)
                .bind(thread_id)
                .execute(&mut *tx)
                .await?;

            if !component.nodes.is_empty() {
                let mut qb = QueryBuilder::new(
                    "INSERT INTO thread_nodes \
                     (mailing_list_id, thread_id, node_key, message_pk, parent_node_key, depth, sort_key, is_dummy)",
                );
                qb.push_values(component.nodes.iter(), |mut b, node| {
                    b.push_bind(mailing_list_id)
                        .push_bind(thread_id)
                        .push_bind(&node.node_key)
                        .push_bind(node.message_pk)
                        .push_bind(&node.parent_node_key)
                        .push_bind(node.depth)
                        .push_bind(&node.sort_key)
                        .push_bind(node.is_dummy);
                });
                qb.build().execute(&mut *tx).await?;

                stats.nodes_written += component.nodes.len() as u64;
                stats.dummy_nodes_written +=
                    component.nodes.iter().filter(|node| node.is_dummy).count() as u64;
            }

            if !component.messages.is_empty() {
                let mut qb = QueryBuilder::new(
                    "INSERT INTO thread_messages \
                     (mailing_list_id, thread_id, message_pk, parent_message_pk, depth, sort_key, is_dummy)",
                );
                qb.push_values(component.messages.iter(), |mut b, msg| {
                    b.push_bind(mailing_list_id)
                        .push_bind(thread_id)
                        .push_bind(msg.message_pk)
                        .push_bind(msg.parent_message_pk)
                        .push_bind(msg.depth)
                        .push_bind(&msg.sort_key)
                        .push_bind(msg.is_dummy);
                });
                qb.build().execute(&mut *tx).await?;

                stats.messages_written += component.messages.len() as u64;
            }

            stats.threads_rebuilt += 1;
        }

        let stale_thread_ids: Vec<i64> = previous_thread_ids
            .iter()
            .copied()
            .filter(|id| !rewritten_thread_ids.contains(id))
            .collect();
        if !stale_thread_ids.is_empty() {
            let deleted = sqlx::query(
                "DELETE FROM threads WHERE mailing_list_id = $1 AND id = ANY($2)",
            )
            .bind(mailing_list_id)
            .bind(&stale_thread_ids)
            .execute(&mut *tx)
            .await?;
            stats.stale_threads_removed = deleted.rows_affected();
        }

        tx.commit().await?;
        Ok(stats)
    }
}
