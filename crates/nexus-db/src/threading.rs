use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};

use crate::Result;

const MAX_QUERY_BIND_PARAMS: usize = 60_000;
const THREAD_UPSERT_BINDS_PER_ROW: usize = 8;
const THREAD_NODE_INSERT_BINDS_PER_ROW: usize = 8;
const THREAD_MESSAGE_INSERT_BINDS_PER_ROW: usize = 7;

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

#[derive(Debug, Clone, sqlx::FromRow)]
struct ExistingThreadRow {
    id: i64,
    root_node_key: String,
    membership_hash: Vec<u8>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct UpsertedThreadRow {
    id: i64,
    root_node_key: String,
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
    pub threads_unchanged_skipped: u64,
    pub nodes_written: u64,
    pub dummy_nodes_written: u64,
    pub messages_written: u64,
    pub stale_threads_removed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ThreadingRunContext {
    source_messages: HashMap<i64, ThreadSourceMessage>,
    missing_source_messages: HashSet<i64>,
    resolved_message_ids: HashMap<String, i64>,
    unresolved_message_ids: HashSet<String>,
    pub ancestor_expansion_seen: BTreeSet<i64>,
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

    pub async fn load_source_messages_cached(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
        context: &mut ThreadingRunContext,
    ) -> Result<Vec<ThreadSourceMessage>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        let mut missing = Vec::new();
        let mut requested = BTreeSet::new();
        for message_pk in message_pks.iter().copied() {
            if message_pk <= 0 || !requested.insert(message_pk) {
                continue;
            }
            if context.source_messages.contains_key(&message_pk)
                || context.missing_source_messages.contains(&message_pk)
            {
                continue;
            }
            missing.push(message_pk);
        }

        if !missing.is_empty() {
            let fetched = self.load_source_messages(mailing_list_id, &missing).await?;
            let mut fetched_ids = HashSet::new();
            for row in fetched {
                fetched_ids.insert(row.message_pk);
                context.source_messages.insert(row.message_pk, row);
            }

            for message_pk in missing {
                if !fetched_ids.contains(&message_pk) {
                    context.missing_source_messages.insert(message_pk);
                }
            }
        }

        let mut out = Vec::with_capacity(requested.len());
        for message_pk in requested {
            if let Some(row) = context.source_messages.get(&message_pk) {
                out.push(row.clone());
            }
        }
        out.sort_by_key(|row| row.message_pk);
        Ok(out)
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

    pub async fn resolve_message_ids_cached(
        &self,
        mailing_list_id: i64,
        message_ids: &[String],
        context: &mut ThreadingRunContext,
    ) -> Result<Vec<(String, i64)>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut unique = BTreeSet::new();
        let mut missing = Vec::new();

        for message_id in message_ids {
            let normalized = message_id.trim();
            if normalized.is_empty() {
                continue;
            }
            if !unique.insert(normalized.to_string()) {
                continue;
            }
            if context.resolved_message_ids.contains_key(normalized)
                || context.unresolved_message_ids.contains(normalized)
            {
                continue;
            }
            missing.push(normalized.to_string());
        }

        if !missing.is_empty() {
            let resolved = self.resolve_message_ids(mailing_list_id, &missing).await?;
            let mut found = HashSet::new();
            for (message_id, message_pk) in resolved {
                found.insert(message_id.clone());
                context.resolved_message_ids.insert(message_id, message_pk);
            }
            for message_id in missing {
                if !found.contains(&message_id) {
                    context.unresolved_message_ids.insert(message_id);
                }
            }
        }

        let mut out = Vec::new();
        for message_id in unique {
            if let Some(message_pk) = context.resolved_message_ids.get(&message_id).copied() {
                out.push((message_id, message_pk));
            }
        }
        Ok(out)
    }

    pub async fn expand_ancestor_closure(
        &self,
        mailing_list_id: i64,
        seeds: &[i64],
    ) -> Result<Vec<i64>> {
        let mut context = ThreadingRunContext::default();
        self.expand_ancestor_closure_cached(mailing_list_id, seeds, &mut context)
            .await
    }

    pub async fn expand_ancestor_closure_cached(
        &self,
        mailing_list_id: i64,
        seeds: &[i64],
        context: &mut ThreadingRunContext,
    ) -> Result<Vec<i64>> {
        if seeds.is_empty() {
            return Ok(Vec::new());
        }

        let mut known: BTreeSet<i64> = seeds.iter().copied().filter(|value| *value > 0).collect();
        let mut frontier: Vec<i64> = known.iter().copied().collect();

        while !frontier.is_empty() {
            let batch = std::mem::take(&mut frontier);
            for message_pk in &batch {
                context.ancestor_expansion_seen.insert(*message_pk);
            }

            let source_messages = self
                .load_source_messages_cached(mailing_list_id, &batch, context)
                .await?;

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
            let resolutions = self
                .resolve_message_ids_cached(mailing_list_id, &lookup, context)
                .await?;

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
        let mut unchanged_thread_ids = BTreeSet::new();
        let mut existing_by_root: HashMap<String, (i64, Vec<u8>)> = HashMap::new();

        if !components.is_empty() {
            let root_node_keys: Vec<String> = components
                .iter()
                .map(|component| component.summary.root_node_key.clone())
                .collect();

            let existing = sqlx::query_as::<_, ExistingThreadRow>(
                r#"SELECT id, root_node_key, membership_hash
                FROM threads
                WHERE mailing_list_id = $1
                  AND root_node_key = ANY($2)"#,
            )
            .bind(mailing_list_id)
            .bind(&root_node_keys)
            .fetch_all(&mut *tx)
            .await?;

            for row in existing {
                existing_by_root.insert(row.root_node_key, (row.id, row.membership_hash));
            }
        }

        let mut rewrite_components: Vec<&ThreadComponentWrite> = Vec::new();
        for component in components {
            if let Some((thread_id, existing_hash)) =
                existing_by_root.get(&component.summary.root_node_key)
                && existing_hash == &component.summary.membership_hash
            {
                unchanged_thread_ids.insert(*thread_id);
                stats.threads_unchanged_skipped += 1;
                continue;
            }
            rewrite_components.push(component);
        }

        if !rewrite_components.is_empty() {
            let mut thread_id_by_root: HashMap<String, i64> =
                HashMap::with_capacity(rewrite_components.len());
            let upsert_chunk_size = (MAX_QUERY_BIND_PARAMS / THREAD_UPSERT_BINDS_PER_ROW).max(1);

            for chunk in rewrite_components.chunks(upsert_chunk_size) {
                let mut upsert_threads = QueryBuilder::new(
                    "INSERT INTO threads \
                     (mailing_list_id, root_node_key, root_message_pk, subject_norm, created_at, \
                      last_activity_at, message_count, membership_hash, updated_at)",
                );
                upsert_threads.push_values(chunk.iter(), |mut b, component| {
                    b.push_bind(mailing_list_id)
                        .push_bind(&component.summary.root_node_key)
                        .push_bind(component.summary.root_message_pk)
                        .push_bind(&component.summary.subject_norm)
                        .push_bind(component.summary.created_at)
                        .push_bind(component.summary.last_activity_at)
                        .push_bind(component.summary.message_count)
                        .push_bind(&component.summary.membership_hash)
                        .push("now()");
                });
                upsert_threads.push(
                    r#" ON CONFLICT (mailing_list_id, root_node_key)
                    DO UPDATE SET root_message_pk = EXCLUDED.root_message_pk,
                                  subject_norm = EXCLUDED.subject_norm,
                                  created_at = EXCLUDED.created_at,
                                  last_activity_at = EXCLUDED.last_activity_at,
                                  message_count = EXCLUDED.message_count,
                                  membership_hash = EXCLUDED.membership_hash,
                                  updated_at = now()
                    RETURNING id, root_node_key"#,
                );

                let upserted_rows = upsert_threads
                    .build_query_as::<UpsertedThreadRow>()
                    .fetch_all(&mut *tx)
                    .await?;

                for row in upserted_rows {
                    rewritten_thread_ids.insert(row.id);
                    thread_id_by_root.insert(row.root_node_key, row.id);
                }
            }

            let rewritten_ids: Vec<i64> = rewritten_thread_ids.iter().copied().collect();
            if !rewritten_ids.is_empty() {
                sqlx::query(
                    "DELETE FROM thread_messages WHERE mailing_list_id = $1 AND thread_id = ANY($2)",
                )
                .bind(mailing_list_id)
                .bind(&rewritten_ids)
                .execute(&mut *tx)
                .await?;

                sqlx::query(
                    "DELETE FROM thread_nodes WHERE mailing_list_id = $1 AND thread_id = ANY($2)",
                )
                .bind(mailing_list_id)
                .bind(&rewritten_ids)
                .execute(&mut *tx)
                .await?;
            }

            let mut node_rows: Vec<(i64, &ThreadNodeWrite)> = Vec::new();
            let mut message_rows: Vec<(i64, &ThreadMessageWrite)> = Vec::new();
            for component in &rewrite_components {
                let Some(thread_id) = thread_id_by_root
                    .get(&component.summary.root_node_key)
                    .copied()
                else {
                    continue;
                };

                for node in &component.nodes {
                    node_rows.push((thread_id, node));
                }
                for message in &component.messages {
                    message_rows.push((thread_id, message));
                }
            }

            if !node_rows.is_empty() {
                let node_chunk_size =
                    (MAX_QUERY_BIND_PARAMS / THREAD_NODE_INSERT_BINDS_PER_ROW).max(1);
                for chunk in node_rows.chunks(node_chunk_size) {
                    let mut insert_nodes = QueryBuilder::new(
                        "INSERT INTO thread_nodes \
                         (mailing_list_id, thread_id, node_key, message_pk, parent_node_key, depth, sort_key, is_dummy)",
                    );
                    insert_nodes.push_values(chunk.iter(), |mut b, (thread_id, node)| {
                        b.push_bind(mailing_list_id)
                            .push_bind(*thread_id)
                            .push_bind(&node.node_key)
                            .push_bind(node.message_pk)
                            .push_bind(&node.parent_node_key)
                            .push_bind(node.depth)
                            .push_bind(&node.sort_key)
                            .push_bind(node.is_dummy);
                    });
                    insert_nodes.build().execute(&mut *tx).await?;
                }

                stats.nodes_written += node_rows.len() as u64;
                stats.dummy_nodes_written +=
                    node_rows.iter().filter(|(_, node)| node.is_dummy).count() as u64;
            }

            if !message_rows.is_empty() {
                let message_chunk_size =
                    (MAX_QUERY_BIND_PARAMS / THREAD_MESSAGE_INSERT_BINDS_PER_ROW).max(1);
                for chunk in message_rows.chunks(message_chunk_size) {
                    let mut insert_messages = QueryBuilder::new(
                        "INSERT INTO thread_messages \
                         (mailing_list_id, thread_id, message_pk, parent_message_pk, depth, sort_key, is_dummy)",
                    );
                    insert_messages.push_values(chunk.iter(), |mut b, (thread_id, msg)| {
                        b.push_bind(mailing_list_id)
                            .push_bind(*thread_id)
                            .push_bind(msg.message_pk)
                            .push_bind(msg.parent_message_pk)
                            .push_bind(msg.depth)
                            .push_bind(&msg.sort_key)
                            .push_bind(msg.is_dummy);
                    });
                    insert_messages.build().execute(&mut *tx).await?;
                }

                stats.messages_written += message_rows.len() as u64;
            }

            stats.threads_rebuilt = rewrite_components.len() as u64;
        }

        let mut retained_thread_ids = rewritten_thread_ids;
        retained_thread_ids.extend(unchanged_thread_ids.iter().copied());
        let stale_thread_ids: Vec<i64> = previous_thread_ids
            .iter()
            .copied()
            .filter(|id| !retained_thread_ids.contains(id))
            .collect();
        if !stale_thread_ids.is_empty() {
            let deleted =
                sqlx::query("DELETE FROM threads WHERE mailing_list_id = $1 AND id = ANY($2)")
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
