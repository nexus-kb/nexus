use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};

use crate::Result;

const MAX_QUERY_BIND_PARAMS: usize = 60_000;
const PATCH_ITEM_FILE_INSERT_BINDS_PER_ROW: usize = 10;
const PATCH_ITEM_FILE_STATS_BINDS_PER_ROW: usize = 5;
const LOAD_MESSAGES_THREAD_CHUNK_SIZE: usize = 512;

fn merge_patch_item_file_row(
    existing: &mut UpsertPatchItemFileInput,
    incoming: &UpsertPatchItemFileInput,
) {
    if existing.old_path.is_none() {
        existing.old_path = incoming.old_path.clone();
    }
    existing.change_type = merged_change_type(&existing.change_type, &incoming.change_type);
    existing.is_binary |= incoming.is_binary;
    existing.additions = existing.additions.saturating_add(incoming.additions);
    existing.deletions = existing.deletions.saturating_add(incoming.deletions);
    existing.hunk_count = existing.hunk_count.saturating_add(incoming.hunk_count);
    existing.diff_start = existing.diff_start.min(incoming.diff_start);
    existing.diff_end = existing.diff_end.max(incoming.diff_end);
}

fn merged_change_type(existing: &str, incoming: &str) -> String {
    if existing == incoming {
        return existing.to_string();
    }
    if existing == "M" {
        return incoming.to_string();
    }
    if incoming == "M" {
        return existing.to_string();
    }
    if existing == "B" || incoming == "B" {
        return "B".to_string();
    }
    if existing == "R" || incoming == "R" {
        return "R".to_string();
    }
    if existing == "C" || incoming == "C" {
        return "C".to_string();
    }
    if existing == "A" || incoming == "A" {
        return "A".to_string();
    }
    if existing == "D" || incoming == "D" {
        return "D".to_string();
    }
    incoming.to_string()
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct LineageSourceMessage {
    pub thread_id: i64,
    pub message_pk: i64,
    pub sort_key: Vec<u8>,
    pub message_id_primary: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub references_ids: Vec<String>,
    pub in_reply_to_ids: Vec<String>,
    pub base_commit: Option<String>,
    pub change_id: Option<String>,
    pub has_diff: bool,
    pub patch_id_stable: Option<String>,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchSeriesRecord {
    pub id: i64,
    pub canonical_subject_norm: String,
    pub author_email: String,
    pub author_name: Option<String>,
    pub change_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub latest_version_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchSeriesVersionRecord {
    pub id: i64,
    pub patch_series_id: i64,
    pub version_num: i32,
    pub is_rfc: bool,
    pub is_resend: bool,
    pub is_partial_reroll: bool,
    pub thread_id: Option<i64>,
    pub cover_message_pk: Option<i64>,
    pub first_patch_message_pk: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub base_commit: Option<String>,
    pub version_fingerprint: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct UpsertPatchSeriesInput {
    pub canonical_subject_norm: String,
    pub author_email: String,
    pub author_name: Option<String>,
    pub change_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct UpsertPatchSeriesVersionInput {
    pub patch_series_id: i64,
    pub version_num: i32,
    pub is_rfc: bool,
    pub is_resend: bool,
    pub thread_id: Option<i64>,
    pub cover_message_pk: Option<i64>,
    pub first_patch_message_pk: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub base_commit: Option<String>,
    pub version_fingerprint: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct UpsertPatchItemInput {
    pub ordinal: i32,
    pub total: Option<i32>,
    pub message_pk: i64,
    pub subject_raw: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub commit_author_name: Option<String>,
    pub commit_author_email: Option<String>,
    pub item_type: String,
    pub has_diff: bool,
    pub patch_id_stable: Option<String>,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
}

#[derive(Debug, Clone)]
pub struct UpsertPatchItemFileInput {
    pub old_path: Option<String>,
    pub new_path: String,
    pub change_type: String,
    pub is_binary: bool,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub diff_start: i32,
    pub diff_end: i32,
}

#[derive(Debug, Clone)]
pub struct PatchItemFileBatchInput {
    pub patch_item_id: i64,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub files: Vec<UpsertPatchItemFileInput>,
}

#[derive(Debug, Clone, Default)]
pub struct PatchFactHydrationOutcome {
    pub hydrated_patch_items: u64,
    pub patch_item_files_written: u64,
    pub missing_patch_item_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemRecord {
    pub id: i64,
    pub patch_series_version_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub message_pk: i64,
    pub subject_raw: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub item_type: String,
    pub has_diff: bool,
    pub patch_id_stable: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemFileRecord {
    pub patch_item_id: i64,
    pub old_path: Option<String>,
    pub new_path: String,
    pub change_type: String,
    pub is_binary: bool,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub diff_start: i32,
    pub diff_end: i32,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemDetailRecord {
    pub patch_item_id: i64,
    pub patch_series_id: i64,
    pub patch_series_version_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub item_type: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub message_pk: i64,
    pub message_id_primary: String,
    pub patch_id_stable: Option<String>,
    pub has_diff: bool,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct AssembledItemRecord {
    pub version_num: i32,
    pub ordinal: i32,
    pub patch_item_id: i64,
    pub patch_id_stable: Option<String>,
    pub title_norm: String,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemDiffRecord {
    pub patch_item_id: i64,
    pub diff_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemFileDiffSliceSource {
    pub new_path: String,
    pub diff_start: i32,
    pub diff_end: i32,
    pub diff_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadSummaryRecord {
    pub thread_id: i64,
    pub subject_norm: String,
    pub last_activity_at: DateTime<Utc>,
    pub membership_hash: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadListItemRecord {
    pub thread_id: i64,
    pub subject_norm: String,
    pub root_message_pk: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: i32,
    pub starter_name: Option<String>,
    pub starter_email: Option<String>,
    pub has_diff: bool,
}

#[derive(Debug, Clone)]
pub struct ListThreadsParams {
    pub sort: String,
    pub from_ts: Option<DateTime<Utc>>,
    pub to_ts: Option<DateTime<Utc>>,
    pub author_email: Option<String>,
    pub has_diff: Option<bool>,
    pub limit: i64,
    pub cursor_ts: Option<DateTime<Utc>>,
    pub cursor_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadParticipantRecord {
    pub thread_id: i64,
    pub from_name: Option<String>,
    pub from_email: String,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadMessageRecord {
    pub message_pk: i64,
    pub parent_message_pk: Option<i64>,
    pub depth: i32,
    pub sort_key: Vec<u8>,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub subject_raw: String,
    pub has_diff: bool,
    pub body_text: Option<String>,
    pub patch_item_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MessageBodyRecord {
    pub message_pk: i64,
    pub subject_raw: String,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MessageDetailRecord {
    pub message_pk: i64,
    pub message_id_primary: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub to_raw: Option<String>,
    pub cc_raw: Option<String>,
    pub references_ids: Vec<String>,
    pub in_reply_to_ids: Vec<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchLogicalRecord {
    pub id: i64,
    pub patch_series_id: i64,
    pub slot: i32,
    pub title_norm: String,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SeriesVersionPatchRef {
    pub patch_series_id: i64,
    pub version_num: i32,
    pub patch_id_stable: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SeriesListItemRecord {
    pub series_id: i64,
    pub canonical_subject_norm: String,
    pub author_email: String,
    pub author_name: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub latest_patchset_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub latest_version_num: i32,
    pub is_rfc_latest: bool,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SeriesVersionSummaryRecord {
    pub id: i64,
    pub patch_series_id: i64,
    pub version_num: i32,
    pub is_rfc: bool,
    pub is_resend: bool,
    pub is_partial_reroll: bool,
    pub thread_id: Option<i64>,
    pub cover_message_pk: Option<i64>,
    pub first_patch_message_pk: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub patch_count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ThreadRefRecord {
    pub thread_id: i64,
    pub list_key: String,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SeriesVersionPatchItemRecord {
    pub patch_item_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub message_pk: i64,
    pub message_id_primary: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub item_type: String,
    pub has_diff: bool,
    pub patch_id_stable: Option<String>,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub inherited_from_version_num: Option<i32>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SeriesLogicalCompareRow {
    pub slot: i32,
    pub title_norm: String,
    pub v1_patch_item_id: Option<i64>,
    pub v1_item_type: Option<String>,
    pub v1_patch_id_stable: Option<String>,
    pub v1_subject_raw: Option<String>,
    pub v2_patch_item_id: Option<i64>,
    pub v2_item_type: Option<String>,
    pub v2_patch_id_stable: Option<String>,
    pub v2_subject_raw: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PatchItemFileAggregateRecord {
    pub path: String,
    pub additions: i64,
    pub deletions: i64,
    pub hunk_count: i64,
}

#[derive(Clone)]
pub struct LineageStore {
    pool: PgPool,
}

impl LineageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load_messages_for_threads(
        &self,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let deduped_thread_ids = thread_ids
            .iter()
            .copied()
            .filter(|thread_id| *thread_id > 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if deduped_thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        // Avoid container /dev/shm pressure from parallel workers for large lineage windows.
        sqlx::query("SET LOCAL max_parallel_workers_per_gather = 0")
            .execute(&mut *tx)
            .await?;

        let out = self
            .load_messages_for_thread_ids_in_tx(&mut tx, mailing_list_id, &deduped_thread_ids)
            .await?;

        tx.commit().await?;
        Ok(out)
    }

    pub async fn load_messages_for_anchors(
        &self,
        mailing_list_id: i64,
        anchor_message_pks: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if anchor_message_pks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        // Avoid container /dev/shm pressure from parallel workers for large lineage windows.
        sqlx::query("SET LOCAL max_parallel_workers_per_gather = 0")
            .execute(&mut *tx)
            .await?;

        let thread_ids = sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT tm.thread_id
            FROM thread_messages tm
            WHERE tm.mailing_list_id = $1
              AND tm.message_pk = ANY($2)
            ORDER BY tm.thread_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(anchor_message_pks)
        .fetch_all(&mut *tx)
        .await?;

        if thread_ids.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        let out = self
            .load_messages_for_thread_ids_in_tx(&mut tx, mailing_list_id, &thread_ids)
            .await?;

        tx.commit().await?;
        Ok(out)
    }

    async fn load_messages_for_thread_ids_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for thread_chunk in thread_ids.chunks(LOAD_MESSAGES_THREAD_CHUNK_SIZE) {
            let mut rows = sqlx::query_as::<_, LineageSourceMessage>(
                r#"SELECT
                    tm.thread_id,
                    tm.message_pk,
                    tm.sort_key,
                    m.message_id_primary,
                    m.subject_raw,
                    m.subject_norm,
                    m.from_name,
                    m.from_email,
                    m.date_utc,
                    m.references_ids,
                    m.in_reply_to_ids,
                    mpf.base_commit,
                    mpf.change_id,
                    COALESCE(mpf.has_diff, mb.has_diff, false) AS has_diff,
                    mpf.patch_id_stable,
                    NULL::text AS body_text,
                    NULL::text AS diff_text
                FROM thread_messages tm
                JOIN messages m
                  ON m.id = tm.message_pk
                JOIN message_bodies mb
                  ON mb.id = m.body_id
                LEFT JOIN message_patch_facts mpf
                  ON mpf.message_pk = m.id
                WHERE tm.mailing_list_id = $1
                  AND tm.thread_id = ANY($2)
                ORDER BY tm.thread_id ASC, tm.sort_key ASC"#,
            )
            .bind(mailing_list_id)
            .bind(thread_chunk)
            .fetch_all(&mut **tx)
            .await?;
            out.append(&mut rows);
        }
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

        let rows = sqlx::query_as::<_, (String, i64)>(
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

        Ok(rows)
    }

    pub async fn find_series_ids_by_message_ids(
        &self,
        mailing_list_id: i64,
        message_ids: &[String],
    ) -> Result<Vec<i64>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT psv.patch_series_id
            FROM message_id_map mim
            JOIN list_message_instances lmi
              ON lmi.message_pk = mim.message_pk
             AND lmi.mailing_list_id = $1
            JOIN patch_items pi
              ON pi.message_pk = mim.message_pk
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE mim.message_id = ANY($2)
            ORDER BY psv.patch_series_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_series_by_change_id(
        &self,
        change_id: &str,
    ) -> Result<Option<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE change_id = $1
            LIMIT 1"#,
        )
        .bind(change_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_series_by_id(&self, series_id: i64) -> Result<Option<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE id = $1
            LIMIT 1"#,
        )
        .bind(series_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_similarity_candidates(
        &self,
        canonical_subject_norm: &str,
        from_ts: DateTime<Utc>,
        to_ts: DateTime<Utc>,
    ) -> Result<Vec<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE canonical_subject_norm = $1
              AND last_seen_at >= $2
              AND last_seen_at <= $3
            ORDER BY last_seen_at DESC, id ASC"#,
        )
        .bind(canonical_subject_norm)
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_latest_patch_ids_for_series(&self, series_id: i64) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"WITH latest AS (
                SELECT id
                FROM patch_series_versions
                WHERE patch_series_id = $1
                ORDER BY version_num DESC, sent_at DESC, id DESC
                LIMIT 1
            )
            SELECT DISTINCT pi.patch_id_stable
            FROM patch_items pi
            JOIN latest l ON l.id = pi.patch_series_version_id
            WHERE pi.item_type = 'patch'
              AND pi.patch_id_stable IS NOT NULL
            ORDER BY pi.patch_id_stable ASC"#,
        )
        .bind(series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_latest_patch_ids_for_series_bulk(
        &self,
        series_ids: &[i64],
    ) -> Result<Vec<(i64, String)>> {
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, (i64, String)>(
            r#"WITH ranked AS (
                SELECT
                    id,
                    patch_series_id,
                    row_number() OVER (
                        PARTITION BY patch_series_id
                        ORDER BY version_num DESC, sent_at DESC, id DESC
                    ) AS rn
                FROM patch_series_versions
                WHERE patch_series_id = ANY($1)
            )
            SELECT
                ranked.patch_series_id,
                pi.patch_id_stable
            FROM ranked
            JOIN patch_items pi
              ON pi.patch_series_version_id = ranked.id
            WHERE ranked.rn = 1
              AND pi.item_type = 'patch'
              AND pi.patch_id_stable IS NOT NULL
            ORDER BY ranked.patch_series_id ASC, pi.patch_id_stable ASC"#,
        )
        .bind(series_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn upsert_series(&self, input: &UpsertPatchSeriesInput) -> Result<PatchSeriesRecord> {
        if let Some(change_id) = input.change_id.as_deref() {
            return sqlx::query_as::<_, PatchSeriesRecord>(
                r#"INSERT INTO patch_series
                (canonical_subject_norm, author_email, author_name, change_id, created_at, last_seen_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (change_id) WHERE change_id IS NOT NULL
                DO UPDATE SET canonical_subject_norm = EXCLUDED.canonical_subject_norm,
                              author_email = EXCLUDED.author_email,
                              author_name = COALESCE(EXCLUDED.author_name, patch_series.author_name),
                              last_seen_at = GREATEST(EXCLUDED.last_seen_at, patch_series.last_seen_at)
                RETURNING id,
                          canonical_subject_norm,
                          author_email,
                          author_name,
                          change_id,
                          created_at,
                          last_seen_at,
                          latest_version_id"#,
            )
            .bind(&input.canonical_subject_norm)
            .bind(&input.author_email)
            .bind(&input.author_name)
            .bind(change_id)
            .bind(input.created_at)
            .bind(input.last_seen_at)
            .fetch_one(&self.pool)
            .await;
        }

        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"INSERT INTO patch_series
            (canonical_subject_norm, author_email, author_name, created_at, last_seen_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id,
                      canonical_subject_norm,
                      author_email,
                      author_name,
                      change_id,
                      created_at,
                      last_seen_at,
                      latest_version_id"#,
        )
        .bind(&input.canonical_subject_norm)
        .bind(&input.author_email)
        .bind(&input.author_name)
        .bind(input.created_at)
        .bind(input.last_seen_at)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn touch_series_seen(
        &self,
        series_id: i64,
        last_seen_at: DateTime<Utc>,
        latest_version_id: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE patch_series
            SET last_seen_at = GREATEST(last_seen_at, $2),
                latest_version_id = CASE
                    WHEN $3 IS NULL THEN latest_version_id
                    WHEN latest_version_id IS NULL THEN $3
                    WHEN (
                        SELECT version_num
                        FROM patch_series_versions
                        WHERE id = $3
                    ) >= (
                        SELECT version_num
                        FROM patch_series_versions
                        WHERE id = latest_version_id
                    ) THEN $3
                    ELSE latest_version_id
                END
            WHERE id = $1"#,
        )
        .bind(series_id)
        .bind(last_seen_at)
        .bind(latest_version_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_series_list_presence(
        &self,
        series_id: i64,
        mailing_list_id: i64,
        seen_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO patch_series_lists
            (patch_series_id, mailing_list_id, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (patch_series_id, mailing_list_id)
            DO UPDATE SET first_seen_at = LEAST(patch_series_lists.first_seen_at, EXCLUDED.first_seen_at),
                          last_seen_at = GREATEST(patch_series_lists.last_seen_at, EXCLUDED.last_seen_at)"#,
        )
        .bind(series_id)
        .bind(mailing_list_id)
        .bind(seen_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_series_version(
        &self,
        input: &UpsertPatchSeriesVersionInput,
    ) -> Result<PatchSeriesVersionRecord> {
        sqlx::query_as::<_, PatchSeriesVersionRecord>(
            r#"INSERT INTO patch_series_versions
            (patch_series_id, version_num, is_rfc, is_resend, thread_id,
             cover_message_pk, first_patch_message_pk, sent_at,
             subject_raw, subject_norm, base_commit, version_fingerprint)
            VALUES ($1, $2, $3, $4, $5,
                    $6, $7, $8,
                    $9, $10, $11, $12)
            ON CONFLICT (patch_series_id, version_num, version_fingerprint)
            DO UPDATE SET is_rfc = EXCLUDED.is_rfc,
                          is_resend = EXCLUDED.is_resend,
                          thread_id = COALESCE(EXCLUDED.thread_id, patch_series_versions.thread_id),
                          cover_message_pk = COALESCE(EXCLUDED.cover_message_pk, patch_series_versions.cover_message_pk),
                          first_patch_message_pk = COALESCE(EXCLUDED.first_patch_message_pk, patch_series_versions.first_patch_message_pk),
                          sent_at = LEAST(EXCLUDED.sent_at, patch_series_versions.sent_at),
                          subject_raw = EXCLUDED.subject_raw,
                          subject_norm = EXCLUDED.subject_norm,
                          base_commit = COALESCE(EXCLUDED.base_commit, patch_series_versions.base_commit)
            RETURNING id,
                      patch_series_id,
                      version_num,
                      is_rfc,
                      is_resend,
                      is_partial_reroll,
                      thread_id,
                      cover_message_pk,
                      first_patch_message_pk,
                      sent_at,
                      subject_raw,
                      subject_norm,
                      base_commit,
                      version_fingerprint"#,
        )
        .bind(input.patch_series_id)
        .bind(input.version_num)
        .bind(input.is_rfc)
        .bind(input.is_resend)
        .bind(input.thread_id)
        .bind(input.cover_message_pk)
        .bind(input.first_patch_message_pk)
        .bind(input.sent_at)
        .bind(&input.subject_raw)
        .bind(&input.subject_norm)
        .bind(&input.base_commit)
        .bind(&input.version_fingerprint)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn upsert_patch_item(
        &self,
        patch_series_version_id: i64,
        input: &UpsertPatchItemInput,
    ) -> Result<PatchItemRecord> {
        sqlx::query_as::<_, PatchItemRecord>(
            r#"INSERT INTO patch_items
            (patch_series_version_id, ordinal, total, message_pk,
             subject_raw, subject_norm, commit_subject, commit_subject_norm,
             commit_author_name, commit_author_email, item_type,
             has_diff, patch_id_stable, file_count, additions, deletions, hunk_count)
            VALUES ($1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11,
                    $12, $13, $14, $15, $16, $17)
            ON CONFLICT (patch_series_version_id, ordinal)
            DO UPDATE SET total = COALESCE(EXCLUDED.total, patch_items.total),
                          message_pk = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.message_pk
                              ELSE EXCLUDED.message_pk
                          END,
                          subject_raw = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_raw
                              ELSE EXCLUDED.subject_raw
                          END,
                          subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_norm
                              ELSE EXCLUDED.subject_norm
                          END,
                          commit_subject = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject
                              ELSE EXCLUDED.commit_subject
                          END,
                          commit_subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject_norm
                              ELSE EXCLUDED.commit_subject_norm
                          END,
                          commit_author_name = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_name
                              ELSE EXCLUDED.commit_author_name
                          END,
                          commit_author_email = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_email
                              ELSE EXCLUDED.commit_author_email
                          END,
                          item_type = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.item_type
                              ELSE EXCLUDED.item_type
                          END,
                          has_diff = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.has_diff
                              ELSE EXCLUDED.has_diff
                          END,
                          patch_id_stable = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.patch_id_stable
                              ELSE COALESCE(EXCLUDED.patch_id_stable, patch_items.patch_id_stable)
                          END,
                          file_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.file_count
                              ELSE EXCLUDED.file_count
                          END,
                          additions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.additions
                              ELSE EXCLUDED.additions
                          END,
                          deletions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.deletions
                              ELSE EXCLUDED.deletions
                          END,
                          hunk_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.hunk_count
                              ELSE EXCLUDED.hunk_count
                          END
            RETURNING id,
                      patch_series_version_id,
                      ordinal,
                      total,
                      message_pk,
                      subject_raw,
                      subject_norm,
                      commit_subject,
                      commit_subject_norm,
                      item_type,
                      has_diff,
                      patch_id_stable"#,
        )
        .bind(patch_series_version_id)
        .bind(input.ordinal)
        .bind(input.total)
        .bind(input.message_pk)
        .bind(&input.subject_raw)
        .bind(&input.subject_norm)
        .bind(&input.commit_subject)
        .bind(&input.commit_subject_norm)
        .bind(&input.commit_author_name)
        .bind(&input.commit_author_email)
        .bind(&input.item_type)
        .bind(input.has_diff)
        .bind(&input.patch_id_stable)
        .bind(input.file_count)
        .bind(input.additions)
        .bind(input.deletions)
        .bind(input.hunk_count)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn upsert_patch_items_batch(
        &self,
        patch_series_version_id: i64,
        inputs: &[UpsertPatchItemInput],
    ) -> Result<Vec<PatchItemRecord>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO patch_items \
            (patch_series_version_id, ordinal, total, message_pk, \
             subject_raw, subject_norm, commit_subject, commit_subject_norm, \
             commit_author_name, commit_author_email, item_type, \
             has_diff, patch_id_stable, file_count, additions, deletions, hunk_count) ",
        );
        qb.push_values(inputs.iter(), |mut b, input| {
            b.push_bind(patch_series_version_id)
                .push_bind(input.ordinal)
                .push_bind(input.total)
                .push_bind(input.message_pk)
                .push_bind(&input.subject_raw)
                .push_bind(&input.subject_norm)
                .push_bind(&input.commit_subject)
                .push_bind(&input.commit_subject_norm)
                .push_bind(&input.commit_author_name)
                .push_bind(&input.commit_author_email)
                .push_bind(&input.item_type)
                .push_bind(input.has_diff)
                .push_bind(&input.patch_id_stable)
                .push_bind(input.file_count)
                .push_bind(input.additions)
                .push_bind(input.deletions)
                .push_bind(input.hunk_count);
        });
        qb.push(
            r#" ON CONFLICT (patch_series_version_id, ordinal)
            DO UPDATE SET total = COALESCE(EXCLUDED.total, patch_items.total),
                          message_pk = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.message_pk
                              ELSE EXCLUDED.message_pk
                          END,
                          subject_raw = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_raw
                              ELSE EXCLUDED.subject_raw
                          END,
                          subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_norm
                              ELSE EXCLUDED.subject_norm
                          END,
                          commit_subject = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject
                              ELSE EXCLUDED.commit_subject
                          END,
                          commit_subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject_norm
                              ELSE EXCLUDED.commit_subject_norm
                          END,
                          commit_author_name = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_name
                              ELSE EXCLUDED.commit_author_name
                          END,
                          commit_author_email = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_email
                              ELSE EXCLUDED.commit_author_email
                          END,
                          item_type = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.item_type
                              ELSE EXCLUDED.item_type
                          END,
                          has_diff = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.has_diff
                              ELSE EXCLUDED.has_diff
                          END,
                          patch_id_stable = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.patch_id_stable
                              ELSE COALESCE(EXCLUDED.patch_id_stable, patch_items.patch_id_stable)
                          END,
                          file_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.file_count
                              ELSE EXCLUDED.file_count
                          END,
                          additions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.additions
                              ELSE EXCLUDED.additions
                          END,
                          deletions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.deletions
                              ELSE EXCLUDED.deletions
                          END,
                          hunk_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.hunk_count
                              ELSE EXCLUDED.hunk_count
                          END
            RETURNING id,
                      patch_series_version_id,
                      ordinal,
                      total,
                      message_pk,
                      subject_raw,
                      subject_norm,
                      commit_subject,
                      commit_subject_norm,
                      item_type,
                      has_diff,
                      patch_id_stable"#,
        );

        qb.build_query_as::<PatchItemRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn replace_patch_item_files(
        &self,
        patch_item_id: i64,
        files: &[UpsertPatchItemFileInput],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM patch_item_files WHERE patch_item_id = $1")
            .bind(patch_item_id)
            .execute(&mut *tx)
            .await?;

        if !files.is_empty() {
            let mut qb = QueryBuilder::new(
                "INSERT INTO patch_item_files \
                 (patch_item_id, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end)",
            );
            qb.push_values(files.iter(), |mut b, file| {
                b.push_bind(patch_item_id)
                    .push_bind(&file.old_path)
                    .push_bind(&file.new_path)
                    .push_bind(&file.change_type)
                    .push_bind(file.is_binary)
                    .push_bind(file.additions)
                    .push_bind(file.deletions)
                    .push_bind(file.hunk_count)
                    .push_bind(file.diff_start)
                    .push_bind(file.diff_end);
            });
            qb.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_patch_item_files_batch(
        &self,
        batches: &[PatchItemFileBatchInput],
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let mut deduped_file_rows: Vec<(i64, UpsertPatchItemFileInput)> = Vec::new();
        let mut row_idx_by_key: BTreeMap<(i64, String), usize> = BTreeMap::new();
        let mut stats_by_patch_item: BTreeMap<i64, (i32, i32, i32, i32)> = BTreeMap::new();

        for batch in batches {
            stats_by_patch_item
                .entry(batch.patch_item_id)
                .or_insert((0, 0, 0, 0));
            for file in &batch.files {
                let key = (batch.patch_item_id, file.new_path.clone());
                if let Some(idx) = row_idx_by_key.get(&key).copied() {
                    merge_patch_item_file_row(&mut deduped_file_rows[idx].1, file);
                } else {
                    let idx = deduped_file_rows.len();
                    deduped_file_rows.push((batch.patch_item_id, file.clone()));
                    row_idx_by_key.insert(key, idx);
                }
            }
        }

        for (patch_item_id, file) in &deduped_file_rows {
            let stats = stats_by_patch_item
                .entry(*patch_item_id)
                .or_insert((0, 0, 0, 0));
            stats.0 = stats.0.saturating_add(1);
            stats.1 = stats.1.saturating_add(file.additions);
            stats.2 = stats.2.saturating_add(file.deletions);
            stats.3 = stats.3.saturating_add(file.hunk_count);
        }

        let patch_item_ids: Vec<i64> = stats_by_patch_item.keys().copied().collect();

        sqlx::query("DELETE FROM patch_item_files WHERE patch_item_id = ANY($1)")
            .bind(&patch_item_ids)
            .execute(&mut *tx)
            .await?;

        if !deduped_file_rows.is_empty() {
            let rows_per_insert =
                (MAX_QUERY_BIND_PARAMS / PATCH_ITEM_FILE_INSERT_BINDS_PER_ROW).max(1);
            for chunk in deduped_file_rows.chunks(rows_per_insert) {
                let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                    "INSERT INTO patch_item_files \
                    (patch_item_id, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end) ",
                );
                qb.push_values(chunk.iter(), |mut b, (patch_item_id, file)| {
                    b.push_bind(patch_item_id)
                        .push_bind(&file.old_path)
                        .push_bind(&file.new_path)
                        .push_bind(&file.change_type)
                        .push_bind(file.is_binary)
                        .push_bind(file.additions)
                        .push_bind(file.deletions)
                        .push_bind(file.hunk_count)
                        .push_bind(file.diff_start)
                        .push_bind(file.diff_end);
                });
                qb.push(
                    " ON CONFLICT (patch_item_id, new_path) DO UPDATE SET \
                      old_path = EXCLUDED.old_path, \
                      change_type = EXCLUDED.change_type, \
                      is_binary = EXCLUDED.is_binary, \
                      additions = EXCLUDED.additions, \
                      deletions = EXCLUDED.deletions, \
                      hunk_count = EXCLUDED.hunk_count, \
                      diff_start = EXCLUDED.diff_start, \
                      diff_end = EXCLUDED.diff_end",
                );
                qb.build().execute(&mut *tx).await?;
            }
        }

        let stats_rows: Vec<(i64, i32, i32, i32, i32)> = stats_by_patch_item
            .into_iter()
            .map(
                |(patch_item_id, (file_count, additions, deletions, hunk_count))| {
                    (patch_item_id, file_count, additions, deletions, hunk_count)
                },
            )
            .collect();
        let stats_rows_per_chunk =
            (MAX_QUERY_BIND_PARAMS / PATCH_ITEM_FILE_STATS_BINDS_PER_ROW).max(1);
        for chunk in stats_rows.chunks(stats_rows_per_chunk) {
            let mut stats_qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                "UPDATE patch_items pi \
                 SET file_count = stats.file_count, \
                     additions = stats.additions, \
                     deletions = stats.deletions, \
                     hunk_count = stats.hunk_count \
                 FROM (",
            );
            stats_qb.push_values(chunk.iter(), |mut b, row| {
                b.push_bind(row.0)
                    .push_bind(row.1)
                    .push_bind(row.2)
                    .push_bind(row.3)
                    .push_bind(row.4);
            });
            stats_qb.push(
                ") AS stats(patch_item_id, file_count, additions, deletions, hunk_count) \
                 WHERE pi.id = stats.patch_item_id",
            );
            stats_qb.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn hydrate_patch_items_from_message_facts(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<PatchFactHydrationOutcome> {
        let mut ids = patch_item_ids
            .iter()
            .copied()
            .filter(|patch_item_id| *patch_item_id > 0)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();

        if ids.is_empty() {
            return Ok(PatchFactHydrationOutcome::default());
        }

        #[derive(sqlx::FromRow)]
        struct PatchFactRow {
            patch_item_id: i64,
            has_fact: bool,
            has_diff: bool,
            patch_id_stable: Option<String>,
        }

        let fact_rows = sqlx::query_as::<_, PatchFactRow>(
            r#"SELECT
                pi.id AS patch_item_id,
                (mpf.message_pk IS NOT NULL) AS has_fact,
                COALESCE(mpf.has_diff, false) AS has_diff,
                mpf.patch_id_stable
            FROM patch_items pi
            LEFT JOIN message_patch_facts mpf
              ON mpf.message_pk = pi.message_pk
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;

        let mut seen_patch_item_ids = BTreeSet::new();
        let mut hydrated_patch_item_ids = Vec::new();
        let mut missing_patch_item_ids = Vec::new();
        let mut patch_id_updates = Vec::new();
        let mut has_diff_updates = Vec::new();

        for row in fact_rows {
            seen_patch_item_ids.insert(row.patch_item_id);
            if row.has_fact {
                hydrated_patch_item_ids.push(row.patch_item_id);
                patch_id_updates.push((row.patch_item_id, row.patch_id_stable));
                has_diff_updates.push((row.patch_item_id, row.has_diff));
            } else {
                missing_patch_item_ids.push(row.patch_item_id);
            }
        }

        for patch_item_id in ids {
            if !seen_patch_item_ids.contains(&patch_item_id) {
                missing_patch_item_ids.push(patch_item_id);
            }
        }

        if hydrated_patch_item_ids.is_empty() {
            missing_patch_item_ids.sort_unstable();
            missing_patch_item_ids.dedup();
            return Ok(PatchFactHydrationOutcome {
                hydrated_patch_items: 0,
                patch_item_files_written: 0,
                missing_patch_item_ids,
            });
        }

        #[derive(sqlx::FromRow)]
        struct PatchFileRow {
            patch_item_id: i64,
            old_path: Option<String>,
            new_path: String,
            change_type: String,
            is_binary: bool,
            additions: i32,
            deletions: i32,
            hunk_count: i32,
            diff_start: i32,
            diff_end: i32,
        }

        let file_rows = sqlx::query_as::<_, PatchFileRow>(
            r#"SELECT
                pi.id AS patch_item_id,
                mpff.old_path,
                mpff.new_path,
                mpff.change_type,
                mpff.is_binary,
                mpff.additions,
                mpff.deletions,
                mpff.hunk_count,
                mpff.diff_start,
                mpff.diff_end
            FROM patch_items pi
            JOIN message_patch_file_facts mpff
              ON mpff.message_pk = pi.message_pk
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC, mpff.new_path ASC"#,
        )
        .bind(&hydrated_patch_item_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut files_by_patch_item = BTreeMap::<i64, Vec<UpsertPatchItemFileInput>>::new();
        for patch_item_id in &hydrated_patch_item_ids {
            files_by_patch_item.insert(*patch_item_id, Vec::new());
        }

        for row in file_rows {
            files_by_patch_item
                .entry(row.patch_item_id)
                .or_default()
                .push(UpsertPatchItemFileInput {
                    old_path: row.old_path,
                    new_path: row.new_path,
                    change_type: row.change_type,
                    is_binary: row.is_binary,
                    additions: row.additions,
                    deletions: row.deletions,
                    hunk_count: row.hunk_count,
                    diff_start: row.diff_start,
                    diff_end: row.diff_end,
                });
        }

        let mut patch_item_files_written = 0u64;
        let mut file_batches = Vec::with_capacity(hydrated_patch_item_ids.len());
        for patch_item_id in &hydrated_patch_item_ids {
            let files = files_by_patch_item
                .remove(patch_item_id)
                .unwrap_or_default();
            patch_item_files_written += files.len() as u64;

            let mut additions = 0i32;
            let mut deletions = 0i32;
            let mut hunk_count = 0i32;
            for file in &files {
                additions = additions.saturating_add(file.additions);
                deletions = deletions.saturating_add(file.deletions);
                hunk_count = hunk_count.saturating_add(file.hunk_count);
            }

            file_batches.push(PatchItemFileBatchInput {
                patch_item_id: *patch_item_id,
                file_count: files.len() as i32,
                additions,
                deletions,
                hunk_count,
                files,
            });
        }

        self.replace_patch_item_files_batch(&file_batches).await?;
        self.set_patch_item_patch_ids_batch(&patch_id_updates)
            .await?;

        const HAS_DIFF_BINDS_PER_ROW: usize = 2;
        let rows_per_chunk = (MAX_QUERY_BIND_PARAMS / HAS_DIFF_BINDS_PER_ROW).max(1);
        for chunk in has_diff_updates.chunks(rows_per_chunk) {
            let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                "UPDATE patch_items pi \
                 SET has_diff = vals.has_diff \
                 FROM (",
            );
            qb.push_values(chunk.iter(), |mut b, (patch_item_id, has_diff)| {
                b.push_bind(*patch_item_id).push_bind(*has_diff);
            });
            qb.push(") AS vals(patch_item_id, has_diff) WHERE pi.id = vals.patch_item_id");
            qb.build().execute(&self.pool).await?;
        }

        missing_patch_item_ids.sort_unstable();
        missing_patch_item_ids.dedup();

        Ok(PatchFactHydrationOutcome {
            hydrated_patch_items: hydrated_patch_item_ids.len() as u64,
            patch_item_files_written,
            missing_patch_item_ids,
        })
    }

    pub async fn update_patch_item_diff_stats(
        &self,
        patch_item_id: i64,
        file_count: i32,
        additions: i32,
        deletions: i32,
        hunk_count: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE patch_items
            SET file_count = $2,
                additions = $3,
                deletions = $4,
                hunk_count = $5
            WHERE id = $1"#,
        )
        .bind(patch_item_id)
        .bind(file_count)
        .bind(additions)
        .bind(deletions)
        .bind(hunk_count)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_patch_item_detail(
        &self,
        patch_item_id: i64,
    ) -> Result<Option<PatchItemDetailRecord>> {
        sqlx::query_as::<_, PatchItemDetailRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                psv.patch_series_id,
                pi.patch_series_version_id,
                pi.ordinal,
                pi.total,
                pi.item_type,
                pi.subject_raw,
                pi.subject_norm,
                pi.commit_subject,
                pi.commit_subject_norm,
                pi.message_pk,
                m.message_id_primary,
                pi.patch_id_stable,
                pi.has_diff,
                pi.file_count,
                pi.additions,
                pi.deletions,
                pi.hunk_count
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            JOIN messages m
              ON m.id = pi.message_pk
            WHERE pi.id = $1
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_patch_item_files(
        &self,
        patch_item_id: i64,
    ) -> Result<Vec<PatchItemFileRecord>> {
        sqlx::query_as::<_, PatchItemFileRecord>(
            r#"SELECT
                patch_item_id,
                old_path,
                new_path,
                change_type,
                is_binary,
                additions,
                deletions,
                hunk_count,
                diff_start,
                diff_end
            FROM patch_item_files
            WHERE patch_item_id = $1
            ORDER BY new_path ASC"#,
        )
        .bind(patch_item_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_patch_item_file_diff_source(
        &self,
        patch_item_id: i64,
        path: &str,
    ) -> Result<Option<PatchItemFileDiffSliceSource>> {
        sqlx::query_as::<_, PatchItemFileDiffSliceSource>(
            r#"SELECT
                pif.new_path,
                pif.diff_start,
                pif.diff_end,
                mb.diff_text
            FROM patch_item_files pif
            JOIN patch_items pi
              ON pi.id = pif.patch_item_id
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE pif.patch_item_id = $1
              AND pif.new_path = $2
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .bind(path)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_patch_item_full_diff(&self, patch_item_id: i64) -> Result<Option<String>> {
        let row = sqlx::query_scalar::<_, Option<String>>(
            r#"SELECT mb.diff_text
            FROM patch_items pi
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE pi.id = $1
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.flatten())
    }

    pub async fn get_message_body(&self, message_pk: i64) -> Result<Option<MessageBodyRecord>> {
        sqlx::query_as::<_, MessageBodyRecord>(
            r#"SELECT
                m.id AS message_pk,
                m.subject_raw,
                mb.body_text,
                mb.diff_text,
                mb.has_diff,
                mb.has_attachments
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1
            LIMIT 1"#,
        )
        .bind(message_pk)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_message_detail(&self, message_pk: i64) -> Result<Option<MessageDetailRecord>> {
        sqlx::query_as::<_, MessageDetailRecord>(
            r#"SELECT
                m.id AS message_pk,
                m.message_id_primary,
                m.subject_raw,
                m.subject_norm,
                m.from_name,
                m.from_email,
                m.date_utc,
                m.to_raw,
                m.cc_raw,
                m.references_ids,
                m.in_reply_to_ids,
                mb.has_diff,
                mb.has_attachments
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1
            LIMIT 1"#,
        )
        .bind(message_pk)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn resolve_message_pk_by_message_id(&self, message_id: &str) -> Result<Option<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT message_pk
            FROM message_id_map
            WHERE message_id = $1
            ORDER BY is_primary DESC, message_pk ASC
            LIMIT 1"#,
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_threads(
        &self,
        mailing_list_id: i64,
        params: &ListThreadsParams,
    ) -> Result<Vec<ThreadListItemRecord>> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                t.id AS thread_id,
                t.subject_norm,
                t.root_message_pk,
                t.created_at,
                t.last_activity_at,
                t.message_count,
                starter_msg.from_name AS starter_name,
                starter_msg.from_email AS starter_email,
                EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    JOIN message_bodies mb ON mb.id = m.body_id
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND mb.has_diff = true
                ) AS has_diff
            FROM threads t
            LEFT JOIN messages starter_msg ON starter_msg.id = t.root_message_pk
            WHERE t.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);

        let (cursor_column, descending) = match params.sort.as_str() {
            "date_desc" => ("t.created_at", true),
            "date_asc" => ("t.created_at", false),
            _ => ("t.last_activity_at", true),
        };

        if let Some(from_ts) = params.from_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" >= ");
            qb.push_bind(from_ts);
        }
        if let Some(to_ts) = params.to_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" < ");
            qb.push_bind(to_ts);
        }
        if let Some(author_email) = params.author_email.as_deref() {
            qb.push(
                r#" AND EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND lower(m.from_email) = lower("#,
            );
            qb.push_bind(author_email);
            qb.push("))");
        }
        if let Some(has_diff) = params.has_diff {
            if has_diff {
                qb.push(
                    r#" AND EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            } else {
                qb.push(
                    r#" AND NOT EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            }
        }

        if let (Some(cursor_ts), Some(cursor_id)) = (params.cursor_ts, params.cursor_id) {
            qb.push(" AND (");
            qb.push(cursor_column);
            qb.push(", t.id) ");
            if descending {
                qb.push("< (");
            } else {
                qb.push("> (");
            }
            qb.push_bind(cursor_ts);
            qb.push(", ");
            qb.push_bind(cursor_id);
            qb.push(")");
        }

        let limit = params.limit.clamp(1, 500);
        qb.push(" ORDER BY ");
        qb.push(cursor_column);
        if descending {
            qb.push(" DESC, t.id DESC LIMIT ");
        } else {
            qb.push(" ASC, t.id ASC LIMIT ");
        }
        qb.push_bind(limit);

        qb.build_query_as::<ThreadListItemRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_threads(
        &self,
        mailing_list_id: i64,
        params: &ListThreadsParams,
    ) -> Result<i64> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT COUNT(*)::bigint
            FROM threads t
            WHERE t.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);

        let cursor_column = if params.sort == "date_desc" || params.sort == "date_asc" {
            "t.created_at"
        } else {
            "t.last_activity_at"
        };

        if let Some(from_ts) = params.from_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" >= ");
            qb.push_bind(from_ts);
        }
        if let Some(to_ts) = params.to_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" < ");
            qb.push_bind(to_ts);
        }
        if let Some(author_email) = params.author_email.as_deref() {
            qb.push(
                r#" AND EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND lower(m.from_email) = lower("#,
            );
            qb.push_bind(author_email);
            qb.push("))");
        }
        if let Some(has_diff) = params.has_diff {
            if has_diff {
                qb.push(
                    r#" AND EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            } else {
                qb.push(
                    r#" AND NOT EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            }
        }

        let row = qb.build_query_scalar::<i64>().fetch_one(&self.pool).await?;
        Ok(row)
    }

    pub async fn list_thread_participants(
        &self,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<ThreadParticipantRecord>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, ThreadParticipantRecord>(
            r#"WITH ranked AS (
                SELECT
                    tm.thread_id,
                    m.from_name,
                    m.from_email,
                    row_number() OVER (
                        PARTITION BY tm.thread_id, lower(m.from_email)
                        ORDER BY m.date_utc DESC NULLS LAST, m.id DESC
                    ) AS rn
                FROM thread_messages tm
                JOIN messages m
                  ON m.id = tm.message_pk
                WHERE tm.mailing_list_id = $1
                  AND tm.thread_id = ANY($2)
            )
            SELECT
                thread_id,
                from_name,
                from_email
            FROM ranked
            WHERE rn = 1
            ORDER BY thread_id ASC, from_email ASC"#,
        )
        .bind(mailing_list_id)
        .bind(thread_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_thread_summary(
        &self,
        mailing_list_id: i64,
        thread_id: i64,
    ) -> Result<Option<ThreadSummaryRecord>> {
        sqlx::query_as::<_, ThreadSummaryRecord>(
            r#"SELECT
                id AS thread_id,
                subject_norm,
                last_activity_at,
                membership_hash
            FROM threads
            WHERE mailing_list_id = $1
              AND id = $2
            LIMIT 1"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_thread_messages(
        &self,
        mailing_list_id: i64,
        thread_id: i64,
        limit: i64,
        cursor_sort_key: Option<&[u8]>,
        cursor_message_pk: Option<i64>,
    ) -> Result<Vec<ThreadMessageRecord>> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                tm.message_pk,
                tm.parent_message_pk,
                tm.depth,
                tm.sort_key,
                m.from_name,
                m.from_email,
                m.date_utc,
                m.subject_raw,
                mb.has_diff,
                mb.body_text,
                patch_ref.patch_item_id
            FROM thread_messages tm
            JOIN messages m
              ON m.id = tm.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            LEFT JOIN LATERAL (
                SELECT pi.id AS patch_item_id
                FROM patch_items pi
                WHERE pi.message_pk = tm.message_pk
                  AND pi.item_type = 'patch'
                ORDER BY pi.id DESC
                LIMIT 1
            ) patch_ref ON true
            WHERE tm.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);
        qb.push(" AND tm.thread_id = ");
        qb.push_bind(thread_id);

        if let (Some(cursor_sort_key), Some(cursor_message_pk)) =
            (cursor_sort_key, cursor_message_pk)
        {
            qb.push(" AND (tm.sort_key, tm.message_pk) > (");
            qb.push_bind(cursor_sort_key.to_vec());
            qb.push(", ");
            qb.push_bind(cursor_message_pk);
            qb.push(")");
        }

        qb.push(" ORDER BY tm.sort_key ASC, tm.message_pk ASC LIMIT ");
        qb.push_bind(limit.clamp(1, 500));

        qb.build_query_as::<ThreadMessageRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_thread_messages(&self, mailing_list_id: i64, thread_id: i64) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(*)::bigint
            FROM thread_messages
            WHERE mailing_list_id = $1
              AND thread_id = $2"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_series_version(
        &self,
        patch_series_id: i64,
        series_version_id: i64,
    ) -> Result<Option<PatchSeriesVersionRecord>> {
        sqlx::query_as::<_, PatchSeriesVersionRecord>(
            r#"SELECT
                id,
                patch_series_id,
                version_num,
                is_rfc,
                is_resend,
                is_partial_reroll,
                thread_id,
                cover_message_pk,
                first_patch_message_pk,
                sent_at,
                subject_raw,
                subject_norm,
                base_commit,
                version_fingerprint
            FROM patch_series_versions
            WHERE patch_series_id = $1
              AND id = $2
            LIMIT 1"#,
        )
        .bind(patch_series_id)
        .bind(series_version_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_series(
        &self,
        list_key: Option<&str>,
        sort: &str,
        limit: i64,
        cursor_ts: Option<DateTime<Utc>>,
        cursor_id: Option<i64>,
    ) -> Result<Vec<SeriesListItemRecord>> {
        let descending = sort != "last_seen_asc";
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                ps.id AS series_id,
                ps.canonical_subject_norm,
                ps.author_email,
                ps.author_name,
                ps.created_at AS first_seen_at,
                COALESCE(latest.sent_at, ps.last_seen_at) AS latest_patchset_at,
                ps.last_seen_at,
                COALESCE(latest.version_num, 1) AS latest_version_num,
                COALESCE(latest.is_rfc, false) AS is_rfc_latest
            FROM patch_series ps
            LEFT JOIN LATERAL (
                SELECT psv.version_num, psv.is_rfc, psv.sent_at
                FROM patch_series_versions psv
                WHERE psv.patch_series_id = ps.id
                ORDER BY psv.version_num DESC, psv.sent_at DESC, psv.id DESC
                LIMIT 1
            ) latest ON true"#,
        );

        if list_key.is_some() {
            qb.push(
                r#" JOIN patch_series_lists psl
                     ON psl.patch_series_id = ps.id
                    JOIN mailing_lists ml
                     ON ml.id = psl.mailing_list_id"#,
            );
        }

        qb.push(" WHERE 1=1");
        if let Some(list_key) = list_key {
            qb.push(" AND ml.list_key = ");
            qb.push_bind(list_key);
        }

        if let (Some(cursor_ts), Some(cursor_id)) = (cursor_ts, cursor_id) {
            if descending {
                qb.push(" AND (ps.last_seen_at, ps.id) < (");
            } else {
                qb.push(" AND (ps.last_seen_at, ps.id) > (");
            }
            qb.push_bind(cursor_ts);
            qb.push(", ");
            qb.push_bind(cursor_id);
            qb.push(")");
        }

        qb.push(" ORDER BY ps.last_seen_at ");
        if descending {
            qb.push("DESC, ps.id DESC LIMIT ");
        } else {
            qb.push("ASC, ps.id ASC LIMIT ");
        }
        qb.push_bind(limit.clamp(1, 500));

        qb.build_query_as::<SeriesListItemRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_series(&self, list_key: Option<&str>) -> Result<i64> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> =
            QueryBuilder::new("SELECT COUNT(*)::bigint FROM patch_series ps");

        if let Some(list_key) = list_key {
            qb.push(
                r#" JOIN patch_series_lists psl
                     ON psl.patch_series_id = ps.id
                    JOIN mailing_lists ml
                     ON ml.id = psl.mailing_list_id
                    WHERE ml.list_key = "#,
            );
            qb.push_bind(list_key);
        }

        qb.build_query_scalar::<i64>().fetch_one(&self.pool).await
    }

    pub async fn list_series_list_keys(&self, patch_series_id: i64) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"SELECT ml.list_key
            FROM patch_series_lists psl
            JOIN mailing_lists ml
              ON ml.id = psl.mailing_list_id
            WHERE psl.patch_series_id = $1
            ORDER BY ml.list_key ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_versions_with_counts(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<SeriesVersionSummaryRecord>> {
        sqlx::query_as::<_, SeriesVersionSummaryRecord>(
            r#"SELECT
                psv.id,
                psv.patch_series_id,
                psv.version_num,
                psv.is_rfc,
                psv.is_resend,
                psv.is_partial_reroll,
                psv.thread_id,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm,
                COUNT(*) FILTER (WHERE pi.item_type = 'patch') AS patch_count
            FROM patch_series_versions psv
            LEFT JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
            GROUP BY
                psv.id,
                psv.patch_series_id,
                psv.version_num,
                psv.is_rfc,
                psv.is_resend,
                psv.is_partial_reroll,
                psv.thread_id,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm
            ORDER BY psv.version_num ASC, psv.sent_at ASC, psv.id ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_thread_ref(&self, thread_id: i64) -> Result<Option<ThreadRefRecord>> {
        sqlx::query_as::<_, ThreadRefRecord>(
            r#"SELECT
                t.id AS thread_id,
                ml.list_key
            FROM threads t
            JOIN mailing_lists ml
              ON ml.id = t.mailing_list_id
            WHERE t.id = $1
            LIMIT 1"#,
        )
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_series_version_patch_items(
        &self,
        patch_series_version_id: i64,
        assembled: bool,
    ) -> Result<Vec<SeriesVersionPatchItemRecord>> {
        if assembled {
            return sqlx::query_as::<_, SeriesVersionPatchItemRecord>(
                r#"SELECT
                    pi.id AS patch_item_id,
                    psvai.ordinal,
                    pi.total,
                    pi.message_pk,
                    m.message_id_primary,
                    pi.subject_raw,
                    pi.subject_norm,
                    pi.commit_subject,
                    pi.commit_subject_norm,
                    pi.item_type,
                    pi.has_diff,
                    pi.patch_id_stable,
                    pi.file_count,
                    pi.additions,
                    pi.deletions,
                    pi.hunk_count,
                    psvai.inherited_from_version_num
                FROM patch_series_version_assembled_items psvai
                JOIN patch_items pi
                  ON pi.id = psvai.patch_item_id
                JOIN messages m
                  ON m.id = pi.message_pk
                WHERE psvai.patch_series_version_id = $1
                ORDER BY psvai.ordinal ASC, pi.id ASC"#,
            )
            .bind(patch_series_version_id)
            .fetch_all(&self.pool)
            .await;
        }

        sqlx::query_as::<_, SeriesVersionPatchItemRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                pi.ordinal,
                pi.total,
                pi.message_pk,
                m.message_id_primary,
                pi.subject_raw,
                pi.subject_norm,
                pi.commit_subject,
                pi.commit_subject_norm,
                pi.item_type,
                pi.has_diff,
                pi.patch_id_stable,
                pi.file_count,
                pi.additions,
                pi.deletions,
                pi.hunk_count,
                NULL::int4 AS inherited_from_version_num
            FROM patch_items pi
            JOIN messages m
              ON m.id = pi.message_pk
            WHERE pi.patch_series_version_id = $1
            ORDER BY pi.ordinal ASC, pi.id ASC"#,
        )
        .bind(patch_series_version_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_logical_compare(
        &self,
        patch_series_id: i64,
        v1_version_num: i32,
        v2_version_num: i32,
    ) -> Result<Vec<SeriesLogicalCompareRow>> {
        sqlx::query_as::<_, SeriesLogicalCompareRow>(
            r#"SELECT
                pl.slot,
                pl.title_norm,
                plv1.patch_item_id AS v1_patch_item_id,
                pi1.item_type AS v1_item_type,
                pi1.patch_id_stable AS v1_patch_id_stable,
                pi1.subject_raw AS v1_subject_raw,
                plv2.patch_item_id AS v2_patch_item_id,
                pi2.item_type AS v2_item_type,
                pi2.patch_id_stable AS v2_patch_id_stable,
                pi2.subject_raw AS v2_subject_raw
            FROM patch_logical pl
            LEFT JOIN patch_logical_versions plv1
              ON plv1.patch_logical_id = pl.id
             AND plv1.version_num = $2
            LEFT JOIN patch_logical_versions plv2
              ON plv2.patch_logical_id = pl.id
             AND plv2.version_num = $3
            LEFT JOIN patch_items pi1
              ON pi1.id = plv1.patch_item_id
            LEFT JOIN patch_items pi2
              ON pi2.id = plv2.patch_item_id
            WHERE pl.patch_series_id = $1
            ORDER BY pl.slot ASC"#,
        )
        .bind(patch_series_id)
        .bind(v1_version_num)
        .bind(v2_version_num)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn aggregate_patch_item_files(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<Vec<PatchItemFileAggregateRecord>> {
        if patch_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, PatchItemFileAggregateRecord>(
            r#"SELECT
                pif.new_path AS path,
                SUM(pif.additions)::bigint AS additions,
                SUM(pif.deletions)::bigint AS deletions,
                SUM(pif.hunk_count)::bigint AS hunk_count
            FROM patch_item_files pif
            WHERE pif.patch_item_id = ANY($1)
            GROUP BY pif.new_path
            ORDER BY pif.new_path ASC"#,
        )
        .bind(patch_item_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_items_for_version(
        &self,
        patch_series_version_id: i64,
    ) -> Result<Vec<PatchItemRecord>> {
        sqlx::query_as::<_, PatchItemRecord>(
            r#"SELECT
                id,
                patch_series_version_id,
                ordinal,
                total,
                message_pk,
                subject_raw,
                subject_norm,
                commit_subject,
                commit_subject_norm,
                item_type,
                has_diff,
                patch_id_stable
            FROM patch_items
            WHERE patch_series_version_id = $1
            ORDER BY ordinal ASC"#,
        )
        .bind(patch_series_version_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn find_inherited_patch_item(
        &self,
        patch_series_id: i64,
        current_version_num: i32,
        ordinal: i32,
    ) -> Result<Option<(i64, i32)>> {
        sqlx::query_as::<_, (i64, i32)>(
            r#"SELECT
                pi.id,
                psv.version_num
            FROM patch_series_versions psv
            JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
              AND psv.version_num < $2
              AND pi.ordinal = $3
              AND pi.item_type = 'patch'
            ORDER BY psv.version_num DESC, psv.sent_at DESC, psv.id DESC
            LIMIT 1"#,
        )
        .bind(patch_series_id)
        .bind(current_version_num)
        .bind(ordinal)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn replace_assembled_items(
        &self,
        patch_series_version_id: i64,
        items: &[(i32, i64, Option<i32>)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "DELETE FROM patch_series_version_assembled_items WHERE patch_series_version_id = $1",
        )
        .bind(patch_series_version_id)
        .execute(&mut *tx)
        .await?;

        for (ordinal, patch_item_id, inherited_from_version_num) in items {
            sqlx::query(
                r#"INSERT INTO patch_series_version_assembled_items
                (patch_series_version_id, ordinal, patch_item_id, inherited_from_version_num)
                VALUES ($1, $2, $3, $4)"#,
            )
            .bind(patch_series_version_id)
            .bind(*ordinal)
            .bind(*patch_item_id)
            .bind(*inherited_from_version_num)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn set_partial_reroll_flag(
        &self,
        patch_series_version_id: i64,
        is_partial_reroll: bool,
    ) -> Result<()> {
        sqlx::query("UPDATE patch_series_versions SET is_partial_reroll = $2 WHERE id = $1")
            .bind(patch_series_version_id)
            .bind(is_partial_reroll)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn load_assembled_items_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<AssembledItemRecord>> {
        sqlx::query_as::<_, AssembledItemRecord>(
            r#"SELECT
                psv.version_num,
                psvai.ordinal,
                psvai.patch_item_id,
                pi.patch_id_stable,
                COALESCE(pi.commit_subject_norm, pi.subject_norm) AS title_norm
            FROM patch_series_versions psv
            JOIN patch_series_version_assembled_items psvai
              ON psvai.patch_series_version_id = psv.id
            JOIN patch_items pi
              ON pi.id = psvai.patch_item_id
            WHERE psv.patch_series_id = $1
            ORDER BY psv.version_num ASC, psvai.ordinal ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_logicals_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<PatchLogicalRecord>> {
        sqlx::query_as::<_, PatchLogicalRecord>(
            r#"SELECT id, patch_series_id, slot, title_norm
            FROM patch_logical
            WHERE patch_series_id = $1
            ORDER BY slot ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn upsert_patch_logical(
        &self,
        patch_series_id: i64,
        slot: i32,
        title_norm: &str,
    ) -> Result<PatchLogicalRecord> {
        sqlx::query_as::<_, PatchLogicalRecord>(
            r#"INSERT INTO patch_logical (patch_series_id, slot, title_norm)
            VALUES ($1, $2, $3)
            ON CONFLICT (patch_series_id, slot)
            DO UPDATE SET title_norm = CASE
                WHEN patch_logical.title_norm = '' THEN EXCLUDED.title_norm
                ELSE patch_logical.title_norm
            END
            RETURNING id, patch_series_id, slot, title_norm"#,
        )
        .bind(patch_series_id)
        .bind(slot)
        .bind(title_norm)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn replace_patch_logical_versions(
        &self,
        patch_series_id: i64,
        mappings: &[(i32, i32, i64)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"DELETE FROM patch_logical_versions
            WHERE patch_logical_id IN (
                SELECT id
                FROM patch_logical
                WHERE patch_series_id = $1
            )"#,
        )
        .bind(patch_series_id)
        .execute(&mut *tx)
        .await?;

        let logical_rows = sqlx::query_as::<_, PatchLogicalRecord>(
            r#"SELECT id, patch_series_id, slot, title_norm
            FROM patch_logical
            WHERE patch_series_id = $1"#,
        )
        .bind(patch_series_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut slot_to_id = std::collections::HashMap::new();
        for logical in logical_rows {
            slot_to_id.insert(logical.slot, logical.id);
        }

        for (slot, version_num, patch_item_id) in mappings {
            if let Some(patch_logical_id) = slot_to_id.get(slot).copied() {
                sqlx::query(
                    r#"INSERT INTO patch_logical_versions
                    (patch_logical_id, patch_item_id, version_num)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (patch_logical_id, version_num)
                    DO UPDATE SET patch_item_id = EXCLUDED.patch_item_id"#,
                )
                .bind(patch_logical_id)
                .bind(*patch_item_id)
                .bind(*version_num)
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn list_versions_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<PatchSeriesVersionRecord>> {
        sqlx::query_as::<_, PatchSeriesVersionRecord>(
            r#"SELECT
                id,
                patch_series_id,
                version_num,
                is_rfc,
                is_resend,
                is_partial_reroll,
                thread_id,
                cover_message_pk,
                first_patch_message_pk,
                sent_at,
                subject_raw,
                subject_norm,
                base_commit,
                version_fingerprint
            FROM patch_series_versions
            WHERE patch_series_id = $1
            ORDER BY version_num ASC, sent_at ASC, id ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_ids_by_series_and_version(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<SeriesVersionPatchRef>> {
        sqlx::query_as::<_, SeriesVersionPatchRef>(
            r#"SELECT
                psv.patch_series_id,
                psv.version_num,
                pi.patch_id_stable
            FROM patch_series_versions psv
            JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
              AND pi.item_type = 'patch'
            ORDER BY psv.version_num ASC, pi.ordinal ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn load_patch_item_diffs(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<Vec<PatchItemDiffRecord>> {
        if patch_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, PatchItemDiffRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                mb.diff_text
            FROM patch_items pi
            JOIN messages m ON m.id = pi.message_pk
            JOIN message_bodies mb ON mb.id = m.body_id
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(patch_item_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn set_patch_item_patch_id(
        &self,
        patch_item_id: i64,
        patch_id_stable: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE patch_items SET patch_id_stable = $2 WHERE id = $1")
            .bind(patch_item_id)
            .bind(patch_id_stable)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_patch_item_patch_ids_batch(
        &self,
        updates: &[(i64, Option<String>)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "UPDATE patch_items pi \
             SET patch_id_stable = vals.patch_id_stable \
             FROM (",
        );
        qb.push_values(updates.iter(), |mut b, (patch_item_id, patch_id)| {
            b.push_bind(*patch_item_id).push_bind(patch_id.clone());
        });
        qb.push(") AS vals(patch_item_id, patch_id_stable) WHERE pi.id = vals.patch_item_id");
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_patch_items_for_message_ids(&self, message_pks: &[i64]) -> Result<Vec<i64>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT pi.id
            FROM patch_items pi
            WHERE pi.message_pk = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub fn unique_message_ids(
        references_ids: &[String],
        in_reply_to_ids: &[String],
    ) -> Vec<String> {
        let mut set = BTreeSet::new();
        for value in references_ids {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                set.insert(trimmed.to_string());
            }
        }
        for value in in_reply_to_ids {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                set.insert(trimmed.to_string());
            }
        }
        set.into_iter().collect()
    }
}
