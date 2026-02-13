use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, QueryBuilder};

use crate::Result;

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
    pub has_diff: bool,
}

#[derive(Debug, Clone)]
pub struct ListThreadsParams {
    pub sort: String,
    pub from_ts: Option<DateTime<Utc>>,
    pub to_ts: Option<DateTime<Utc>>,
    pub author_email: Option<String>,
    pub has_diff: Option<bool>,
    pub cursor: Option<(DateTime<Utc>, i64)>,
    pub limit: i64,
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
pub struct SeriesExportMessageRecord {
    pub ordinal: i32,
    pub item_type: String,
    pub message_pk: i64,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub subject_raw: String,
    pub raw_rfc822: Vec<u8>,
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

    pub async fn load_messages_for_anchors(
        &self,
        mailing_list_id: i64,
        anchor_message_pks: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if anchor_message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, LineageSourceMessage>(
            r#"WITH anchor_threads AS (
                SELECT DISTINCT tm.thread_id
                FROM thread_messages tm
                WHERE tm.mailing_list_id = $1
                  AND tm.message_pk = ANY($2)
            )
            SELECT
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
                mb.body_text,
                mb.diff_text
            FROM thread_messages tm
            JOIN anchor_threads at
              ON at.thread_id = tm.thread_id
            JOIN messages m
              ON m.id = tm.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE tm.mailing_list_id = $1
            ORDER BY tm.thread_id ASC, tm.sort_key ASC"#,
        )
        .bind(mailing_list_id)
        .bind(anchor_message_pks)
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
            DO UPDATE SET total = EXCLUDED.total,
                          message_pk = EXCLUDED.message_pk,
                          subject_raw = EXCLUDED.subject_raw,
                          subject_norm = EXCLUDED.subject_norm,
                          commit_subject = EXCLUDED.commit_subject,
                          commit_subject_norm = EXCLUDED.commit_subject_norm,
                          commit_author_name = EXCLUDED.commit_author_name,
                          commit_author_email = EXCLUDED.commit_author_email,
                          item_type = EXCLUDED.item_type,
                          has_diff = EXCLUDED.has_diff,
                          patch_id_stable = EXCLUDED.patch_id_stable,
                          file_count = EXCLUDED.file_count,
                          additions = EXCLUDED.additions,
                          deletions = EXCLUDED.deletions,
                          hunk_count = EXCLUDED.hunk_count
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

    pub async fn get_message_raw_rfc822(&self, message_pk: i64) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar::<_, Vec<u8>>(
            r#"SELECT mb.raw_rfc822
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
            WHERE t.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);

        let cursor_column = if params.sort == "date_desc" {
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
        if let Some((cursor_ts, cursor_id)) = params.cursor {
            qb.push(" AND (");
            qb.push(cursor_column);
            qb.push(" < ");
            qb.push_bind(cursor_ts);
            qb.push(" OR (");
            qb.push(cursor_column);
            qb.push(" = ");
            qb.push_bind(cursor_ts);
            qb.push(" AND t.id < ");
            qb.push_bind(cursor_id);
            qb.push("))");
        }

        qb.push(" ORDER BY ");
        qb.push(cursor_column);
        qb.push(" DESC, t.id DESC LIMIT ");
        qb.push_bind(params.limit.clamp(1, 200));

        qb.build_query_as::<ThreadListItemRecord>()
            .fetch_all(&self.pool)
            .await
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
    ) -> Result<Vec<ThreadMessageRecord>> {
        sqlx::query_as::<_, ThreadMessageRecord>(
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
            WHERE tm.mailing_list_id = $1
              AND tm.thread_id = $2
            ORDER BY tm.sort_key ASC"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_all(&self.pool)
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
        cursor: Option<(DateTime<Utc>, i64)>,
        limit: i64,
    ) -> Result<Vec<SeriesListItemRecord>> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                ps.id AS series_id,
                ps.canonical_subject_norm,
                ps.author_email,
                ps.author_name,
                ps.last_seen_at,
                COALESCE(latest.version_num, 1) AS latest_version_num,
                COALESCE(latest.is_rfc, false) AS is_rfc_latest
            FROM patch_series ps
            LEFT JOIN LATERAL (
                SELECT psv.version_num, psv.is_rfc
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

        if let Some((cursor_ts, cursor_id)) = cursor {
            qb.push(" AND (ps.last_seen_at < ");
            qb.push_bind(cursor_ts);
            qb.push(" OR (ps.last_seen_at = ");
            qb.push_bind(cursor_ts);
            qb.push(" AND ps.id < ");
            qb.push_bind(cursor_id);
            qb.push("))");
        }

        qb.push(" ORDER BY ps.last_seen_at DESC, ps.id DESC LIMIT ");
        qb.push_bind(limit.clamp(1, 200));

        qb.build_query_as::<SeriesListItemRecord>()
            .fetch_all(&self.pool)
            .await
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

    pub async fn list_export_messages(
        &self,
        patch_series_id: i64,
        series_version_id: i64,
        assembled: bool,
        include_cover: bool,
    ) -> Result<Vec<SeriesExportMessageRecord>> {
        if assembled {
            return sqlx::query_as::<_, SeriesExportMessageRecord>(
                r#"SELECT
                    psvai.ordinal,
                    pi.item_type,
                    m.id AS message_pk,
                    m.from_email,
                    m.date_utc,
                    m.subject_raw,
                    mb.raw_rfc822
                FROM patch_series_versions psv
                JOIN patch_series_version_assembled_items psvai
                  ON psvai.patch_series_version_id = psv.id
                JOIN patch_items pi
                  ON pi.id = psvai.patch_item_id
                JOIN messages m
                  ON m.id = pi.message_pk
                JOIN message_bodies mb
                  ON mb.id = m.body_id
                WHERE psv.patch_series_id = $1
                  AND psv.id = $2
                  AND (
                    pi.item_type = 'patch'
                    OR ($3::boolean AND pi.item_type = 'cover')
                  )
                ORDER BY psvai.ordinal ASC, pi.id ASC"#,
            )
            .bind(patch_series_id)
            .bind(series_version_id)
            .bind(include_cover)
            .fetch_all(&self.pool)
            .await;
        }

        sqlx::query_as::<_, SeriesExportMessageRecord>(
            r#"SELECT
                pi.ordinal,
                pi.item_type,
                m.id AS message_pk,
                m.from_email,
                m.date_utc,
                m.subject_raw,
                mb.raw_rfc822
            FROM patch_series_versions psv
            JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE psv.patch_series_id = $1
              AND psv.id = $2
              AND (
                pi.item_type = 'patch'
                OR ($3::boolean AND pi.item_type = 'cover')
              )
            ORDER BY pi.ordinal ASC, pi.id ASC"#,
        )
        .bind(patch_series_id)
        .bind(series_version_id)
        .bind(include_cover)
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
