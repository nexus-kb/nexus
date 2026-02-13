use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

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
        sqlx::query(
            "UPDATE patch_series_versions SET is_partial_reroll = $2 WHERE id = $1",
        )
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
        sqlx::query(
            "UPDATE patch_items SET patch_id_stable = $2 WHERE id = $1",
        )
        .bind(patch_item_id)
        .bind(patch_id_stable)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_patch_items_for_message_ids(
        &self,
        message_pks: &[i64],
    ) -> Result<Vec<i64>> {
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

    pub fn unique_message_ids(references_ids: &[String], in_reply_to_ids: &[String]) -> Vec<String> {
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
