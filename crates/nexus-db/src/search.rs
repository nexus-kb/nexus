use chrono::{DateTime, Datelike, Utc};
use serde_json::json;
use sqlx::PgPool;

use crate::Result;

#[derive(Debug, Clone, sqlx::FromRow)]
struct PatchItemDocRow {
    id: i64,
    patch_series_id: i64,
    series_version_id: i64,
    version_num: i32,
    ordinal: i32,
    is_rfc: bool,
    author_email: String,
    sent_at: DateTime<Utc>,
    subject: String,
    commit_subject: Option<String>,
    has_diff: bool,
    message_id_primary: String,
    diff_text: Option<String>,
    file_paths: Vec<String>,
    list_keys: Vec<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PatchSeriesDocRow {
    id: i64,
    canonical_subject: String,
    author_email: String,
    last_seen_at: DateTime<Utc>,
    latest_version_num: Option<i32>,
    latest_version_id: Option<i64>,
    latest_version_is_rfc: Option<bool>,
    cover_body: Option<String>,
    has_diff: Option<bool>,
    patch_subjects: Vec<String>,
    list_keys: Vec<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ThreadDocRow {
    id: i64,
    list_key: String,
    subject: String,
    created_at: DateTime<Utc>,
    last_activity_at: DateTime<Utc>,
    message_count: i32,
    starter_name: Option<String>,
    starter_email: Option<String>,
    participants: Vec<String>,
    snippet_corpus: Option<String>,
    has_diff: Option<bool>,
}

#[derive(Clone)]
pub struct SearchStore {
    pool: PgPool,
}

impl SearchStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_thread_ids_for_message_pks(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
    ) -> Result<Vec<i64>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT tm.thread_id
            FROM thread_messages tm
            WHERE tm.mailing_list_id = $1
              AND tm.message_pk = ANY($2)
            ORDER BY tm.thread_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn build_patch_item_docs(&self, ids: &[i64]) -> Result<Vec<serde_json::Value>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, PatchItemDocRow>(
            r#"SELECT
                pi.id,
                psv.patch_series_id,
                psv.id AS series_version_id,
                psv.version_num,
                pi.ordinal,
                psv.is_rfc,
                ps.author_email,
                psv.sent_at,
                pi.subject_raw AS subject,
                pi.commit_subject,
                pi.has_diff,
                m.message_id_primary,
                mb.diff_text,
                COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT pif.new_path), NULL), ARRAY[]::text[]) AS file_paths,
                COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT ml.list_key), NULL), ARRAY[]::text[]) AS list_keys
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            JOIN patch_series ps
              ON ps.id = psv.patch_series_id
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            LEFT JOIN patch_item_files pif
              ON pif.patch_item_id = pi.id
            LEFT JOIN patch_series_lists psl
              ON psl.patch_series_id = psv.patch_series_id
            LEFT JOIN mailing_lists ml
              ON ml.id = psl.mailing_list_id
            WHERE pi.id = ANY($1)
            GROUP BY pi.id, psv.patch_series_id, psv.id, psv.version_num, pi.ordinal,
                     psv.is_rfc, ps.author_email, psv.sent_at, pi.subject_raw, pi.commit_subject,
                     pi.has_diff, m.message_id_primary, mb.diff_text
            ORDER BY pi.id ASC"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let subject = row.subject;
                let (date_utc, date_ts, year, month) = split_date(Some(row.sent_at));
                let snippet = if let Some(commit_subject) = row.commit_subject.as_deref() {
                    snippet_from_text(Some(commit_subject), 240)
                } else {
                    snippet_from_text(Some(subject.as_str()), 240)
                };
                json!({
                    "id": row.id,
                    "scope": "patch_item",
                    "title": subject.clone(),
                    "subject": subject,
                    "snippet": snippet,
                    "author_email": row.author_email,
                    "date_utc": date_utc,
                    "date_ts": date_ts,
                    "year": year,
                    "month": month,
                    "list_keys": row.list_keys,
                    "has_diff": row.has_diff,
                    "patch_series_id": row.patch_series_id,
                    "series_version_id": row.series_version_id,
                    "version_num": row.version_num,
                    "ordinal": row.ordinal,
                    "is_rfc": row.is_rfc,
                    "file_paths": row.file_paths,
                    "commit_subject": row.commit_subject,
                    "diff_search": limit_text(row.diff_text.as_deref().unwrap_or(""), 60_000),
                    "message_id_primary": row.message_id_primary,
                    "route": format!("/diff/{}", row.id),
                })
            })
            .collect())
    }

    pub async fn build_patch_series_docs(&self, ids: &[i64]) -> Result<Vec<serde_json::Value>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, PatchSeriesDocRow>(
            r#"SELECT
                ps.id,
                ps.canonical_subject_norm AS canonical_subject,
                ps.author_email,
                ps.last_seen_at,
                psv.version_num AS latest_version_num,
                psv.id AS latest_version_id,
                psv.is_rfc AS latest_version_is_rfc,
                mb.body_text AS cover_body,
                BOOL_OR(COALESCE(pi.has_diff, false)) AS has_diff,
                COALESCE(
                    ARRAY_REMOVE(
                        ARRAY_AGG(DISTINCT pi.subject_raw) FILTER (WHERE pi.item_type = 'patch'),
                        NULL
                    ),
                    ARRAY[]::text[]
                ) AS patch_subjects,
                COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT ml.list_key), NULL), ARRAY[]::text[]) AS list_keys
            FROM patch_series ps
            LEFT JOIN patch_series_versions psv
              ON psv.id = ps.latest_version_id
            LEFT JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            LEFT JOIN messages m
              ON m.id = psv.cover_message_pk
            LEFT JOIN message_bodies mb
              ON mb.id = m.body_id
            LEFT JOIN patch_series_lists psl
              ON psl.patch_series_id = ps.id
            LEFT JOIN mailing_lists ml
              ON ml.id = psl.mailing_list_id
            WHERE ps.id = ANY($1)
            GROUP BY ps.id, ps.canonical_subject_norm, ps.author_email, ps.last_seen_at,
                     psv.version_num, psv.id, psv.is_rfc, mb.body_text
            ORDER BY ps.id ASC"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let canonical_subject = row.canonical_subject;
                let (date_utc, date_ts, year, month) = split_date(Some(row.last_seen_at));
                let cover_abstract = limit_text(row.cover_body.as_deref().unwrap_or(""), 4_000);
                let patch_subjects_joined = row.patch_subjects.join(" ");
                let snippet = if !cover_abstract.is_empty() {
                    snippet_from_text(Some(cover_abstract.as_str()), 320)
                } else {
                    snippet_from_text(Some(canonical_subject.as_str()), 320)
                };
                json!({
                    "id": row.id,
                    "scope": "series",
                    "title": canonical_subject.clone(),
                    "canonical_subject": canonical_subject,
                    "snippet": snippet,
                    "author_email": row.author_email,
                    "date_utc": date_utc,
                    "date_ts": date_ts,
                    "year": year,
                    "month": month,
                    "list_keys": row.list_keys,
                    "has_diff": row.has_diff.unwrap_or(false),
                    "latest_version_num": row.latest_version_num.unwrap_or(1),
                    "latest_version_id": row.latest_version_id,
                    "is_rfc_latest": row.latest_version_is_rfc.unwrap_or(false),
                    "cover_abstract": cover_abstract,
                    "patch_subjects_joined": limit_text(&patch_subjects_joined, 20_000),
                    "route": format!("/series/{}", row.id),
                })
            })
            .collect())
    }

    pub async fn build_thread_docs(&self, ids: &[i64]) -> Result<Vec<serde_json::Value>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, ThreadDocRow>(
            r#"WITH targets AS (
                SELECT
                    t.id,
                    t.mailing_list_id,
                    t.subject_norm,
                    t.created_at,
                    t.root_message_pk,
                    t.message_count,
                    t.last_activity_at
                FROM threads t
                WHERE t.id = ANY($1)
            ),
            starter AS (
                SELECT
                    t.id AS thread_id,
                    m.from_name AS starter_name,
                    m.from_email AS starter_email
                FROM targets t
                LEFT JOIN messages m
                  ON m.id = t.root_message_pk
            ),
            participants AS (
                SELECT
                    tm.thread_id,
                    COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT m.from_email), NULL), ARRAY[]::text[]) AS participant_emails
                FROM thread_messages tm
                JOIN targets t
                  ON t.id = tm.thread_id
                 AND t.mailing_list_id = tm.mailing_list_id
                JOIN messages m
                  ON m.id = tm.message_pk
                GROUP BY tm.thread_id
            ),
            snippets AS (
                SELECT
                    tm.thread_id,
                    STRING_AGG(nexus_safe_prefix(mb.search_text, 400), ' ' ORDER BY tm.sort_key) AS snippet_corpus,
                    BOOL_OR(mb.has_diff) AS has_diff
                FROM thread_messages tm
                JOIN targets t
                  ON t.id = tm.thread_id
                 AND t.mailing_list_id = tm.mailing_list_id
                JOIN messages m
                  ON m.id = tm.message_pk
                JOIN message_bodies mb
                  ON mb.id = m.body_id
                GROUP BY tm.thread_id
            )
            SELECT
                t.id,
                ml.list_key,
                t.subject_norm AS subject,
                t.created_at,
                t.last_activity_at,
                t.message_count,
                st.starter_name,
                st.starter_email,
                COALESCE(p.participant_emails, ARRAY[]::text[]) AS participants,
                s.snippet_corpus,
                s.has_diff
            FROM targets t
            JOIN mailing_lists ml
              ON ml.id = t.mailing_list_id
            LEFT JOIN starter st
              ON st.thread_id = t.id
            LEFT JOIN participants p
              ON p.thread_id = t.id
            LEFT JOIN snippets s
              ON s.thread_id = t.id
            ORDER BY t.id ASC"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let subject = row.subject;
                let list_key = row.list_key;
                let participants = row.participants;
                let (date_utc, date_ts, year, month) = split_date(Some(row.last_activity_at));
                let participants_joined = participants.join(" ");
                let snippet_corpus =
                    limit_text(row.snippet_corpus.as_deref().unwrap_or(""), 24_000);
                let snippet = snippet_from_text(Some(snippet_corpus.as_str()), 320);
                json!({
                    "id": row.id,
                    "scope": "thread",
                    "title": subject.clone(),
                    "subject": subject,
                    "snippet": snippet,
                    "date_utc": date_utc,
                    "date_ts": date_ts,
                    "year": year,
                    "month": month,
                    "created_at": row.created_at.to_rfc3339(),
                    "last_activity_at": row.last_activity_at.to_rfc3339(),
                    "message_count": row.message_count,
                    "starter_name": row.starter_name,
                    "starter_email": row.starter_email,
                    "list_key": list_key.clone(),
                    "list_keys": vec![list_key.clone()],
                    "participants": participants.clone(),
                    "author_emails": participants,
                    "participants_joined": participants_joined,
                    "snippet_corpus": snippet_corpus,
                    "has_diff": row.has_diff.unwrap_or(false),
                    "route": format!("/lists/{list_key}/threads/{}", row.id),
                })
            })
            .collect())
    }
}

fn split_date(
    value: Option<DateTime<Utc>>,
) -> (Option<String>, Option<i64>, Option<i32>, Option<i32>) {
    if let Some(value) = value {
        (
            Some(value.to_rfc3339()),
            Some(value.timestamp()),
            Some(value.year()),
            Some(value.month() as i32),
        )
    } else {
        (None, None, None, None)
    }
}

fn limit_text(raw: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut out = String::new();
    for ch in raw.chars().take(max_chars) {
        out.push(ch);
    }
    out
}

fn snippet_from_text(raw: Option<&str>, max_chars: usize) -> Option<String> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    if collapsed.chars().count() <= max_chars {
        return Some(collapsed);
    }
    Some(format!(
        "{}...",
        collapsed.chars().take(max_chars).collect::<String>()
    ))
}
