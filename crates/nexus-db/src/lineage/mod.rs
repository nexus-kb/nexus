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
}

mod logical;
mod read_messages;
mod read_series;
mod source;
mod upsert_files;
mod upsert_series_items;
