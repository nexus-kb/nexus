use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use nexus_db::{
    ListThreadsParams, PatchItemDetailRecord, SeriesExportMessageRecord, SeriesLogicalCompareRow,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::state::ApiState;

const CACHE_THREAD: &str = "public, max-age=300, stale-while-revalidate=86400";
const CACHE_LONG: &str = "public, max-age=86400, stale-while-revalidate=604800";

#[derive(Debug, Serialize)]
pub struct PatchItemResponse {
    pub patch_item_id: i64,
    pub series_id: i64,
    pub series_version_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub item_type: String,
    pub subject: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub message_id: i64,
    pub message_id_primary: String,
    pub patch_id_stable: Option<String>,
    pub has_diff: bool,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunks: i32,
}

#[derive(Debug, Serialize)]
pub struct PatchItemFileResponse {
    pub patch_item_id: i64,
    pub path: String,
    pub old_path: Option<String>,
    pub change_type: String,
    pub is_binary: bool,
    pub additions: i32,
    pub deletions: i32,
    pub hunks: i32,
    pub diff_start: i32,
    pub diff_end: i32,
}

#[derive(Debug, Serialize)]
pub struct PatchItemDiffResponse {
    pub patch_item_id: i64,
    pub diff_text: String,
}

#[derive(Debug, Serialize)]
pub struct PatchItemFileDiffResponse {
    pub patch_item_id: i64,
    pub path: String,
    pub diff_text: String,
}

#[derive(Debug, Deserialize)]
pub struct PatchItemFileDiffPath {
    pub patch_item_id: i64,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct MessageBodyQuery {
    #[serde(default)]
    pub include_diff: Option<bool>,
    #[serde(default)]
    pub strip_quotes: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct MessageBodyResponse {
    pub message_id: i64,
    pub subject: String,
    pub body_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_text: Option<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub message_id: i64,
    pub message_id_primary: String,
    pub subject: String,
    pub subject_norm: String,
    pub from: ThreadMessageParticipant,
    pub date_utc: Option<DateTime<Utc>>,
    pub to_raw: Option<String>,
    pub cc_raw: Option<String>,
    pub references: Vec<String>,
    pub in_reply_to: Vec<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Deserialize)]
pub struct MessageIdPath {
    pub msgid: String,
}

#[derive(Debug, Serialize)]
pub struct ThreadMessageParticipant {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct ThreadMessageEntry {
    pub message_id: i64,
    pub parent_message_id: Option<i64>,
    pub depth: i32,
    pub sort_key: String,
    pub from: ThreadMessageParticipant,
    pub date_utc: Option<DateTime<Utc>>,
    pub subject: String,
    pub has_diff: bool,
    pub patch_item_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_text: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ThreadDetailResponse {
    pub thread_id: i64,
    pub list_key: String,
    pub subject: String,
    pub membership_hash: String,
    pub last_activity_at: DateTime<Utc>,
    pub messages: Vec<ThreadMessageEntry>,
}

#[derive(Debug, Serialize)]
pub struct ThreadListItemResponse {
    pub thread_id: i64,
    pub subject: String,
    pub root_message_id: Option<i64>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: i32,
    pub participants: Vec<ThreadMessageParticipant>,
    pub has_diff: bool,
}

#[derive(Debug, Serialize)]
pub struct ThreadListResponse {
    pub items: Vec<ThreadListItemResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ThreadListQuery {
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub has_diff: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ThreadMessagesQuery {
    #[serde(default)]
    pub view: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ThreadMessagesResponse {
    pub thread_id: i64,
    pub list_key: String,
    pub view: String,
    pub messages: Vec<ThreadMessageEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ThreadPath {
    pub list_key: String,
    pub thread_id: i64,
}

#[derive(Debug, Deserialize)]
pub struct SeriesVersionPath {
    pub series_id: i64,
    pub series_version_id: i64,
}

#[derive(Debug, Deserialize)]
pub struct ExportMboxQuery {
    #[serde(default)]
    pub assembled: Option<bool>,
    #[serde(default)]
    pub include_cover: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SeriesListQuery {
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SeriesListItemResponse {
    pub series_id: i64,
    pub canonical_subject: String,
    pub author_email: String,
    pub last_seen_at: DateTime<Utc>,
    pub latest_version_num: i32,
    pub is_rfc_latest: bool,
}

#[derive(Debug, Serialize)]
pub struct SeriesListResponse {
    pub items: Vec<SeriesListItemResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SeriesAuthorResponse {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct SeriesThreadRefResponse {
    pub list_key: String,
    pub thread_id: i64,
}

#[derive(Debug, Serialize)]
pub struct SeriesVersionSummaryResponse {
    pub series_version_id: i64,
    pub version_num: i32,
    pub is_rfc: bool,
    pub is_resend: bool,
    pub sent_at: DateTime<Utc>,
    pub cover_message_id: Option<i64>,
    pub thread_refs: Vec<SeriesThreadRefResponse>,
    pub patch_count: i64,
    pub is_partial_reroll: bool,
}

#[derive(Debug, Serialize)]
pub struct SeriesDetailResponse {
    pub series_id: i64,
    pub canonical_subject: String,
    pub author: SeriesAuthorResponse,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub lists: Vec<String>,
    pub versions: Vec<SeriesVersionSummaryResponse>,
    pub latest_version_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SeriesVersionQuery {
    #[serde(default)]
    pub assembled: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct SeriesVersionPatchItemResponse {
    pub patch_item_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub item_type: String,
    pub subject: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub message_id: i64,
    pub message_id_primary: String,
    pub patch_id_stable: Option<String>,
    pub has_diff: bool,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunks: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_from_version_num: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct SeriesVersionResponse {
    pub series_id: i64,
    pub series_version_id: i64,
    pub version_num: i32,
    pub is_rfc: bool,
    pub is_resend: bool,
    pub is_partial_reroll: bool,
    pub sent_at: DateTime<Utc>,
    pub subject: String,
    pub subject_norm: String,
    pub cover_message_id: Option<i64>,
    pub first_patch_message_id: Option<i64>,
    pub assembled: bool,
    pub patch_items: Vec<SeriesVersionPatchItemResponse>,
}

#[derive(Debug, Deserialize)]
pub struct SeriesCompareQuery {
    pub v1: i64,
    pub v2: i64,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SeriesCompareSummary {
    pub v1_patch_count: i64,
    pub v2_patch_count: i64,
    pub patch_count_delta: i64,
    pub changed: i64,
    pub added: i64,
    pub removed: i64,
}

#[derive(Debug, Serialize)]
pub struct SeriesComparePatchRow {
    pub slot: i32,
    pub title_norm: String,
    pub status: String,
    pub v1_patch_item_id: Option<i64>,
    pub v1_patch_id_stable: Option<String>,
    pub v1_subject: Option<String>,
    pub v2_patch_item_id: Option<i64>,
    pub v2_patch_id_stable: Option<String>,
    pub v2_subject: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SeriesCompareFileRow {
    pub path: String,
    pub status: String,
    pub additions_delta: i64,
    pub deletions_delta: i64,
    pub hunks_delta: i64,
}

#[derive(Debug, Serialize)]
pub struct SeriesCompareResponse {
    pub series_id: i64,
    pub v1: i64,
    pub v2: i64,
    pub mode: String,
    pub summary: SeriesCompareSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patches: Option<Vec<SeriesComparePatchRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<SeriesCompareFileRow>>,
}

pub async fn patch_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(item) = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(&headers, &map_patch_item(item), CACHE_LONG, None)
}

pub async fn patch_item_files(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let exists = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }

    let files = state
        .lineage
        .list_patch_item_files(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|file| PatchItemFileResponse {
            patch_item_id: file.patch_item_id,
            path: file.new_path,
            old_path: file.old_path,
            change_type: file.change_type,
            is_binary: file.is_binary,
            additions: file.additions,
            deletions: file.deletions,
            hunks: file.hunk_count,
            diff_start: file.diff_start,
            diff_end: file.diff_end,
        })
        .collect::<Vec<_>>();

    json_response_with_cache(&headers, &files, CACHE_LONG, None)
}

pub async fn patch_item_file_diff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<PatchItemFileDiffPath>,
) -> Result<Response, StatusCode> {
    let Some(source) = state
        .lineage
        .get_patch_item_file_diff_source(path.patch_item_id, &path.path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let diff_text = source.diff_text.ok_or(StatusCode::NOT_FOUND)?;
    let sliced = slice_by_offsets(&diff_text, source.diff_start, source.diff_end)
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;

    json_response_with_cache(
        &headers,
        &PatchItemFileDiffResponse {
            patch_item_id: path.patch_item_id,
            path: source.new_path,
            diff_text: sliced,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn patch_item_diff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(diff_text) = state
        .lineage
        .get_patch_item_full_diff(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(
        &headers,
        &PatchItemDiffResponse {
            patch_item_id,
            diff_text,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn message_body(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(message_id): Path<i64>,
    Query(query): Query<MessageBodyQuery>,
) -> Result<Response, StatusCode> {
    let include_diff = query.include_diff.unwrap_or(false);
    let strip_quotes = query.strip_quotes.unwrap_or(false);

    let Some(record) = state
        .lineage
        .get_message_body(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let body_text = record
        .body_text
        .as_deref()
        .map(|body| strip_quoted_lines(body, strip_quotes));

    json_response_with_cache(
        &headers,
        &MessageBodyResponse {
            message_id: record.message_pk,
            subject: record.subject_raw,
            body_text,
            diff_text: if include_diff { record.diff_text } else { None },
            has_diff: record.has_diff,
            has_attachments: record.has_attachments,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn message_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(message_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(record) = state
        .lineage
        .get_message_detail(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(
        &headers,
        &MessageResponse {
            message_id: record.message_pk,
            message_id_primary: record.message_id_primary,
            subject: record.subject_raw,
            subject_norm: record.subject_norm,
            from: ThreadMessageParticipant {
                name: record.from_name,
                email: record.from_email,
            },
            date_utc: record.date_utc,
            to_raw: record.to_raw,
            cc_raw: record.cc_raw,
            references: record.references_ids,
            in_reply_to: record.in_reply_to_ids,
            has_diff: record.has_diff,
            has_attachments: record.has_attachments,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn message_raw(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(message_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(raw_rfc822) = state
        .lineage
        .get_message_raw_rfc822(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    bytes_response_with_cache(&headers, raw_rfc822, "message/rfc822", CACHE_LONG, None)
}

pub async fn message_id_redirect(
    State(state): State<ApiState>,
    Path(path): Path<MessageIdPath>,
) -> Result<Response, StatusCode> {
    let mut candidates = vec![path.msgid.clone()];
    if !path.msgid.starts_with('<') && !path.msgid.ends_with('>') {
        candidates.push(format!("<{}>", path.msgid));
    }

    for candidate in candidates {
        if let Some(message_pk) = state
            .lineage
            .resolve_message_pk_by_message_id(&candidate)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            return redirect_response(&format!("/api/v1/messages/{message_pk}"));
        }
    }

    Err(StatusCode::NOT_FOUND)
}

pub async fn list_threads(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(list_key): Path<String>,
    Query(query): Query<ThreadListQuery>,
) -> Result<Response, StatusCode> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let sort = query.sort.as_deref().unwrap_or("activity_desc");
    if sort != "activity_desc" && sort != "date_desc" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let cursor = match query.cursor.as_deref() {
        Some(raw) => Some(decode_time_cursor(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };
    let from_ts = match query.from.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };
    let to_ts = match query.to.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };

    let items = state
        .lineage
        .list_threads(
            list.id,
            &ListThreadsParams {
                sort: sort.to_string(),
                from_ts,
                to_ts,
                author_email: query.author.clone(),
                has_diff: query.has_diff,
                cursor,
                limit,
            },
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let thread_ids = items.iter().map(|item| item.thread_id).collect::<Vec<_>>();
    let mut participants_by_thread: HashMap<i64, Vec<ThreadMessageParticipant>> = HashMap::new();
    for participant in state
        .lineage
        .list_thread_participants(list.id, &thread_ids)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let entries = participants_by_thread
            .entry(participant.thread_id)
            .or_default();
        if entries.len() < 8 {
            entries.push(ThreadMessageParticipant {
                name: participant.from_name,
                email: participant.from_email,
            });
        }
    }

    let next_cursor = if items.len() as i64 == limit {
        items.last().map(|last| {
            let ts = if sort == "date_desc" {
                last.created_at
            } else {
                last.last_activity_at
            };
            encode_time_cursor(ts, last.thread_id)
        })
    } else {
        None
    };

    let response = ThreadListResponse {
        items: items
            .into_iter()
            .map(|item| ThreadListItemResponse {
                thread_id: item.thread_id,
                subject: item.subject_norm,
                root_message_id: item.root_message_pk,
                last_activity_at: item.last_activity_at,
                message_count: item.message_count,
                participants: participants_by_thread
                    .remove(&item.thread_id)
                    .unwrap_or_default(),
                has_diff: item.has_diff,
            })
            .collect(),
        next_cursor,
    };

    json_response_with_cache(&headers, &response, CACHE_THREAD, None)
}

pub async fn list_thread_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<ThreadPath>,
) -> Result<Response, StatusCode> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&path.list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let Some(summary) = state
        .lineage
        .get_thread_summary(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let messages = state
        .lineage
        .list_thread_messages(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|msg| ThreadMessageEntry {
            message_id: msg.message_pk,
            parent_message_id: msg.parent_message_pk,
            depth: msg.depth,
            sort_key: hex_encode(&msg.sort_key),
            from: ThreadMessageParticipant {
                name: msg.from_name,
                email: msg.from_email,
            },
            date_utc: msg.date_utc,
            subject: msg.subject_raw,
            has_diff: msg.has_diff,
            patch_item_id: msg.patch_item_id,
            snippet: build_snippet(msg.body_text.as_deref()),
            body_text: None,
        })
        .collect::<Vec<_>>();

    let etag = format!("\"{}\"", hex_encode(&summary.membership_hash));
    json_response_with_cache(
        &headers,
        &ThreadDetailResponse {
            thread_id: path.thread_id,
            list_key: path.list_key,
            subject: summary.subject_norm,
            membership_hash: hex_encode(&summary.membership_hash),
            last_activity_at: summary.last_activity_at,
            messages,
        },
        CACHE_THREAD,
        Some(etag),
    )
}

pub async fn thread_messages(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<ThreadPath>,
    Query(query): Query<ThreadMessagesQuery>,
) -> Result<Response, StatusCode> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&path.list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let view = query.view.as_deref().unwrap_or("snippets");
    let include_body = match view {
        "full" => true,
        "snippets" => false,
        _ => return Err(StatusCode::UNPROCESSABLE_ENTITY),
    };

    let exists = state
        .lineage
        .get_thread_summary(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }

    let messages = state
        .lineage
        .list_thread_messages(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|msg| ThreadMessageEntry {
            message_id: msg.message_pk,
            parent_message_id: msg.parent_message_pk,
            depth: msg.depth,
            sort_key: hex_encode(&msg.sort_key),
            from: ThreadMessageParticipant {
                name: msg.from_name,
                email: msg.from_email,
            },
            date_utc: msg.date_utc,
            subject: msg.subject_raw,
            has_diff: msg.has_diff,
            patch_item_id: msg.patch_item_id,
            snippet: if include_body {
                None
            } else {
                build_snippet(msg.body_text.as_deref())
            },
            body_text: if include_body { msg.body_text } else { None },
        })
        .collect::<Vec<_>>();

    json_response_with_cache(
        &headers,
        &ThreadMessagesResponse {
            thread_id: path.thread_id,
            list_key: path.list_key,
            view: view.to_string(),
            messages,
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_list(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SeriesListQuery>,
) -> Result<Response, StatusCode> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let cursor = match query.cursor.as_deref() {
        Some(raw) => Some(decode_time_cursor(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };

    let items = state
        .lineage
        .list_series(query.list_key.as_deref(), cursor, limit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let next_cursor = if items.len() as i64 == limit {
        items
            .last()
            .map(|last| encode_time_cursor(last.last_seen_at, last.series_id))
    } else {
        None
    };

    json_response_with_cache(
        &headers,
        &SeriesListResponse {
            items: items
                .into_iter()
                .map(|item| SeriesListItemResponse {
                    series_id: item.series_id,
                    canonical_subject: item.canonical_subject_norm,
                    author_email: item.author_email,
                    last_seen_at: item.last_seen_at,
                    latest_version_num: item.latest_version_num,
                    is_rfc_latest: item.is_rfc_latest,
                })
                .collect(),
            next_cursor,
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(series_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(series) = state
        .lineage
        .get_series_by_id(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let lists = state
        .lineage
        .list_series_list_keys(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let versions = state
        .lineage
        .list_series_versions_with_counts(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut version_items = Vec::with_capacity(versions.len());
    for version in versions {
        let thread_refs = match version.thread_id {
            Some(thread_id) => state
                .lineage
                .get_thread_ref(thread_id)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .map(|thread| {
                    vec![SeriesThreadRefResponse {
                        list_key: thread.list_key,
                        thread_id: thread.thread_id,
                    }]
                })
                .unwrap_or_default(),
            None => Vec::new(),
        };

        version_items.push(SeriesVersionSummaryResponse {
            series_version_id: version.id,
            version_num: version.version_num,
            is_rfc: version.is_rfc,
            is_resend: version.is_resend,
            sent_at: version.sent_at,
            cover_message_id: version.cover_message_pk,
            thread_refs,
            patch_count: version.patch_count,
            is_partial_reroll: version.is_partial_reroll,
        });
    }

    json_response_with_cache(
        &headers,
        &SeriesDetailResponse {
            series_id: series.id,
            canonical_subject: series.canonical_subject_norm,
            author: SeriesAuthorResponse {
                name: series.author_name,
                email: series.author_email,
            },
            first_seen_at: series.created_at,
            last_seen_at: series.last_seen_at,
            lists,
            versions: version_items,
            latest_version_id: series.latest_version_id,
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_version(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<SeriesVersionPath>,
    Query(query): Query<SeriesVersionQuery>,
) -> Result<Response, StatusCode> {
    let assembled = query.assembled.unwrap_or(true);
    let Some(version) = state
        .lineage
        .get_series_version(path.series_id, path.series_version_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let patch_items = state
        .lineage
        .list_series_version_patch_items(version.id, assembled)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|item| SeriesVersionPatchItemResponse {
            patch_item_id: item.patch_item_id,
            ordinal: item.ordinal,
            total: item.total,
            item_type: item.item_type,
            subject: item.subject_raw,
            subject_norm: item.subject_norm,
            commit_subject: item.commit_subject,
            commit_subject_norm: item.commit_subject_norm,
            message_id: item.message_pk,
            message_id_primary: item.message_id_primary,
            patch_id_stable: item.patch_id_stable,
            has_diff: item.has_diff,
            file_count: item.file_count,
            additions: item.additions,
            deletions: item.deletions,
            hunks: item.hunk_count,
            inherited_from_version_num: item.inherited_from_version_num,
        })
        .collect::<Vec<_>>();

    json_response_with_cache(
        &headers,
        &SeriesVersionResponse {
            series_id: version.patch_series_id,
            series_version_id: version.id,
            version_num: version.version_num,
            is_rfc: version.is_rfc,
            is_resend: version.is_resend,
            is_partial_reroll: version.is_partial_reroll,
            sent_at: version.sent_at,
            subject: version.subject_raw,
            subject_norm: version.subject_norm,
            cover_message_id: version.cover_message_pk,
            first_patch_message_id: version.first_patch_message_pk,
            assembled,
            patch_items,
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_compare(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(series_id): Path<i64>,
    Query(query): Query<SeriesCompareQuery>,
) -> Result<Response, StatusCode> {
    let mode = query.mode.as_deref().unwrap_or("summary");
    if mode != "summary" && mode != "per_patch" && mode != "per_file" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    let Some(v1) = state
        .lineage
        .get_series_version(series_id, query.v1)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };
    let Some(v2) = state
        .lineage
        .get_series_version(series_id, query.v2)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let logical_rows = state
        .lineage
        .list_series_logical_compare(series_id, v1.version_num, v2.version_num)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let patch_rows = to_patch_compare_rows(logical_rows);

    let changed = patch_rows
        .iter()
        .filter(|row| row.status == "changed")
        .count() as i64;
    let added = patch_rows
        .iter()
        .filter(|row| row.status == "added")
        .count() as i64;
    let removed = patch_rows
        .iter()
        .filter(|row| row.status == "removed")
        .count() as i64;
    let v1_patch_count = patch_rows
        .iter()
        .filter(|row| row.v1_patch_item_id.is_some())
        .count() as i64;
    let v2_patch_count = patch_rows
        .iter()
        .filter(|row| row.v2_patch_item_id.is_some())
        .count() as i64;

    let summary = SeriesCompareSummary {
        v1_patch_count,
        v2_patch_count,
        patch_count_delta: v2_patch_count - v1_patch_count,
        changed,
        added,
        removed,
    };

    let files = if mode == "per_file" {
        let v1_ids = patch_rows
            .iter()
            .filter_map(|row| row.v1_patch_item_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let v2_ids = patch_rows
            .iter()
            .filter_map(|row| row.v2_patch_item_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let v1_files = state
            .lineage
            .aggregate_patch_item_files(&v1_ids)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let v2_files = state
            .lineage
            .aggregate_patch_item_files(&v2_ids)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut v1_map = BTreeMap::new();
        for row in v1_files {
            let key = row.path.clone();
            v1_map.insert(key, row);
        }
        let mut v2_map = BTreeMap::new();
        for row in v2_files {
            let key = row.path.clone();
            v2_map.insert(key, row);
        }

        let mut paths = BTreeSet::new();
        for path in v1_map.keys() {
            paths.insert(path.clone());
        }
        for path in v2_map.keys() {
            paths.insert(path.clone());
        }

        let mut rows = Vec::with_capacity(paths.len());
        for path in paths {
            let left = v1_map.get(&path);
            let right = v2_map.get(&path);
            let (status, additions_delta, deletions_delta, hunks_delta) = match (left, right) {
                (None, Some(newer)) => (
                    "added".to_string(),
                    newer.additions,
                    newer.deletions,
                    newer.hunk_count,
                ),
                (Some(older), None) => (
                    "removed".to_string(),
                    -older.additions,
                    -older.deletions,
                    -older.hunk_count,
                ),
                (Some(older), Some(newer)) => {
                    let additions_delta = newer.additions - older.additions;
                    let deletions_delta = newer.deletions - older.deletions;
                    let hunks_delta = newer.hunk_count - older.hunk_count;
                    let status = if additions_delta == 0 && deletions_delta == 0 && hunks_delta == 0
                    {
                        "unchanged".to_string()
                    } else {
                        "changed".to_string()
                    };
                    (status, additions_delta, deletions_delta, hunks_delta)
                }
                (None, None) => continue,
            };

            rows.push(SeriesCompareFileRow {
                path,
                status,
                additions_delta,
                deletions_delta,
                hunks_delta,
            });
        }
        Some(rows)
    } else {
        None
    };

    json_response_with_cache(
        &headers,
        &SeriesCompareResponse {
            series_id,
            v1: query.v1,
            v2: query.v2,
            mode: mode.to_string(),
            summary,
            patches: if mode == "per_patch" {
                Some(patch_rows)
            } else {
                None
            },
            files,
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_version_export_mbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<SeriesVersionPath>,
    Query(query): Query<ExportMboxQuery>,
) -> Result<Response, StatusCode> {
    let assembled = query.assembled.unwrap_or(true);
    let include_cover = query.include_cover.unwrap_or(false);

    let Some(version) = state
        .lineage
        .get_series_version(path.series_id, path.series_version_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let messages = state
        .lineage
        .list_export_messages(path.series_id, version.id, assembled, include_cover)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if messages.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let mbox = render_mbox(&messages);
    let filename = format!(
        "{}-v{}.mbox",
        slugify_for_filename(&version.subject_norm),
        version.version_num
    );
    bytes_response_with_cache(
        &headers,
        mbox,
        "application/mbox",
        CACHE_LONG,
        Some(&format!("attachment; filename=\"{filename}\"")),
    )
}

pub async fn openapi_json(headers: HeaderMap) -> Result<Response, StatusCode> {
    let doc = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Nexus KB API",
            "version": "phase0"
        },
        "paths": {
            "/api/v1/healthz": { "get": { "summary": "Health probe" } },
            "/api/v1/readyz": { "get": { "summary": "Readiness probe" } },
            "/api/v1/version": { "get": { "summary": "Build metadata" } },
            "/api/v1/openapi.json": { "get": { "summary": "OpenAPI contract" } },
            "/api/v1/lists/{list_key}/threads": { "get": { "summary": "List threads for list" } },
            "/api/v1/lists/{list_key}/threads/{thread_id}": { "get": { "summary": "Thread detail" } },
            "/api/v1/lists/{list_key}/threads/{thread_id}/messages": { "get": { "summary": "Thread messages (full|snippets)" } },
            "/api/v1/messages/{message_id}": { "get": { "summary": "Message metadata" } },
            "/api/v1/messages/{message_id}/body": { "get": { "summary": "Message body payload" } },
            "/api/v1/messages/{message_id}/raw": { "get": { "summary": "Raw RFC822" } },
            "/api/v1/r/{msgid}": { "get": { "summary": "Message-ID redirector" } },
            "/api/v1/series": { "get": { "summary": "Series list" } },
            "/api/v1/series/{series_id}": { "get": { "summary": "Series detail timeline" } },
            "/api/v1/series/{series_id}/versions/{series_version_id}": { "get": { "summary": "Series version detail" } },
            "/api/v1/series/{series_id}/compare": { "get": { "summary": "Compare series versions" } },
            "/api/v1/series/{series_id}/versions/{series_version_id}/export/mbox": { "get": { "summary": "Export version mbox" } },
            "/api/v1/patch-items/{patch_item_id}": { "get": { "summary": "Patch item metadata" } },
            "/api/v1/patch-items/{patch_item_id}/files": { "get": { "summary": "Patch item files metadata" } },
            "/api/v1/patch-items/{patch_item_id}/files/{path}/diff": { "get": { "summary": "Patch file diff slice" } },
            "/api/v1/patch-items/{patch_item_id}/diff": { "get": { "summary": "Patch full diff text" } }
        }
    });

    json_response_with_cache(&headers, &doc, CACHE_THREAD, None)
}

fn map_patch_item(item: PatchItemDetailRecord) -> PatchItemResponse {
    PatchItemResponse {
        patch_item_id: item.patch_item_id,
        series_id: item.patch_series_id,
        series_version_id: item.patch_series_version_id,
        ordinal: item.ordinal,
        total: item.total,
        item_type: item.item_type,
        subject: item.subject_raw,
        subject_norm: item.subject_norm,
        commit_subject: item.commit_subject,
        commit_subject_norm: item.commit_subject_norm,
        message_id: item.message_pk,
        message_id_primary: item.message_id_primary,
        patch_id_stable: item.patch_id_stable,
        has_diff: item.has_diff,
        file_count: item.file_count,
        additions: item.additions,
        deletions: item.deletions,
        hunks: item.hunk_count,
    }
}

fn to_patch_compare_rows(rows: Vec<SeriesLogicalCompareRow>) -> Vec<SeriesComparePatchRow> {
    rows.into_iter()
        .filter_map(|row| {
            let v1_is_patch = row.v1_item_type.as_deref() == Some("patch");
            let v2_is_patch = row.v2_item_type.as_deref() == Some("patch");
            if !v1_is_patch && !v2_is_patch {
                return None;
            }
            let status = match (row.v1_patch_item_id, row.v2_patch_item_id) {
                (Some(v1), Some(v2)) if v1 == v2 => "unchanged",
                (Some(_), Some(_)) => "changed",
                (None, Some(_)) => "added",
                (Some(_), None) => "removed",
                (None, None) => return None,
            };
            Some(SeriesComparePatchRow {
                slot: row.slot,
                title_norm: row.title_norm,
                status: status.to_string(),
                v1_patch_item_id: row.v1_patch_item_id,
                v1_patch_id_stable: row.v1_patch_id_stable,
                v1_subject: row.v1_subject_raw,
                v2_patch_item_id: row.v2_patch_item_id,
                v2_patch_id_stable: row.v2_patch_id_stable,
                v2_subject: row.v2_subject_raw,
            })
        })
        .collect()
}

fn json_response_with_cache<T: Serialize>(
    headers: &HeaderMap,
    payload: &T,
    cache_control: &str,
    etag_override: Option<String>,
) -> Result<Response, StatusCode> {
    let body = serde_json::to_vec(payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let etag = etag_override.unwrap_or_else(|| strong_etag(&body));

    if if_none_match_matches(headers, &etag) {
        return not_modified_response(cache_control, &etag);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ETAG, etag)
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn bytes_response_with_cache(
    headers: &HeaderMap,
    body: Vec<u8>,
    content_type: &str,
    cache_control: &str,
    content_disposition: Option<&str>,
) -> Result<Response, StatusCode> {
    let etag = strong_etag(&body);
    if if_none_match_matches(headers, &etag) {
        return not_modified_response(cache_control, &etag);
    }

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ETAG, etag);
    if let Some(content_disposition) = content_disposition {
        builder = builder.header(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(content_disposition)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
    }
    builder
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn not_modified_response(cache_control: &str, etag: &str) -> Result<Response, StatusCode> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ETAG, etag)
        .body(Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let Some(value) = headers.get(header::IF_NONE_MATCH) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };

    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        if candidate == "*" {
            return true;
        }
        let candidate = candidate.strip_prefix("W/").unwrap_or(candidate);
        candidate == etag
    })
}

fn strong_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("\"{:x}\"", hasher.finalize())
}

fn redirect_response(location: &str) -> Result<Response, StatusCode> {
    let location =
        HeaderValue::from_str(location).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .body(Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn encode_time_cursor(ts: DateTime<Utc>, id: i64) -> String {
    format!("{}:{id}", ts.timestamp_millis())
}

fn decode_time_cursor(raw: &str) -> Option<(DateTime<Utc>, i64)> {
    let (left, right) = raw.split_once(':')?;
    let millis = i64::from_str(left).ok()?;
    let id = i64::from_str(right).ok()?;
    let ts = Utc.timestamp_millis_opt(millis).single()?;
    Some((ts, id))
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Some(parsed.with_timezone(&Utc));
    }
    let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?;
    date.and_hms_opt(0, 0, 0)
        .map(|naive| Utc.from_utc_datetime(&naive))
}

fn slice_by_offsets(text: &str, start: i32, end: i32) -> Option<String> {
    if start < 0 || end < 0 || end < start {
        return None;
    }
    let start = start as usize;
    let end = end as usize;
    let bytes = text.as_bytes();
    if start > bytes.len() || end > bytes.len() {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes[start..end]).to_string())
}

fn strip_quoted_lines(body: &str, strip_quotes: bool) -> String {
    if !strip_quotes {
        return body.to_string();
    }

    let mut out = String::new();
    for line in body.lines() {
        if line.trim_start().starts_with('>') {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

fn build_snippet(body: Option<&str>) -> Option<String> {
    let body = body?.trim();
    if body.is_empty() {
        return None;
    }

    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    let char_count = collapsed.chars().count();
    if char_count <= 220 {
        return Some(collapsed);
    }
    Some(format!(
        "{}...",
        collapsed.chars().take(220).collect::<String>()
    ))
}

fn render_mbox(messages: &[SeriesExportMessageRecord]) -> Vec<u8> {
    let mut out = Vec::new();

    for msg in messages {
        let date_part = msg
            .date_utc
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"))
            .format("%a %b %e %H:%M:%S %Y")
            .to_string();
        let separator = format!("From {} {}\n", msg.from_email, date_part);
        out.extend_from_slice(separator.as_bytes());

        let normalized = normalize_newlines(&String::from_utf8_lossy(&msg.raw_rfc822));
        for line in normalized.lines() {
            if line.starts_with("From ") {
                out.extend_from_slice(b">");
            }
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
        out.push(b'\n');
    }

    out
}

fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn slugify_for_filename(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if (ch == '-' || ch == '_' || ch == '.') && !out.ends_with(ch) {
            out.push(ch);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-')
        .chars()
        .take(80)
        .collect::<String>()
        .if_empty_then("series")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

trait IfEmptyThen {
    fn if_empty_then(self, fallback: &str) -> String;
}

impl IfEmptyThen for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use axum::http::{HeaderMap, header};
    use chrono::Utc;
    use nexus_db::{SeriesExportMessageRecord, SeriesLogicalCompareRow};

    use super::{
        decode_time_cursor, encode_time_cursor, if_none_match_matches, parse_timestamp,
        render_mbox, slice_by_offsets, strip_quoted_lines, to_patch_compare_rows,
    };

    #[test]
    fn slices_diff_using_offsets() {
        let diff = concat!(
            "diff --git a/foo.c b/foo.c\n",
            "--- a/foo.c\n",
            "+++ b/foo.c\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n",
        );
        let sliced = slice_by_offsets(diff, 0, diff.len() as i32).expect("slice");
        assert_eq!(sliced, diff);
    }

    #[test]
    fn strips_quoted_lines_when_requested() {
        let body = "hello\n> quote\nworld\n";
        assert_eq!(strip_quoted_lines(body, false), body);
        assert_eq!(strip_quoted_lines(body, true), "hello\nworld");
    }

    #[test]
    fn cursor_round_trip() {
        let ts = Utc::now();
        let cursor = encode_time_cursor(ts, 42);
        let decoded = decode_time_cursor(&cursor).expect("cursor decode");
        assert_eq!(decoded.1, 42);
        assert_eq!(decoded.0.timestamp_millis(), ts.timestamp_millis());
    }

    #[test]
    fn parses_rfc3339_and_date() {
        assert!(parse_timestamp("2026-02-13T12:30:00Z").is_some());
        assert!(parse_timestamp("2026-02-13").is_some());
        assert!(parse_timestamp("nope").is_none());
    }

    #[test]
    fn if_none_match_handles_weak_tags() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, "W/\"abc\"".parse().expect("header"));
        assert!(if_none_match_matches(&headers, "\"abc\""));
    }

    #[test]
    fn patch_compare_filters_non_patch_rows() {
        let rows = vec![
            SeriesLogicalCompareRow {
                slot: 1,
                title_norm: "cover".to_string(),
                v1_patch_item_id: None,
                v1_item_type: Some("cover".to_string()),
                v1_patch_id_stable: None,
                v1_subject_raw: None,
                v2_patch_item_id: Some(9),
                v2_item_type: Some("cover".to_string()),
                v2_patch_id_stable: None,
                v2_subject_raw: Some("[PATCH 0/3] demo".to_string()),
            },
            SeriesLogicalCompareRow {
                slot: 2,
                title_norm: "part two".to_string(),
                v1_patch_item_id: Some(7),
                v1_item_type: Some("patch".to_string()),
                v1_patch_id_stable: Some("old".to_string()),
                v1_subject_raw: Some("[PATCH 2/3] part two".to_string()),
                v2_patch_item_id: Some(10),
                v2_item_type: Some("patch".to_string()),
                v2_patch_id_stable: Some("new".to_string()),
                v2_subject_raw: Some("[PATCH v2 2/3] part two".to_string()),
            },
        ];

        let mapped = to_patch_compare_rows(rows);
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].slot, 2);
        assert_eq!(mapped[0].status, "changed");
    }

    #[test]
    fn rendered_mbox_applies_with_git_am() {
        if Command::new("git").arg("--version").output().is_err() {
            return;
        }

        let temp_root = std::env::temp_dir().join(format!(
            "nexus-mbox-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default().abs()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");

        init_repo_with_patch(&temp_root);
        let raw_patch = run_git(
            &temp_root,
            &["format-patch", "-1", "--stdout"],
            "format patch",
        );

        run_git(
            &temp_root,
            &["reset", "--hard", "HEAD~1"],
            "reset to pre-patch",
        );

        let rendered = render_mbox(&[SeriesExportMessageRecord {
            ordinal: 1,
            item_type: "patch".to_string(),
            message_pk: 1,
            from_email: "dev@example.com".to_string(),
            date_utc: Some(Utc::now()),
            subject_raw: "[PATCH] demo".to_string(),
            raw_rfc822: raw_patch.into_bytes(),
        }]);

        let mbox_path = temp_root.join("series.mbox");
        fs::write(&mbox_path, rendered).expect("write mbox");

        let status = Command::new("git")
            .current_dir(&temp_root)
            .arg("am")
            .arg(&mbox_path)
            .status()
            .expect("run git am");
        assert!(status.success());

        let content = fs::read_to_string(temp_root.join("foo.txt")).expect("read patched file");
        assert_eq!(content.trim(), "new");
    }

    fn init_repo_with_patch(repo: &Path) {
        run_git(repo, &["init"], "git init");
        run_git(
            repo,
            &["config", "user.name", "Nexus Test"],
            "git config user.name",
        );
        run_git(
            repo,
            &["config", "user.email", "dev@example.com"],
            "git config user.email",
        );

        fs::write(repo.join("foo.txt"), "old\n").expect("write file");
        run_git(repo, &["add", "foo.txt"], "git add");
        run_git(repo, &["commit", "-m", "base"], "commit base");

        fs::write(repo.join("foo.txt"), "new\n").expect("write patch file");
        run_git(repo, &["add", "foo.txt"], "git add updated");
        run_git(repo, &["commit", "-m", "update foo"], "commit patch");
    }

    fn run_git(repo: &Path, args: &[&str], context: &str) -> String {
        let output = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("run git command");
        assert!(
            output.status.success(),
            "{context} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    }
}
