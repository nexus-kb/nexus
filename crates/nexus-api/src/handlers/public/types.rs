use super::*;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct PageInfoResponse {
    pub limit: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CursorQuery {
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListSummaryResponse {
    pub list_key: String,
    pub description: Option<String>,
    pub posting_address: Option<String>,
    pub latest_activity_at: Option<DateTime<Utc>>,
    pub thread_count_30d: i64,
    pub message_count_30d: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListCatalogResponse {
    pub items: Vec<ListSummaryResponse>,
    pub page_info: PageInfoResponse,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListMirrorStateResponse {
    pub active_repos: i64,
    pub total_repos: i64,
    pub latest_repo_watermark_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListCountsResponse {
    pub messages: i64,
    pub threads: i64,
    pub patch_series: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListFacetsHintResponse {
    pub default_scope: String,
    pub available_scopes: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListDetailResponse {
    pub list_key: String,
    pub description: Option<String>,
    pub posting_address: Option<String>,
    pub mirror_state: ListMirrorStateResponse,
    pub counts: ListCountsResponse,
    pub facets_hint: ListFacetsHintResponse,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListStatsQuery {
    #[serde(default)]
    pub window: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListTopAuthorResponse {
    pub from_email: String,
    pub from_name: Option<String>,
    pub message_count: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListActivityByDayResponse {
    pub day_utc: DateTime<Utc>,
    pub messages: i64,
    pub threads: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListStatsResponse {
    pub messages: i64,
    pub threads: i64,
    pub patch_series: i64,
    pub top_authors: Vec<ListTopAuthorResponse>,
    pub activity_by_day: Vec<ListActivityByDayResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct PatchItemFilesResponse {
    pub items: Vec<PatchItemFileResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PatchItemDiffResponse {
    pub patch_item_id: i64,
    pub diff_text: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PatchItemFileDiffResponse {
    pub patch_item_id: i64,
    pub path: String,
    pub diff_text: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PatchItemFileDiffPath {
    pub patch_item_id: i64,
    pub path: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MessageBodyQuery {
    #[serde(default)]
    pub include_diff: Option<bool>,
    #[serde(default)]
    pub strip_quotes: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MessageBodyResponse {
    pub message_id: i64,
    pub subject: String,
    pub body_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_text: Option<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct MessageIdPath {
    pub msgid: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ThreadMessageParticipant {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct ThreadDetailResponse {
    pub thread_id: i64,
    pub list_key: String,
    pub subject: String,
    pub membership_hash: String,
    pub last_activity_at: DateTime<Utc>,
    pub messages: Vec<ThreadMessageEntry>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ThreadListItemResponse {
    pub thread_id: i64,
    pub subject: String,
    pub root_message_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: i32,
    pub participants: Vec<ThreadMessageParticipant>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starter: Option<ThreadMessageParticipant>,
    pub has_diff: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ThreadListResponse {
    pub items: Vec<ThreadListItemResponse>,
    pub page_info: PageInfoResponse,
}

#[derive(Debug, Deserialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct ThreadMessagesQuery {
    #[serde(default)]
    pub view: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ThreadMessagesResponse {
    pub thread_id: i64,
    pub list_key: String,
    pub view: String,
    pub messages: Vec<ThreadMessageEntry>,
    pub page_info: PageInfoResponse,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ThreadPath {
    pub list_key: String,
    pub thread_id: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SeriesVersionPath {
    pub series_id: i64,
    pub series_version_id: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SeriesListQuery {
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesListItemResponse {
    pub series_id: i64,
    pub canonical_subject: String,
    pub author_email: String,
    pub author_name: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub latest_patchset_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub latest_version_num: i32,
    pub is_rfc_latest: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesListResponse {
    pub items: Vec<SeriesListItemResponse>,
    pub page_info: PageInfoResponse,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesAuthorResponse {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesThreadRefResponse {
    pub list_key: String,
    pub thread_id: i64,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct SeriesVersionQuery {
    #[serde(default)]
    pub assembled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct SeriesCompareQuery {
    pub v1: i64,
    pub v2: i64,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesCompareSummary {
    pub v1_patch_count: i64,
    pub v2_patch_count: i64,
    pub patch_count_delta: i64,
    pub changed: i64,
    pub added: i64,
    pub removed: i64,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct SeriesCompareFileRow {
    pub path: String,
    pub status: String,
    pub additions_delta: i64,
    pub deletions_delta: i64,
    pub hunks_delta: i64,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub has_diff: Option<bool>,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub hybrid: Option<bool>,
    #[serde(default)]
    pub semantic_ratio: Option<f32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SearchItemResponse {
    pub scope: String,
    pub id: i64,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    pub route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_utc: Option<String>,
    pub list_keys: Vec<String>,
    pub has_diff: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author_email: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResponse {
    pub items: Vec<SearchItemResponse>,
    pub facets: Value,
    pub highlights: BTreeMap<String, Value>,
    pub page_info: PageInfoResponse,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(super) struct ListCatalogCursorToken {
    pub(super) v: u8,
    pub(super) list_key: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(super) struct ThreadListCursorToken {
    pub(super) v: u8,
    pub(super) h: String,
    pub(super) ts: i64,
    pub(super) id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(super) struct ThreadMessagesCursorToken {
    pub(super) v: u8,
    pub(super) h: String,
    pub(super) sort_key: String,
    pub(super) message_id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(super) struct SeriesListCursorToken {
    pub(super) v: u8,
    pub(super) h: String,
    pub(super) ts: i64,
    pub(super) id: i64,
}
