use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoScanPayload {
    pub list_key: String,
    pub repo_key: String,
    pub mirror_path: String,
    pub since_commit_oid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestCommitBatchPayload {
    pub list_key: String,
    pub repo_key: String,
    pub chunk_index: u32,
    pub expected_prev_commit_oid: Option<String>,
    pub commit_oids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoIngestRunPayload {
    pub list_key: String,
    pub repo_key: String,
    pub mirror_path: String,
    pub since_commit_oid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadingUpdateWindowPayload {
    pub list_key: String,
    pub anchor_message_pks: Vec<i64>,
    pub source_job_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadingRebuildListPayload {
    pub list_key: String,
    pub from_seen_at: Option<DateTime<Utc>>,
    pub to_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageRebuildListPayload {
    pub list_key: String,
    pub from_seen_at: Option<DateTime<Utc>>,
    pub to_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchExtractWindowPayload {
    pub list_key: String,
    pub anchor_message_pks: Vec<i64>,
    pub source_job_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchIdComputeBatchPayload {
    pub patch_item_ids: Vec<i64>,
    pub source_job_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffParsePatchItemsPayload {
    pub patch_item_ids: Vec<i64>,
    pub source_job_id: Option<i64>,
}
