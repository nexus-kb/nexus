use chrono::{DateTime, Utc};
use nexus_core::search::MeiliIndexKind;
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
pub struct PipelineStageIngestPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStageThreadingPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStageLineageDiffPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStageSearchPayload {
    pub run_id: i64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeiliUpsertBatchPayload {
    pub index: MeiliIndexKind,
    pub ids: Vec<i64>,
    pub source_job_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingScope {
    Thread,
    Series,
}

impl EmbeddingScope {
    pub fn as_str(self) -> &'static str {
        match self {
            EmbeddingScope::Thread => "thread",
            EmbeddingScope::Series => "series",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingBackfillRunPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingGenerateBatchPayload {
    pub scope: EmbeddingScope,
    pub list_key: Option<String>,
    pub ids: Vec<i64>,
    pub model_key: String,
    pub source_job_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{EmbeddingGenerateBatchPayload, EmbeddingScope, MeiliUpsertBatchPayload};
    use nexus_core::search::MeiliIndexKind;

    #[test]
    fn meili_payload_serializes_index_kind_as_snake_case() {
        let payload = MeiliUpsertBatchPayload {
            index: MeiliIndexKind::ThreadDocs,
            ids: vec![1, 2, 3],
            source_job_id: Some(77),
        };
        let encoded = serde_json::to_string(&payload).expect("serialize payload");
        assert!(encoded.contains("\"thread_docs\""));
    }

    #[test]
    fn embedding_scope_serializes_as_snake_case() {
        let payload = EmbeddingGenerateBatchPayload {
            scope: EmbeddingScope::Thread,
            list_key: Some("lkml".to_string()),
            ids: vec![11, 22],
            model_key: "qwen/qwen3-embedding-4b".to_string(),
            source_job_id: Some(91),
        };
        let encoded = serde_json::to_string(&payload).expect("serialize embedding payload");
        assert!(encoded.contains("\"scope\":\"thread\""));
    }
}
