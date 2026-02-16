use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Pipeline stage payloads (4 - the core pipeline) ────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineIngestPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineThreadingPayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineLineagePayload {
    pub run_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSearchPayload {
    pub run_id: i64,
}

// ── Admin/maintenance payloads (4 - kept) ──────────────────────

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
    use super::{EmbeddingGenerateBatchPayload, EmbeddingScope};

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
