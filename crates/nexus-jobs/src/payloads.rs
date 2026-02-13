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
