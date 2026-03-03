use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type, ToSchema)]
#[sqlx(type_name = "job_state", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Scheduled,
    Running,
    Succeeded,
    FailedRetryable,
    FailedTerminal,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct Job {
    pub id: i64,
    pub job_type: String,
    pub state: JobState,
    pub priority: i32,
    pub run_after: DateTime<Utc>,
    pub claimed_by: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub dedupe_key: Option<String>,
    pub dedupe_scope: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub last_error: Option<String>,
    pub last_error_kind: Option<String>,
    pub cancel_requested: bool,
    pub payload_json: serde_json::Value,
    pub result_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct PipelineRun {
    pub id: i64,
    pub mailing_list_id: i64,
    pub list_key: String,
    pub state: String,
    pub current_stage: String,
    pub source: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub ingest_window_from: Option<DateTime<Utc>>,
    pub ingest_window_to: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub progress_json: serde_json::Value,
    pub batch_id: Option<i64>,
    pub batch_position: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct EmbeddingBackfillRun {
    pub id: i64,
    pub scope: String,
    pub list_key: Option<String>,
    pub from_seen_at: Option<DateTime<Utc>>,
    pub to_seen_at: Option<DateTime<Utc>>,
    pub state: String,
    pub model_key: String,
    pub cursor_id: i64,
    pub total_candidates: i64,
    pub processed_count: i64,
    pub embedded_count: i64,
    pub failed_count: i64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub progress_json: serde_json::Value,
    pub result_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct MeiliBootstrapRun {
    pub id: i64,
    pub scope: String,
    pub list_key: Option<String>,
    pub state: String,
    pub embedder_name: String,
    pub model_key: String,
    pub job_id: Option<i64>,
    pub thread_cursor_id: i64,
    pub series_cursor_id: i64,
    pub total_candidates_thread: i64,
    pub total_candidates_series: i64,
    pub processed_thread: i64,
    pub processed_series: i64,
    pub docs_upserted: i64,
    pub vectors_attached: i64,
    pub placeholders_written: i64,
    pub failed_batches: i64,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub progress_json: serde_json::Value,
    pub result_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
