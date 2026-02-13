use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
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

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
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
