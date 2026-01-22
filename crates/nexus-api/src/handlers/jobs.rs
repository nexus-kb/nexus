use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use nexus_db::{Job, JobStats, JobStore};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::handlers::{default_list_limit, default_queue};
use crate::http::{ApiError, internal_error};
use crate::state::ApiState;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EnqueueRequest {
    #[serde(default = "default_queue")]
    pub queue: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct EnqueueResponse {
    pub id: i64,
    pub queue: String,
    pub status: String,
    pub run_at: DateTime<Utc>,
}

/// Enqueue a job into the background worker queue.
pub async fn enqueue_job(
    State(state): State<ApiState>,
    Json(body): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, ApiError> {
    let store = JobStore::new(state.db.pool().clone());
    let job = store
        .enqueue(&body.queue, body.payload, body.run_at, body.max_attempts)
        .await
        .map_err(internal_error)?;

    Ok(Json(EnqueueResponse {
        id: job.id,
        queue: job.queue,
        status: job.status,
        run_at: job.run_at,
    }))
}

/// Fetch a single job by id.
pub async fn job_status(
    State(state): State<ApiState>,
    Path(id): Path<i64>,
) -> Result<Json<Job>, axum::http::StatusCode> {
    let store = JobStore::new(state.db.pool().clone());
    match store
        .find_by_id(id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(job) => Ok(Json(job)),
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct JobListParams {
    pub status: Option<String>,
    #[serde(default = "default_list_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

/// List jobs with optional filtering.
pub async fn list_jobs(
    State(state): State<ApiState>,
    Query(params): Query<JobListParams>,
) -> Result<Json<Vec<Job>>, ApiError> {
    let limit = params.limit.clamp(1, 500);
    let offset = params.offset.max(0);
    let store = JobStore::new(state.db.pool().clone());
    let jobs = store
        .list(params.status.as_deref(), limit, offset)
        .await
        .map_err(internal_error)?;
    Ok(Json(jobs))
}

/// Aggregate job statistics.
pub async fn job_stats(State(state): State<ApiState>) -> Result<Json<JobStats>, ApiError> {
    let store = JobStore::new(state.db.pool().clone());
    let stats = store.stats().await.map_err(internal_error)?;
    Ok(Json(stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_list_params_defaults() {
        let params: JobListParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.status, None);
        assert_eq!(params.limit, 100);
        assert_eq!(params.offset, 0);
    }
}
