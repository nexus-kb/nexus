use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use chrono::{DateTime, Utc};
use nexus_db::{EnqueueJobParams, Job, JobState, ListJobsParams};
use nexus_jobs::payloads::RepoScanPayload;
use serde::{Deserialize, Serialize};

use crate::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    pub job_type: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub dedupe_scope: Option<String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub run_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct EnqueueResponse {
    pub job_id: i64,
}

pub async fn enqueue_job(
    State(state): State<ApiState>,
    Json(body): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, axum::http::StatusCode> {
    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: body.job_type,
            payload_json: body.payload,
            priority: body.priority.unwrap_or(0),
            dedupe_scope: body.dedupe_scope,
            dedupe_key: body.dedupe_key,
            run_after: body.run_after,
            max_attempts: body.max_attempts,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    #[serde(default)]
    pub state: Option<JobState>,
    #[serde(default)]
    pub job_type: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<i64>,
}

fn default_limit() -> i64 {
    100
}

pub async fn list_jobs(
    State(state): State<ApiState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<Vec<Job>>, axum::http::StatusCode> {
    let jobs = state
        .jobs
        .list(ListJobsParams {
            state: query.state,
            job_type: query.job_type,
            limit: query.limit,
            cursor: query.cursor,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(jobs))
}

pub async fn get_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> Result<Json<Job>, axum::http::StatusCode> {
    match state
        .jobs
        .get(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(job) => Ok(Json(job)),
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

#[derive(Debug, Serialize)]
pub struct ActionResponse {
    pub ok: bool,
}

pub async fn cancel_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> Result<Json<ActionResponse>, axum::http::StatusCode> {
    let changed = state
        .jobs
        .request_cancel(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();

    if !changed {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    Ok(Json(ActionResponse { ok: true }))
}

pub async fn retry_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> Result<Json<ActionResponse>, axum::http::StatusCode> {
    let changed = state
        .jobs
        .retry(job_id, Utc::now())
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();

    if !changed {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    Ok(Json(ActionResponse { ok: true }))
}

#[derive(Debug, Deserialize)]
pub struct IngestSyncQuery {
    pub list_key: String,
}

#[derive(Debug, Serialize)]
pub struct IngestSyncResponse {
    pub queued: usize,
    pub repos: Vec<String>,
}

pub async fn ingest_sync(
    State(state): State<ApiState>,
    Query(query): Query<IngestSyncQuery>,
) -> Result<Json<IngestSyncResponse>, axum::http::StatusCode> {
    let list = state
        .catalog
        .ensure_mailing_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let list_root = Path::new(&state.settings.mail.mirror_root).join(&query.list_key);
    let repos = discover_repo_relpaths(&list_root)
        .map_err(|_| axum::http::StatusCode::BAD_REQUEST)?;

    if repos.is_empty() {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let mut queued = 0usize;
    for repo_relpath in &repos {
        state
            .catalog
            .ensure_repo(list.id, repo_relpath, repo_relpath)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

        let payload = RepoScanPayload {
            list_key: query.list_key.clone(),
            repo_key: repo_relpath.clone(),
            mirror_path: list_root.join(repo_relpath).display().to_string(),
            since_commit_oid: None,
        };

        state
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "repo_scan".to_string(),
                payload_json: serde_json::to_value(payload)
                    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
                priority: 20,
                dedupe_scope: Some(format!("list:{}", query.list_key)),
                dedupe_key: None,
                run_after: None,
                max_attempts: Some(8),
            })
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

        queued += 1;
    }

    Ok(Json(IngestSyncResponse {
        queued,
        repos,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ResetWatermarkQuery {
    pub list_key: String,
    pub repo_key: String,
}

pub async fn reset_watermark(
    State(state): State<ApiState>,
    Query(query): Query<ResetWatermarkQuery>,
) -> Result<Json<ActionResponse>, axum::http::StatusCode> {
    let changed = state
        .catalog
        .reset_watermark(&query.list_key, &query.repo_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    if !changed {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    Ok(Json(ActionResponse { ok: true }))
}

fn discover_repo_relpaths(list_root: &Path) -> Result<Vec<String>, std::io::Error> {
    if !list_root.exists() {
        return Ok(Vec::new());
    }

    let mut repos = BTreeSet::new();

    let all_git = list_root.join("all.git");
    if all_git.is_dir() {
        repos.insert("all.git".to_string());
    }

    let git_dir = list_root.join("git");
    if git_dir.is_dir() {
        for entry in fs::read_dir(&git_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() && path.file_name().and_then(|v| v.to_str()).unwrap_or("").ends_with(".git") {
                let relpath = PathBuf::from("git")
                    .join(path.file_name().unwrap_or_default())
                    .display()
                    .to_string();
                repos.insert(relpath);
            }
        }
    }

    if repos.is_empty() && looks_like_bare_repo(list_root) {
        repos.insert(".".to_string());
    }

    Ok(repos.into_iter().collect())
}

fn looks_like_bare_repo(path: &Path) -> bool {
    path.join("objects").exists() && path.join("refs").exists()
}
