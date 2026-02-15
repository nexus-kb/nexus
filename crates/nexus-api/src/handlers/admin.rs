use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use chrono::{DateTime, Utc};
use nexus_core::config::PipelineExecutionMode;
use nexus_db::{EnqueueJobParams, Job, JobState, ListJobsParams, ListPipelineRunsParams};
use nexus_jobs::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingScope, LineageRebuildListPayload,
    PipelineStageIngestPayload, RepoScanPayload, ThreadingRebuildListPayload,
};
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
    pub mode: String,
    pub pipeline_run_id: Option<i64>,
    pub current_stage: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct IngestGrokmirrorResponse {
    pub mirror_root: String,
    pub mode: String,
    pub discovered_lists: usize,
    pub queued_lists: usize,
    pub queued_jobs: usize,
    pub results: Vec<IngestGrokmirrorListResult>,
}

#[derive(Debug, Serialize)]
pub struct IngestGrokmirrorListResult {
    pub list_key: String,
    pub status: String,
    pub queued: usize,
    pub repos: Vec<String>,
    pub pipeline_run_id: Option<i64>,
    pub current_stage: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug)]
struct IngestQueueError {
    status: axum::http::StatusCode,
    message: String,
}

impl IngestQueueError {
    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

#[derive(Debug)]
struct MirrorListCandidate {
    list_key: String,
    repos: Vec<String>,
}

pub async fn ingest_sync(
    State(state): State<ApiState>,
    Query(query): Query<IngestSyncQuery>,
) -> Result<Json<IngestSyncResponse>, axum::http::StatusCode> {
    let list_root = Path::new(&state.settings.mail.mirror_root).join(&query.list_key);
    let repos =
        discover_repo_relpaths(&list_root).map_err(|_| axum::http::StatusCode::BAD_REQUEST)?;

    if repos.is_empty() {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    queue_list_ingest(&state, &query.list_key, repos, "admin_ingest_sync")
        .await
        .map(Json)
        .map_err(|err| err.status)
}

pub async fn ingest_grokmirror(
    State(state): State<ApiState>,
) -> Result<Json<IngestGrokmirrorResponse>, axum::http::StatusCode> {
    let mirror_root = Path::new(&state.settings.mail.mirror_root);
    let candidates = discover_mirror_lists(mirror_root)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let mode = match state.settings.worker.pipeline_execution_mode {
        PipelineExecutionMode::Staged => "staged".to_string(),
        _ => "legacy".to_string(),
    };
    let discovered_lists = candidates.len();
    let mut queued_lists = 0usize;
    let mut queued_jobs = 0usize;
    let mut results = Vec::with_capacity(discovered_lists);

    for candidate in candidates {
        match queue_list_ingest(
            &state,
            &candidate.list_key,
            candidate.repos.clone(),
            "admin_ingest_grokmirror",
        )
        .await
        {
            Ok(response) => {
                queued_lists += 1;
                queued_jobs += response.queued;
                results.push(IngestGrokmirrorListResult {
                    list_key: candidate.list_key,
                    status: "queued".to_string(),
                    queued: response.queued,
                    repos: response.repos,
                    pipeline_run_id: response.pipeline_run_id,
                    current_stage: response.current_stage,
                    error: None,
                });
            }
            Err(err) => {
                results.push(IngestGrokmirrorListResult {
                    list_key: candidate.list_key,
                    status: "error".to_string(),
                    queued: 0,
                    repos: candidate.repos,
                    pipeline_run_id: None,
                    current_stage: None,
                    error: Some(err.message),
                });
            }
        }
    }

    Ok(Json(IngestGrokmirrorResponse {
        mirror_root: state.settings.mail.mirror_root.clone(),
        mode,
        discovered_lists,
        queued_lists,
        queued_jobs,
        results,
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

#[derive(Debug, Deserialize)]
pub struct ThreadingRebuildQuery {
    pub list_key: String,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

pub async fn threading_rebuild(
    State(state): State<ApiState>,
    Query(query): Query<ThreadingRebuildQuery>,
) -> Result<Json<EnqueueResponse>, axum::http::StatusCode> {
    if matches!(
        state.settings.worker.pipeline_execution_mode,
        PipelineExecutionMode::Staged
    ) {
        let ingest_active = state
            .pipeline
            .is_list_ingest_stage_active(&query.list_key)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
        if ingest_active {
            return Err(axum::http::StatusCode::CONFLICT);
        }
    }

    let exists = state
        .catalog
        .get_mailing_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let payload = ThreadingRebuildListPayload {
        list_key: query.list_key.clone(),
        from_seen_at: query.from,
        to_seen_at: query.to,
    };

    let dedupe_key = format!(
        "{}:{}:{}",
        query.list_key,
        query
            .from
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string()),
        query
            .to
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string())
    );

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "threading_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 11,
            dedupe_scope: Some(format!("list:{}", query.list_key)),
            dedupe_key: Some(dedupe_key),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[derive(Debug, Deserialize)]
pub struct LineageRebuildQuery {
    pub list_key: String,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

pub async fn lineage_rebuild(
    State(state): State<ApiState>,
    Query(query): Query<LineageRebuildQuery>,
) -> Result<Json<EnqueueResponse>, axum::http::StatusCode> {
    if matches!(
        state.settings.worker.pipeline_execution_mode,
        PipelineExecutionMode::Staged
    ) {
        let ingest_active = state
            .pipeline
            .is_list_ingest_stage_active(&query.list_key)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
        if ingest_active {
            return Err(axum::http::StatusCode::CONFLICT);
        }
    }

    let exists = state
        .catalog
        .get_mailing_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let payload = LineageRebuildListPayload {
        list_key: query.list_key.clone(),
        from_seen_at: query.from,
        to_seen_at: query.to,
    };

    let dedupe_key = format!(
        "{}:{}:{}:lineage",
        query.list_key,
        query
            .from
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string()),
        query
            .to
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string())
    );

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "lineage_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 11,
            dedupe_scope: Some(format!("list:{}", query.list_key)),
            dedupe_key: Some(dedupe_key),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[derive(Debug, Deserialize)]
pub struct PipelineRunsQuery {
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ListPipelineRunsResponse {
    pub items: Vec<nexus_db::PipelineRun>,
    pub next_cursor: Option<i64>,
}

pub async fn list_pipeline_runs(
    State(state): State<ApiState>,
    Query(query): Query<PipelineRunsQuery>,
) -> Result<Json<ListPipelineRunsResponse>, axum::http::StatusCode> {
    let runs = state
        .pipeline
        .list_runs(ListPipelineRunsParams {
            list_key: query.list_key,
            state: query.state,
            limit: query.limit.clamp(1, 200),
            cursor: query.cursor,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let next_cursor = if runs.len() >= query.limit.clamp(1, 200) as usize {
        runs.last().map(|run| run.id)
    } else {
        None
    };

    Ok(Json(ListPipelineRunsResponse {
        items: runs,
        next_cursor,
    }))
}

#[derive(Debug, Serialize)]
pub struct PipelineRunDetailResponse {
    pub run: nexus_db::PipelineRun,
    pub stages: Vec<nexus_db::PipelineStageRun>,
    pub artifacts: Vec<nexus_db::PipelineArtifactCount>,
}

pub async fn get_pipeline_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<PipelineRunDetailResponse>, axum::http::StatusCode> {
    let Some(run) = state
        .pipeline
        .get_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };

    let stages = state
        .pipeline
        .list_stage_runs(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let artifacts = state
        .pipeline
        .list_artifact_counts(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(PipelineRunDetailResponse {
        run,
        stages,
        artifacts,
    }))
}

#[derive(Debug, Deserialize)]
pub struct SearchEmbeddingsBackfillQuery {
    pub scope: String,
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SearchEmbeddingsBackfillResponse {
    pub run_id: i64,
    pub job_id: i64,
    pub scope: String,
    pub model_key: String,
}

pub async fn search_embeddings_backfill(
    State(state): State<ApiState>,
    Query(query): Query<SearchEmbeddingsBackfillQuery>,
) -> Result<Json<SearchEmbeddingsBackfillResponse>, axum::http::StatusCode> {
    if !state.settings.embeddings.enabled {
        return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    }
    let scope =
        parse_embedding_scope(&query.scope).ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;

    if let Some(list_key) = query.list_key.as_deref() {
        let exists = state
            .catalog
            .get_mailing_list(list_key)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
            .is_some();
        if !exists {
            return Err(axum::http::StatusCode::NOT_FOUND);
        }
    }

    let from = match query.from.as_deref() {
        Some(raw) => Some(
            parse_backfill_timestamp(raw).ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?,
        ),
        None => None,
    };
    let to = match query.to.as_deref() {
        Some(raw) => Some(
            parse_backfill_timestamp(raw).ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?,
        ),
        None => None,
    };

    let run = state
        .embeddings
        .create_backfill_run(
            scope.as_str(),
            query.list_key.as_deref(),
            from,
            to,
            &state.settings.embeddings.model,
        )
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let payload = EmbeddingBackfillRunPayload { run_id: run.id };
    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "embedding_backfill_run".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 5,
            dedupe_scope: Some(format!("embeddings:{}", scope.as_str())),
            dedupe_key: Some(format!("run:{}", run.id)),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(SearchEmbeddingsBackfillResponse {
        run_id: run.id,
        job_id: job.id,
        scope: scope.as_str().to_string(),
        model_key: run.model_key,
    }))
}

pub async fn get_search_embeddings_backfill(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<nexus_db::EmbeddingBackfillRun>, axum::http::StatusCode> {
    let Some(run) = state
        .embeddings
        .get_backfill_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };
    Ok(Json(run))
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
            if path.is_dir()
                && path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("")
                    .ends_with(".git")
            {
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

fn discover_mirror_lists(mirror_root: &Path) -> Result<Vec<MirrorListCandidate>, std::io::Error> {
    let mut out = Vec::new();

    for entry in fs::read_dir(mirror_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(list_key) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let repos = discover_repo_relpaths(&path)?;
        if repos.is_empty() {
            continue;
        }
        out.push(MirrorListCandidate {
            list_key: list_key.to_string(),
            repos,
        });
    }

    out.sort_by(|a, b| a.list_key.cmp(&b.list_key));
    Ok(out)
}

async fn queue_list_ingest(
    state: &ApiState,
    list_key: &str,
    repos: Vec<String>,
    source: &str,
) -> Result<IngestSyncResponse, IngestQueueError> {
    let list = state
        .catalog
        .ensure_mailing_list(list_key)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!("failed to ensure mailing list {list_key}: {err}"))
        })?;

    for repo_relpath in &repos {
        state
            .catalog
            .ensure_repo(list.id, repo_relpath, repo_relpath)
            .await
            .map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to ensure repo metadata for list {list_key} repo {repo_relpath}: {err}"
                ))
            })?;
    }

    if matches!(
        state.settings.worker.pipeline_execution_mode,
        PipelineExecutionMode::Staged
    ) {
        let run = state
            .pipeline
            .create_or_get_active_run(list.id, list_key, source)
            .await
            .map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to create/get active pipeline run for list {list_key}: {err}"
                ))
            })?;

        let payload = PipelineStageIngestPayload { run_id: run.id };
        state
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "pipeline_stage_ingest".to_string(),
                payload_json: serde_json::to_value(payload).map_err(|err| {
                    IngestQueueError::internal(format!(
                        "failed to serialize pipeline payload for list {list_key}: {err}"
                    ))
                })?,
                priority: 20,
                dedupe_scope: Some(format!("list:{list_key}:pipeline")),
                dedupe_key: Some(format!("run:{}:stage:ingest", run.id)),
                run_after: None,
                max_attempts: Some(8),
            })
            .await
            .map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to enqueue ingest stage for list {list_key}: {err}"
                ))
            })?;

        return Ok(IngestSyncResponse {
            queued: 1,
            repos,
            mode: "staged".to_string(),
            pipeline_run_id: Some(run.id),
            current_stage: Some(run.current_stage),
        });
    }

    let list_root = Path::new(&state.settings.mail.mirror_root).join(list_key);
    let mut queued = 0usize;
    for repo_relpath in &repos {
        let payload = RepoScanPayload {
            list_key: list_key.to_string(),
            repo_key: repo_relpath.clone(),
            mirror_path: list_root.join(repo_relpath).display().to_string(),
            since_commit_oid: None,
        };

        state
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "repo_scan".to_string(),
                payload_json: serde_json::to_value(payload).map_err(|err| {
                    IngestQueueError::internal(format!(
                        "failed to serialize repo scan payload for list {list_key} repo {repo_relpath}: {err}"
                    ))
                })?,
                priority: 20,
                dedupe_scope: Some(format!("list:{list_key}")),
                dedupe_key: None,
                run_after: None,
                max_attempts: Some(8),
            })
            .await
            .map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to enqueue repo scan for list {list_key} repo {repo_relpath}: {err}"
                ))
            })?;

        queued += 1;
    }

    Ok(IngestSyncResponse {
        queued,
        repos,
        mode: "legacy".to_string(),
        pipeline_run_id: None,
        current_stage: None,
    })
}

fn looks_like_bare_repo(path: &Path) -> bool {
    path.join("objects").exists() && path.join("refs").exists()
}

fn parse_embedding_scope(raw: &str) -> Option<EmbeddingScope> {
    match raw {
        "thread" => Some(EmbeddingScope::Thread),
        "series" => Some(EmbeddingScope::Series),
        _ => None,
    }
}

fn parse_backfill_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Some(ts.with_timezone(&Utc));
    }
    let date = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?;
    let naive = date.and_hms_opt(0, 0, 0)?;
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{env, fs};

    use super::{discover_mirror_lists, discover_repo_relpaths};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = env::temp_dir().join(format!(
                "nexus-api-admin-{prefix}-{}-{now_nanos}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn discover_repo_relpaths_covers_supported_layouts() {
        let temp = TempDir::new("repo-layouts");

        let all_git = temp.path().join("all-layout");
        fs::create_dir_all(all_git.join("all.git")).expect("all.git");
        assert_eq!(
            discover_repo_relpaths(&all_git).expect("discover all.git"),
            vec!["all.git".to_string()]
        );

        let git_epoch = temp.path().join("git-layout");
        fs::create_dir_all(git_epoch.join("git/0.git")).expect("git epoch");
        assert_eq!(
            discover_repo_relpaths(&git_epoch).expect("discover git epoch"),
            vec!["git/0.git".to_string()]
        );

        let bare = temp.path().join("bare-layout");
        fs::create_dir_all(bare.join("objects")).expect("objects");
        fs::create_dir_all(bare.join("refs")).expect("refs");
        assert_eq!(
            discover_repo_relpaths(&bare).expect("discover bare"),
            vec![".".to_string()]
        );
    }

    #[test]
    fn discover_mirror_lists_uses_immediate_repo_dirs_only() {
        let temp = TempDir::new("mirror-lists");

        fs::create_dir_all(temp.path().join("zzz/all.git")).expect("zzz");
        fs::create_dir_all(temp.path().join("alpha/git/1.git")).expect("alpha");
        fs::create_dir_all(temp.path().join("beta/objects")).expect("beta objects");
        fs::create_dir_all(temp.path().join("beta/refs")).expect("beta refs");
        fs::create_dir_all(temp.path().join("no-repo/subdir")).expect("no repo");
        fs::create_dir_all(temp.path().join("nested/child/all.git")).expect("nested");
        fs::write(temp.path().join("manifest.js.gz"), b"placeholder").expect("manifest");

        let discovered = discover_mirror_lists(temp.path()).expect("discover mirror lists");
        let keys: Vec<&str> = discovered
            .iter()
            .map(|item| item.list_key.as_str())
            .collect();

        assert_eq!(keys, vec!["alpha", "beta", "zzz"]);
        assert_eq!(discovered[0].repos, vec!["git/1.git".to_string()]);
        assert_eq!(discovered[1].repos, vec![".".to_string()]);
        assert_eq!(discovered[2].repos, vec!["all.git".to_string()]);
    }
}
