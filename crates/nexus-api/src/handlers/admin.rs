use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use chrono::{DateTime, Utc};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    DbListStorageRecord, DbStorageTotalsRecord, EnqueueJobParams, Job, JobState, JobStateCount,
    JobTypeStateCount, ListJobsParams, ListMeiliBootstrapRunsParams, ListPipelineRunsParams,
    RunningJobSnapshot,
};
use nexus_jobs::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingScope, LineageRebuildListPayload,
    MeiliBootstrapRunPayload, MeiliBootstrapScope, PipelineIngestPayload,
    ThreadingRebuildListPayload,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

#[derive(Debug, Deserialize)]
pub struct JobAttemptsQuery {
    #[serde(default = "default_attempts_limit")]
    pub limit: i64,
}

fn default_attempts_limit() -> i64 {
    50
}

#[derive(Debug, Serialize)]
pub struct JobAttemptsResponse {
    pub items: Vec<nexus_db::JobAttempt>,
}

pub async fn list_job_attempts(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
    Query(query): Query<JobAttemptsQuery>,
) -> Result<Json<JobAttemptsResponse>, axum::http::StatusCode> {
    let exists = state
        .jobs
        .get(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let attempts = state
        .jobs
        .list_attempts(job_id, query.limit)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(JobAttemptsResponse { items: attempts }))
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

#[derive(Debug, Serialize, Default)]
pub struct QueueStateCountsResponse {
    pub scheduled: i64,
    pub queued: i64,
    pub running: i64,
    pub succeeded: i64,
    pub failed_retryable: i64,
    pub failed_terminal: i64,
    pub cancelled: i64,
}

#[derive(Debug, Serialize)]
pub struct QueueDiagnosticsResponse {
    pub generated_at: DateTime<Utc>,
    pub counts_by_state: QueueStateCountsResponse,
    pub counts_by_job_type: Vec<JobTypeStateCount>,
    pub running_jobs: Vec<RunningJobSnapshot>,
}

pub async fn diagnostics_queue(
    State(state): State<ApiState>,
) -> Result<Json<QueueDiagnosticsResponse>, axum::http::StatusCode> {
    let state_counts = state
        .jobs
        .list_state_counts()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let job_type_counts = state
        .jobs
        .list_job_type_state_counts(500)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let running_jobs = state
        .jobs
        .list_running_jobs(200)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(QueueDiagnosticsResponse {
        generated_at: Utc::now(),
        counts_by_state: to_queue_state_counts(state_counts),
        counts_by_job_type: job_type_counts,
        running_jobs,
    }))
}

#[derive(Debug, Serialize)]
pub struct StorageDbTotalsResponse {
    pub mailing_lists: i64,
    pub messages: i64,
    pub threads: i64,
    pub patch_series: i64,
    pub patch_items: i64,
    pub jobs: i64,
    pub job_attempts: i64,
    pub embedding_vectors: i64,
}

#[derive(Debug, Serialize)]
pub struct StorageDbListRepoCountsResponse {
    pub active: i64,
    pub total: i64,
}

#[derive(Debug, Serialize)]
pub struct StorageDbListCountsResponse {
    pub messages: i64,
    pub threads: i64,
    pub patch_series: i64,
    pub patch_items: i64,
}

#[derive(Debug, Serialize)]
pub struct StorageDbListResponse {
    pub list_key: String,
    pub repos: StorageDbListRepoCountsResponse,
    pub counts: StorageDbListCountsResponse,
}

#[derive(Debug, Serialize)]
pub struct StorageDbResponse {
    pub totals: StorageDbTotalsResponse,
    pub lists: Vec<StorageDbListResponse>,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct StorageMeiliIndexResponse {
    pub documents: i64,
    pub is_indexing: bool,
    pub embedded_documents: i64,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct StorageMeiliIndexesResponse {
    pub thread_docs: StorageMeiliIndexResponse,
    pub patch_series_docs: StorageMeiliIndexResponse,
    pub patch_item_docs: StorageMeiliIndexResponse,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct StorageMeiliTotalsResponse {
    pub database_size_bytes: Option<u64>,
    pub used_database_size_bytes: Option<u64>,
    pub last_update: Option<String>,
    pub indexes: StorageMeiliIndexesResponse,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct StorageMeiliListResponse {
    pub list_key: String,
    pub thread_docs: i64,
    pub patch_series_docs: i64,
    pub patch_item_docs: i64,
}

#[derive(Debug, Serialize)]
pub struct StorageMeiliResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub totals: StorageMeiliTotalsResponse,
    pub lists: Vec<StorageMeiliListResponse>,
}

#[derive(Debug, Serialize)]
pub struct StorageDriftResponse {
    pub list_key: String,
    pub threads_db: i64,
    pub threads_meili: i64,
    pub threads_delta: i64,
    pub patch_series_db: i64,
    pub patch_series_meili: i64,
    pub patch_series_delta: i64,
    pub patch_items_db: i64,
    pub patch_items_meili: i64,
    pub patch_items_delta: i64,
}

#[derive(Debug, Serialize)]
pub struct StorageDiagnosticsResponse {
    pub generated_at: DateTime<Utc>,
    pub db: StorageDbResponse,
    pub meili: StorageMeiliResponse,
    pub drift: Vec<StorageDriftResponse>,
}

pub async fn diagnostics_storage(
    State(state): State<ApiState>,
) -> Result<Json<StorageDiagnosticsResponse>, axum::http::StatusCode> {
    let db_totals = state
        .catalog
        .get_db_storage_totals()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let db_lists = state
        .catalog
        .list_db_storage_by_list()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let meili_payload = fetch_meili_storage(&state).await;
    let drift = build_storage_drift(&db_lists, &meili_payload.list_counts);

    Ok(Json(StorageDiagnosticsResponse {
        generated_at: Utc::now(),
        db: StorageDbResponse {
            totals: map_db_totals(db_totals),
            lists: db_lists.into_iter().map(map_db_list).collect(),
        },
        meili: meili_payload.response,
        drift,
    }))
}

#[derive(Debug, Deserialize)]
pub struct StartMeiliBootstrapQuery {
    pub scope: String,
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize)]
pub struct MeiliBootstrapEstimatesResponse {
    pub thread_candidates: i64,
    pub series_candidates: i64,
    pub total_candidates: i64,
}

#[derive(Debug, Serialize)]
pub struct StartMeiliBootstrapResponse {
    pub run_id: Option<i64>,
    pub job_id: Option<i64>,
    pub scope: String,
    pub list_key: Option<String>,
    pub state: String,
    pub embedder_name: String,
    pub model_key: String,
    pub dry_run: bool,
    pub estimates: MeiliBootstrapEstimatesResponse,
}

pub async fn start_meili_bootstrap(
    State(state): State<ApiState>,
    Query(query): Query<StartMeiliBootstrapQuery>,
) -> Result<Json<StartMeiliBootstrapResponse>, axum::http::StatusCode> {
    if !state.settings.embeddings.enabled {
        return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    }
    let scope = parse_meili_bootstrap_scope(&query.scope)
        .ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;

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

    let active_pipeline_run = if let Some(list_key) = query.list_key.as_deref() {
        state
            .pipeline
            .get_active_run_for_list(list_key)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        state
            .pipeline
            .get_any_active_run()
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    };
    if active_pipeline_run.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
    }

    let active_bootstrap = state
        .embeddings
        .get_active_meili_bootstrap_run(scope.as_str(), query.list_key.as_deref())
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    if active_bootstrap.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
    }

    let thread_candidates = state
        .embeddings
        .count_candidate_ids("thread", query.list_key.as_deref(), None, None)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let series_candidates = state
        .embeddings
        .count_candidate_ids("series", query.list_key.as_deref(), None, None)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let estimates = MeiliBootstrapEstimatesResponse {
        thread_candidates,
        series_candidates,
        total_candidates: thread_candidates + series_candidates,
    };

    if query.dry_run {
        return Ok(Json(StartMeiliBootstrapResponse {
            run_id: None,
            job_id: None,
            scope: scope.as_str().to_string(),
            list_key: query.list_key,
            state: "dry_run".to_string(),
            embedder_name: state.settings.embeddings.embedder_name.clone(),
            model_key: state.settings.embeddings.model.clone(),
            dry_run: true,
            estimates,
        }));
    }

    let run = state
        .embeddings
        .create_meili_bootstrap_run(
            scope.as_str(),
            query.list_key.as_deref(),
            &state.settings.embeddings.embedder_name,
            &state.settings.embeddings.model,
        )
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let payload = MeiliBootstrapRunPayload { run_id: run.id };
    let job = match state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "meili_bootstrap_run".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 5,
            dedupe_scope: Some(format!("diagnostics:meili_bootstrap:{}", scope.as_str())),
            dedupe_key: Some(format!("run:{}", run.id)),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = state
                .embeddings
                .mark_meili_bootstrap_failed(
                    run.id,
                    format!("failed to enqueue meili_bootstrap_run: {err}").as_str(),
                )
                .await;
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    state
        .embeddings
        .set_meili_bootstrap_job_id(run.id, job.id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(StartMeiliBootstrapResponse {
        run_id: Some(run.id),
        job_id: Some(job.id),
        scope: scope.as_str().to_string(),
        list_key: run.list_key,
        state: "queued".to_string(),
        embedder_name: run.embedder_name,
        model_key: run.model_key,
        dry_run: false,
        estimates,
    }))
}

pub async fn get_meili_bootstrap_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<nexus_db::MeiliBootstrapRun>, axum::http::StatusCode> {
    let Some(run) = state
        .embeddings
        .get_meili_bootstrap_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };
    Ok(Json(run))
}

#[derive(Debug, Deserialize)]
pub struct MeiliBootstrapRunsQuery {
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
pub struct ListMeiliBootstrapRunsResponse {
    pub items: Vec<nexus_db::MeiliBootstrapRun>,
    pub next_cursor: Option<i64>,
}

pub async fn list_meili_bootstrap_runs(
    State(state): State<ApiState>,
    Query(query): Query<MeiliBootstrapRunsQuery>,
) -> Result<Json<ListMeiliBootstrapRunsResponse>, axum::http::StatusCode> {
    let limit = query.limit.clamp(1, 200);
    let runs = state
        .embeddings
        .list_meili_bootstrap_runs(ListMeiliBootstrapRunsParams {
            list_key: query.list_key,
            state: query.state,
            limit,
            cursor: query.cursor,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let next_cursor = if runs.len() >= limit as usize {
        runs.last().map(|run| run.id)
    } else {
        None
    };

    Ok(Json(ListMeiliBootstrapRunsResponse {
        items: runs,
        next_cursor,
    }))
}

pub async fn cancel_meili_bootstrap_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<ActionResponse>, axum::http::StatusCode> {
    let Some(run) = state
        .embeddings
        .get_meili_bootstrap_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };

    if matches!(run.state.as_str(), "succeeded" | "failed" | "cancelled") {
        return Ok(Json(ActionResponse { ok: true }));
    }

    let Some(job_id) = run.job_id else {
        return Err(axum::http::StatusCode::CONFLICT);
    };

    let Some(job) = state
        .jobs
        .request_cancel(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };

    if job.state == JobState::Cancelled {
        state
            .embeddings
            .mark_meili_bootstrap_cancelled(run.id, "cancel requested")
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
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

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::CONFLICT,
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

    let discovered_lists = candidates.len();
    let mut queued_lists = 0usize;
    let mut queued_jobs = 0usize;
    let mut results = Vec::with_capacity(discovered_lists);

    // Get a batch ID for ordering all lists in this grokmirror run
    let batch_id = state
        .pipeline
        .next_batch_id()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    // Phase 1: ensure catalog entries and create pending runs for all lists
    let mut pending_runs: Vec<(String, Vec<String>, i64)> = Vec::new(); // (list_key, repos, run_id)

    for (position, candidate) in candidates.iter().enumerate() {
        match ensure_catalog_entries(&state, &candidate.list_key, &candidate.repos).await {
            Ok(list_id) => {
                match state
                    .pipeline
                    .create_pending_run(
                        list_id,
                        &candidate.list_key,
                        "admin_ingest_grokmirror",
                        batch_id,
                        position as i32,
                    )
                    .await
                {
                    Ok(run) => {
                        pending_runs.push((
                            candidate.list_key.clone(),
                            candidate.repos.clone(),
                            run.id,
                        ));
                    }
                    Err(err) => {
                        results.push(IngestGrokmirrorListResult {
                            list_key: candidate.list_key.clone(),
                            status: "error".to_string(),
                            queued: 0,
                            repos: candidate.repos.clone(),
                            pipeline_run_id: None,
                            current_stage: None,
                            error: Some(format!("failed to create pipeline run: {err}")),
                        });
                    }
                }
            }
            Err(err) => {
                results.push(IngestGrokmirrorListResult {
                    list_key: candidate.list_key.clone(),
                    status: "error".to_string(),
                    queued: 0,
                    repos: candidate.repos.clone(),
                    pipeline_run_id: None,
                    current_stage: None,
                    error: Some(err.message),
                });
            }
        }
    }

    // Phase 2: activate the first pending run and enqueue its ingest job.
    // If enqueue fails after activation, mark that run failed and try the next pending run.
    let mut started_run_id: Option<i64> = None;
    let mut launch_errors: BTreeMap<i64, String> = BTreeMap::new();
    if !pending_runs.is_empty() {
        loop {
            let activated = match state.pipeline.activate_next_pending_run(batch_id).await {
                Ok(Some(run)) => run,
                Ok(None) => break,
                Err(err) => {
                    tracing::error!(batch_id, "failed to activate pending run: {err}");
                    break;
                }
            };

            let payload = PipelineIngestPayload {
                run_id: activated.id,
            };
            match state
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "pipeline_ingest".to_string(),
                    payload_json: serde_json::to_value(payload).unwrap_or_default(),
                    priority: 20,
                    dedupe_scope: Some(format!("pipeline:run:{}", activated.id)),
                    dedupe_key: Some("ingest".to_string()),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                Ok(_) => {
                    queued_jobs += 1;
                    started_run_id = Some(activated.id);
                    break;
                }
                Err(err) => {
                    let reason =
                        format!("failed to enqueue pipeline_ingest after activation: {err}");
                    launch_errors.insert(activated.id, reason.clone());
                    tracing::error!(
                        batch_id,
                        run_id = activated.id,
                        "failed to enqueue pipeline_ingest for activated run: {err}"
                    );

                    if let Err(mark_err) =
                        state.pipeline.mark_run_failed(activated.id, &reason).await
                    {
                        tracing::error!(
                            batch_id,
                            run_id = activated.id,
                            "failed to mark run failed after enqueue error: {mark_err}"
                        );
                        break;
                    }
                }
            }
        }

        // Build result entries for all successfully created pending runs.
        for (list_key, repos, run_id) in &pending_runs {
            if let Some(error) = launch_errors.get(run_id) {
                results.push(IngestGrokmirrorListResult {
                    list_key: list_key.clone(),
                    status: "error".to_string(),
                    queued: 0,
                    repos: repos.clone(),
                    pipeline_run_id: Some(*run_id),
                    current_stage: Some("ingest".to_string()),
                    error: Some(error.clone()),
                });
                continue;
            }

            queued_lists += 1;
            if Some(*run_id) == started_run_id {
                results.push(IngestGrokmirrorListResult {
                    list_key: list_key.clone(),
                    status: "queued".to_string(),
                    queued: 1,
                    repos: repos.clone(),
                    pipeline_run_id: Some(*run_id),
                    current_stage: Some("ingest".to_string()),
                    error: None,
                });
            } else {
                results.push(IngestGrokmirrorListResult {
                    list_key: list_key.clone(),
                    status: "pending".to_string(),
                    queued: 0,
                    repos: repos.clone(),
                    pipeline_run_id: Some(*run_id),
                    current_stage: Some("ingest".to_string()),
                    error: None,
                });
            }
        }
    }

    Ok(Json(IngestGrokmirrorResponse {
        mirror_root: state.settings.mail.mirror_root.clone(),
        mode: "pipeline".to_string(),
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
    // Reject if there's an active pipeline run for this list
    let active_run = state
        .pipeline
        .get_active_run_for_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    if active_run.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
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
    // Reject if there's an active pipeline run for this list
    let active_run = state
        .pipeline
        .get_active_run_for_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    if active_run.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
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

pub async fn get_pipeline_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<nexus_db::PipelineRun>, axum::http::StatusCode> {
    let Some(run) = state
        .pipeline
        .get_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };

    Ok(Json(run))
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

#[derive(Debug)]
struct MeiliStoragePayload {
    response: StorageMeiliResponse,
    list_counts: BTreeMap<String, StorageMeiliListResponse>,
}

fn to_queue_state_counts(rows: Vec<JobStateCount>) -> QueueStateCountsResponse {
    let mut counts = QueueStateCountsResponse::default();
    for row in rows {
        match row.state {
            JobState::Scheduled => counts.scheduled = row.count,
            JobState::Queued => counts.queued = row.count,
            JobState::Running => counts.running = row.count,
            JobState::Succeeded => counts.succeeded = row.count,
            JobState::FailedRetryable => counts.failed_retryable = row.count,
            JobState::FailedTerminal => counts.failed_terminal = row.count,
            JobState::Cancelled => counts.cancelled = row.count,
        }
    }
    counts
}

fn map_db_totals(raw: DbStorageTotalsRecord) -> StorageDbTotalsResponse {
    StorageDbTotalsResponse {
        mailing_lists: raw.mailing_lists,
        messages: raw.messages,
        threads: raw.threads,
        patch_series: raw.patch_series,
        patch_items: raw.patch_items,
        jobs: raw.jobs,
        job_attempts: raw.job_attempts,
        embedding_vectors: raw.embedding_vectors,
    }
}

fn map_db_list(raw: DbListStorageRecord) -> StorageDbListResponse {
    StorageDbListResponse {
        list_key: raw.list_key,
        repos: StorageDbListRepoCountsResponse {
            active: raw.active_repo_count,
            total: raw.total_repo_count,
        },
        counts: StorageDbListCountsResponse {
            messages: raw.message_count,
            threads: raw.thread_count,
            patch_series: raw.patch_series_count,
            patch_items: raw.patch_item_count,
        },
    }
}

fn build_storage_drift(
    db_lists: &[DbListStorageRecord],
    meili_lists: &BTreeMap<String, StorageMeiliListResponse>,
) -> Vec<StorageDriftResponse> {
    db_lists
        .iter()
        .map(|row| {
            let meili = meili_lists.get(&row.list_key).cloned().unwrap_or_else(|| {
                StorageMeiliListResponse {
                    list_key: row.list_key.clone(),
                    ..StorageMeiliListResponse::default()
                }
            });
            StorageDriftResponse {
                list_key: row.list_key.clone(),
                threads_db: row.thread_count,
                threads_meili: meili.thread_docs,
                threads_delta: meili.thread_docs - row.thread_count,
                patch_series_db: row.patch_series_count,
                patch_series_meili: meili.patch_series_docs,
                patch_series_delta: meili.patch_series_docs - row.patch_series_count,
                patch_items_db: row.patch_item_count,
                patch_items_meili: meili.patch_item_docs,
                patch_items_delta: meili.patch_item_docs - row.patch_item_count,
            }
        })
        .collect()
}

async fn fetch_meili_storage(state: &ApiState) -> MeiliStoragePayload {
    let mut response = StorageMeiliResponse {
        ok: true,
        error: None,
        totals: StorageMeiliTotalsResponse::default(),
        lists: Vec::new(),
    };
    let mut list_counts: BTreeMap<String, StorageMeiliListResponse> = BTreeMap::new();

    let client = reqwest::Client::new();
    let base_url = state.settings.meili.url.trim_end_matches('/');
    let stats_result = client
        .get(format!("{base_url}/stats"))
        .bearer_auth(&state.settings.meili.master_key)
        .send()
        .await;

    let stats_value = match stats_result {
        Ok(resp) => {
            if !resp.status().is_success() {
                response.ok = false;
                response.error = Some(format!("meili /stats returned http {}", resp.status()));
                return MeiliStoragePayload {
                    response,
                    list_counts,
                };
            }
            match resp.json::<Value>().await {
                Ok(value) => value,
                Err(err) => {
                    response.ok = false;
                    response.error = Some(format!("failed to parse meili /stats response: {err}"));
                    return MeiliStoragePayload {
                        response,
                        list_counts,
                    };
                }
            }
        }
        Err(err) => {
            response.ok = false;
            response.error = Some(format!("meili /stats request failed: {err}"));
            return MeiliStoragePayload {
                response,
                list_counts,
            };
        }
    };

    response.totals = parse_meili_totals(&stats_value);

    for index_kind in [
        MeiliIndexKind::ThreadDocs,
        MeiliIndexKind::PatchSeriesDocs,
        MeiliIndexKind::PatchItemDocs,
    ] {
        match fetch_meili_list_counts(&client, state, index_kind).await {
            Ok(counts) => merge_meili_list_counts(index_kind, counts, &mut list_counts),
            Err(err) => {
                response.ok = false;
                if response.error.is_none() {
                    response.error = Some(err);
                }
            }
        }
    }

    response.lists = list_counts.values().cloned().collect();
    MeiliStoragePayload {
        response,
        list_counts,
    }
}

fn parse_meili_totals(raw: &Value) -> StorageMeiliTotalsResponse {
    let indexes = raw.get("indexes").and_then(Value::as_object);
    StorageMeiliTotalsResponse {
        database_size_bytes: raw.get("databaseSize").and_then(Value::as_u64),
        used_database_size_bytes: raw.get("usedDatabaseSize").and_then(Value::as_u64),
        last_update: raw
            .get("lastUpdate")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        indexes: StorageMeiliIndexesResponse {
            thread_docs: parse_meili_index(indexes.and_then(|m| m.get("thread_docs"))),
            patch_series_docs: parse_meili_index(indexes.and_then(|m| m.get("patch_series_docs"))),
            patch_item_docs: parse_meili_index(indexes.and_then(|m| m.get("patch_item_docs"))),
        },
    }
}

fn parse_meili_index(raw: Option<&Value>) -> StorageMeiliIndexResponse {
    let raw = match raw {
        Some(value) => value,
        None => return StorageMeiliIndexResponse::default(),
    };
    StorageMeiliIndexResponse {
        documents: raw
            .get("numberOfDocuments")
            .and_then(Value::as_i64)
            .or_else(|| {
                raw.get("numberOfDocuments")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .unwrap_or(0),
        is_indexing: raw
            .get("isIndexing")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        embedded_documents: raw
            .get("numberOfEmbeddedDocuments")
            .and_then(Value::as_i64)
            .or_else(|| {
                raw.get("numberOfEmbeddedDocuments")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .or_else(|| {
                raw.get("numberOfEmbeddings")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .unwrap_or(0),
    }
}

async fn fetch_meili_list_counts(
    client: &reqwest::Client,
    state: &ApiState,
    index_kind: MeiliIndexKind,
) -> Result<BTreeMap<String, i64>, String> {
    let base_url = state.settings.meili.url.trim_end_matches('/');
    let index_uid = index_kind.uid();
    let response = client
        .post(format!("{base_url}/indexes/{index_uid}/search"))
        .bearer_auth(&state.settings.meili.master_key)
        .json(&json!({
            "q": "",
            "limit": 0,
            "facets": ["list_keys"]
        }))
        .send()
        .await
        .map_err(|err| format!("meili facet query failed for {index_uid}: {err}"))?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(BTreeMap::new());
    }
    if !response.status().is_success() {
        return Err(format!(
            "meili facet query for {index_uid} returned http {}",
            response.status()
        ));
    }
    let payload = response
        .json::<Value>()
        .await
        .map_err(|err| format!("failed to parse meili facet response for {index_uid}: {err}"))?;
    Ok(parse_meili_facet_counts(&payload))
}

fn parse_meili_facet_counts(payload: &Value) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let Some(by_list) = payload
        .get("facetDistribution")
        .and_then(|facets| facets.get("list_keys"))
        .and_then(Value::as_object)
    else {
        return out;
    };

    for (list_key, count_value) in by_list {
        if let Some(count) = count_value
            .as_i64()
            .or_else(|| count_value.as_u64().map(|value| value as i64))
        {
            out.insert(list_key.clone(), count);
        }
    }
    out
}

fn merge_meili_list_counts(
    index_kind: MeiliIndexKind,
    counts: BTreeMap<String, i64>,
    by_list: &mut BTreeMap<String, StorageMeiliListResponse>,
) {
    for (list_key, count) in counts {
        let entry = by_list
            .entry(list_key.clone())
            .or_insert_with(|| StorageMeiliListResponse {
                list_key,
                ..StorageMeiliListResponse::default()
            });

        match index_kind {
            MeiliIndexKind::ThreadDocs => entry.thread_docs = count,
            MeiliIndexKind::PatchSeriesDocs => entry.patch_series_docs = count,
            MeiliIndexKind::PatchItemDocs => entry.patch_item_docs = count,
        }
    }
}

fn discover_repo_relpaths(list_root: &Path) -> Result<Vec<String>, std::io::Error> {
    if !list_root.exists() {
        return Ok(Vec::new());
    }

    let mut epoch_repos = BTreeSet::new();
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
                epoch_repos.insert(relpath);
            }
        }
    }

    if !epoch_repos.is_empty() {
        return Ok(epoch_repos.into_iter().collect());
    }

    let all_git = list_root.join("all.git");
    if all_git.is_dir() {
        return Ok(vec!["all.git".to_string()]);
    }

    if looks_like_bare_repo(list_root) {
        return Ok(vec![".".to_string()]);
    }

    Ok(Vec::new())
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

/// Ensure mailing list and repo catalog entries exist, returning the mailing list ID.
async fn ensure_catalog_entries(
    state: &ApiState,
    list_key: &str,
    repos: &[String],
) -> Result<i64, IngestQueueError> {
    let list = state
        .catalog
        .ensure_mailing_list(list_key)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!("failed to ensure mailing list {list_key}: {err}"))
        })?;

    for repo_relpath in repos {
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

    Ok(list.id)
}

/// Create a pipeline run for a single list and enqueue its ingest job.
async fn queue_list_ingest(
    state: &ApiState,
    list_key: &str,
    repos: Vec<String>,
    source: &str,
) -> Result<IngestSyncResponse, IngestQueueError> {
    if let Some(active) = state
        .pipeline
        .get_active_run_for_list(list_key)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!(
                "failed to check active pipeline run for list {list_key}: {err}"
            ))
        })?
    {
        return Err(IngestQueueError::conflict(format!(
            "pipeline run {} is already running for list {list_key}",
            active.id
        )));
    }

    let list_id = ensure_catalog_entries(state, list_key, &repos).await?;

    let run = state
        .pipeline
        .create_running_run(list_id, list_key, source)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!(
                "failed to create pipeline run for list {list_key}: {err}"
            ))
        })?;

    let payload = PipelineIngestPayload { run_id: run.id };
    let enqueue_result = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "pipeline_ingest".to_string(),
            payload_json: serde_json::to_value(payload).map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to serialize pipeline payload for list {list_key}: {err}"
                ))
            })?,
            priority: 20,
            dedupe_scope: Some(format!("pipeline:run:{}", run.id)),
            dedupe_key: Some("ingest".to_string()),
            run_after: None,
            max_attempts: Some(8),
        })
        .await;

    if let Err(err) = enqueue_result {
        let reason = format!("failed to enqueue pipeline_ingest for list {list_key}: {err}");
        if let Err(mark_err) = state
            .pipeline
            .mark_run_failed(run.id, &format!("initial enqueue error: {err}"))
            .await
        {
            tracing::error!(
                run_id = run.id,
                list_key,
                "failed to mark run failed after enqueue error: {mark_err}"
            );
        }
        return Err(IngestQueueError::internal(reason));
    }

    Ok(IngestSyncResponse {
        queued: 1,
        repos,
        mode: "pipeline".to_string(),
        pipeline_run_id: Some(run.id),
        current_stage: Some(run.current_stage),
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

fn parse_meili_bootstrap_scope(raw: &str) -> Option<MeiliBootstrapScope> {
    match raw {
        "embedding_indexes" => Some(MeiliBootstrapScope::EmbeddingIndexes),
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

    use nexus_core::search::MeiliIndexKind;
    use serde_json::json;

    use super::{
        StorageMeiliListResponse, discover_mirror_lists, discover_repo_relpaths,
        merge_meili_list_counts, parse_meili_facet_counts, parse_meili_totals,
    };

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

    #[test]
    fn parse_meili_totals_extracts_expected_fields() {
        let payload = json!({
            "databaseSize": 10,
            "usedDatabaseSize": 8,
            "lastUpdate": "2026-02-15T18:22:01.592981161Z",
            "indexes": {
                "thread_docs": {
                    "numberOfDocuments": 11,
                    "isIndexing": false,
                    "numberOfEmbeddedDocuments": 9
                },
                "patch_series_docs": {
                    "numberOfDocuments": 22,
                    "isIndexing": true,
                    "numberOfEmbeddings": 7
                }
            }
        });

        let parsed = parse_meili_totals(&payload);
        assert_eq!(parsed.database_size_bytes, Some(10));
        assert_eq!(parsed.used_database_size_bytes, Some(8));
        assert_eq!(
            parsed.last_update.as_deref(),
            Some("2026-02-15T18:22:01.592981161Z")
        );
        assert_eq!(parsed.indexes.thread_docs.documents, 11);
        assert_eq!(parsed.indexes.thread_docs.embedded_documents, 9);
        assert_eq!(parsed.indexes.patch_series_docs.documents, 22);
        assert_eq!(parsed.indexes.patch_series_docs.embedded_documents, 7);
        assert_eq!(parsed.indexes.patch_item_docs.documents, 0);
    }

    #[test]
    fn parse_meili_facet_counts_reads_list_key_distribution() {
        let payload = json!({
            "facetDistribution": {
                "list_keys": {
                    "bpf": 10244,
                    "lkml": 77
                }
            }
        });
        let parsed = parse_meili_facet_counts(&payload);
        assert_eq!(parsed.get("bpf"), Some(&10244));
        assert_eq!(parsed.get("lkml"), Some(&77));
    }

    #[test]
    fn merge_meili_list_counts_tracks_index_families_per_list() {
        let mut out = std::collections::BTreeMap::<String, StorageMeiliListResponse>::new();
        merge_meili_list_counts(
            MeiliIndexKind::ThreadDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 10), ("lkml".to_string(), 4)]),
            &mut out,
        );
        merge_meili_list_counts(
            MeiliIndexKind::PatchSeriesDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 3)]),
            &mut out,
        );
        merge_meili_list_counts(
            MeiliIndexKind::PatchItemDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 20)]),
            &mut out,
        );

        let bpf = out.get("bpf").expect("bpf");
        assert_eq!(bpf.thread_docs, 10);
        assert_eq!(bpf.patch_series_docs, 3);
        assert_eq!(bpf.patch_item_docs, 20);

        let lkml = out.get("lkml").expect("lkml");
        assert_eq!(lkml.thread_docs, 4);
        assert_eq!(lkml.patch_series_docs, 0);
        assert_eq!(lkml.patch_item_docs, 0);
    }
}
