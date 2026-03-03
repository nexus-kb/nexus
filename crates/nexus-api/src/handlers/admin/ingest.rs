use super::*;

/// JSON body for `POST /admin/v1/ingest/sync`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct IngestSyncRequest {
    /// Mailing list key to ingest from on-disk mirror repositories.
    pub list_key: String,
}

/// Response payload for ingest sync enqueue.
#[derive(Debug, Serialize, ToSchema)]
pub struct IngestSyncResponse {
    /// Number of jobs enqueued by this request.
    pub queued: usize,
    /// Relative mirror repository paths used for this ingest.
    pub repos: Vec<String>,
    /// Pipeline mode (`queued`/`pending`/`running`).
    pub mode: String,
    /// Pipeline run id if one exists or was created.
    pub pipeline_run_id: Option<i64>,
    /// Current stage for pipeline run, when available.
    pub current_stage: Option<String>,
}

/// Response payload for grokmirror-wide ingest sync.
#[derive(Debug, Serialize, ToSchema)]
pub struct IngestGrokmirrorResponse {
    /// Absolute mirror root scanned for list repositories.
    pub mirror_root: String,
    /// Execution mode for this batch.
    pub mode: String,
    /// Number of candidate lists discovered from mirror layout.
    pub discovered_lists: usize,
    /// Number of lists accepted for queueing or already active.
    pub queued_lists: usize,
    /// Number of jobs enqueued.
    pub queued_jobs: usize,
    /// Per-list result breakdown.
    pub results: Vec<IngestGrokmirrorListResult>,
}

/// Per-list result entry for grokmirror ingest sync.
#[derive(Debug, Serialize, ToSchema)]
pub struct IngestGrokmirrorListResult {
    /// Mailing list key.
    pub list_key: String,
    /// Result state (`queued`, `pending`, `running`, `error`, ...).
    pub status: String,
    /// Number of jobs queued for this list.
    pub queued: usize,
    /// Relative mirror repository paths used for this list.
    pub repos: Vec<String>,
    /// Pipeline run id if known.
    pub pipeline_run_id: Option<i64>,
    /// Current pipeline stage if known.
    pub current_stage: Option<String>,
    /// Error message when status is `error`.
    pub error: Option<String>,
}

#[derive(Debug)]
pub(super) struct IngestQueueError {
    status: axum::http::StatusCode,
    message: String,
}

impl IngestQueueError {
    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    pub(super) fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    pub(super) fn into_api_error(self) -> ApiError {
        ApiError::from(self.status).with_detail(self.message)
    }
}

#[derive(Debug)]
pub(super) struct MirrorListCandidate {
    pub(super) list_key: String,
    pub(super) repos: Vec<String>,
}

pub async fn ingest_sync(
    State(state): State<ApiState>,
    Json(body): Json<IngestSyncRequest>,
) -> HandlerResult<Json<IngestSyncResponse>> {
    if body.list_key.trim().is_empty() {
        return Err(ApiError::bad_request("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }

    let list_root = Path::new(&state.settings.mail.mirror_root).join(&body.list_key);
    let repos = discover_repo_relpaths(&list_root)
        .map_err(|_| ApiError::bad_request("failed to discover repos for list"))?;

    if repos.is_empty() {
        return Err(
            ApiError::bad_request("no mirrored repositories found for list_key")
                .with_invalid_param(
                    "list_key",
                    "list does not contain all.git, git/*.git, or bare-repo layout",
                ),
        );
    }

    queue_list_ingest(&state, &body.list_key, repos, "admin_ingest_sync")
        .await
        .map(Json)
        .map_err(IngestQueueError::into_api_error)
}

pub async fn ingest_grokmirror(
    State(state): State<ApiState>,
) -> HandlerResult<Json<IngestGrokmirrorResponse>> {
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

    // Phase 1: ensure catalog entries and create pending runs for lists that do not already
    // have an open run.
    let mut pending_runs: Vec<(String, Vec<String>, i64)> = Vec::new(); // (list_key, repos, run_id)

    for (position, candidate) in candidates.iter().enumerate() {
        match ensure_catalog_entries(&state, &candidate.list_key, &candidate.repos).await {
            Ok(list_id) => {
                let open_run = match state
                    .pipeline
                    .get_open_run_for_list(&candidate.list_key)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        results.push(IngestGrokmirrorListResult {
                            list_key: candidate.list_key.clone(),
                            status: "error".to_string(),
                            queued: 0,
                            repos: candidate.repos.clone(),
                            pipeline_run_id: None,
                            current_stage: None,
                            error: Some(format!("failed to check open pipeline run: {err}")),
                        });
                        continue;
                    }
                };

                if let Some(run) = open_run {
                    queued_lists += 1;
                    results.push(IngestGrokmirrorListResult {
                        list_key: candidate.list_key.clone(),
                        status: run.state,
                        queued: 0,
                        repos: candidate.repos.clone(),
                        pipeline_run_id: Some(run.id),
                        current_stage: Some(run.current_stage),
                        error: None,
                    });
                    continue;
                }

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

    // Phase 2: if no pipeline run is currently active, activate the first pending run in this
    // batch and enqueue its ingest job. If enqueue fails after activation, mark that run failed
    // and try the next pending run.
    let mut started_run_id: Option<i64> = None;
    let mut launch_errors: BTreeMap<i64, String> = BTreeMap::new();
    let has_active_run = state
        .pipeline
        .get_any_active_run()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !pending_runs.is_empty() && !has_active_run {
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
                    payload_json: json!({ "run_id": payload.run_id }),
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct ResetWatermarkRequest {
    pub list_key: String,
    pub repo_key: String,
}

pub async fn reset_watermark(
    State(state): State<ApiState>,
    Json(body): Json<ResetWatermarkRequest>,
) -> HandlerResult<Json<ActionResponse>> {
    if body.list_key.trim().is_empty() {
        return Err(ApiError::bad_request("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }
    if body.repo_key.trim().is_empty() {
        return Err(ApiError::bad_request("repo_key must not be empty")
            .with_invalid_param("repo_key", "expected non-empty string"));
    }

    let changed = state
        .catalog
        .reset_watermark(&body.list_key, &body.repo_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    if !changed {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    }

    Ok(Json(ActionResponse { ok: true }))
}
