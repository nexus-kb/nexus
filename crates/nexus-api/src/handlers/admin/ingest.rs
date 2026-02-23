use super::*;

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
}

#[derive(Debug)]
pub(super) struct MirrorListCandidate {
    pub(super) list_key: String,
    pub(super) repos: Vec<String>,
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
