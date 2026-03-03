use super::*;

/// JSON body for `POST /admin/v1/diagnostics/meili/bootstrap`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct StartMeiliBootstrapRequest {
    /// Bootstrap scope. Current supported value: `embedding_indexes`.
    pub scope: String,
    /// Optional list key filter.
    #[serde(default)]
    pub list_key: Option<String>,
    /// When true, computes estimates without creating a run or queue job.
    #[serde(default)]
    pub dry_run: bool,
}

/// Start or dry-run response for Meilisearch bootstrap diagnostics.
#[derive(Debug, Serialize, ToSchema)]
pub struct MeiliBootstrapEstimatesResponse {
    /// Candidate thread document count for embeddings migration.
    pub thread_candidates: i64,
    /// Candidate series document count for embeddings migration.
    pub series_candidates: i64,
    /// Total candidate count across scopes.
    pub total_candidates: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StartMeiliBootstrapResponse {
    /// Run identifier, or `null` when `dry_run=true`.
    pub run_id: Option<i64>,
    /// Queue job identifier, or `null` when `dry_run=true`.
    pub job_id: Option<i64>,
    /// Effective bootstrap scope.
    pub scope: String,
    /// Optional list filter.
    pub list_key: Option<String>,
    /// Current run state (`dry_run` or `queued` on initial response).
    pub state: String,
    /// Embedder name configured for write target indexes.
    pub embedder_name: String,
    /// Model key configured for write target indexes.
    pub model_key: String,
    /// Echo of dry-run mode.
    pub dry_run: bool,
    /// Estimated candidate counts.
    pub estimates: MeiliBootstrapEstimatesResponse,
}

pub async fn start_meili_bootstrap(
    State(state): State<ApiState>,
    Json(request): Json<StartMeiliBootstrapRequest>,
) -> HandlerResult<Json<StartMeiliBootstrapResponse>> {
    let scope = parse_meili_bootstrap_scope(request.scope.trim()).ok_or_else(|| {
        ApiError::validation("scope must be: embedding_indexes")
            .with_invalid_param("scope", "unsupported diagnostics scope")
    })?;

    let list_key = request
        .list_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if request.list_key.is_some() && list_key.is_none() {
        return Err(ApiError::validation("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }

    if let Some(list_key) = list_key {
        let exists = state
            .catalog
            .get_mailing_list(list_key)
            .await
            .map_err(|_| ApiError::internal("failed to look up mailing list"))?
            .is_some();
        if !exists {
            return Err(ApiError::from(axum::http::StatusCode::NOT_FOUND)
                .with_detail("mailing list not found for list_key")
                .with_invalid_param("list_key", "unknown mailing list key"));
        }
    }

    let active_pipeline_run = if let Some(list_key) = list_key {
        state
            .pipeline
            .get_active_run_for_list(list_key)
            .await
            .map_err(|_| ApiError::internal("failed to check active pipeline run"))?
    } else {
        state
            .pipeline
            .get_any_active_run()
            .await
            .map_err(|_| ApiError::internal("failed to check active pipeline run"))?
    };
    if active_pipeline_run.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("cannot start meili bootstrap while an active pipeline run exists"));
    }

    let active_bootstrap = state
        .embeddings
        .get_active_meili_bootstrap_run(scope.as_str(), list_key)
        .await
        .map_err(|_| ApiError::internal("failed to check active meili bootstrap runs"))?;
    if active_bootstrap.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("a meili bootstrap run is already active for this scope/list"));
    }

    let thread_candidates = state
        .embeddings
        .count_candidate_ids("thread", list_key, None, None)
        .await
        .map_err(|_| ApiError::internal("failed to count thread candidates"))?;
    let series_candidates = state
        .embeddings
        .count_candidate_ids("series", list_key, None, None)
        .await
        .map_err(|_| ApiError::internal("failed to count series candidates"))?;
    let estimates = MeiliBootstrapEstimatesResponse {
        thread_candidates,
        series_candidates,
        total_candidates: thread_candidates + series_candidates,
    };

    if request.dry_run {
        return Ok(Json(StartMeiliBootstrapResponse {
            run_id: None,
            job_id: None,
            scope: scope.as_str().to_string(),
            list_key: list_key.map(ToString::to_string),
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
            list_key,
            &state.settings.embeddings.embedder_name,
            &state.settings.embeddings.model,
        )
        .await
        .map_err(|_| ApiError::internal("failed to create meili bootstrap run"))?;

    let payload = MeiliBootstrapRunPayload { run_id: run.id };
    let job = match state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "meili_bootstrap_run".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| ApiError::internal("failed to serialize meili bootstrap payload"))?,
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
            return Err(ApiError::internal("failed to enqueue meili bootstrap job"));
        }
    };

    state
        .embeddings
        .set_meili_bootstrap_job_id(run.id, job.id)
        .await
        .map_err(|_| ApiError::internal("failed to bind meili bootstrap run to job id"))?;

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
) -> HandlerResult<Json<nexus_db::MeiliBootstrapRun>> {
    let Some(run) = state
        .embeddings
        .get_meili_bootstrap_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    };
    Ok(Json(run))
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MeiliBootstrapRunsQuery {
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListMeiliBootstrapRunsResponse {
    pub items: Vec<nexus_db::MeiliBootstrapRun>,
    pub page_info: CursorPageInfoResponse,
}

pub async fn list_meili_bootstrap_runs(
    State(state): State<ApiState>,
    Query(query): Query<MeiliBootstrapRunsQuery>,
) -> HandlerResult<Json<ListMeiliBootstrapRunsResponse>> {
    let limit = normalize_limit(query.limit, 100, 200);
    let cursor_hash = short_hash(&json!({
        "endpoint": "meili_bootstrap_runs",
        "list_key": query.list_key.as_deref().unwrap_or(""),
        "state": query.state.as_deref().unwrap_or(""),
    }));
    let cursor_id = if let Some(raw_cursor) = query.cursor.as_deref() {
        let token: IdCursorToken =
            decode_cursor_token(raw_cursor).ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;
        if token.v != 1 || token.h != cursor_hash {
            return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY.into());
        }
        Some(token.id)
    } else {
        None
    };

    let mut runs = state
        .embeddings
        .list_meili_bootstrap_runs(ListMeiliBootstrapRunsParams {
            list_key: query.list_key.clone(),
            state: query.state.clone(),
            limit: limit + 1,
            cursor: cursor_id,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = runs.len() > limit_usize;
    if has_more {
        runs.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        runs.last().and_then(|run| {
            encode_cursor_token(&IdCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                id: run.id,
            })
        })
    } else {
        None
    };

    Ok(Json(ListMeiliBootstrapRunsResponse {
        items: runs,
        page_info: build_page_info(limit, next_cursor),
    }))
}

pub async fn cancel_meili_bootstrap_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> HandlerResult<Json<ActionResponse>> {
    let Some(run) = state
        .embeddings
        .get_meili_bootstrap_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    };

    if matches!(run.state.as_str(), "succeeded" | "failed" | "cancelled") {
        return Ok(Json(ActionResponse { ok: true }));
    }

    let Some(job_id) = run.job_id else {
        return Err(axum::http::StatusCode::CONFLICT.into());
    };

    let Some(job) = state
        .jobs
        .request_cancel(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
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
