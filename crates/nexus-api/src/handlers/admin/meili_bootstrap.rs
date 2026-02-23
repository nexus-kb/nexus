use super::*;

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
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListMeiliBootstrapRunsResponse {
    pub items: Vec<nexus_db::MeiliBootstrapRun>,
    pub page_info: CursorPageInfoResponse,
}

pub async fn list_meili_bootstrap_runs(
    State(state): State<ApiState>,
    Query(query): Query<MeiliBootstrapRunsQuery>,
) -> Result<Json<ListMeiliBootstrapRunsResponse>, axum::http::StatusCode> {
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
            return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
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
    let has_more = runs.len() > limit as usize;
    if has_more {
        runs.truncate(limit as usize);
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
