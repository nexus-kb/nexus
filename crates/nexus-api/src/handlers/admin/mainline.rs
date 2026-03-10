use std::path::Path;

use super::*;

const MAINLINE_REF_NAME: &str = "refs/heads/master";
const MAINLINE_SCAN_PRIORITY: i32 = 6;

#[derive(Debug, Serialize, ToSchema)]
pub struct StartMainlineScanResponse {
    pub run_id: i64,
    pub job_id: i64,
    pub repo_key: String,
    pub ref_name: String,
    pub mode: String,
    pub state: String,
    pub head_commit_oid: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MainlineScanRunsQuery {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListMainlineScanRunsResponse {
    pub items: Vec<nexus_db::MainlineScanRun>,
    pub page_info: CursorPageInfoResponse,
}

pub async fn start_mainline_scan(
    State(state): State<ApiState>,
) -> HandlerResult<Json<StartMainlineScanResponse>> {
    let repo_path = state.settings.mainline.repo_path.trim();
    if repo_path.is_empty() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("mainline repo path is not configured"));
    }
    if !Path::new(repo_path).exists() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail(format!("mainline repo path does not exist: {repo_path}")));
    }

    let repo_key = format!("mainline:{repo_path}");
    let state_row = state
        .mainline
        .ensure_state(&repo_key, MAINLINE_REF_NAME)
        .await
        .map_err(|_| ApiError::internal("failed to ensure mainline scan state"))?;
    let mode = if state_row.bootstrap_completed_at.is_some() {
        "incremental"
    } else {
        "bootstrap"
    };

    if mode == "bootstrap" {
        let active_pipeline = state
            .pipeline
            .get_any_active_run()
            .await
            .map_err(|_| ApiError::internal("failed to check active pipeline runs"))?;
        if active_pipeline.is_some() {
            return Err(
                ApiError::from(axum::http::StatusCode::CONFLICT).with_detail(
                    "cannot start bootstrap mainline scan while a pipeline run is active",
                ),
            );
        }
    }

    let active_run = state
        .mainline
        .get_active_run(&repo_key)
        .await
        .map_err(|_| ApiError::internal("failed to check active mainline scan runs"))?;
    if active_run.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("a mainline scan run is already active"));
    }

    let head_commit_oid = resolve_head_commit_oid(repo_path, MAINLINE_REF_NAME)
        .await
        .map_err(|err| ApiError::internal(format!("failed to resolve mainline ref: {err}")))?;

    let run = state
        .mainline
        .create_run(nexus_db::CreateMainlineScanRunParams {
            repo_key: &repo_key,
            mode,
            ref_name: MAINLINE_REF_NAME,
            head_commit_oid: Some(&head_commit_oid),
        })
        .await
        .map_err(|_| ApiError::internal("failed to create mainline scan run"))?;

    let payload = nexus_jobs::payloads::MainlineScanRunPayload { run_id: run.id };
    let job = match state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "mainline_scan_run".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| ApiError::internal("failed to serialize mainline scan payload"))?,
            priority: MAINLINE_SCAN_PRIORITY,
            dedupe_scope: Some(repo_key.clone()),
            dedupe_key: Some(format!("run:{}", run.id)),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = state
                .mainline
                .mark_run_failed(
                    run.id,
                    format!("failed to enqueue mainline_scan_run: {err}").as_str(),
                )
                .await;
            return Err(ApiError::internal("failed to enqueue mainline scan job"));
        }
    };

    state
        .mainline
        .set_run_job_id(run.id, job.id)
        .await
        .map_err(|_| ApiError::internal("failed to bind mainline scan run to job id"))?;

    Ok(Json(StartMainlineScanResponse {
        run_id: run.id,
        job_id: job.id,
        repo_key,
        ref_name: MAINLINE_REF_NAME.to_string(),
        mode: mode.to_string(),
        state: "queued".to_string(),
        head_commit_oid,
    }))
}

pub async fn list_mainline_scan_runs(
    State(state): State<ApiState>,
    Query(query): Query<MainlineScanRunsQuery>,
) -> HandlerResult<Json<ListMainlineScanRunsResponse>> {
    let limit = normalize_limit(query.limit, 100, 200);
    let cursor_hash = short_hash(&json!({
        "endpoint": "mainline_scan_runs",
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
        .mainline
        .list_runs(nexus_db::ListMainlineScanRunsParams {
            state: query.state.clone(),
            limit: limit + 1,
            cursor: cursor_id,
        })
        .await
        .map_err(|_| ApiError::internal("failed to list mainline scan runs"))?;

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

    Ok(Json(ListMainlineScanRunsResponse {
        items: runs,
        page_info: build_page_info(limit, next_cursor),
    }))
}

pub async fn get_mainline_scan_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> HandlerResult<Json<nexus_db::MainlineScanRun>> {
    let Some(run) = state
        .mainline
        .get_run(run_id)
        .await
        .map_err(|_| ApiError::internal("failed to load mainline scan run"))?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    };
    Ok(Json(run))
}

pub async fn cancel_mainline_scan_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> HandlerResult<Json<ActionResponse>> {
    let Some(run) = state
        .mainline
        .get_run(run_id)
        .await
        .map_err(|_| ApiError::internal("failed to load mainline scan run"))?
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
        .map_err(|_| ApiError::internal("failed to request mainline scan cancellation"))?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    };

    if job.state == JobState::Cancelled {
        state
            .mainline
            .mark_run_cancelled(run.id, "cancel requested")
            .await
            .map_err(|_| ApiError::internal("failed to mark mainline scan cancelled"))?;
    }

    Ok(Json(ActionResponse { ok: true }))
}

async fn resolve_head_commit_oid(
    repo_path: &str,
    ref_name: &str,
) -> Result<String, std::io::Error> {
    let repo = gix::open(repo_path)
        .map_err(|err| std::io::Error::other(format!("failed to open repo: {err}")))?;
    let reference = repo
        .find_reference(ref_name)
        .map_err(|err| std::io::Error::other(format!("failed to find ref {ref_name}: {err}")))?;
    Ok(reference
        .into_fully_peeled_id()
        .map_err(|err| std::io::Error::other(format!("failed to peel ref {ref_name}: {err}")))?
        .to_string())
}
