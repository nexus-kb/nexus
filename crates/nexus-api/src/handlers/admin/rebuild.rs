use super::*;

/// JSON body for `POST /admin/v1/threading/rebuild`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ThreadingRebuildRequest {
    /// Mailing list key to rebuild threading for.
    pub list_key: String,
    /// Optional inclusive lower bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub from: Option<String>,
    /// Optional exclusive upper bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub to: Option<String>,
}

pub async fn threading_rebuild(
    State(state): State<ApiState>,
    Json(request): Json<ThreadingRebuildRequest>,
) -> HandlerResult<Json<EnqueueResponse>> {
    let list_key = request.list_key.trim();
    if list_key.is_empty() {
        return Err(ApiError::validation("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }

    let from_seen_at =
        parse_optional_timestamp_query(request.from.as_deref()).ok_or_else(|| {
            ApiError::validation("invalid from timestamp")
                .with_invalid_param("from", "expected RFC3339 or YYYY-MM-DD")
        })?;
    let to_seen_at = parse_optional_timestamp_query(request.to.as_deref()).ok_or_else(|| {
        ApiError::validation("invalid to timestamp")
            .with_invalid_param("to", "expected RFC3339 or YYYY-MM-DD")
    })?;
    if let (Some(from), Some(to)) = (from_seen_at, to_seen_at)
        && from >= to
    {
        return Err(ApiError::validation("from must be earlier than to")
            .with_invalid_param("from", "must be earlier than to")
            .with_invalid_param("to", "must be later than from"));
    }

    // Reject if there's an active pipeline run for this list.
    let active_run = state
        .pipeline
        .get_active_run_for_list(list_key)
        .await
        .map_err(|_| ApiError::internal("failed to check active pipeline run"))?;
    if active_run.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("cannot enqueue threading rebuild while pipeline run is active"));
    }

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

    let payload = ThreadingRebuildListPayload {
        list_key: list_key.to_string(),
        from_seen_at,
        to_seen_at,
    };

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "threading_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| ApiError::internal("failed to serialize threading rebuild payload"))?,
            priority: 11,
            dedupe_scope: Some(format!("list:{list_key}")),
            // Manual rebuilds should always enqueue a fresh job so operators can rerun
            // full-list rebuilds on demand.
            dedupe_key: None,
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| ApiError::internal("failed to enqueue threading rebuild job"))?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

/// JSON body for `POST /admin/v1/lineage/rebuild`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct LineageRebuildRequest {
    /// Mailing list key to rebuild lineage for.
    pub list_key: String,
    /// Optional inclusive lower bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub from: Option<String>,
    /// Optional exclusive upper bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub to: Option<String>,
}

pub async fn lineage_rebuild(
    State(state): State<ApiState>,
    Json(request): Json<LineageRebuildRequest>,
) -> HandlerResult<Json<EnqueueResponse>> {
    let list_key = request.list_key.trim();
    if list_key.is_empty() {
        return Err(ApiError::validation("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }

    let from_seen_at =
        parse_optional_timestamp_query(request.from.as_deref()).ok_or_else(|| {
            ApiError::validation("invalid from timestamp")
                .with_invalid_param("from", "expected RFC3339 or YYYY-MM-DD")
        })?;
    let to_seen_at = parse_optional_timestamp_query(request.to.as_deref()).ok_or_else(|| {
        ApiError::validation("invalid to timestamp")
            .with_invalid_param("to", "expected RFC3339 or YYYY-MM-DD")
    })?;
    if let (Some(from), Some(to)) = (from_seen_at, to_seen_at)
        && from >= to
    {
        return Err(ApiError::validation("from must be earlier than to")
            .with_invalid_param("from", "must be earlier than to")
            .with_invalid_param("to", "must be later than from"));
    }

    // Reject if there's an active pipeline run for this list.
    let active_run = state
        .pipeline
        .get_active_run_for_list(list_key)
        .await
        .map_err(|_| ApiError::internal("failed to check active pipeline run"))?;
    if active_run.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("cannot enqueue lineage rebuild while pipeline run is active"));
    }

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

    let payload = LineageRebuildListPayload {
        list_key: list_key.to_string(),
        from_seen_at,
        to_seen_at,
    };

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "lineage_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| ApiError::internal("failed to serialize lineage rebuild payload"))?,
            priority: 11,
            dedupe_scope: Some(format!("list:{list_key}")),
            // Manual rebuilds should always enqueue a fresh job so operators can rerun
            // full-list rebuilds on demand.
            dedupe_key: None,
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| ApiError::internal("failed to enqueue lineage rebuild job"))?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}
