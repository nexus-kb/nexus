use super::*;

/// JSON body for `POST /admin/v1/search/embeddings/backfill`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct SearchEmbeddingsBackfillRequest {
    /// Embedding scope. Supported values: `thread`, `series`.
    pub scope: String,
    /// Optional list key filter.
    #[serde(default)]
    pub list_key: Option<String>,
    /// Optional inclusive lower bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub from: Option<String>,
    /// Optional exclusive upper bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub to: Option<String>,
}

/// Enqueue response for embeddings backfill.
#[derive(Debug, Serialize, ToSchema)]
pub struct SearchEmbeddingsBackfillResponse {
    /// Backfill run identifier.
    pub run_id: i64,
    /// Queue job identifier.
    pub job_id: i64,
    /// Effective scope for this run.
    pub scope: String,
    /// Embedding model key used for this run.
    pub model_key: String,
}

pub async fn search_embeddings_backfill(
    State(state): State<ApiState>,
    Json(request): Json<SearchEmbeddingsBackfillRequest>,
) -> HandlerResult<Json<SearchEmbeddingsBackfillResponse>> {
    let scope = parse_embedding_scope(request.scope.trim()).ok_or_else(|| {
        ApiError::validation("scope must be one of: thread, series")
            .with_invalid_param("scope", "unsupported embedding scope")
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

    let from = parse_optional_timestamp_query(request.from.as_deref()).ok_or_else(|| {
        ApiError::validation("invalid from timestamp")
            .with_invalid_param("from", "expected RFC3339 or YYYY-MM-DD")
    })?;
    let to = parse_optional_timestamp_query(request.to.as_deref()).ok_or_else(|| {
        ApiError::validation("invalid to timestamp")
            .with_invalid_param("to", "expected RFC3339 or YYYY-MM-DD")
    })?;
    if let (Some(from), Some(to)) = (from, to)
        && from >= to
    {
        return Err(ApiError::validation("from must be earlier than to")
            .with_invalid_param("from", "must be earlier than to")
            .with_invalid_param("to", "must be later than from"));
    }

    let run = state
        .embeddings
        .create_backfill_run(
            scope.as_str(),
            list_key,
            from,
            to,
            &state.settings.embeddings.model,
        )
        .await
        .map_err(|_| ApiError::internal("failed to create embeddings backfill run"))?;

    let payload = EmbeddingBackfillRunPayload { run_id: run.id };
    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "embedding_backfill_run".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| ApiError::internal("failed to serialize embeddings run payload"))?,
            priority: 5,
            dedupe_scope: Some(format!("embeddings:{}", scope.as_str())),
            dedupe_key: Some(format!("run:{}", run.id)),
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| ApiError::internal("failed to enqueue embeddings backfill job"))?;

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
) -> HandlerResult<Json<nexus_db::EmbeddingBackfillRun>> {
    let Some(run) = state
        .embeddings
        .get_backfill_run(run_id)
        .await
        .map_err(|_| ApiError::internal("failed to fetch embeddings backfill run"))?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    };
    Ok(Json(run))
}

#[derive(Debug)]
pub(super) struct MeiliStoragePayload {
    pub(super) response: StorageMeiliResponse,
    pub(super) list_counts: BTreeMap<String, StorageMeiliListResponse>,
}
