use super::*;

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

    let from = parse_optional_timestamp_query(query.from.as_deref())
        .ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;
    let to = parse_optional_timestamp_query(query.to.as_deref())
        .ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;

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
pub(super) struct MeiliStoragePayload {
    pub(super) response: StorageMeiliResponse,
    pub(super) list_counts: BTreeMap<String, StorageMeiliListResponse>,
}
