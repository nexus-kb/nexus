use super::*;

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
