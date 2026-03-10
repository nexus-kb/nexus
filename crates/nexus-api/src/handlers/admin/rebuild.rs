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

/// JSON body for `POST /admin/v1/lineage/thread-refs/backfill`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct LineageThreadRefsBackfillRequest {
    /// Mailing list key to backfill series-version thread refs for.
    pub list_key: String,
    /// Optional inclusive lower bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub from: Option<String>,
    /// Optional exclusive upper bound (`RFC3339` or `YYYY-MM-DD`).
    #[serde(default)]
    pub to: Option<String>,
}

type RebuildWindow = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

fn normalize_rebuild_window(
    from_raw: Option<&str>,
    to_raw: Option<&str>,
) -> HandlerResult<RebuildWindow> {
    let from_seen_at = parse_optional_timestamp_query(from_raw).ok_or_else(|| {
        ApiError::validation("invalid from timestamp")
            .with_invalid_param("from", "expected RFC3339 or YYYY-MM-DD")
    })?;
    let to_seen_at = parse_optional_timestamp_query(to_raw).ok_or_else(|| {
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
    Ok((from_seen_at, to_seen_at))
}

async fn ensure_rebuild_list_ready(state: &ApiState, list_key: &str) -> HandlerResult<()> {
    let active_run = state
        .pipeline
        .get_active_run_for_list(list_key)
        .await
        .map_err(|_| ApiError::internal("failed to check active pipeline run"))?;
    if active_run.is_some() {
        return Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("cannot enqueue maintenance job while pipeline run is active"));
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

    Ok(())
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

    let (from_seen_at, to_seen_at) =
        normalize_rebuild_window(request.from.as_deref(), request.to.as_deref())?;
    ensure_rebuild_list_ready(&state, list_key).await?;

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

    let (from_seen_at, to_seen_at) =
        normalize_rebuild_window(request.from.as_deref(), request.to.as_deref())?;
    ensure_rebuild_list_ready(&state, list_key).await?;

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

pub async fn lineage_thread_refs_backfill(
    State(state): State<ApiState>,
    Json(request): Json<LineageThreadRefsBackfillRequest>,
) -> HandlerResult<Json<EnqueueResponse>> {
    let list_key = request.list_key.trim();
    if list_key.is_empty() {
        return Err(ApiError::validation("list_key must not be empty")
            .with_invalid_param("list_key", "expected non-empty string"));
    }

    let (from_seen_at, to_seen_at) =
        normalize_rebuild_window(request.from.as_deref(), request.to.as_deref())?;
    ensure_rebuild_list_ready(&state, list_key).await?;

    let payload = LineageThreadRefsBackfillListPayload {
        list_key: list_key.to_string(),
        from_seen_at,
        to_seen_at,
    };

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "lineage_thread_refs_backfill_list".to_string(),
            payload_json: serde_json::to_value(payload).map_err(|_| {
                ApiError::internal("failed to serialize lineage thread refs backfill payload")
            })?,
            priority: 10,
            dedupe_scope: Some(format!("list:{list_key}")),
            dedupe_key: None,
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| ApiError::internal("failed to enqueue lineage thread refs backfill job"))?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[cfg(test)]
mod tests {
    use axum::Json;
    use axum::extract::State;
    use nexus_core::config::{
        AdminConfig, AppConfig, DatabaseConfig, EmbeddingsConfig, MailConfig, MainlineConfig,
        MeiliConfig, Settings, WorkerConfig,
    };
    use nexus_db::Db;

    use crate::state::ApiState;

    use super::{LineageThreadRefsBackfillRequest, lineage_thread_refs_backfill};

    fn test_settings(database_url: String) -> Settings {
        Settings {
            database: DatabaseConfig {
                url: database_url,
                max_connections: 4,
            },
            app: AppConfig::default(),
            admin: AdminConfig::default(),
            mail: MailConfig::default(),
            mainline: MainlineConfig::default(),
            meili: MeiliConfig::default(),
            embeddings: EmbeddingsConfig::default(),
            worker: WorkerConfig::default(),
        }
    }

    #[tokio::test]
    async fn lineage_thread_refs_backfill_enqueues_job_for_existing_list()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let settings = test_settings(database_url);
        let db = Db::connect(&settings.database).await?;
        db.migrate().await?;
        let state = ApiState::new(settings, db.clone());

        let list_key = format!(
            "thread-refs-backfill-test-{}",
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
                .abs()
        );
        state.catalog.ensure_mailing_list(&list_key).await?;

        let Json(response) = lineage_thread_refs_backfill(
            State(state.clone()),
            Json(LineageThreadRefsBackfillRequest {
                list_key: list_key.clone(),
                from: None,
                to: None,
            }),
        )
        .await
        .expect("thread-ref backfill enqueue should succeed");

        let job = state
            .jobs
            .get(response.job_id)
            .await?
            .expect("job must exist");
        assert_eq!(job.job_type, "lineage_thread_refs_backfill_list");
        let payload = job.payload_json;
        assert_eq!(
            payload.get("list_key").and_then(|value| value.as_str()),
            Some(list_key.as_str())
        );

        Ok(())
    }
}
