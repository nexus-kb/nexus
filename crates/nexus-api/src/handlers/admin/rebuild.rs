use super::*;

#[derive(Debug, Deserialize)]
pub struct ThreadingRebuildQuery {
    pub list_key: String,
    #[serde(default, deserialize_with = "deserialize_optional_datetime_query")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "deserialize_optional_datetime_query")]
    pub to: Option<DateTime<Utc>>,
}

pub async fn threading_rebuild(
    State(state): State<ApiState>,
    Query(query): Query<ThreadingRebuildQuery>,
) -> Result<Json<EnqueueResponse>, axum::http::StatusCode> {
    // Reject if there's an active pipeline run for this list
    let active_run = state
        .pipeline
        .get_active_run_for_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    if active_run.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
    }

    let exists = state
        .catalog
        .get_mailing_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let payload = ThreadingRebuildListPayload {
        list_key: query.list_key.clone(),
        from_seen_at: query.from,
        to_seen_at: query.to,
    };

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "threading_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 11,
            dedupe_scope: Some(format!("list:{}", query.list_key)),
            // Manual rebuilds should always enqueue a fresh job so operators can rerun
            // full-list rebuilds on demand.
            dedupe_key: None,
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[derive(Debug, Deserialize)]
pub struct LineageRebuildQuery {
    pub list_key: String,
    #[serde(default, deserialize_with = "deserialize_optional_datetime_query")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "deserialize_optional_datetime_query")]
    pub to: Option<DateTime<Utc>>,
}

pub async fn lineage_rebuild(
    State(state): State<ApiState>,
    Query(query): Query<LineageRebuildQuery>,
) -> Result<Json<EnqueueResponse>, axum::http::StatusCode> {
    // Reject if there's an active pipeline run for this list
    let active_run = state
        .pipeline
        .get_active_run_for_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    if active_run.is_some() {
        return Err(axum::http::StatusCode::CONFLICT);
    }

    let exists = state
        .catalog
        .get_mailing_list(&query.list_key)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let payload = LineageRebuildListPayload {
        list_key: query.list_key.clone(),
        from_seen_at: query.from,
        to_seen_at: query.to,
    };

    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "lineage_rebuild_list".to_string(),
            payload_json: serde_json::to_value(payload)
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?,
            priority: 11,
            dedupe_scope: Some(format!("list:{}", query.list_key)),
            // Manual rebuilds should always enqueue a fresh job so operators can rerun
            // full-list rebuilds on demand.
            dedupe_key: None,
            run_after: None,
            max_attempts: Some(8),
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}
