use super::*;
use nexus_db::RetryJobResult;

#[derive(Debug, Deserialize, ToSchema)]
pub struct EnqueueRequest {
    pub job_type: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub dedupe_scope: Option<String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub run_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EnqueueResponse {
    pub job_id: i64,
}

pub async fn enqueue_job(
    State(state): State<ApiState>,
    Json(body): Json<EnqueueRequest>,
) -> HandlerResult<Json<EnqueueResponse>> {
    let job = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: body.job_type,
            payload_json: body.payload,
            priority: body.priority.unwrap_or(0),
            dedupe_scope: body.dedupe_scope,
            dedupe_key: body.dedupe_key,
            run_after: body.run_after,
            max_attempts: body.max_attempts,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(EnqueueResponse { job_id: job.id }))
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListJobsQuery {
    #[serde(default)]
    pub state: Option<JobState>,
    #[serde(default)]
    pub job_type: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
}

pub(super) fn default_limit() -> i64 {
    100
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CursorPageInfoResponse {
    pub limit: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListJobsResponse {
    pub items: Vec<Job>,
    pub page_info: CursorPageInfoResponse,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(super) struct IdCursorToken {
    pub(super) v: u8,
    pub(super) h: String,
    pub(super) id: i64,
}

pub async fn list_jobs(
    State(state): State<ApiState>,
    Query(query): Query<ListJobsQuery>,
) -> HandlerResult<Json<ListJobsResponse>> {
    let limit = normalize_limit(query.limit, 100, 200);
    let cursor_hash = short_hash(&json!({
        "endpoint": "jobs",
        "state": query.state.as_ref().map(|value| format!("{value:?}")),
        "job_type": query.job_type.as_deref().unwrap_or(""),
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

    let mut jobs = state
        .jobs
        .list(ListJobsParams {
            state: query.state,
            job_type: query.job_type.clone(),
            limit: limit + 1,
            cursor: cursor_id,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = jobs.len() > limit_usize;
    if has_more {
        jobs.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        jobs.last().and_then(|job| {
            encode_cursor_token(&IdCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                id: job.id,
            })
        })
    } else {
        None
    };

    Ok(Json(ListJobsResponse {
        items: jobs,
        page_info: build_page_info(limit, next_cursor),
    }))
}

pub async fn get_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> HandlerResult<Json<Job>> {
    match state
        .jobs
        .get(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(job) => Ok(Json(job)),
        None => Err(axum::http::StatusCode::NOT_FOUND.into()),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct JobAttemptsQuery {
    #[serde(default = "default_attempts_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
}

fn default_attempts_limit() -> i64 {
    50
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JobAttemptsResponse {
    pub items: Vec<nexus_db::JobAttempt>,
    pub page_info: CursorPageInfoResponse,
}

pub async fn list_job_attempts(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
    Query(query): Query<JobAttemptsQuery>,
) -> HandlerResult<Json<JobAttemptsResponse>> {
    let exists = state
        .jobs
        .get(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    }

    let limit = normalize_limit(query.limit, 50, 200);
    let cursor_hash = short_hash(&json!({
        "endpoint": "job_attempts",
        "job_id": job_id,
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

    let mut attempts = state
        .jobs
        .list_attempts(job_id, limit + 1, cursor_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = attempts.len() > limit_usize;
    if has_more {
        attempts.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        attempts.last().and_then(|attempt| {
            encode_cursor_token(&IdCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                id: attempt.id,
            })
        })
    } else {
        None
    };

    Ok(Json(JobAttemptsResponse {
        items: attempts,
        page_info: build_page_info(limit, next_cursor),
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ActionResponse {
    pub ok: bool,
}

pub async fn cancel_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> HandlerResult<Json<ActionResponse>> {
    let changed = state
        .jobs
        .request_cancel(job_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();

    if !changed {
        return Err(axum::http::StatusCode::NOT_FOUND.into());
    }

    Ok(Json(ActionResponse { ok: true }))
}

pub async fn retry_job(
    State(state): State<ApiState>,
    AxumPath(job_id): AxumPath<i64>,
) -> HandlerResult<Json<ActionResponse>> {
    match state
        .jobs
        .retry(job_id, Utc::now())
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    {
        RetryJobResult::Updated(_) => Ok(Json(ActionResponse { ok: true })),
        RetryJobResult::NotFound => Err(axum::http::StatusCode::NOT_FOUND.into()),
        RetryJobResult::RunningConflict => Err(ApiError::from(axum::http::StatusCode::CONFLICT)
            .with_detail("cannot retry a running job; wait for it to leave running state")),
    }
}

#[cfg(test)]
mod tests {
    use axum::Json;
    use axum::extract::{Path as AxumPath, State};
    use axum::response::IntoResponse;
    use nexus_core::config::{
        AdminConfig, AppConfig, DatabaseConfig, EmbeddingsConfig, MailConfig, MeiliConfig,
        Settings, WorkerConfig,
    };
    use nexus_db::{Db, EnqueueJobParams, JobState};

    use crate::state::ApiState;

    use super::{ActionResponse, retry_job};

    fn test_settings(database_url: String) -> Settings {
        Settings {
            database: DatabaseConfig {
                url: database_url,
                max_connections: 4,
            },
            app: AppConfig::default(),
            admin: AdminConfig::default(),
            mail: MailConfig::default(),
            meili: MeiliConfig::default(),
            embeddings: EmbeddingsConfig::default(),
            worker: WorkerConfig::default(),
        }
    }

    #[tokio::test]
    async fn retry_job_returns_conflict_for_running_jobs() -> Result<(), Box<dyn std::error::Error>>
    {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let settings = test_settings(database_url);
        let db = Db::connect(&settings.database).await?;
        db.migrate().await?;
        let state = ApiState::new(settings, db.clone());

        let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let job = state
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "test_api_retry_running_conflict".to_string(),
                payload_json: serde_json::json!({ "test": unique }),
                priority: 1,
                dedupe_scope: Some(format!("tests:{unique}")),
                dedupe_key: Some("api-retry-conflict".to_string()),
                run_after: None,
                max_attempts: Some(3),
            })
            .await?;

        sqlx::query(
            r#"UPDATE jobs
            SET state = 'running',
                claimed_by = 'test-worker',
                lease_until = now() + interval '30 seconds',
                attempt = 1
            WHERE id = $1"#,
        )
        .bind(job.id)
        .execute(state.db.pool())
        .await?;

        let result = retry_job(State(state.clone()), AxumPath(job.id)).await;
        let err = result.expect_err("running job retry should conflict");
        let response = err.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id)
            .execute(state.db.pool())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn retry_job_requeues_non_running_jobs() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let settings = test_settings(database_url);
        let db = Db::connect(&settings.database).await?;
        db.migrate().await?;
        let state = ApiState::new(settings, db.clone());

        let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let job = state
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "test_api_retry_non_running".to_string(),
                payload_json: serde_json::json!({ "test": unique }),
                priority: 1,
                dedupe_scope: Some(format!("tests:{unique}")),
                dedupe_key: Some("api-retry-success".to_string()),
                run_after: None,
                max_attempts: Some(3),
            })
            .await?;

        sqlx::query(
            r#"UPDATE jobs
            SET state = 'failed_retryable',
                run_after = now() + interval '5 minutes',
                last_error = 'transient failure',
                last_error_kind = 'db'
            WHERE id = $1"#,
        )
        .bind(job.id)
        .execute(state.db.pool())
        .await?;

        let Json(ActionResponse { ok }) = retry_job(State(state.clone()), AxumPath(job.id))
            .await
            .expect("non-running retry should succeed");
        assert!(ok);

        let updated = state.jobs.get(job.id).await?.expect("job must exist");
        assert_eq!(updated.state, JobState::Queued);
        assert!(updated.last_error.is_none());
        assert!(updated.last_error_kind.is_none());

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id)
            .execute(state.db.pool())
            .await?;

        Ok(())
    }
}
