use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get, routing::post};
use chrono::{DateTime, Utc};
use nexus_core::config;
use nexus_db::{Db, Job, JobStats, JobStore};
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct ApiState {
    db: Db,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging setup respects RUST_LOG / NEXUS_LOG, defaults to info.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_level(true)
        .init();

    let settings = config::load()?;

    let db = Db::connect(&settings.database).await?;
    db.migrate().await?;
    let state = ApiState { db };

    let app = Router::new()
        .route("/hello", get(hello))
        .route("/health", get(health))
        .route("/jobs", post(enqueue_job).get(list_jobs))
        .route("/jobs/{id}", get(job_status))
        .route("/jobs/stats", get(job_stats))
        .with_state(state);

    // Bind using IP address; empty host falls back to 0.0.0.0. Hostnames are not allowed here.
    let bind_host = settings.app.host.trim();
    let bind_host = if bind_host.is_empty() {
        "0.0.0.0"
    } else {
        bind_host
    };

    // Validate that host parses as an IP address; otherwise bail early with a clear error.
    bind_host
        .parse::<std::net::IpAddr>()
        .map_err(|_| format!("Invalid IP for NEXUS__APP__HOST: {}", bind_host))?;

    let listener = tokio::net::TcpListener::bind((bind_host, settings.app.port)).await?;
    let addr = listener.local_addr()?;
    info!(%addr, host = bind_host, port = settings.app.port, "nexus api listening");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn hello() -> &'static str {
    "Hello, Nexus!"
}

async fn health() -> impl IntoResponse {
    axum::http::StatusCode::OK
}

#[derive(Debug, Deserialize)]
struct EnqueueRequest {
    #[serde(default = "default_queue")]
    queue: String,
    payload: serde_json::Value,
    #[serde(default)]
    run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    max_attempts: Option<i32>,
}

fn default_queue() -> String {
    "default".to_string()
}

#[derive(Debug, Serialize)]
struct EnqueueResponse {
    id: i64,
    queue: String,
    status: String,
    run_at: DateTime<Utc>,
}

async fn enqueue_job(
    State(state): State<ApiState>,
    Json(body): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, (axum::http::StatusCode, String)> {
    let store = JobStore::new(state.db.pool().clone());
    let job = store
        .enqueue(&body.queue, body.payload, body.run_at, body.max_attempts)
        .await
        .map_err(internal_error)?;

    Ok(Json(EnqueueResponse {
        id: job.id,
        queue: job.queue,
        status: job.status,
        run_at: job.run_at,
    }))
}

async fn job_status(
    State(state): State<ApiState>,
    Path(id): Path<i64>,
) -> Result<Json<Job>, axum::http::StatusCode> {
    let store = JobStore::new(state.db.pool().clone());
    match store
        .find_by_id(id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(job) => Ok(Json(job)),
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

fn internal_error<E: std::fmt::Display>(err: E) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.to_string(),
    )
}

#[derive(Debug, Deserialize)]
struct ListParams {
    status: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_list_limit() -> i64 {
    100
}

async fn list_jobs(
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<Job>>, (axum::http::StatusCode, String)> {
    let limit = params.limit.clamp(1, 500);
    let offset = params.offset.max(0);
    let store = JobStore::new(state.db.pool().clone());
    let jobs = store
        .list(params.status.as_deref(), limit, offset)
        .await
        .map_err(internal_error)?;
    Ok(Json(jobs))
}

async fn job_stats(
    State(state): State<ApiState>,
) -> Result<Json<JobStats>, (axum::http::StatusCode, String)> {
    let store = JobStore::new(state.db.pool().clone());
    let stats = store.stats().await.map_err(internal_error)?;
    Ok(Json(stats))
}
