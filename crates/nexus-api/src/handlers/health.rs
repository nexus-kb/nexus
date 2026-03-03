use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::ApiState;

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    /// Service liveness flag.
    pub ok: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyResponse {
    /// Readiness flag computed from dependency checks.
    pub ok: bool,
    /// Per-dependency readiness state.
    pub deps: ReadyDeps,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyDeps {
    /// Postgres readiness (`ok` or `error:<reason>`).
    pub postgres: String,
    /// Meilisearch readiness (`ok` or `error:<reason>`).
    pub meili: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct VersionResponse {
    /// Git commit SHA embedded at build time.
    pub git_sha: String,
    /// Build timestamp embedded at build time.
    pub build_time: String,
    /// Schema/version marker for API compatibility checks.
    pub schema_version: String,
}

pub async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

pub async fn readyz(State(state): State<ApiState>) -> impl IntoResponse {
    let postgres = match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(state.db.pool())
        .await
    {
        Ok(_) => "ok".to_string(),
        Err(err) => format!("error:{err}"),
    };

    let meili = match state
        .http_client
        .get(format!(
            "{}/health",
            state.settings.meili.url.trim_end_matches('/')
        ))
        .bearer_auth(&state.settings.meili.master_key)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => "ok".to_string(),
        Ok(resp) => format!("error:http_{}", resp.status().as_u16()),
        Err(err) => format!("error:{err}"),
    };

    let ok = postgres == "ok" && meili == "ok";

    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(ReadyResponse {
            ok,
            deps: ReadyDeps { postgres, meili },
        }),
    )
}

pub async fn version(State(state): State<ApiState>) -> Json<VersionResponse> {
    Json(VersionResponse {
        git_sha: state.settings.app.build_sha.clone(),
        build_time: state.settings.app.build_time.clone(),
        schema_version: state.settings.app.schema_version.clone(),
    })
}
