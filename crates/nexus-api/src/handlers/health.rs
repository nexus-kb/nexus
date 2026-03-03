use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::state::ApiState;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
}

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ok: bool,
    pub deps: ReadyDeps,
}

#[derive(Debug, Serialize)]
pub struct ReadyDeps {
    pub postgres: String,
    pub meili: String,
}

#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub git_sha: String,
    pub build_time: String,
    pub schema_version: String,
}

pub async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

pub async fn readyz(State(state): State<ApiState>) -> Json<ReadyResponse> {
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

    Json(ReadyResponse {
        ok,
        deps: ReadyDeps { postgres, meili },
    })
}

pub async fn version(State(state): State<ApiState>) -> Json<VersionResponse> {
    Json(VersionResponse {
        git_sha: state.settings.app.build_sha.clone(),
        build_time: state.settings.app.build_time.clone(),
        schema_version: state.settings.app.schema_version.clone(),
    })
}
