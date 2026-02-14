mod auth;
mod handlers;
mod router;
mod state;

use axum::http::{HeaderValue, Method, header::CONTENT_TYPE};
use nexus_core::config;
use nexus_db::Db;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::router::build_router;
use crate::state::ApiState;

const DEFAULT_ALLOWED_ORIGINS: &str = "http://127.0.0.1:3001,http://localhost:3001,http://host.containers.internal:3001,http://host.docker.internal:3001";

fn build_cors() -> CorsLayer {
    let raw_origins = std::env::var("NEXUS_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| DEFAULT_ALLOWED_ORIGINS.to_owned());

    let mut allowed_origins = Vec::new();
    for origin in raw_origins.split(',') {
        let trimmed = origin.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(value) = HeaderValue::from_str(trimmed) {
            allowed_origins.push(value);
        }
    }

    if allowed_origins.is_empty() {
        allowed_origins.push("http://127.0.0.1:3001".parse::<HeaderValue>().unwrap());
    }

    CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers([CONTENT_TYPE])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let state = ApiState::new(settings.clone(), db);
    let app = build_router(state).layer(build_cors());

    let listener =
        tokio::net::TcpListener::bind((settings.app.host.as_str(), settings.app.port)).await?;
    let addr = listener.local_addr()?;
    info!(%addr, "nexus phase0 api listening");

    axum::serve(listener, app).await?;
    Ok(())
}
