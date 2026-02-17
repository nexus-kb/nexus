mod auth;
mod handlers;
mod logging;
mod router;
mod state;

use std::net::SocketAddr;

use axum::http::HeaderValue;
use axum::middleware;
use nexus_core::config;
use nexus_db::Db;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::router::build_router;
use crate::state::ApiState;

const DEFAULT_ALLOWED_ORIGINS: &str = "*";

fn build_cors() -> CorsLayer {
    let raw_origins = std::env::var("NEXUS_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| DEFAULT_ALLOWED_ORIGINS.to_owned());

    if raw_origins.split(',').any(|origin| origin.trim() == "*") {
        return CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);
    }

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
        return CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);
    }

    CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods(Any)
        .allow_headers(Any)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive(
            "access_clf=off"
                .parse()
                .expect("valid access_clf directive"),
        );

    let app_log_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_level(true)
        .with_filter(app_filter);
    let access_log_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_level(false)
        .without_time()
        .with_ansi(false)
        .with_filter(Targets::new().with_target("access_clf", tracing::Level::INFO));

    tracing_subscriber::registry()
        .with(app_log_layer)
        .with(access_log_layer)
        .init();

    let settings = config::load()?;
    let db = Db::connect(&settings.database).await?;
    db.migrate().await?;

    let state = ApiState::new(settings.clone(), db);
    let app = build_router(state)
        .layer(build_cors())
        .layer(middleware::from_fn(logging::common_log_middleware));

    let listener =
        tokio::net::TcpListener::bind((settings.app.host.as_str(), settings.app.port)).await?;
    let addr = listener.local_addr()?;
    info!(%addr, "nexus phase0 api listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
