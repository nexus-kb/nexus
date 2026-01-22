mod error;
mod handlers;
mod http;
mod middleware;
mod models;
mod openapi;
mod params;
mod response;
mod router;
mod state;

use nexus_core::config;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::router::build_router;
use crate::state::ApiState;

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

    let db = nexus_db::Db::connect(&settings.database).await?;
    db.migrate().await?;
    let state = ApiState {
        db,
        webhook_secret: settings.mail.webhook_secret.clone(),
    };

    let app = build_router(state);

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
