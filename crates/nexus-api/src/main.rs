mod auth;
mod handlers;
mod router;
mod state;

use nexus_core::config;
use nexus_db::Db;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::router::build_router;
use crate::state::ApiState;

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
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind((settings.app.host.as_str(), settings.app.port)).await?;
    let addr = listener.local_addr()?;
    info!(%addr, "nexus phase0 api listening");

    axum::serve(listener, app).await?;
    Ok(())
}
