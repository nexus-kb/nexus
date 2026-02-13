use nexus_core::config;
use nexus_db::Db;
use nexus_jobs::Phase0Worker;
use tracing_subscriber::EnvFilter;

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

    let worker = Phase0Worker::new(settings, db);
    worker.run().await?;

    Ok(())
}
