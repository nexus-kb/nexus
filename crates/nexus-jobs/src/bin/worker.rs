use nexus_core::config;
use nexus_db::Db;
use nexus_jobs::{JobHandler, JobResult, JobRunner, JobRunnerConfig};
use tracing::info;
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

    let runner = JobRunner::new(db, JobRunnerConfig::default());
    runner.run(PrintJobHandler).await?;
    Ok(())
}

#[derive(Clone)]
struct PrintJobHandler;

#[async_trait::async_trait]
impl JobHandler for PrintJobHandler {
    async fn handle(&self, job: nexus_db::Job) -> JobResult {
        info!(id = job.id, queue = %job.queue, payload = %job.payload, "processing job");
        // Replace with real work; this is a placeholder that always succeeds.
        JobResult::Success
    }
}
