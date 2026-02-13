use clap::{Parser, Subcommand};
use nexus_core::config;
use nexus_db::{CatalogStore, Db};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "nexus-cli", about = "Nexus KB phase0 admin CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Seed pilot mailing lists (lkml + bpf)
    SeedPilot,
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

    let cli = Cli::parse();
    let settings = config::load()?;
    let db = Db::connect(&settings.database).await?;
    db.migrate().await?;

    match cli.command {
        Commands::SeedPilot => {
            let catalog = CatalogStore::new(db.pool().clone());
            catalog.ensure_mailing_list("lkml").await?;
            catalog.ensure_mailing_list("bpf").await?;
            println!("Seeded mailing lists: lkml, bpf");
        }
    }

    Ok(())
}
