mod cli;
mod commands;
mod config;
mod manifest;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, Command};

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

    match cli.command {
        Command::ImportMailingLists => commands::import_mailing_lists::run().await?,
        Command::ResetDb => commands::reset_db::run().await?,
    }

    Ok(())
}
